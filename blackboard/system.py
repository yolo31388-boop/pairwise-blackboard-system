"""
pairwise-blackboard-system - BlackboardSystem

A thread-safe blackboard for multi-AI data sharing.

Fixes implemented:
- write_data:        version check (stale writes rejected), writer recorded,
                     subscribers notified on change.
- read_data:         freshness check (expired data not returned),
                     subscription support, per-key read permissions.
- update_lifecycle:  TTL expiration, cleanup of useless data,
                     priority-aware eviction (important data kept).
- query_data:        indexed lookup, filter support, result caching.
- sync_blackboard:   incremental (delta) sync, conflict resolution,
                     sync frequency control.
"""

import time
import threading
from collections import defaultdict


class BlackboardSystem:
    def __init__(self, sync_interval=0.1, default_ttl=None, max_entries=None):
        self._lock = threading.RLock()
        # key -> entry dict
        self._data = {}
        # index: indexed field value -> set of keys (O(1) candidate lookup)
        self._index = defaultdict(lambda: defaultdict(set))
        self._indexed_fields = ("writer", "priority", "private")
        # key -> list of subscriber callbacks; "*" listens to all keys
        self._subscribers = defaultdict(list)
        # query result cache: frozen filter -> (timestamp, result)
        self._query_cache = {}
        self._cache_ttl = 1.0
        # sync state
        self._sync_interval = sync_interval
        self._last_sync = 0.0
        # peer id -> {key: version} already known by that peer (for delta sync)
        self._peer_versions = defaultdict(dict)
        # lifecycle
        self._default_ttl = default_ttl
        self._max_entries = max_entries
        self.state = {}

    # ------------------------------------------------------------------ #
    # helpers
    # ------------------------------------------------------------------ #
    def _now(self, now=None):
        return time.time() if now is None else now

    def _is_expired(self, entry, now=None):
        expires_at = entry.get("expires_at")
        return expires_at is not None and self._now(now) >= expires_at

    def _index_entry(self, entry):
        for field in self._indexed_fields:
            self._index[field][entry.get(field)].add(entry["key"])

    def _deindex_entry(self, entry):
        for field in self._indexed_fields:
            bucket = self._index[field].get(entry.get(field))
            if bucket is not None:
                bucket.discard(entry["key"])

    def _invalidate_cache(self):
        self._query_cache.clear()

    def _notify(self, entry, event="write"):
        """Notify subscribers of the key (and global subscribers) of a change."""
        notification = {
            "event": event,
            "key": entry["key"],
            "value": entry.get("value"),
            "version": entry.get("version"),
            "writer": entry.get("writer"),
        }
        callbacks = list(self._subscribers.get(entry["key"], ()))
        callbacks += list(self._subscribers.get("*", ()))
        for callback in callbacks:
            callback(notification)

    def _can_read(self, entry, reader=None):
        """Private entries are readable only by the writer/allowed readers."""
        if not entry.get("private"):
            return True
        if reader is None:
            return False
        if reader == entry.get("writer"):
            return True
        return reader in (entry.get("allowed_readers") or ())

    # ------------------------------------------------------------------ #
    # write
    # ------------------------------------------------------------------ #
    def write_data(self, key=None, value=None, writer=None, version=None,
                   ttl=None, priority=0, private=False, allowed_readers=None,
                   now=None, **kwargs):
        """Write data with version check, writer record and change notification.

        A write carrying a version not newer than the stored one is rejected
        as a conflict, so stale data can never overwrite fresher data.
        """
        if isinstance(key, dict) and value is None:
            payload = dict(key)
            payload.update(kwargs)
            return self.write_data(**payload)
        if key is None:
            key = kwargs.get("key")
        if value is None and "value" in kwargs:
            value = kwargs["value"]

        with self._lock:
            now = self._now(now)
            existing = self._data.get(key)
            if existing is not None and self._is_expired(existing, now):
                self._remove_entry(existing, now, event="expire")
                existing = None

            old_version = existing["version"] if existing else 0
            # version check: reject stale/older writes
            if version is not None and version <= old_version:
                return {
                    "success": False,
                    "conflict": True,
                    "reason": "stale_version",
                    "key": key,
                    "version": old_version,
                    "writer": existing.get("writer") if existing else None,
                }

            new_version = version if version is not None else old_version + 1
            effective_ttl = ttl if ttl is not None else self._default_ttl
            expires_at = now + effective_ttl if effective_ttl is not None else None

            if existing is not None:
                self._deindex_entry(existing)

            entry = {
                "key": key,
                "value": value,
                "version": new_version,
                "writer": writer,
                "timestamp": now,
                "ttl": effective_ttl,
                "expires_at": expires_at,
                "priority": priority,
                "private": private,
                "allowed_readers": set(allowed_readers or ()),
            }
            self._data[key] = entry
            self._index_entry(entry)
            self._invalidate_cache()

        self._notify(entry, event="write")
        return {
            "success": True,
            "conflict": False,
            "key": key,
            "value": value,
            "version": new_version,
            "writer": writer,
        }

    # ------------------------------------------------------------------ #
    # read
    # ------------------------------------------------------------------ #
    def read_data(self, key=None, reader=None, now=None, **kwargs):
        """Read data with freshness check and permission control.

        Expired entries are dropped instead of returned; private entries are
        only visible to their writer and explicitly allowed readers.
        """
        with self._lock:
            now = self._now(now)
            if key is None:
                # snapshot of everything the reader is allowed to see
                return {
                    k: e["value"]
                    for k, e in list(self._data.items())
                    if not self._is_expired(e, now) and self._can_read(e, reader)
                }

            entry = self._data.get(key)
            if entry is None:
                return None
            # freshness: expired data must not be used
            if self._is_expired(entry, now):
                self._remove_entry(entry, now, event="expire")
                return None
            # permission: private data is not world-readable
            if not self._can_read(entry, reader):
                return None
            return entry["value"]

    def subscribe(self, key, callback):
        """Subscribe to changes of a key ("*" subscribes to all keys)."""
        with self._lock:
            if callback not in self._subscribers[key]:
                self._subscribers[key].append(callback)
        return True

    def unsubscribe(self, key, callback):
        with self._lock:
            if callback in self._subscribers.get(key, ()):
                self._subscribers[key].remove(callback)
                return True
        return False

    # ------------------------------------------------------------------ #
    # lifecycle
    # ------------------------------------------------------------------ #
    def _remove_entry(self, entry, now=None, event="expire"):
        self._deindex_entry(entry)
        self._data.pop(entry["key"], None)
        self._invalidate_cache()
        return entry

    def update_lifecycle(self, now=None, **kwargs):
        """Expire stale data, clean up useless entries, evict by priority.

        Entries past their TTL expire; when over capacity the lowest
        priority entries are evicted first so important data survives.
        """
        with self._lock:
            now = self._now(now)
            expired = []
            for key, entry in list(self._data.items()):
                if self._is_expired(entry, now):
                    expired.append(key)
                    self._remove_entry(entry, now, event="expire")

            evicted = []
            if self._max_entries is not None:
                overflow = len(self._data) - self._max_entries
                if overflow > 0:
                    # lowest priority first, then oldest
                    candidates = sorted(
                        self._data.values(),
                        key=lambda e: (e.get("priority", 0), e["timestamp"]),
                    )
                    for entry in candidates[:overflow]:
                        evicted.append(entry["key"])
                        self._remove_entry(entry, now, event="evict")

        for key in expired:
            self._notify({"key": key, "value": None, "version": None,
                          "writer": None}, event="expire")
        for key in evicted:
            self._notify({"key": key, "value": None, "version": None,
                          "writer": None}, event="evict")
        return {
            "expired": expired,
            "evicted": evicted,
            "remaining": len(self._data),
        }

    # ------------------------------------------------------------------ #
    # query
    # ------------------------------------------------------------------ #
    def query_data(self, filters=None, now=None, use_cache=True, **kwargs):
        """Query with index-accelerated filtering and result caching.

        `filters` maps entry fields (e.g. writer, priority, private) to the
        required value. Indexed fields are resolved via the index instead of
        a full O(n) scan; results are cached until the next write.
        """
        if filters is None:
            filters = {k: v for k, v in kwargs.items() if k in self._indexed_fields
                       or k in ("key", "value", "version")}
        with self._lock:
            now = self._now(now)
            cache_key = tuple(sorted((str(k), repr(v)) for k, v in filters.items()))
            if use_cache and cache_key in self._query_cache:
                cached_at, cached_result = self._query_cache[cache_key]
                if now - cached_at <= self._cache_ttl:
                    return list(cached_result)

            # use the index to narrow candidates when possible
            candidate_keys = None
            for field in self._indexed_fields:
                if field in filters:
                    keys = set(self._index[field].get(filters[field], ()))
                    candidate_keys = keys if candidate_keys is None \
                        else candidate_keys & keys
            if candidate_keys is None:
                candidate_keys = set(self._data.keys())

            results = []
            for key in candidate_keys:
                entry = self._data.get(key)
                if entry is None or self._is_expired(entry, now):
                    continue
                if all(entry.get(f) == v for f, v in filters.items()):
                    results.append(dict(entry))
            results.sort(key=lambda e: e["key"])

            self._query_cache[cache_key] = (now, list(results))
            return results

    # ------------------------------------------------------------------ #
    # sync
    # ------------------------------------------------------------------ #
    def sync_blackboard(self, peer=None, force=False, now=None, **kwargs):
        """Incremental sync with conflict resolution and frequency control.

        Only entries newer than what the peer already knows are transferred
        (delta sync). Conflicts are resolved by version (then timestamp, then
        writer id) so concurrent writes converge. Syncs more frequent than
        `sync_interval` are skipped unless forced.
        """
        with self._lock:
            now = self._now(now)
            if not force and now - self._last_sync < self._sync_interval:
                return {
                    "synced": False,
                    "reason": "rate_limited",
                    "sent": 0,
                    "keys": [],
                    "conflicts_resolved": 0,
                    "retry_after": self._sync_interval - (now - self._last_sync),
                }
            self._last_sync = now

            peer_id = getattr(peer, "_peer_id", None) or (
                str(id(peer)) if peer is not None else "broadcast")
            known = self._peer_versions[peer_id]

            # delta: only entries the peer has never seen or has older
            delta = []
            for key, entry in self._data.items():
                if self._is_expired(entry, now):
                    continue
                if known.get(key, 0) < entry["version"]:
                    delta.append(entry)

            conflicts = 0
            if peer is not None and hasattr(peer, "receive_sync"):
                conflicts = peer.receive_sync(delta, source=self, now=now)

            for entry in delta:
                known[entry["key"]] = entry["version"]

            return {
                "synced": True,
                "sent": len(delta),
                "keys": [e["key"] for e in delta],
                "conflicts_resolved": conflicts,
                "full_sync": False,
            }

    def receive_sync(self, entries, source=None, now=None):
        """Merge entries pushed by a peer, resolving conflicts by version."""
        with self._lock:
            now = self._now(now)
            conflicts = 0
            for entry in entries:
                key = entry["key"]
                existing = self._data.get(key)
                if existing is not None and not self._is_expired(existing, now):
                    local = (existing["version"], existing["timestamp"],
                             str(existing.get("writer")))
                    remote = (entry["version"], entry["timestamp"],
                              str(entry.get("writer")))
                    if local >= remote:
                        conflicts += 1
                        continue  # local copy wins, ignore stale remote
                    conflicts += 1
                    self._deindex_entry(existing)
                merged = dict(entry)
                merged["allowed_readers"] = set(entry.get("allowed_readers") or ())
                self._data[key] = merged
                self._index_entry(merged)
            if entries:
                self._invalidate_cache()
        for entry in entries:
            self._notify(entry, event="sync")
        return conflicts
