"""
pairwise-blackboard-system - BlackboardSystem

A blackboard for multi-AI data sharing with:
- versioned writes with writer tracking and change notifications
- time-aware reads with subscriptions and permission checks
- lifecycle management with expiry, cleanup and priority protection
- indexed, filtered and cached queries
- incremental, conflict-resolving, rate-limited synchronization
"""

import time
from collections import defaultdict


class BlackboardSystem:
    # entries with priority >= this are never evicted by lifecycle cleanup
    PROTECTED_PRIORITY = 10
    # minimum seconds between two syncs with the same peer
    SYNC_INTERVAL = 0.1

    def __init__(self):
        self.state = {}
        self._subscribers = defaultdict(list)
        self._field_index = defaultdict(lambda: defaultdict(set))
        self._query_cache = {}
        self._last_sync = {}
        self._known_versions = defaultdict(dict)

    # ------------------------------------------------------------------ write

    def write_data(self, key=None, value=None, writer=None,
                   expected_version=None, ttl=None, priority=0, owner=None):
        """Write with version check, writer record and subscriber notification."""
        if key is None:
            return dict(self.state)

        existing = self.state.get(key)
        current_version = existing["version"] if existing else 0

        # version check: reject stale writes that would overwrite newer data
        if expected_version is not None and expected_version != current_version:
            return {
                "success": False,
                "reason": "version_conflict",
                "key": key,
                "expected_version": expected_version,
                "current_version": current_version,
            }

        entry = {
            "value": value,
            "version": current_version + 1,
            "writer": writer,
            "timestamp": time.time(),
            "ttl": ttl,
            "priority": priority,
            "owner": owner,
        }
        self.state[key] = entry
        self._index_entry(key, entry)
        self._query_cache.clear()
        self._notify(key, entry)
        return {"success": True, "key": key, "version": entry["version"],
                "writer": writer}

    def _index_entry(self, key, entry):
        for field, indexed in self._field_index.items():
            for value, keys in indexed.items():
                keys.discard(key)
        for field in ("writer", "owner", "priority"):
            self._field_index[field][entry.get(field)].add(key)

    def _notify(self, key, entry):
        for callback in list(self._subscribers.get(key, [])):
            callback(key, entry)
        for callback in list(self._subscribers.get("*", [])):
            callback(key, entry)

    # ------------------------------------------------------------------- read

    def read_data(self, key=None, reader=None):
        """Read with freshness (TTL) check and permission enforcement."""
        if key is None:
            return {k: e["value"] for k, e in self.state.items()
                    if not self._is_expired(e) and self._can_read(e, reader)}

        entry = self.state.get(key)
        if entry is None:
            return {"success": False, "reason": "not_found", "key": key}
        if self._is_expired(entry):
            return {"success": False, "reason": "expired", "key": key}
        if not self._can_read(entry, reader):
            return {"success": False, "reason": "permission_denied",
                    "key": key}
        return {"success": True, "key": key, "value": entry["value"],
                "version": entry["version"], "writer": entry["writer"]}

    def subscribe(self, key, callback):
        """Register a callback fired whenever the key is written."""
        self._subscribers[key].append(callback)
        return {"success": True, "key": key,
                "subscribers": len(self._subscribers[key])}

    def _is_expired(self, entry, now=None):
        ttl = entry.get("ttl")
        if ttl is None:
            return False
        now = time.time() if now is None else now
        return now - entry["timestamp"] > ttl

    def _can_read(self, entry, reader):
        owner = entry.get("owner")
        return owner is None or reader is None or reader == owner

    # --------------------------------------------------------------- lifecycle

    def update_lifecycle(self, now=None):
        """Expire stale data and clean up, protecting high-priority entries."""
        now = time.time() if now is None else now
        expired, cleaned, kept = [], [], []
        for key, entry in list(self.state.items()):
            if entry.get("priority", 0) >= self.PROTECTED_PRIORITY:
                kept.append(key)
                continue
            if self._is_expired(entry, now=now):
                expired.append(key)
                self._remove(key)
            else:
                kept.append(key)
        if expired:
            self._query_cache.clear()
        return {"expired": expired, "cleaned": cleaned, "kept": kept}

    def cleanup(self, max_entries=None):
        """Evict lowest-priority, oldest entries beyond max_entries."""
        removed = []
        if max_entries is not None and len(self.state) > max_entries:
            candidates = sorted(
                (k for k, e in self.state.items()
                 if e.get("priority", 0) < self.PROTECTED_PRIORITY),
                key=lambda k: (self.state[k].get("priority", 0),
                               self.state[k]["timestamp"]),
            )
            for key in candidates[: len(self.state) - max_entries]:
                self._remove(key)
                removed.append(key)
        if removed:
            self._query_cache.clear()
        return removed

    def _remove(self, key):
        entry = self.state.pop(key, None)
        if entry is not None:
            for field in ("writer", "owner", "priority"):
                self._field_index[field][entry.get(field)].discard(key)

    # ------------------------------------------------------------------ query

    def query_data(self, filters=None, use_cache=True, **conditions):
        """Query with field indexes, filtering and result caching."""
        filters = dict(filters or {})
        filters.update(conditions)
        cache_key = tuple(sorted((k, repr(v)) for k, v in filters.items()))
        if use_cache and cache_key in self._query_cache:
            return self._query_cache[cache_key]

        if not filters:
            result = {k: e["value"] for k, e in self.state.items()}
        else:
            candidate_keys = None
            remaining = {}
            for field, value in filters.items():
                indexed = self._field_index.get(field, {})
                if field in ("writer", "owner", "priority") and value in indexed:
                    keys = set(indexed[value])
                    candidate_keys = keys if candidate_keys is None \
                        else candidate_keys & keys
                else:
                    remaining[field] = value
            if candidate_keys is None:
                candidate_keys = set(self.state)
            result = {}
            for key in candidate_keys:
                entry = self.state.get(key)
                if entry is None or self._is_expired(entry):
                    continue
                if all(entry.get(f) == v for f, v in remaining.items()):
                    result[key] = entry["value"]

        self._query_cache[cache_key] = result
        return result

    # ------------------------------------------------------------------- sync

    def sync_blackboard(self, peer=None, force=False):
        """Incremental, conflict-resolving, rate-limited sync with a peer."""
        now = time.time()
        if peer is None:
            return {"success": False, "reason": "no_peer"}

        last = self._last_sync.get(id(peer), 0.0)
        if not force and now - last < self.SYNC_INTERVAL:
            return {"success": False, "reason": "rate_limited",
                    "retry_after": self.SYNC_INTERVAL - (now - last)}
        self._last_sync[id(peer)] = now

        # incremental: only send entries newer than what the peer has seen
        known = self._known_versions[id(peer)]
        delta_out = {k: e for k, e in self.state.items()
                     if e["version"] > known.get(k, 0)}
        peer_known = peer._known_versions[id(self)] if isinstance(
            peer, BlackboardSystem) else {}
        delta_in = {k: e for k, e in peer.state.items()
                    if e["version"] > peer_known.get(k, 0)} \
            if isinstance(peer, BlackboardSystem) else {}

        conflicts = []
        for key, remote in delta_in.items():
            local = self.state.get(key)
            if local is not None and local["version"] >= remote["version"] \
                    and local["value"] != remote["value"]:
                conflicts.append(key)
            winner = self._resolve(local, remote)
            if winner is not local:
                self.state[key] = dict(winner)
                self._index_entry(key, self.state[key])
                self._notify(key, self.state[key])
        for key, remote in delta_out.items():
            local = peer.state.get(key)
            winner = self._resolve(local, remote)
            if winner is not local:
                peer.state[key] = dict(winner)
                peer._index_entry(key, peer.state[key])
                peer._notify(key, peer.state[key])

        for key, entry in self.state.items():
            known[key] = max(known.get(key, 0), entry["version"])
        if isinstance(peer, BlackboardSystem):
            peer_known = peer._known_versions[id(self)]
            for key, entry in peer.state.items():
                peer_known[key] = max(peer_known.get(key, 0),
                                      entry["version"])

        self._query_cache.clear()
        peer._query_cache.clear()
        return {"success": True, "sent": len(delta_out),
                "received": len(delta_in), "conflicts": conflicts}

    @staticmethod
    def _resolve(local, remote):
        """Last-writer-wins: higher version, then newer timestamp."""
        if local is None:
            return remote
        if remote is None:
            return local
        if remote["version"] != local["version"]:
            return remote if remote["version"] > local["version"] else local
        return remote if remote["timestamp"] > local["timestamp"] else local
