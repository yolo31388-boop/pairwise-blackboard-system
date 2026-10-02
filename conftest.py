"""Provide unittest-style assertions to plain test classes.

The tests in tests/ call self.assertIsNotNone without inheriting from
unittest.TestCase. Test files must not be modified, so the assertion
helpers are attached here at collection time.
"""

import unittest


def pytest_collection_modifyitems(items):
    for item in items:
        cls = getattr(item, "cls", None)
        if cls is not None and not hasattr(cls, "assertIsNotNone"):
            cls.assertIsNotNone = unittest.TestCase.assertIsNotNone
