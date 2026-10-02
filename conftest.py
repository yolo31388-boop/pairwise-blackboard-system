"""Pytest bootstrap.

The shipped test class calls unittest-style ``self.assertIsNotNone`` without
inheriting ``unittest.TestCase``. Expose those assertion helpers on the test
class so the template tests can run; test files themselves are untouched.
"""
import unittest

from tests.test_blackboard import TestBlackboardSystem

for _name in dir(unittest.TestCase):
    if _name.startswith("assert"):
        setattr(TestBlackboardSystem, _name, getattr(unittest.TestCase, _name))
