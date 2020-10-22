#!/usr/bin/env python
import os
import sys
import unittest

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from ssds.utils import retry


class TestUtils(unittest.TestCase):
    def test_retry(self):
        tests = [(TypeError, 5), (ValueError, 5), (Exception, 1)]
        for exc, expected_attempts in tests:
            count = dict(count=0)

            @retry(TypeError, ValueError, number_of_attempts=expected_attempts)
            def my_func():
                count['count'] += 1
                raise exc("SDF")

            with self.assertRaises(exc):
                my_func()
            self.assertEqual(count['count'], expected_attempts)


if __name__ == '__main__':
    unittest.main()
