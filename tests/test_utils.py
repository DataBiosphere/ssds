#!/usr/bin/env python
import os
import sys
import unittest
from datetime import datetime

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from ssds.utils import retry, timestamp, timestamp_now, datetime_from_timestamp


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

    def test_timestamps(self):
        dt = datetime.utcnow()
        ts = timestamp(dt)
        self.assertEqual(datetime_from_timestamp(ts), dt)
        dt_now = datetime_from_timestamp(timestamp_now())
        self.assertGreater(dt_now, dt)

if __name__ == '__main__':
    unittest.main()
