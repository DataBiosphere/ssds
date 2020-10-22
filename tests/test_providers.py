#!/usr/bin/env python
import os
import sys
import unittest

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from ssds import aws, gcp

from tests import infra


class TestAWS(infra.SuppressWarningsMixin, unittest.TestCase):
    def test_get_identity(self):
        print(aws.get_identity())

class TestGCP(infra.SuppressWarningsMixin, unittest.TestCase):
    def test_get_identity(self):
        print(gcp.get_identity())

if __name__ == '__main__':
    unittest.main()
