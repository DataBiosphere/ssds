import os
import sys
import unittest
from unittest.mock import Mock, MagicMock, patch
from argparse import Namespace

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from ssds.cli import staging as staging_cli
from ssds import deployment


class TestStagingCLI(unittest.TestCase):
    def test_upload(self):
        mock_staging = MagicMock()
        mock_staging.upload = MagicMock()
        with patch("ssds.deployment.Staging.ssds", mock_staging):
            args = Namespace(submission_id="foo", name="bar", path="asf")
            staging_cli.upload(args)
            expected_path = os.path.abspath(os.path.normpath(args.path))
            mock_staging.upload.assert_called_with(expected_path, args.submission_id, args.name)

    def test_list(self):
        mock_staging = Mock()
        mock_staging.list = MagicMock()
        with patch("ssds.deployment.Staging.ssds", mock_staging):
            args = Namespace()
            staging_cli.list(args)
            mock_staging.list.assert_called()

    def test_list_submission(self):
        mock_staging = Mock()
        mock_staging.list_submission = MagicMock()
        with patch("ssds.deployment.Staging.ssds", mock_staging):
            args = Namespace(submission_id="foo")
            staging_cli.list_submission(args)
            mock_staging.list_submission.assert_called_with(args.submission_id)

if __name__ == '__main__':
    unittest.main()
