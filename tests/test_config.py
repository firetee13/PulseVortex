import unittest
import tempfile
import os
from pathlib import Path

from monitor.config import (
    project_root,
    default_db_path,
    resolve_db_path,
    db_path_str,
)


class TestConfig(unittest.TestCase):
    def test_project_root_returns_path_object(self):
        result = project_root()
        self.assertIsInstance(result, Path)
        # Should point to the monitor directory's parent
        self.assertTrue(result.name == 'monitor' or 'monitor' in str(result))

    def test_default_db_path_returns_path_object(self):
        result = default_db_path()
        self.assertIsInstance(result, Path)

    def test_resolve_db_path_with_none_returns_default(self):
        result = resolve_db_path(None)
        default = default_db_path()
        self.assertEqual(result, default)

    def test_resolve_db_path_with_relative_path(self):
        result = resolve_db_path("test.db")
        self.assertIsInstance(result, Path)
        # Should be relative to project root
        self.assertTrue(str(project_root()) in str(result))

    def test_db_path_str_returns_string(self):
        result = db_path_str()
        self.assertIsInstance(result, str)

    def test_db_path_str_with_candidate(self):
        result = db_path_str("test.db")
        self.assertIsInstance(result, str)
        self.assertTrue("test.db" in result)


if __name__ == '__main__':
    unittest.main()