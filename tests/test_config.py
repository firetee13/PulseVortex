import os
import unittest
from pathlib import Path

from monitor.core.config import db_path_str, default_db_path, project_root, resolve_db_path


class ConfigHelpersTests(unittest.TestCase):

    def _set_env(self, key: str, value):
        original = os.environ.get(key)

        def restore():
            if original is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = original

        self.addCleanup(restore)
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value

    def test_project_root_matches_package_parent(self) -> None:
        expected = Path(__file__).resolve().parent.parent
        self.assertEqual(project_root(), expected)

    def test_default_db_path_without_env_uses_project_root(self) -> None:
        self._set_env("TIMELAPSE_DB_PATH", None)
        expected = project_root() / "timelapse.db"
        self.assertEqual(default_db_path(), expected)

    def test_default_db_path_with_env_override(self) -> None:
        self._set_env("TIMELAPSE_DB_PATH", "backup/custom.db")
        expected = project_root() / "backup" / "custom.db"
        self.assertEqual(default_db_path(), expected)

    def test_resolve_db_path_handles_absolute_and_relative(self) -> None:
        relative = resolve_db_path("storage/alt.db")
        self.assertTrue(str(relative).endswith("storage" + os.sep + "alt.db"))
        absolute_candidate = Path(project_root(), "nested", "file.db").resolve()
        self.assertEqual(resolve_db_path(str(absolute_candidate)), absolute_candidate)

    def test_db_path_str_is_string_wrapper(self) -> None:
        target = resolve_db_path("timelapse.db")
        self.assertEqual(db_path_str("timelapse.db"), str(target))


if __name__ == "__main__":
    unittest.main()
