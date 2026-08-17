import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent


class RepositoryLayoutTest(unittest.TestCase):
    def test_future_dataset_is_ignored_and_uses_lfs(self):
        path = "downloads/future/deep_base.fbin"
        ignored = subprocess.run(
            ["git", "check-ignore", "--no-index", "-q", path],
            cwd=ROOT,
            check=False,
        )
        self.assertEqual(ignored.returncode, 0)

        attributes = subprocess.check_output(
            ["git", "check-attr", "filter", "diff", "merge", "text", "--", path],
            cwd=ROOT,
            text=True,
        )
        self.assertIn("filter: lfs", attributes)
        self.assertIn("diff: lfs", attributes)
        self.assertIn("merge: lfs", attributes)
        self.assertIn("text: unset", attributes)

    def test_download_placeholder_remains_a_normal_git_file(self):
        attributes = subprocess.check_output(
            [
                "git",
                "check-attr",
                "filter",
                "diff",
                "merge",
                "text",
                "--",
                "downloads/.gitkeep",
            ],
            cwd=ROOT,
            text=True,
        )
        self.assertIn("filter: unspecified", attributes)
        self.assertIn("diff: unspecified", attributes)
        self.assertIn("merge: unspecified", attributes)
        self.assertIn("text: set", attributes)


if __name__ == "__main__":
    unittest.main()
