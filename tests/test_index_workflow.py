#!/usr/bin/env python3
"""索引映射、清理边界和总工作流静态回归门禁。"""

import os
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
BUILD = (ROOT / "bin/build-index.sh").read_text(encoding="utf-8")
CLEAN = (ROOT / "bin/clean.sh").read_text(encoding="utf-8")
RUNNER = (ROOT / "bin/run-clean-test.sh").read_text(encoding="utf-8")
PERF_RUNNER = (ROOT / "bin/run-perf.sh").read_text(encoding="utf-8")
MATRIX = (ROOT / "bin/run-matrix.py").read_text(encoding="utf-8")
INDEX_TEST = (ROOT / "bin/test-index.sh").read_text(encoding="utf-8")


class IndexWorkflowTest(unittest.TestCase):
    def test_build_index_uses_exact_abi_pairs(self):
        self.assertIn("ivf_flat:ivf)", BUILD)
        self.assertIn("ivf_pq:ivf_pq)", BUILD)
        self.assertIn("btree:btree)", BUILD)
        self.assertNotIn("ivf_flat:ivf_flat)", BUILD)
        self.assertIn("builtin.ivf_flat@2", BUILD)
        self.assertIn("builtin.ivf_pq@1", BUILD)

    def test_example_defaults_to_pq(self):
        for profile, filename in (
            ("mvp", "mvp.env.example"),
            ("perf", "perf.env.example"),
        ):
            config = (ROOT / "config" / filename).read_text(encoding="utf-8")
            self.assertIn(f"MVP_CONFIG_PROFILE={profile}", config)
            self.assertIn("MVP_INDEX_TYPE=ivf_pq", config)
            self.assertIn("MVP_INDEX_IMPLEMENTATION=ivf_pq", config)

    def test_performance_and_query_scale_defaults(self):
        perf = (ROOT / "config/perf.env.example").read_text(encoding="utf-8")
        self.assertIn("MVP_NUM_CLUSTERS=1024", perf)
        self.assertIn("MVP_SAMPLE_RATE=100000", perf)
        self.assertIn("MVP_NPROBE=10", perf)
        self.assertIn("MVP_PYICEBERG_BATCH_ROWS=1000000", perf)
        self.assertIn("MVP_RUST_BATCH_ROWS=1000000", perf)
        self.assertIn("MVP_RECALL_NQ=10000", perf)
        self.assertIn("MVP_QUERY_SAMPLING=equidistant", perf)
        self.assertIn("MVP_MATRIX_NQ=100", perf)
        self.assertIn("MVP_MATRIX_ROUNDS=1", perf)
        self.assertIn("MVP_PERF_K=10,100", perf)
        self.assertIn("MVP_PERF_DOP=1,8", perf)
        self.assertIn("MVP_PERF_NQ=100", perf)
        self.assertIn('config.get("MVP_MATRIX_NQ", "100")', MATRIX)
        self.assertIn('config.get("MVP_MATRIX_ROUNDS", "1")', MATRIX)
        self.assertIn('scope="${2:-quick}"', INDEX_TEST)
        self.assertIn('test_nq="${MVP_RECALL_NQ:-10000}"', INDEX_TEST)
        self.assertIn('--query-sampling "${MVP_QUERY_SAMPLING:-first}"', INDEX_TEST)
        self.assertIn('config.get("MVP_QUERY_SAMPLING", "first")', MATRIX)

    def test_profile_switch_updates_complete_pair(self):
        source = ROOT / "config/mvp.env.example"
        with tempfile.TemporaryDirectory() as directory:
            env_file = Path(directory) / "mvp.env"
            env_file.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
            environment = os.environ | {"MVP_ENV_FILE": str(env_file)}

            subprocess.run(
                ["bash", str(ROOT / "bin/configure-index.sh"), "flat"],
                check=True,
                env=environment,
                capture_output=True,
                text=True,
            )
            flat = env_file.read_text(encoding="utf-8")
            self.assertIn("MVP_INDEX_NAME=idx_sift_ivfflat", flat)
            self.assertIn("MVP_INDEX_TYPE=ivf_flat", flat)
            self.assertIn("MVP_INDEX_IMPLEMENTATION=ivf\n", flat)

            subprocess.run(
                ["bash", str(ROOT / "bin/configure-index.sh"), "pq"],
                check=True,
                env=environment,
                capture_output=True,
                text=True,
            )
            pq = env_file.read_text(encoding="utf-8")
            self.assertIn("MVP_INDEX_NAME=idx_sift_ivfpq", pq)
            self.assertIn("MVP_INDEX_TYPE=ivf_pq", pq)
            self.assertIn("MVP_INDEX_IMPLEMENTATION=ivf_pq", pq)

    def test_index_cleanup_uses_catalog_apis_and_verifies_rows(self):
        self.assertIn("iceberg_catalog.drop_index", CLEAN)
        self.assertIn("iceberg_catalog.vacuum_index", CLEAN)
        self.assertIn("clean-index-artifacts.py", CLEAN)
        self.assertIn("SELECT count(*) FROM iceberg_catalog.table_indexes", CLEAN)
        self.assertNotIn("DELETE FROM iceberg_catalog.table_indexes", CLEAN)

    def test_cleanup_stops_when_catalog_state_query_fails(self):
        self.assertIn("load_catalog_state", CLEAN)
        self.assertIn("-v ON_ERROR_STOP=1", CLEAN)
        self.assertNotIn("catalog_installed", CLEAN)

    def test_runner_supports_fresh_and_reuse_in_expected_order(self):
        self.assertIn('"$mode" == "fresh"', RUNNER)
        self.assertIn('[fresh|reuse]', RUNNER)
        fullscan = RUNNER.index('test-fullscan.sh')
        flat_config = RUNNER.index('configure-index.sh" flat', fullscan)
        flat_test = RUNNER.index('test-index.sh" flat', flat_config)
        index_clean = RUNNER.index('clean.sh" index', flat_test)
        pq_config = RUNNER.index('configure-index.sh" pq', index_clean)
        pq_test = RUNNER.index('test-index.sh" pq', pq_config)
        self.assertEqual(
            [fullscan, flat_config, flat_test, index_clean, pq_config, pq_test],
            sorted([fullscan, flat_config, flat_test, index_clean, pq_config, pq_test]),
        )

    def test_mvp_entrypoint_defaults_to_fresh_pyiceberg(self):
        self.assertIn('mode="${1:-fresh}"', RUNNER)
        self.assertIn('provider="${2:-pyiceberg}"', RUNNER)
        self.assertIn('MVP_CONFIG_PROFILE:-', RUNNER)

    def test_perf_runner_covers_flat_pq_and_clean_fullscan_in_order(self):
        self.assertIn('MVP_CONFIG_PROFILE:-', PERF_RUNNER)
        self.assertIn('MVP_PERF_K:-10,100', PERF_RUNNER)
        self.assertIn('MVP_PERF_DOP:-1,8', PERF_RUNNER)
        self.assertIn('MVP_PERF_NQ:-100', PERF_RUNNER)
        flat_config = PERF_RUNNER.index('configure-index.sh" flat')
        flat_matrix = PERF_RUNNER.index('run_matrix index "$root_dir/state/perf/flat"')
        flat_clean = PERF_RUNNER.index('clean.sh" index', flat_matrix)
        pq_config = PERF_RUNNER.index('configure-index.sh" pq', flat_clean)
        pq_matrix = PERF_RUNNER.index('run_matrix index "$root_dir/state/perf/pq"')
        pq_clean = PERF_RUNNER.index('clean.sh" index', pq_matrix)
        fullscan = PERF_RUNNER.index('run_matrix fullscan', pq_clean)
        expected_order = [
            flat_config,
            flat_matrix,
            flat_clean,
            pq_config,
            pq_matrix,
            pq_clean,
            fullscan,
        ]
        self.assertEqual(
            expected_order,
            sorted(expected_order),
        )

    def test_config_initializer_creates_one_profile_and_refuses_mismatch(self):
        with tempfile.TemporaryDirectory() as directory:
            env_file = Path(directory) / "mvp.env"
            environment = os.environ | {
                "MVP_ENV_FILE": str(env_file),
                "MVP_NAMESPACE": "auto_config_ns",
            }
            subprocess.run(
                ["bash", str(ROOT / "bin/init-env.sh"), "mvp"],
                check=True,
                env=environment,
                capture_output=True,
                text=True,
            )
            initialized = env_file.read_text()
            self.assertIn("MVP_CONFIG_PROFILE=mvp", initialized)
            self.assertIn("MVP_NAMESPACE=auto_config_ns", initialized)
            mismatch = subprocess.run(
                ["bash", str(ROOT / "bin/init-env.sh"), "perf"],
                check=False,
                env=environment,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(mismatch.returncode, 0)
            self.assertIn("期望 perf", mismatch.stderr)

    def test_config_initializer_uses_locked_pyiceberg_installer(self):
        initializer = (ROOT / "bin/init-env.sh").read_text(encoding="utf-8")
        self.assertIn("install-pyiceberg-offline.sh", initializer)
        self.assertIn("wheelhouse/SHA256SUMS", initializer)

    def test_new_workflow_does_not_reference_delta_objects(self):
        for filename in (
            "clean.sh",
            "clean-index-artifacts.py",
            "configure-index.sh",
            "deploy.sh",
            "init-env.sh",
            "run-clean-test.sh",
            "run-perf.sh",
            "supply-data.sh",
            "test-fullscan.sh",
            "test-index.sh",
            "verify-table.sh",
        ):
            script = (ROOT / "bin" / filename).read_text(encoding="utf-8")
            self.assertNotIn("iceberg_delta", script)
            self.assertNotIn("_delta", script)


if __name__ == "__main__":
    unittest.main()
