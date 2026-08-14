#!/usr/bin/env python3
"""GIST1M 配置隔离、供数边界和索引 ABI 的静态回归门禁。"""

import os
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
CONFIG = (ROOT / "config/gist-perf.env.example").read_text(encoding="utf-8")
RUNNER = (ROOT / "bin/run-gist-perf.sh").read_text(encoding="utf-8")
SUPPLY = (ROOT / "bin/supply-data.sh").read_text(encoding="utf-8")
VERIFY = (ROOT / "bin/verify-gist1m.sh").read_text(encoding="utf-8")
BUILD = (ROOT / "bin/build-index.sh").read_text(encoding="utf-8")
DOWNLOAD = (ROOT / "bin/download-gist1m.sh").read_text(encoding="utf-8")
BUNDLE = (ROOT / "bin/make-offline-bundle.sh").read_text(encoding="utf-8")


class GistWorkflowTest(unittest.TestCase):
    def test_config_encodes_official_dataset_contract(self):
        for contract in (
            "MVP_DATASET=gist1m",
            "MVP_VECTOR_DIM=960",
            "MVP_ROW_COUNT=1000000",
            "MVP_QUERY_COUNT=1000",
            "MVP_GT_K=100",
            "MVP_PARTITION_BUCKETS=32",
            "MVP_PYICEBERG_BATCH_ROWS=1000000",
            "MVP_QUERY_SAMPLING=equidistant",
            "MVP_RECALL_NQ=1000",
        ):
            self.assertIn(contract, CONFIG)

    def test_sift_profiles_declare_their_own_dataset_contract(self):
        for filename in ("mvp.env.example", "perf.env.example"):
            config = (ROOT / "config" / filename).read_text(encoding="utf-8")
            self.assertIn("MVP_DATASET=sift1m", config)
            self.assertIn("MVP_VECTOR_DIM=128", config)
            self.assertIn("MVP_QUERY_COUNT=10000", config)

    def test_gist_uses_explicit_pq_abi_and_parameters(self):
        self.assertIn("MVP_INDEX_TYPE=ivf_pq", CONFIG)
        self.assertIn("MVP_INDEX_IMPLEMENTATION=ivf_pq", CONFIG)
        self.assertIn("MVP_NUM_SUB_QUANTIZERS=60", CONFIG)
        self.assertIn("MVP_PQ_NBITS=8", CONFIG)
        self.assertIn("builtin.ivf_flat@2", BUILD)
        self.assertIn("builtin.ivf_pq@1", BUILD)
        self.assertIn("num_sub_quantizers", BUILD)
        self.assertIn("pq_nbits", BUILD)

    def test_profile_switch_keeps_gist_index_names(self):
        with tempfile.TemporaryDirectory() as directory:
            env_file = Path(directory) / "gist.env"
            env_file.write_text(CONFIG, encoding="utf-8")
            environment = os.environ | {"MVP_ENV_FILE": str(env_file)}

            subprocess.run(
                ["bash", str(ROOT / "bin/configure-index.sh"), "flat"],
                check=True,
                env=environment,
                capture_output=True,
                text=True,
            )
            self.assertIn("MVP_INDEX_NAME=idx_gist_ivfflat", env_file.read_text())
            subprocess.run(
                ["bash", str(ROOT / "bin/configure-index.sh"), "pq"],
                check=True,
                env=environment,
                capture_output=True,
                text=True,
            )
            self.assertIn("MVP_INDEX_NAME=idx_gist_ivfpq", env_file.read_text())

    def test_entrypoint_isolates_config_state_and_provider(self):
        self.assertIn('env_file="${MVP_ENV_FILE:-$root_dir/gist.env}"', RUNNER)
        self.assertIn('MVP_STATE_DIR:-$root_dir/state/gist1m', RUNNER)
        self.assertIn('run-perf.sh" "$mode" pyiceberg', RUNNER)
        self.assertIn('"MVP_VECTOR_DIM:${MVP_VECTOR_DIM:-}:960"', RUNNER)
        self.assertIn('"MVP_PARTITION_BUCKETS:${MVP_PARTITION_BUCKETS:-}:32"', RUNNER)
        self.assertIn("gist1m:pyiceberg)", SUPPLY)
        self.assertNotIn("gist1m:spark)", SUPPLY)
        self.assertNotIn("gist1m:rust)", SUPPLY)

    def test_verifier_checks_all_fixed_file_contracts(self):
        for contract in (
            "gist_base.fvecs]=3844000000",
            "gist_query.fvecs]=3844000",
            "gist_groundtruth.ivecs]=404000",
            '("gist_base.fvecs", 1_000_000, 960)',
            '("gist_query.fvecs", 1_000, 960)',
            '("gist_groundtruth.ivecs", 1_000, 100)',
            "GIST1M_SHA256SUMS",
        ):
            self.assertIn(contract, VERIFY)

    def test_download_pins_archive_size_and_sha256(self):
        self.assertIn(
            "mirror_revision=a98d7415dba638216300552059013cc627293409",
            DOWNLOAD,
        )
        self.assertIn('hf download "$mirror_repo" gist.tar.gz', DOWNLOAD)
        self.assertIn("expected_archive_size=2740172684", DOWNLOAD)
        self.assertIn(
            "01469a7f1c3768853525e543d537e2dfa1adece927616405e360952e3f67df73",
            DOWNLOAD,
        )

    def test_offline_bundle_selects_one_dataset(self):
        self.assertIn("MVP_OFFLINE_DATASET:-sift1m", BUNDLE)
        self.assertIn('dataset" == "gist1m', BUNDLE)
        self.assertIn('downloads/sift_*', BUNDLE)
        self.assertIn('downloads/gist_*', BUNDLE)


if __name__ == "__main__":
    unittest.main()
