#!/usr/bin/env python3
"""不依赖 Spark/GaussVector 的二进制读取和统计单元测试。"""

import importlib.util
import struct
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parent.parent / "bin" / "benchmark.py"
SPEC = importlib.util.spec_from_file_location("sift1m_mvp_benchmark", MODULE_PATH)
assert SPEC and SPEC.loader
BENCHMARK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BENCHMARK)


class BinaryReaderTest(unittest.TestCase):
    def test_fvecs_and_ivecs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            query = root / "query.fvecs"
            groundtruth = root / "groundtruth.ivecs"
            query.write_bytes(struct.pack("<i128f", 128, *range(128)))
            groundtruth.write_bytes(struct.pack("<i100i", 100, *range(100)))

            vectors = BENCHMARK.read_fvecs(query, [0])
            neighbors = BENCHMARK.read_ivecs(groundtruth, [0], 10, 1)
            self.assertEqual(len(vectors[0]), 128)
            self.assertEqual(vectors[0][127], 127.0)
            self.assertEqual(neighbors[0], list(range(1, 11)))
            with query.open("rb") as handle:
                loaded = BENCHMARK.base_vector(handle, 1, 1, {})
            self.assertEqual(loaded[127], 127.0)
            self.assertEqual(BENCHMARK.euclidean_distance(loaded, loaded), 0.0)

    def test_multiple_query_and_groundtruth_records_remain_aligned(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            query = root / "query.fvecs"
            groundtruth = root / "groundtruth.ivecs"
            query.write_bytes(
                struct.pack("<i128f", 128, *range(128))
                + struct.pack("<i128f", 128, *range(1000, 1128))
            )
            groundtruth.write_bytes(
                struct.pack("<i100i", 100, *range(100))
                + struct.pack("<i100i", 100, *range(1000, 1100))
            )

            vectors = BENCHMARK.read_fvecs(query, [0, 1])
            neighbors = BENCHMARK.read_ivecs(groundtruth, [0, 1], 10, 1)
            self.assertEqual(vectors[1][0], 1000.0)
            self.assertEqual(vectors[1][-1], 1127.0)
            self.assertEqual(neighbors[1], list(range(1001, 1011)))

    def test_gist_dimension_uses_the_same_binary_reader(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = root / "gist_base.fvecs"
            query = root / "gist_query.fvecs"
            groundtruth = root / "gist_groundtruth.ivecs"
            vector = list(range(960))
            base.write_bytes(struct.pack("<i960f", 960, *vector))
            query.write_bytes(struct.pack("<i960f", 960, *vector))
            groundtruth.write_bytes(struct.pack("<i100i", 100, *range(100)))

            vectors = BENCHMARK.read_fvecs(query, [0], 960)
            neighbors = BENCHMARK.read_ivecs(groundtruth, [0], 100, 1, 100)
            with base.open("rb") as handle:
                loaded = BENCHMARK.base_vector(handle, 1, 1, {}, 960, 1)

            self.assertEqual(len(vectors[0]), 960)
            self.assertEqual(vectors[0][-1], 959.0)
            self.assertEqual(neighbors[0], list(range(1, 101)))
            self.assertEqual(loaded[-1], 959.0)

    def test_equidistant_sampling_uses_matching_query_and_groundtruth_indices(self):
        indices = BENCHMARK.select_query_indices(10_000, 100, "equidistant")
        self.assertEqual(indices, list(range(0, 10_000, 101)))
        self.assertEqual(BENCHMARK.select_query_indices(10_000, 1, "equidistant"), [0])

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            query = root / "query.fvecs"
            groundtruth = root / "groundtruth.ivecs"
            query.write_bytes(
                b"".join(
                    struct.pack("<i128f", 128, *([index] * 128))
                    for index in range(4)
                )
            )
            groundtruth.write_bytes(
                b"".join(
                    struct.pack("<i100i", 100, *range(index * 100, index * 100 + 100))
                    for index in range(4)
                )
            )

            vectors = BENCHMARK.read_fvecs(query, [0, 3])
            neighbors = BENCHMARK.read_ivecs(groundtruth, [0, 3], 10, 1)
            self.assertEqual([vector[0] for vector in vectors], [0.0, 3.0])
            self.assertEqual(neighbors[1], list(range(301, 311)))

    def test_percentile(self):
        self.assertEqual(BENCHMARK.percentile([1.0, 2.0, 3.0], 50), 2.0)
        self.assertAlmostEqual(BENCHMARK.percentile([1.0, 3.0], 95), 2.9)

    def test_parse_gsql_output(self):
        output = """noise
__MVP_QUERY_BEGIN_0__
3
7
Time: 12.500 ms
__MVP_QUERY_END_0__
__MVP_QUERY_BEGIN_1__
4
Time: 8.250 ms
__MVP_QUERY_END_1__
"""
        ids, times = BENCHMARK.parse_gsql_output(output, 2)
        self.assertEqual(ids, [[3, 7], [4]])
        self.assertEqual(times, [12.5, 8.25])

    def test_mode_settings_include_query_dop(self):
        index = BENCHMARK.mode_settings("index", 8)
        fullscan = BENCHMARK.mode_settings("fullscan", 2)
        self.assertIn("SET query_dop=8;", index)
        self.assertIn("SET enable_vectorsearch=on;", index)
        self.assertIn("SET query_dop=2;", fullscan)
        self.assertIn("SET enable_vectorsearch=off;", fullscan)


if __name__ == "__main__":
    unittest.main()
