#!/usr/bin/env python3
import contextlib
import copy
import io
import json
import os
import pathlib
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import common
import normalize_model
import normalize_raw


class BoundedJsonlTests(unittest.TestCase):
    def test_rejects_duplicate_keys_and_noncanonical_numbers(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            duplicate = root / "duplicate.jsonl"
            duplicate.write_bytes(b'{"a":1,"a":2}\n')
            with self.assertRaises(common.EvidenceError):
                common.read_jsonl(duplicate, 100, 100, 1)
            floating = root / "floating.jsonl"
            floating.write_bytes(b'{"a":1.0}\n')
            with self.assertRaises(common.EvidenceError):
                common.read_jsonl(floating, 100, 100, 1)

    def test_rejects_missing_newline_and_every_limit(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            path = root / "input.jsonl"
            path.write_bytes(b"{}")
            with self.assertRaises(common.EvidenceError):
                common.read_jsonl(path, 2, 2, 1)
            path.write_bytes(b"{}\n")
            with self.assertRaises(common.EvidenceError):
                common.read_jsonl(path, 2, 3, 1)
            with self.assertRaises(common.EvidenceError):
                common.read_jsonl(path, 3, 2, 1)
            path.write_bytes(b"{}\n{}\n")
            with self.assertRaises(common.EvidenceError):
                common.read_jsonl(path, 6, 3, 1)

    def test_rejects_symbolic_link_input(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            target = root / "target.jsonl"
            target.write_bytes(b"{}\n")
            link = root / "link.jsonl"
            link.symlink_to(target)
            with self.assertRaises(common.EvidenceError):
                common.read_jsonl(link, 3, 3, 1)

    def test_canonical_output_has_sorted_keys_and_no_floating_values(self):
        self.assertEqual(
            common.canonical_json({"z": 1, "a": [True, None]}),
            '{"a":[true,null],"z":1}',
        )
        with self.assertRaises(common.EvidenceError):
            common.canonical_json({"value": 1.0})


class LogicalNormalizationTests(unittest.TestCase):
    def leaf(self):
        return {
            "physical_type": "INT32",
            "type_length": {"present": False, "value": None},
            "converted_type": {"present": False, "value": None},
            "scale": {"present": False, "value": None},
            "precision": {"present": False, "value": None},
            "logical_type": {
                "present": False,
                "member": None,
                "parameters": None,
            },
        }

    def test_synthesizes_legacy_decimal_and_temporal_parameters(self):
        decimal = self.leaf()
        decimal["converted_type"] = {"present": True, "value": "DECIMAL"}
        decimal["scale"] = {"present": True, "value": 2}
        decimal["precision"] = {"present": True, "value": 9}
        normalized = normalize_raw.effective_leaf(decimal)
        self.assertEqual(normalized["logical_type"], "DECIMAL")
        self.assertEqual(normalized["precision"], 9)
        self.assertEqual(normalized["scale"], 2)
        temporal = self.leaf()
        temporal["converted_type"] = {
            "present": True,
            "value": "TIME_MILLIS",
        }
        normalized = normalize_raw.effective_leaf(temporal)
        self.assertEqual(normalized["logical_type"], "TIME")
        self.assertEqual(normalized["time_unit"], "MILLIS")
        self.assertIs(normalized["is_adjusted_to_utc"], True)

    def test_modern_parameters_win_but_legacy_pair_must_match(self):
        temporal = self.leaf()
        temporal["converted_type"] = {
            "present": True,
            "value": "TIME_MICROS",
        }
        temporal["physical_type"] = "INT64"
        temporal["logical_type"] = {
            "present": True,
            "member": "TIME",
            "parameters": {
                "unit": "MICROS",
                "is_adjusted_to_utc": False,
            },
        }
        normalized = normalize_raw.effective_leaf(temporal)
        self.assertIs(normalized["is_adjusted_to_utc"], False)
        temporal["converted_type"]["value"] = "TIME_MILLIS"
        with self.assertRaises(common.EvidenceError):
            normalize_raw.effective_leaf(temporal)

    def test_rejects_unknown_future_logical_member(self):
        leaf = self.leaf()
        leaf["logical_type"] = {
            "present": True,
            "member": None,
            "parameters": None,
        }
        with self.assertRaises(common.EvidenceError):
            normalize_raw.effective_leaf(leaf)


class CorpusManifestTests(unittest.TestCase):
    def test_parses_canonical_rows_and_rejects_duplicates(self):
        first = b"0" * 64 + b"  data/a.parquet\n"
        second = b"1" * 64 + b"  data/b.parquet\n"
        self.assertEqual(normalize_raw.parse_corpus_manifest(first + second), {
            "data/a.parquet": "0" * 64,
            "data/b.parquet": "1" * 64,
        })
        with self.assertRaises(common.EvidenceError):
            normalize_raw.parse_corpus_manifest(first + first)
        with self.assertRaises(common.EvidenceError):
            normalize_raw.parse_corpus_manifest(
                b"0" * 64 + b" data/a.parquet\n")

    def test_projects_only_verified_generated_fixture_identities(self):
        generated = {
            "id": "generated-case",
            "status": "verified",
            "source_kind": "julia-writer-generated",
            "output_file": "generated/case.parquet",
            "output_identity_status": "verified",
            "output_sha256": "a" * 64,
            "output_size": 12,
            "row_group_count": 1,
            "leaf_count": 1,
            "normalized_record_count": 2,
            "capabilities": ["wire.statistics.counts"],
        }
        cases = normalize_raw.fixture_cases({
            "fixture": [],
            "generated_case": [generated],
        }, "generated")
        self.assertEqual(cases[0]["file"], "generated/case.parquet")
        self.assertEqual(cases[0]["sha256"], "a" * 64)
        self.assertEqual(cases[0]["size"], 12)
        planned = copy.deepcopy(generated)
        planned["output_identity_status"] = "planned"
        with self.assertRaises(common.EvidenceError):
            normalize_raw.fixture_cases({
                "fixture": [],
                "generated_case": [planned],
            }, "generated")

    def test_apache_fixture_projection_remains_the_default(self):
        fixture = {
            "id": "apache-case",
            "status": "verified",
            "source_kind": "apache-corpus",
            "file": "data/case.parquet",
            "sha256": "b" * 64,
            "size": 12,
        }
        cases = normalize_raw.fixture_cases({
            "fixture": [fixture],
            "generated_case": [],
        }, "apache")
        self.assertEqual(cases, [fixture])
        with self.assertRaises(common.EvidenceError):
            normalize_raw.fixture_cases({
                "fixture": [fixture],
                "generated_case": [],
            }, "unknown")

    def test_preserves_explicit_unsupported_authority_claims(self):
        context = {
            "capabilities": {"authority": [{
                "id": "n6-raw-java",
                "version": "parquet-2.13-raw-footer-v3",
                "revision": normalize_raw.FORMAT_COMMIT,
                "toolchain_sha256": ["a" * 64],
                "claim": [{
                    "capability": "semantic.type-order",
                    "status": "unsupported",
                    "cases": ["atomic-bound-family"],
                }],
            }]},
            "raw_entry": {
                "authority": "n6-raw-java",
                "toolchain_sha256": "a" * 64,
            },
        }
        self.assertEqual(normalize_raw._unsupported_claims(context), [
            ("atomic-bound-family", "semantic.type-order"),
        ])
        result = normalize_raw._unsupported_result(
            "atomic-bound-family", "semantic.type-order")
        self.assertEqual(result["status"], "UNSUPPORTED")
        self.assertIsNone(result["expected_sha256"])
        self.assertIsNone(result["actual_sha256"])


class ColumnOrderTests(unittest.TestCase):
    def test_preserves_known_unknown_wrong_type_empty_and_absent(self):
        cases = (
            (None, "ABSENT", None),
            ({"state": "known", "field_id": 1, "wire_type": 12,
                "header_hex": "1c", "member": "TYPE_ORDER"},
                "TYPE_ORDER", "1c"),
            ({"state": "unknown", "field_id": 7, "wire_type": 12,
                "header_hex": "7c", "member": None}, "UNKNOWN", "7c"),
            ({"state": "wrong_type", "field_id": 2, "wire_type": 8,
                "header_hex": "25", "member": None}, "WRONG_TYPE", "25"),
            ({"state": "empty", "field_id": None, "wire_type": None,
                "header_hex": "00", "member": None}, "EMPTY", "00"),
        )
        for raw, state, header in cases:
            with self.subTest(state=state):
                normalized = normalize_raw.normalize_order(raw)
                self.assertEqual(normalized["state"], state)
                self.assertEqual(normalized["header_hex"], header)

    def test_statistics_counts_become_canonical_int64_strings(self):
        fields = []
        names = (
            "max", "min", "null_count", "distinct_count", "max_value",
            "min_value", "is_max_value_exact", "is_min_value_exact",
            "nan_count",
        )
        values = (None, None, 0, 3, None, None, None, None, 2)
        for field_id, (name, value) in enumerate(zip(names, values), 1):
            fields.append({
                "field_id": field_id,
                "name": name,
                "present": value is not None,
                "value": value,
            })
        normalized = normalize_raw.normalize_statistics({
            "present": True,
            "fields": fields,
        })
        self.assertEqual(normalized["null_count"], "0")
        self.assertEqual(normalized["distinct_count"], "3")
        self.assertEqual(normalized["nan_count"], "2")


class AtomicOutputTests(unittest.TestCase):
    def test_check_mode_and_atomic_replace(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "evidence.jsonl"
            common.publish_bytes(path, b"first\n", False, ())
            self.assertEqual(path.read_bytes(), b"first\n")
            common.publish_bytes(path, b"first\n", True, ())
            with self.assertRaises(common.EvidenceError):
                common.publish_bytes(path, b"second\n", True, ())
            self.assertEqual(path.read_bytes(), b"first\n")

    def test_rejects_input_alias_and_output_symlink(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            source = root / "source.jsonl"
            source.write_bytes(b"source\n")
            with self.assertRaises(common.EvidenceError):
                common.publish_bytes(source, b"changed\n", False, (source,))
            target = root / "target.jsonl"
            target.write_bytes(b"target\n")
            link = root / "link.jsonl"
            link.symlink_to(target)
            with self.assertRaises(common.EvidenceError):
                common.publish_bytes(link, b"changed\n", False, ())
            self.assertEqual(target.read_bytes(), b"target\n")

    def test_normalization_failure_preserves_existing_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            raw = root / "raw.jsonl"
            raw.write_bytes(b'{"bad":true}\n')
            output = root / "output.jsonl"
            output.write_bytes(b"sentinel\n")
            errors = io.StringIO()
            with contextlib.redirect_stderr(errors):
                status = normalize_raw.main([
                    "--input", str(raw),
                    "--output", str(output),
                ])
            self.assertNotEqual(status, 0)
            self.assertEqual(output.read_bytes(), b"sentinel\n")

    def test_model_failure_preserves_existing_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            normalized = root / "normalized.jsonl"
            normalized.write_bytes(b'{"bad":true}\n')
            raw = root / "raw.jsonl"
            raw.write_bytes(b'{"bad":true}\n')
            output = root / "output.jsonl"
            output.write_bytes(b"sentinel\n")
            errors = io.StringIO()
            with contextlib.redirect_stderr(errors):
                status = normalize_model.main([
                    "--input", str(normalized),
                    "--raw-input", str(raw),
                    "--output", str(output),
                    "--julia-executable", os.environ.get(
                        "PARQUET_N6_TEST_JULIA_EXECUTABLE", "/absent/julia"),
                ])
            self.assertNotEqual(status, 0)
            self.assertEqual(output.read_bytes(), b"sentinel\n")

    def test_semantic_validator_rejects_schema_only_payload(self):
        context = normalize_raw.load_context()
        with self.assertRaises(common.EvidenceError):
            normalize_raw.validate_normalized_payload(b"{}\n", context)


class IndependentModelBridgeTests(unittest.TestCase):
    def julia_executable(self):
        executable = os.environ.get("PARQUET_N6_TEST_JULIA_EXECUTABLE", "")
        if not executable:
            self.fail("PARQUET_N6_TEST_JULIA_EXECUTABLE is required")
        return executable

    def run_model(self, columns, files, command, run_suite=False):
        context = normalize_raw.load_context()
        normalize_model._verify_models(context)
        with normalize_model.model_producer_snapshot(context) as producer_root:
            return normalize_model.run_model(columns, files, command,
                producer_root, run_suite=run_suite)

    def test_model_producer_descriptor_binds_all_execution_sources(self):
        context = normalize_raw.load_context()
        self.assertIsNone(normalize_model._verify_models(context))
        descriptor = normalize_model.PRODUCER_DESCRIPTOR_PATH
        self.assertEqual(
            context["model_snapshots"][descriptor],
            context["manifest"]["model_producer_descriptor_sha256"],
        )
        broken = dict(context)
        broken["manifest"] = dict(context["manifest"])
        broken["manifest"]["model_producer_descriptor_sha256"] = "0" * 64
        with self.assertRaisesRegex(common.EvidenceError, "descriptor hash"):
            normalize_model._verify_models(broken)

    def test_model_evidence_binds_normalized_raw_input(self):
        context = normalize_raw.load_context()
        upstream = normalize_model._raw_upstream(context, b"raw evidence\n")
        self.assertEqual(upstream, [{
            "evidence_id": "normalized-raw-java-apache-corpus",
            "file": "test/conformance/n6/evidence/"
                "raw-java-apache-corpus.normalized.jsonl",
            "sha256": common.sha256_bytes(b"raw evidence\n"),
        }])

    def sample(self):
        file_record = {
            "case_id": "bridge-test",
            "leaf_count": 1,
            "created_by_present": True,
            "created_by": "parquet-mr version 1.10.0",
        }
        column = {
            "case_id": "bridge-test",
            "row_group": 0,
            "leaf": 0,
            "leaf_schema": {
                "physical_type": "INT32",
                "logical_type": "NONE",
                "type_length": None,
                "bit_width": None,
                "is_signed": None,
                "precision": None,
                "time_unit": None,
            },
            "column_order": {"state": "TYPE_ORDER"},
            "num_values": "2",
            "deprecated_min_hex": None,
            "deprecated_max_hex": None,
            "min_value_hex": "ffffffff",
            "max_value_hex": "02000000",
            "is_min_value_exact": True,
            "is_max_value_exact": True,
            "null_count": "0",
            "distinct_count": "2",
            "nan_count": None,
        }
        return file_record, column

    def test_interprets_signed_bounds_with_exact_toolchain_identity(self):
        file_record, column = self.sample()
        toolchain, results = self.run_model(
            [column], {"bridge-test": file_record}, [self.julia_executable()])
        self.assertIn(toolchain, (item["runtime_tree"] for item in
            normalize_model.ALLOWED_JULIA_TOOLCHAINS.values()))
        result = results[("bridge-test", 0, 0)]
        self.assertEqual(result["outcome"], "OK")
        self.assertEqual(result["lower"]["value"], {
            "kind": "SIGNED",
            "value": "-1",
        })
        self.assertEqual(result["upper"]["value"], {
            "kind": "SIGNED",
            "value": "2",
        })

    def test_rejects_any_model_format_error_before_claims(self):
        file_record, column = self.sample()
        malformed = copy.deepcopy(column)
        malformed["min_value_hex"] = "ff"
        _, results = self.run_model(
            [malformed], {"bridge-test": file_record},
            [self.julia_executable()])
        self.assertEqual(
            results[("bridge-test", 0, 0)]["outcome"], "FORMAT_ERROR")
        with self.assertRaises(common.EvidenceError):
            normalize_model.require_model_success(results)

    def test_rejects_unpinned_model_command(self):
        file_record, column = self.sample()
        with self.assertRaisesRegex(common.EvidenceError,
                "absolute and canonical"):
            self.run_model(
                [column], {"bridge-test": file_record}, ["julia"])

    def test_runs_frozen_independent_model_suite(self):
        file_record, column = self.sample()
        toolchain, results = self.run_model(
            [column], {"bridge-test": file_record}, [self.julia_executable()],
            run_suite=True)
        self.assertIn(toolchain,
            (item["runtime_tree"] for item in
                normalize_model.ALLOWED_JULIA_TOOLCHAINS.values()))
        self.assertEqual(results[("bridge-test", 0, 0)]["outcome"], "OK")

    def test_preserves_ieee_and_float16_raw_bits(self):
        file_record, ieee = self.sample()
        file_record["leaf_count"] = 2
        ieee["leaf_schema"]["physical_type"] = "FLOAT"
        ieee["column_order"]["state"] = "IEEE_754_TOTAL_ORDER"
        ieee["min_value_hex"] = "00000080"
        ieee["max_value_hex"] = "00000000"
        ieee["nan_count"] = "0"
        float16 = copy.deepcopy(ieee)
        float16["leaf"] = 1
        float16["leaf_schema"].update({
            "physical_type": "FIXED_LEN_BYTE_ARRAY",
            "logical_type": "FLOAT16",
            "type_length": 2,
        })
        float16["column_order"]["state"] = "TYPE_ORDER"
        float16["min_value_hex"] = "00c0"
        float16["max_value_hex"] = "0040"
        float16["nan_count"] = None
        _, results = self.run_model(
            [ieee, float16], {"bridge-test": file_record},
            [self.julia_executable()])
        ieee_result = results[("bridge-test", 0, 0)]
        self.assertEqual(ieee_result["comparator"], "COMPARATOR_IEEE_FLOAT")
        self.assertEqual(ieee_result["lower"]["value"]["bits_hex"], "80000000")
        self.assertEqual(ieee_result["upper"]["value"]["bits_hex"], "00000000")
        half_result = results[("bridge-test", 0, 1)]
        self.assertEqual(half_result["comparator"], "COMPARATOR_TYPE_FLOAT")
        self.assertEqual(half_result["lower"]["value"]["bits_hex"], "c000")
        self.assertEqual(half_result["upper"]["value"]["bits_hex"], "4000")

    def test_bridge_has_no_production_dependency(self):
        bridge = normalize_model.BRIDGE_PATH.read_text(encoding="utf-8")
        forbidden = (
            "using " + "Parquet",
            "import " + "Parquet",
            "src/" + "statistics.jl",
            "src/" + "write_statistics.jl",
        )
        for token in forbidden:
            self.assertNotIn(token, bridge)


if __name__ == "__main__":
    unittest.main()
