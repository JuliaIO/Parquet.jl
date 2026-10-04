#!/usr/bin/env python3
import argparse
import json
import pathlib
import sys
import tempfile
import tomllib
import types
import unittest

SCRIPT_DIRECTORY = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIRECTORY))
import run as harness
sys.path.pop(0)


EXPECTED_LOGICAL_DIGESTS = {
    "apache-alltypes-dictionary":
        "f2c929bbd05fdfba22f2c0b2099cda5abee2736540c0e3c08c35c2ccfe7faa64",
    "apache-alltypes-plain":
        "7e7fe74a6cbcee312b5d69c37118b1ddee5e012dd0ec2cacc94367f732c611d3",
    "apache-binary":
        "abbae1d98f07cc72a89cc0d6cb3f2c062139148f320091ab34f82116608fa603",
    "apache-bson":
        "eeb323c79a61256a0dc724c3cb527e5f95e11c38c24b66d98a345e977342458a",
    "apache-byte-array-decimal":
        "72d58c34be4872a9511f6aa90e7aa85da7cd5f6b74bf94670c25e5e103f91345",
    "apache-fixed-length-byte-array":
        "b774417b00a0769e81247e81a13ba8ae2235655ff7fa7d19c5c63b0189b29f43",
    "apache-fixed-length-decimal":
        "695d04714cd772edd99e27dadc1c48e21c0ba50c6905641b8d0e26294813bb24",
    "apache-fixed-length-decimal-legacy":
        "ce517a369e588bbacded5d71385f357d5135664b0cf4c664609f1b9a4eaaebfb",
    "apache-int32-decimal":
        "457098e384bc3398752bc6c584730a4f66a8b589f124df2049538509b74f8788",
    "apache-int32-with-null-pages":
        "40ad3a2c665a0cf20c7be47b8468874831baa3294275ae3b5698991cb15af5c7",
    "apache-int64-decimal":
        "995659f0eae712e82d5a40cf15a07743550708c97b6c30630a8415492f80ebab",
    "apache-json":
        "34cad7fad382e26359ad83599fcf1d58d1528fcfb2f6b05f648d1da4ded933a9",
    "apache-rle-boolean-encoding":
        "898e936d35669810ecbc7f2cb85ed6885cc769c07d8157292b93936dddf743fa",
}
EXPECTED_TYPE_ORDER_DIGESTS = {
    "apache-binary":
        "398ef274d55a8ecbc60a45dad6d53767ff64690f02041ad62fbe18d4d8ade9b2",
    "apache-binary-truncated-min-max":
        "386d4867bea929fc5421a102d4ad9f94d279622822fb64ea7ea28f72ec07abae",
    "apache-bson":
        "5064c7fe7c81fcfa0c01a590d864b9fd332c724bd4bddc90a2ca07b8492ace7c",
    "apache-fixed-length-byte-array":
        "a31a068e65e43cac96ee043dfdd836b8d412001c4bea16472579e2dc00e787af",
    "apache-float16-nonzeros-and-nans":
        "495cb01fc4407b043081b8e7815151eb93a56a576cdce8ede97bbb262c5c444b",
    "apache-float16-zeros-and-nans":
        "1199284c5ebe019e4f99695de404506d50d612fce387e040b9bc48d995b69665",
    "apache-int32-with-null-pages":
        "febe5e5ab7300d06cee411673b296e372b115b7834ddf23dd03b2235ec8d275a",
    "apache-json":
        "f92c2356f5af0856938651d50f667dead0b5e510f4137b66ce4a53dc83d89a04",
    "apache-nan-in-stats":
        "3c3ec0157bc53c7e83c4f2c86357037471e104f67d1c1db3024da46d6780b3c7",
    "apache-single-nan":
        "6467555af73803cc0ea6d17fe2a2f03948c40dd48549bdf73eb5da2baec22c2d",
}


def leaf(physical, logical="NONE", **fields):
    schema = {
        "physical_type": physical,
        "logical_type": logical,
        "converted_type": None,
        "type_length": None,
        "precision": None,
        "scale": None,
        "bit_width": None,
        "is_signed": None,
        "time_unit": None,
        "is_adjusted_to_utc": None,
        "crs": None,
        "geography_algorithm": None,
    }
    schema.update(fields)
    return {"leaf_schema": schema, "path": ["value"]}


class ArrowRustHarnessTests(unittest.TestCase):
    def test_timestamp_normalization_is_exact_to_nanoseconds(self):
        self.assertEqual(harness.timestamp_nanoseconds("1970-01-01T00:00:00"),
            "0")
        self.assertEqual(harness.timestamp_nanoseconds(
            "1970-01-01T00:00:00.000000001"), "1")
        self.assertEqual(harness.timestamp_nanoseconds(
            "1969-12-31T23:59:59.999999999"), "-1")
        with self.assertRaises(harness.HarnessError):
            harness.timestamp_nanoseconds("1970-01-01T00:00:00Z")

    def test_canonical_values_preserve_bits_bytes_and_decimals(self):
        decimal_leaf = leaf("INT32", "DECIMAL", converted_type="DECIMAL",
            precision=4, scale=2)
        value = {"data_type": "Decimal128(4, 2)", "display": "-1.25"}
        self.assertEqual(harness.canonical_leaf_value(value, -1.25,
            decimal_leaf, "Decimal128(4, 2)"),
            {"decimal_scale": 2, "unscaled": "-125"})
        float_leaf = leaf("FLOAT")
        self.assertEqual(harness.canonical_leaf_value("0x80000000",
            harness.decimal.Decimal("-0.0"), float_leaf, "Float32"),
            {"float32_bits": "80000000"})
        binary_leaf = leaf("BYTE_ARRAY")
        self.assertEqual(harness.canonical_leaf_value("ignored", "00ff",
            binary_leaf, "Binary"), {"bytes_hex": "00ff"})
        fixed_leaf = leaf("FIXED_LEN_BYTE_ARRAY", type_length=2)
        self.assertEqual(harness.canonical_leaf_value(
            {"data_type": "FixedSizeBinary(2)", "display": "00ff"}, "00ff",
            fixed_leaf, "FixedSizeBinary(2)"), {"bytes_hex": "00ff"})
        with self.assertRaises(harness.HarnessError):
            harness.canonical_leaf_value("ignored", "00", fixed_leaf,
                "FixedSizeBinary(2)")

    def test_scalar_and_int96_values_are_checked(self):
        self.assertIs(harness.canonical_leaf_value(True, True,
            leaf("BOOLEAN"), "Boolean"), True)
        self.assertEqual(harness.canonical_leaf_value(7, 7, leaf("INT32"),
            "Int32"), 7)
        timestamp = {
            "data_type": "Timestamp(Nanosecond, None)",
            "display": "1970-01-01T00:00:00.000000001",
        }
        self.assertEqual(harness.canonical_leaf_value(timestamp,
            timestamp["display"], leaf("INT96"), timestamp["data_type"]),
            {"timestamp_nanoseconds": "1"})
        self.assertIsNone(harness.canonical_leaf_value(None, None,
            leaf("INT32"), "Int32"))

    def test_docker_audit_has_no_network_and_read_only_inputs(self):
        arguments = types.SimpleNamespace(docker="docker")
        descriptor = {
            "image_platform": "linux/amd64",
            "binary_path": "/usr/local/bin/oracle",
            "image_id": "sha256:" + "1" * 64,
            "image_reference": "image:tag",
        }
        command = harness.audit_command(arguments, descriptor,
            pathlib.Path("/private/tmp/corpus"), "data/input.parquet")
        self.assertIn("none", command)
        self.assertEqual(command[command.index("--network") + 1], "none")
        self.assertEqual(command[command.index("--pull") + 1], "never")
        self.assertIn("--read-only", command)
        self.assertIn("no-new-privileges", command)
        self.assertIn(descriptor["image_id"], command)
        self.assertNotIn(descriptor["image_reference"], command)
        mount = command[command.index("--mount") + 1]
        self.assertTrue(mount.endswith(",target=/corpus,readonly"))
        metadata = harness.metadata_command(arguments, descriptor,
            pathlib.Path("/private/tmp/corpus"), "data/input.parquet",
            ".metadata-oracle")
        self.assertIn(descriptor["image_id"], metadata)
        self.assertNotIn(descriptor["image_reference"], metadata)
        self.assertEqual(metadata[metadata.index("--entrypoint") + 1],
            "/corpus/.metadata-oracle")
        with self.assertRaises(harness.HarnessError):
            harness.audit_command(arguments, descriptor,
                pathlib.Path("/private/tmp/bad,corpus"), "data/input.parquet")

    def test_type_order_claim_scope_is_exact(self):
        claims = {(case_id, "read.logical-values"): "planned"
            for case_id in harness.EXPECTED_LOGICAL_CASES}
        claims.update({key: "unsupported"
            for key in harness.EXPECTED_UNSUPPORTED})
        self.assertIsNone(harness.validate_claim_scope(claims))
        claims.update({(case_id, "wire.column-order.type"): "planned"
            for case_id in harness.EXPECTED_TYPE_ORDER_CASES})
        self.assertIsNone(harness.validate_claim_scope(claims))
        del claims[(next(iter(harness.EXPECTED_TYPE_ORDER_CASES)),
            "wire.column-order.type")]
        with self.assertRaises(harness.HarnessError):
            harness.validate_claim_scope(claims)

    def test_type_order_observations_are_normalized(self):
        raw_file = {"case_id": "case", "record": "file"}
        raw_column = {
            "case_id": "case",
            "column_order": {"state": "TYPE_ORDER"},
            "leaf": 0,
            "num_values": "3",
            "path": ["value"],
            "leaf_schema": {"physical_type": "BYTE_ARRAY"},
            "record": "column_statistics",
            "row_group": 0,
        }
        document = {
            "row_groups": [{
                "columns": [{
                    "column_order": "TYPE_ORDER",
                    "has_min_max": True,
                    "max_hex": "ff",
                    "min_hex": "00",
                    "null_count": 1,
                    "num_values": 3,
                    "path": ["value"],
                    "physical_type": "BYTE_ARRAY",
                }],
                "row_group": 0,
                "row_count": 3,
            }],
        }
        raw_columns = {("case", 0, 0): raw_column}
        expected = [{"file": raw_file, "columns": [raw_column]}]
        self.assertEqual(harness.type_order_observations(document, "case",
            raw_file, raw_columns), expected)
        document["row_groups"][0]["columns"][0]["column_order"] = \
            "UNDEFINED"
        with self.assertRaises(harness.HarnessError):
            harness.type_order_observations(document, "case", raw_file,
                raw_columns)

    def test_bounded_process_caps_stdout_and_rejects_failure(self):
        stdout, stderr = harness.run_bounded(
            [sys.executable, "-c", "print('ok')"], 16, 16, 5)
        self.assertEqual(stdout, b"ok\n")
        self.assertEqual(stderr, b"")
        with self.assertRaises(harness.HarnessError):
            harness.run_bounded([sys.executable, "-c",
                "import os; os.write(1, b'x' * 1024)"], 16, 16, 5)
        with self.assertRaises(harness.HarnessError):
            harness.run_bounded([sys.executable, "-c",
                "raise SystemExit(3)"], 16, 1024, 5)

    def test_output_rejects_symbolic_link_components(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory).resolve(strict=True)
            target = root / "target"
            target.mkdir()
            linked = root / "linked"
            linked.symlink_to(target, target_is_directory=True)
            with self.assertRaises(harness.HarnessError):
                harness.checked_output_path(linked / "evidence.jsonl")
            output = target / "evidence.jsonl"
            self.assertEqual(harness.checked_output_path(output), output)
            destination_link = target / "linked.jsonl"
            destination_link.symlink_to(output)
            with self.assertRaises(harness.HarnessError):
                harness.checked_output_path(destination_link)

    def test_input_rejects_symbolic_link_components(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory).resolve(strict=True)
            target = root / "target"
            target.mkdir()
            source = target / "input.parquet"
            source.write_bytes(b"PAR1")
            self.assertEqual(harness.checked_regular_file(source, "input"),
                source)
            linked_file = target / "linked.parquet"
            linked_file.symlink_to(source)
            with self.assertRaises(harness.HarnessError):
                harness.checked_regular_file(linked_file, "input")
            linked_directory = root / "linked"
            linked_directory.symlink_to(target, target_is_directory=True)
            with self.assertRaises(harness.HarnessError):
                harness.checked_regular_file(linked_directory / source.name,
                    "input")
            with self.assertRaises(harness.HarnessError):
                harness.checked_directory(linked_directory, "input root")

    def test_toolchain_descriptor_pins_current_wrapper_sources(self):
        repository = SCRIPT_DIRECTORY.parents[4]
        authority_record = {
            "version": "59.2.0",
            "revision": "782e5a685501a9db6cc8e9a3b7cbff894940c47a",
        }
        with (SCRIPT_DIRECTORY / "toolchain.toml").open("rb") as stream:
            descriptor_input = tomllib.load(stream)
        descriptor = harness.validate_descriptor(repository,
            descriptor_input, authority_record)
        self.assertEqual(descriptor["producer"], "arrow-rs")
        self.assertEqual(len(descriptor["source"]), 12)
        self.assertEqual(len(descriptor["wrapper"]), 8)


def integration_arguments(arguments, output, check):
    repository = pathlib.Path(arguments.repository).resolve(strict=True)
    return types.SimpleNamespace(
        repository=str(repository),
        manifest=str(repository / "test/conformance/n6/manifest.toml"),
        capabilities=str(repository / "test/conformance/n6/capabilities.toml"),
        fixtures=str(repository / "test/conformance/n6/fixtures.toml"),
        descriptor=str(SCRIPT_DIRECTORY / "toolchain.toml"),
        raw_evidence=arguments.raw_evidence,
        corpus_root=arguments.corpus_root,
        output=str(output),
        docker=arguments.docker,
        check=check,
        draft=arguments.draft,
    )


def validate_integration_records(path):
    records = [json.loads(line) for line in path.read_text().splitlines()]
    if len(records) != 45:
        raise AssertionError(f"expected 45 records, got {len(records)}")
    logical = {
        record["case_id"]: record["actual_sha256"]
        for record in records
        if record["record"] == "case_result" and
            record["capability_id"] == "read.logical-values"
    }
    if logical != EXPECTED_LOGICAL_DIGESTS:
        raise AssertionError("Arrow Rust logical digests differ")
    type_order = {
        record["case_id"]: record["actual_sha256"]
        for record in records
        if record["record"] == "case_result" and
            record["capability_id"] == "wire.column-order.type"
    }
    if type_order != EXPECTED_TYPE_ORDER_DIGESTS:
        raise AssertionError("Arrow Rust TYPE_ORDER digests differ")
    unsupported = {
        (record["case_id"], record["capability_id"])
        for record in records
        if record["record"] == "case_result" and
            record["status"] == "UNSUPPORTED"
    }
    if unsupported != harness.EXPECTED_UNSUPPORTED:
        raise AssertionError("Arrow Rust unsupported results differ")
    if len({record["case_id"] for record in records
            if record["record"] == "file"}) != 19:
        raise AssertionError("Arrow Rust file coverage differs")
    return None


def run_integration(arguments):
    repository = pathlib.Path(arguments.repository).resolve(strict=True)
    output = repository / \
        "test/conformance/n6/evidence/arrow-rs.normalized.jsonl"
    harness.generate(integration_arguments(arguments, output, False))
    first = output.read_bytes()
    validate_integration_records(output)
    harness.generate(integration_arguments(arguments, output, True))
    if output.read_bytes() != first:
        raise AssertionError("Arrow Rust check mode changed the output")
    print(f"Arrow Rust integration passed: 45 records, {len(first)} bytes")
    return None


def parser():
    result = argparse.ArgumentParser()
    result.add_argument("--integration", action="store_true")
    result.add_argument("--repository")
    result.add_argument("--corpus-root")
    result.add_argument("--raw-evidence")
    result.add_argument("--docker", default="docker")
    result.add_argument("--draft", action="store_true")
    return result


def main():
    arguments = parser().parse_args()
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(
        ArrowRustHarnessTests)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    if not result.wasSuccessful():
        return 1
    if arguments.integration:
        required = (arguments.repository, arguments.corpus_root,
            arguments.raw_evidence)
        if any(value is None for value in required):
            raise SystemExit(
                "--integration requires --repository, --corpus-root, and --raw-evidence")
        run_integration(arguments)
    return 0


if __name__ == "__main__":
    sys.exit(main())
