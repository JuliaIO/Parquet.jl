#!/usr/bin/env python3
import argparse
import json
import pathlib
import shutil
import subprocess
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
EXPECTED_LEGACY_DIGESTS = {
    "apache-fixed-length-decimal":
        "ed5bdd8b71b0b625d793eb846f962ab257e36d1611070b4a33b2a916ec3f9e4d",
    "apache-fixed-length-decimal-legacy":
        "acd3c50a51cf263624be52f9e43beb2ce75939f6a9991a91084ef425df321ecb",
    "apache-int32-decimal":
        "d0ff4a81cfe9b6d19fdb0400828f2749288515bfc1e9a8b02a4b137c998608d5",
    "apache-int64-decimal":
        "c7187495c163ebd92ad33036367bb9d88ffb6211e1340e42f982c3195d0d738d",
}
EXPECTED_POLICY_DIGEST = \
    "cdaa409cc6dada3fada0752543aa6911174cd2523a38f996490e814bc1f58b1a"
EXPECTED_POLICY_FACTS = (
    ("null-binary", None, "BINARY", "not_applicable", "error", None,
        None, None, True),
    ("empty-binary", "", "BINARY", "not_applicable", "error", None,
        None, None, True),
    ("empty-application-binary", " version 1.0.0", "BINARY",
        "not_applicable", "error", None, None, None, True),
    ("unparsable-binary", "garbage!", "BINARY", "not_applicable",
        "error", None, None, None, True),
    ("unrelated-binary", "impala version 1.0.0", "BINARY",
        "not_applicable", "parsed", "impala", "1.0.0", None, False),
    ("missing-semver-binary", "parquet-mr version", "BINARY",
        "not_applicable", "parsed", "parquet-mr", None, None, True),
    ("before-fix-distinct-binary", "parquet-mr version 1.7.9", "BINARY",
        "distinct", "parsed", "parquet-mr", "1.7.9", None, True),
    ("before-fix-equal-binary", "parquet-mr version 1.7.9", "BINARY",
        "equal", "parsed", "parquet-mr", "1.7.9", None, True),
    ("release-candidate-binary", "parquet-mr version 1.8.0-rc1", "BINARY",
        "not_applicable", "parsed", "parquet-mr", "1.8.0-rc1", None,
        True),
    ("fixed-binary", "parquet-mr version 1.8.0", "BINARY",
        "not_applicable", "parsed", "parquet-mr", "1.8.0", None, False),
    ("cdh-before-binary", "parquet-mr version 1.5.0-cdh5.4.9", "BINARY",
        "not_applicable", "parsed", "parquet-mr", "1.5.0-cdh5.4.9",
        None, True),
    ("cdh-start-binary", "parquet-mr version 1.5.0-cdh5.5.0", "BINARY",
        "not_applicable", "parsed", "parquet-mr", "1.5.0-cdh5.5.0",
        None, False),
    ("cdh-end-binary", "parquet-mr version 1.5.0", "BINARY",
        "not_applicable", "parsed", "parquet-mr", "1.5.0", None, True),
    ("before-fix-fixed", "parquet-mr version 1.7.9",
        "FIXED_LEN_BYTE_ARRAY", "not_applicable", "parsed", "parquet-mr",
        "1.7.9", None, True),
    ("fixed-fixed", "parquet-mr version 1.8.0",
        "FIXED_LEN_BYTE_ARRAY", "not_applicable", "parsed", "parquet-mr",
        "1.8.0", None, False),
    ("before-fix-int32", "parquet-mr version 1.7.9", "INT32",
        "not_applicable", "parsed", "parquet-mr", "1.7.9", None, False),
)


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


class ParquetJavaHarnessTests(unittest.TestCase):
    def test_canonical_values_preserve_bits_bytes_and_decimals(self):
        decimal_leaf = leaf("INT32", "DECIMAL", converted_type="DECIMAL",
            precision=4, scale=2)
        self.assertEqual(harness.canonical_value(
            {"kind": "int32", "value": "-125"}, decimal_leaf),
            {"decimal_scale": 2, "unscaled": "-125"})
        binary_decimal = leaf("BYTE_ARRAY", "DECIMAL",
            converted_type="DECIMAL", precision=4, scale=2)
        self.assertEqual(harness.canonical_value(
            {"kind": "binary", "value": "ff83"}, binary_decimal),
            {"decimal_scale": 2, "unscaled": "-125"})
        self.assertEqual(harness.canonical_value(
            {"kind": "float32_bits", "value": "80000000"},
            leaf("FLOAT")), {"float32_bits": "80000000"})
        self.assertEqual(harness.canonical_value(
            {"kind": "binary", "value": "00ff"}, leaf("BYTE_ARRAY")),
            {"bytes_hex": "00ff"})

    def test_int96_conversion_is_exact(self):
        value = {"kind": "int96", "value":
            "01000000000000008c3d2500"}
        self.assertEqual(harness.canonical_value(value, leaf("INT96")),
            {"timestamp_nanoseconds": "1"})
        invalid = {"kind": "int96", "value":
            "00004f91944e00008c3d2500"}
        with self.assertRaises(harness.HarnessError):
            harness.canonical_value(invalid, leaf("INT96"))

    def test_java_command_has_fixed_limits_and_no_build_tool(self):
        toolchain = {
            "harness": pathlib.Path("/inputs/harness.jar"),
            "parquet-cli-runtime": pathlib.Path("/inputs/parquet.jar"),
            "hadoop-client-api": pathlib.Path("/inputs/hadoop-api.jar"),
            "hadoop-client-runtime": pathlib.Path("/inputs/hadoop-runtime.jar"),
        }
        command = harness.java_command(pathlib.Path("/jdk"), toolchain,
            "audit", pathlib.Path("/inputs/file.parquet"))
        self.assertEqual(command[0], "/jdk/bin/java")
        self.assertIn("-Xmx256m", command)
        self.assertIn("-XX:MaxMetaspaceSize=128m", command)
        self.assertNotIn("mvn", " ".join(command).lower())
        self.assertNotIn("http", " ".join(command).lower())
        self.assertEqual(command[-2:], ["audit", "/inputs/file.parquet"])

    def test_java_environment_drops_ambient_injection_variables(self):
        environment = harness.java_environment(pathlib.Path("/jdk"))
        self.assertEqual(set(environment),
            {"JAVA_HOME", "LANG", "LC_ALL", "TZ"})
        self.assertNotIn("CLASSPATH", environment)
        self.assertNotIn("JAVA_TOOL_OPTIONS", environment)

    def test_bounded_process_caps_stdout_and_rejects_failure(self):
        environment = {"PATH": "/usr/bin:/bin"}
        cwd = pathlib.Path.cwd()
        stdout, stderr = harness.run_bounded(
            [sys.executable, "-c", "print('ok')"], environment, cwd,
            16, 16, 5)
        self.assertEqual(stdout, b"ok\n")
        self.assertEqual(stderr, b"")
        with self.assertRaises(harness.HarnessError):
            harness.run_bounded([sys.executable, "-c",
                "import os; os.write(1, b'x' * 1024)"], environment, cwd,
                16, 16, 5)
        with self.assertRaises(harness.HarnessError):
            harness.run_bounded([sys.executable, "-c",
                "raise SystemExit(3)"], environment, cwd, 16, 1024, 5)

    def test_input_and_output_reject_symbolic_links(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory).resolve(strict=True)
            target = root / "target"
            target.mkdir()
            source = target / "input.parquet"
            source.write_bytes(b"PAR1")
            linked = root / "linked"
            linked.symlink_to(target, target_is_directory=True)
            with self.assertRaises(harness.HarnessError):
                harness.checked_regular_file(linked / source.name, "input")
            with self.assertRaises(harness.HarnessError):
                harness.checked_output_path(linked / "output.jsonl")

    def test_claim_scope_is_exact(self):
        claims = {(case_id, "read.logical-values"): "planned"
            for case_id in harness.EXPECTED_LOGICAL_CASES}
        claims.update({(case_id, "compat.legacy-statistics"): "planned"
            for case_id in harness.EXPECTED_LEGACY_CASES})
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
        claims = {(case_id, "read.logical-values"): "planned"
            for case_id in harness.EXPECTED_LOGICAL_CASES}
        claims.update({(case_id, "compat.legacy-statistics"): "planned"
            for case_id in harness.EXPECTED_LEGACY_CASES})
        claims.update({key: "unsupported"
            for key in harness.EXPECTED_UNSUPPORTED})
        claims[("extra", "read.logical-values")] = "planned"
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
            "schema": [{"column_order": "TYPE_DEFINED_ORDER"}],
            "row_groups": [{
                "columns": [{
                    "has_non_null_value": True,
                    "max_hex": "ff",
                    "min_hex": "00",
                    "num_nulls": 1,
                    "path": ["value"],
                    "physical_type": "BINARY",
                }],
                "row_group": 0,
                "row_count": 3,
            }],
        }
        raw_columns = {("case", 0, 0): raw_column}
        expected = [{"file": raw_file, "columns": [raw_column]}]
        self.assertEqual(harness.type_order_observations(document, "case",
            raw_file, raw_columns), expected)
        document["schema"][0]["column_order"] = "UNDEFINED"
        with self.assertRaises(harness.HarnessError):
            harness.type_order_observations(document, "case", raw_file,
                raw_columns)

    def test_build_rejects_unsafe_write_paths_before_inputs(self):
        cases = (
            ("build-link", "build root must be a real directory"),
            ("build-file", "build root must be a real directory"),
            ("artifacts-link", "artifact directory must be a real directory"),
            ("artifacts-file", "artifact directory must be a real directory"),
            ("output-link", "artifact output must be a regular non-link file"),
            ("output-directory",
                "artifact output must be a regular non-link file"),
        )
        for scenario, expected in cases:
            with self.subTest(scenario=scenario), \
                    tempfile.TemporaryDirectory() as directory:
                oracle = pathlib.Path(directory) / "oracle"
                oracle.mkdir()
                script = oracle / "build.sh"
                shutil.copyfile(SCRIPT_DIRECTORY / "build.sh", script)
                target = pathlib.Path(directory) / "target"
                target.mkdir()
                build = oracle / "build"
                if scenario == "build-link":
                    build.symlink_to(target, target_is_directory=True)
                elif scenario == "build-file":
                    build.write_text("not a directory")
                else:
                    build.mkdir()
                    artifacts = build / "artifacts"
                    if scenario == "artifacts-link":
                        artifacts.symlink_to(target, target_is_directory=True)
                    elif scenario == "artifacts-file":
                        artifacts.write_text("not a directory")
                    else:
                        artifacts.mkdir()
                        output = artifacts / \
                            "parquet-java-n6-harness.jar"
                        if scenario == "output-link":
                            output.symlink_to(target / "artifact.jar")
                        else:
                            output.mkdir()
                result = subprocess.run(["/bin/sh", str(script)],
                    check=False, capture_output=True, text=True,
                    env={"PATH": "/usr/bin:/bin"})
                self.assertEqual(result.returncode, 1)
                self.assertIn(expected, result.stderr)

    def test_policy_observations_and_digest_are_frozen(self):
        self.assertEqual(harness.EXPECTED_POLICY_FACTS,
            EXPECTED_POLICY_FACTS)
        observations = harness.expected_policy_observations()
        self.assertEqual(len(observations), 16)
        self.assertEqual(
            harness.common.observation_digest(harness.POLICY_CASE,
                "compat.legacy-statistics", observations),
            EXPECTED_POLICY_DIGEST)

    def test_descriptor_pins_current_wrapper_sources(self):
        repository = SCRIPT_DIRECTORY.parents[4]
        with (SCRIPT_DIRECTORY / "toolchain.toml").open("rb") as stream:
            descriptor = tomllib.load(stream)
        authority = {
            "version": "1.17.1",
            "revision": "78a8d3230eb4769db93de5f2f2e18363c04cae81",
        }
        validated = harness.validate_descriptor(repository, descriptor,
            authority)
        self.assertEqual(validated["producer"], "parquet-java")
        self.assertEqual(len(validated["source"]), 9)
        self.assertEqual(len(validated["wrapper"]), 7)


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
        java_root=arguments.java_root,
        jdk_root=arguments.jdk_root,
        output=str(output),
        check=check,
        draft=arguments.draft,
    )


def validate_integration_records(path):
    records = [json.loads(line) for line in path.read_text().splitlines()]
    if len(records) != harness.EXPECTED_RECORD_COUNT:
        raise AssertionError(
            f"expected {harness.EXPECTED_RECORD_COUNT} records, got {len(records)}")
    results = {(record["case_id"], record["capability_id"]): record
        for record in records if record["record"] == "case_result"}
    logical = {case_id: results[(case_id,
        "read.logical-values")]["actual_sha256"]
        for case_id in harness.EXPECTED_LOGICAL_CASES}
    if logical != EXPECTED_LOGICAL_DIGESTS:
        raise AssertionError("Parquet Java logical digests differ")
    type_order = {case_id: results[(case_id,
        "wire.column-order.type")]["actual_sha256"]
        for case_id in harness.EXPECTED_TYPE_ORDER_CASES}
    if type_order != EXPECTED_TYPE_ORDER_DIGESTS:
        raise AssertionError("Parquet Java TYPE_ORDER digests differ")
    legacy = {case_id: results[(case_id,
        "compat.legacy-statistics")]["actual_sha256"]
        for case_id in EXPECTED_LEGACY_DIGESTS}
    if legacy != EXPECTED_LEGACY_DIGESTS:
        raise AssertionError("Parquet Java legacy digests differ")
    policy = results[(harness.POLICY_CASE,
        "compat.legacy-statistics")]["actual_sha256"]
    if policy != EXPECTED_POLICY_DIGEST:
        raise AssertionError("Parquet Java policy digest differs")
    unsupported = {key for key, record in results.items()
        if record["status"] == "UNSUPPORTED"}
    if unsupported != harness.EXPECTED_UNSUPPORTED:
        raise AssertionError("Parquet Java unsupported results differ")
    if len({record["case_id"] for record in records
            if record["record"] == "file"}) != 19:
        raise AssertionError("Parquet Java file coverage differs")
    if sum(record["status"] == "PASS" for record in results.values()) != 28:
        raise AssertionError("Parquet Java PASS count differs")
    return None


def run_integration(arguments):
    repository = pathlib.Path(arguments.repository).resolve(strict=True)
    output = repository / \
        "test/conformance/n6/evidence/parquet-java.normalized.jsonl"
    harness.generate(integration_arguments(arguments, output, False))
    first = output.read_bytes()
    validate_integration_records(output)
    harness.generate(integration_arguments(arguments, output, True))
    if output.read_bytes() != first:
        raise AssertionError("Parquet Java check mode changed the output")
    print(f"Parquet Java integration passed: "
        f"{harness.EXPECTED_RECORD_COUNT} records, {len(first)} bytes")
    return None


def parser():
    result = argparse.ArgumentParser()
    result.add_argument("--integration", action="store_true")
    result.add_argument("--repository")
    result.add_argument("--corpus-root")
    result.add_argument("--raw-evidence")
    result.add_argument("--java-root")
    result.add_argument("--jdk-root")
    result.add_argument("--draft", action="store_true")
    return result


def main():
    arguments = parser().parse_args()
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(
        ParquetJavaHarnessTests)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    if not result.wasSuccessful():
        return 1
    if arguments.integration:
        required = (arguments.repository, arguments.corpus_root,
            arguments.raw_evidence, arguments.java_root, arguments.jdk_root)
        if any(value is None for value in required):
            raise SystemExit("--integration requires --repository, "
                "--corpus-root, --raw-evidence, --java-root, and --jdk-root")
        run_integration(arguments)
    return 0


if __name__ == "__main__":
    sys.exit(main())
