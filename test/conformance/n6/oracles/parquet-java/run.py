#!/usr/bin/env python3
import argparse
import decimal
import io
import json
import os
import pathlib
import re
import selectors
import stat
import subprocess
import sys
import tempfile
import time
import zipfile

SCRIPT_DIRECTORY = pathlib.Path(__file__).resolve().parent
HARNESS_DIRECTORY = SCRIPT_DIRECTORY.parents[1] / "harnesses"
sys.path.insert(0, str(HARNESS_DIRECTORY))
import common
sys.path = [entry for entry in sys.path
    if pathlib.Path(entry or ".").resolve() != HARNESS_DIRECTORY]


HarnessError = common.HarnessError
PRODUCER = "parquet-java"
EVIDENCE_ID = "normalized-parquet-java"
POLICY_CASE = "producer-parquet-251"
DESCRIPTOR_RELATIVE = \
    "test/conformance/n6/oracles/parquet-java/toolchain.toml"
JAVA_MAIN = "org.julialang.parquet.n6.java.AuditMain"
JAVA_STDOUT_LIMIT = 8 * 1024 * 1024
JAVA_STDERR_LIMIT = 1024 * 1024
JAVA_TIMEOUT_SECONDS = 30
EXPECTED_RECORD_COUNT = 49
EXPECTED_LOGICAL_CASES = {
    "apache-alltypes-dictionary",
    "apache-alltypes-plain",
    "apache-binary",
    "apache-bson",
    "apache-byte-array-decimal",
    "apache-fixed-length-byte-array",
    "apache-fixed-length-decimal",
    "apache-fixed-length-decimal-legacy",
    "apache-int32-decimal",
    "apache-int32-with-null-pages",
    "apache-int64-decimal",
    "apache-json",
    "apache-rle-boolean-encoding",
}
EXPECTED_TYPE_ORDER_CASES = {
    "apache-binary",
    "apache-binary-truncated-min-max",
    "apache-bson",
    "apache-fixed-length-byte-array",
    "apache-float16-nonzeros-and-nans",
    "apache-float16-zeros-and-nans",
    "apache-int32-with-null-pages",
    "apache-json",
    "apache-nan-in-stats",
    "apache-single-nan",
}
EXPECTED_LEGACY_CASES = {
    "apache-fixed-length-decimal",
    "apache-fixed-length-decimal-legacy",
    "apache-int32-decimal",
    "apache-int64-decimal",
    POLICY_CASE,
}
EXPECTED_UNSUPPORTED = {
    ("apache-floating-orders-nan-count", "wire.statistics.nan-count"),
}
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
EXPECTED_SOURCE_FILES = {
    "parquet-common/src/main/java/org/apache/parquet/VersionParser.java",
    "parquet-common/src/main/java/org/apache/parquet/SemanticVersion.java",
    "parquet-common/src/main/java/org/apache/parquet/io/LocalInputFile.java",
    "parquet-column/src/main/java/org/apache/parquet/CorruptStatistics.java",
    "parquet-column/src/main/java/org/apache/parquet/example/data/Group.java",
    "parquet-hadoop/src/main/java/org/apache/parquet/hadoop/ParquetReader.java",
    "parquet-hadoop/src/main/java/org/apache/parquet/hadoop/ParquetFileReader.java",
    "parquet-hadoop/src/main/java/org/apache/parquet/hadoop/example/GroupReadSupport.java",
    "parquet-hadoop/src/main/java/org/apache/parquet/format/converter/ParquetMetadataConverter.java",
}
EXPECTED_WRAPPERS = {
    "test/conformance/n6/harnesses/common.py",
    "test/conformance/n6/oracles/parquet-java/build.sh",
    "test/conformance/n6/oracles/parquet-java/check.sh",
    "test/conformance/n6/oracles/parquet-java/run.py",
    "test/conformance/n6/oracles/parquet-java/run.sh",
    "test/conformance/n6/oracles/parquet-java/runtests.py",
    "test/conformance/n6/oracles/parquet-java/src/org/julialang/parquet/n6/java/AuditMain.java",
}
EXPECTED_ARTIFACTS = {
    "hadoop-client-api",
    "hadoop-client-runtime",
    "parquet-cli-runtime",
}
HEX_PATTERN = re.compile(r"^[0-9a-f]*$")


def expected_policy_observations():
    observations = []
    for scenario, created_by, physical, relation, status, application, \
            version, build, ignored in EXPECTED_POLICY_FACTS:
        version_parse = {"status": status}
        if status == "parsed":
            version_parse.update({
                "application": application,
                "version": version,
                "build": build,
            })
        observations.append({
            "scenario_id": scenario,
            "created_by": created_by,
            "physical_type": physical,
            "bound_relation": relation,
            "version_parse": version_parse,
            "should_ignore_statistics": ignored,
        })
    return observations


def reject_symlink_components(path, label, allow_missing_leaf=False):
    absolute = pathlib.Path(os.path.abspath(path))
    current = pathlib.Path(absolute.anchor)
    parts = absolute.parts[1:] if absolute.anchor else absolute.parts
    for index, part in enumerate(parts):
        current = current / part
        try:
            metadata = current.lstat()
        except FileNotFoundError:
            if allow_missing_leaf and index == len(parts) - 1:
                return absolute
            raise HarnessError(f"{label} does not exist: {current}")
        if stat.S_ISLNK(metadata.st_mode):
            raise HarnessError(f"{label} contains a symbolic link: {current}")
    return absolute


def checked_directory(path, label):
    path = reject_symlink_components(path, label)
    if not path.is_dir():
        raise HarnessError(f"{label} is not a directory")
    return path.resolve(strict=True)


def checked_regular_file(path, label):
    path = reject_symlink_components(path, label)
    if not path.is_file():
        raise HarnessError(f"{label} is not a regular file")
    return path.resolve(strict=True)


def checked_output_path(path):
    requested = pathlib.Path(os.path.abspath(path))
    checked_directory(requested.parent, "evidence output parent")
    return reject_symlink_components(requested, "evidence output",
        allow_missing_leaf=True)


def canonical_integer(value):
    if value != "0" and (value.startswith("0") or value.startswith("-0")):
        raise HarnessError(f"noncanonical JSON integer: {value}")
    return int(value)


def unique_object(pairs):
    output = {}
    for key, value in pairs:
        if key in output:
            raise HarnessError(f"duplicate JSON object key: {key}")
        output[key] = value
    return output


def invalid_number(value):
    raise HarnessError(f"invalid JSON number: {value}")


def load_oracle_json(value):
    try:
        decoded = value.decode("utf-8")
        result = json.loads(decoded, object_pairs_hook=unique_object,
            parse_constant=invalid_number, parse_float=invalid_number,
            parse_int=canonical_integer)
    except (UnicodeError, json.JSONDecodeError) as error:
        raise HarnessError("Parquet Java returned invalid JSON") from error
    if not isinstance(result, dict):
        raise HarnessError("Parquet Java did not return a JSON object")
    return result


def run_bounded(command, environment, working_directory,
        maximum_stdout=JAVA_STDOUT_LIMIT,
        maximum_stderr=JAVA_STDERR_LIMIT,
        timeout_seconds=JAVA_TIMEOUT_SECONDS):
    process = subprocess.Popen(command, stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=environment,
        cwd=working_directory, close_fds=True)
    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ, "stdout")
    selector.register(process.stderr, selectors.EVENT_READ, "stderr")
    output = {"stdout": bytearray(), "stderr": bytearray()}
    limits = {"stdout": maximum_stdout, "stderr": maximum_stderr}
    deadline = time.monotonic() + timeout_seconds
    try:
        while selector.get_map():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise HarnessError("bounded Java command timed out")
            events = selector.select(remaining)
            if not events:
                raise HarnessError("bounded Java command timed out")
            for key, _ in events:
                chunk = os.read(key.fileobj.fileno(), 65536)
                if not chunk:
                    selector.unregister(key.fileobj)
                    continue
                name = key.data
                output[name].extend(chunk)
                if len(output[name]) > limits[name]:
                    raise HarnessError(
                        f"bounded Java command {name} exceeds its limit")
        status = process.wait(timeout=max(0.1, deadline - time.monotonic()))
    except BaseException:
        process.kill()
        process.wait()
        raise
    finally:
        selector.close()
    if status != 0:
        diagnostic = bytes(output["stderr"]).decode(
            "utf-8", "replace").strip()
        if len(diagnostic) > 1000:
            diagnostic = diagnostic[:1000] + "..."
        raise HarnessError(
            f"bounded Java command failed with status {status}: {diagnostic}")
    return bytes(output["stdout"]), bytes(output["stderr"])


def validate_claim_scope(claims):
    expected = {
        (case_id, "read.logical-values"): "planned"
        for case_id in EXPECTED_LOGICAL_CASES
    }
    expected.update({
        (case_id, "compat.legacy-statistics"): "planned"
        for case_id in EXPECTED_LEGACY_CASES
    })
    expected.update({key: "unsupported" for key in EXPECTED_UNSUPPORTED})
    type_order = {(case_id, "wire.column-order.type")
        for case_id in EXPECTED_TYPE_ORDER_CASES}
    claimed_type_order = {key for key in claims
        if key[1] == "wire.column-order.type"}
    if claimed_type_order and claimed_type_order != type_order:
        raise HarnessError(
            "Parquet Java TYPE_ORDER capability scope differs")
    if claimed_type_order:
        expected.update({key: "planned" for key in type_order})
    if set(claims) != set(expected):
        raise HarnessError("Parquet Java capability claim scope differs")
    for key, expected_status in expected.items():
        status = claims[key]
        if expected_status == "planned" and status not in (
                "planned", "verified"):
            raise HarnessError(f"Parquet Java claim status differs: {key}")
        if expected_status == "unsupported" and status != "unsupported":
            raise HarnessError(f"Parquet Java unsupported status differs: {key}")
    return None


def validate_identity_list(items, expected_paths, key):
    if not isinstance(items, list) or not items:
        raise HarnessError(f"Parquet Java {key} identity list is empty")
    observed = set()
    for item in items:
        if set(item) != {key, "sha256"} or item[key] in observed or \
                re.fullmatch(r"[0-9a-f]{64}", item["sha256"]) is None:
            raise HarnessError(f"Parquet Java {key} identity is invalid")
        observed.add(item[key])
    if observed != expected_paths:
        raise HarnessError(f"Parquet Java {key} coverage differs")
    return None


def validate_descriptor(repository, descriptor, authority_record):
    required = {
        "descriptor_version", "status", "producer", "producer_version",
        "source_revision", "platform", "python_version",
        "python_distribution_url", "python_distribution_sha256",
        "python_tree_policy", "python_executable_sha256",
        "python_tree_sha256", "java_vendor",
        "java_version", "java_executable_sha256", "javac_executable_sha256",
        "java_release_sha256", "jdk_tree_sha256", "hadoop_version",
        "harness_main", "harness_jar_file", "harness_jar_sha256",
        "harness_jar_size", "artifact", "source", "wrapper",
    }
    if set(descriptor) != required or descriptor["descriptor_version"] != 1:
        raise HarnessError("Parquet Java descriptor has invalid keys")
    expected = {
        "producer": PRODUCER,
        "producer_version": authority_record["version"],
        "source_revision": authority_record["revision"],
        "platform": "macos-15-arm64",
        "python_version": "3.12.8",
        "java_vendor": "Eclipse-Adoptium-Temurin",
        "java_version": "21.0.8+9",
        "hadoop_version": "3.3.0",
        "harness_main": JAVA_MAIN,
    }
    for field, value in expected.items():
        if descriptor[field] != value:
            raise HarnessError(f"Parquet Java descriptor has stale {field}")
    if descriptor["status"] not in ("planned", "verified"):
        raise HarnessError("Parquet Java descriptor has invalid status")
    for field in ("python_executable_sha256", "python_tree_sha256",
            "java_executable_sha256", "javac_executable_sha256",
            "java_release_sha256", "jdk_tree_sha256",
            "harness_jar_sha256"):
        if re.fullmatch(r"[0-9a-f]{64}", descriptor[field]) is None:
            raise HarnessError(f"Parquet Java descriptor has invalid {field}")
    if not isinstance(descriptor["harness_jar_size"], int) or \
            isinstance(descriptor["harness_jar_size"], bool) or \
            descriptor["harness_jar_size"] <= 0:
        raise HarnessError("Parquet Java harness jar size is invalid")
    validate_identity_list(descriptor["source"], EXPECTED_SOURCE_FILES, "file")
    validate_identity_list(descriptor["wrapper"], EXPECTED_WRAPPERS, "path")
    for item in descriptor["wrapper"]:
        candidate = common.repository_input(repository, item["path"])
        if common.sha256_file(candidate) != item["sha256"]:
            raise HarnessError(
                f"Parquet Java wrapper digest differs: {item['path']}")
    artifacts = descriptor["artifact"]
    if not isinstance(artifacts, list) or \
            {item.get("id") for item in artifacts} != EXPECTED_ARTIFACTS:
        raise HarnessError("Parquet Java artifact coverage differs")
    for item in artifacts:
        if set(item) != {"id", "file", "url", "sha256", "size"} or \
                re.fullmatch(r"[0-9a-f]{64}", item["sha256"]) is None or \
                not isinstance(item["size"], int) or \
                isinstance(item["size"], bool) or item["size"] <= 0 or \
                not item["url"].startswith(
                    "https://repo.maven.apache.org/maven2/"):
            raise HarnessError("Parquet Java artifact identity is invalid")
        common.repository_input(repository, item["file"])
    common.repository_input(repository, descriptor["harness_jar_file"])
    common.verify_python_runtime(descriptor)
    return descriptor


def validate_java_toolchain(descriptor, java_root, jdk_root):
    if common.sha256_file(jdk_root / "bin/java") != \
            descriptor["java_executable_sha256"]:
        raise HarnessError("Parquet Java runtime executable differs")
    if common.sha256_file(jdk_root / "bin/javac") != \
            descriptor["javac_executable_sha256"]:
        raise HarnessError("Parquet Java compiler executable differs")
    if common.sha256_file(jdk_root / "release") != \
            descriptor["java_release_sha256"]:
        raise HarnessError("Parquet Java JDK release file differs")
    if common.tree_sha256(jdk_root) != descriptor["jdk_tree_sha256"]:
        raise HarnessError("Parquet Java JDK tree differs")
    for item in descriptor["source"]:
        candidate = java_root.joinpath(
            *pathlib.PurePosixPath(item["file"]).parts)
        checked_regular_file(candidate, "Parquet Java authority source")
        if common.sha256_file(candidate) != item["sha256"]:
            raise HarnessError(
                f"Parquet Java authority source differs: {item['file']}")
    return None


def snapshot_input(path, expected_sha256, expected_size, root, name):
    value = common.regular_file_bytes(path, expected_size,
        f"toolchain artifact {name}")
    if len(value) != expected_size or \
            common.sha256_bytes(value) != expected_sha256:
        raise HarnessError(f"Parquet Java artifact identity differs: {name}")
    destination = pathlib.Path(root) / name
    descriptor = os.open(destination,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(value)
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        try:
            os.unlink(destination)
        except FileNotFoundError:
            pass
        raise
    return destination, value


def verify_parquet_manifest(value, descriptor):
    try:
        with zipfile.ZipFile(io.BytesIO(value)) as archive:
            names = archive.namelist()
            if len(names) != len(set(names)):
                raise HarnessError("Parquet CLI jar has duplicate entries")
            manifest = archive.read("META-INF/MANIFEST.MF").decode("utf-8")
    except (KeyError, UnicodeError, zipfile.BadZipFile) as error:
        raise HarnessError("Parquet CLI jar manifest is invalid") from error
    expected = {
        f"Implementation-Version: {descriptor['producer_version']}",
        f"git-SHA-1: {descriptor['source_revision']}",
    }
    lines = set(manifest.replace("\r\n", "\n").splitlines())
    if not expected <= lines:
        raise HarnessError("Parquet CLI jar authority differs")
    return None


def snapshot_toolchain(repository, descriptor, root):
    output = {}
    for item in descriptor["artifact"]:
        source = common.repository_input(repository, item["file"])
        path, value = snapshot_input(source, item["sha256"], item["size"],
            root, pathlib.PurePosixPath(item["file"]).name)
        output[item["id"]] = path
        if item["id"] == "parquet-cli-runtime":
            verify_parquet_manifest(value, descriptor)
    harness_source = common.repository_input(repository,
        descriptor["harness_jar_file"])
    harness, _ = snapshot_input(harness_source,
        descriptor["harness_jar_sha256"], descriptor["harness_jar_size"],
        root, pathlib.PurePosixPath(descriptor["harness_jar_file"]).name)
    output["harness"] = harness
    for path in output.values():
        os.chmod(path, 0o444)
    return output


def java_command(jdk_root, toolchain, action, path=None):
    classpath = os.pathsep.join(str(toolchain[name]) for name in (
        "harness", "parquet-cli-runtime", "hadoop-client-api",
        "hadoop-client-runtime"))
    if any(character in classpath for character in ("\n", "\r")):
        raise HarnessError("Parquet Java classpath is invalid")
    command = [
        str(jdk_root / "bin/java"),
        "-Xms16m",
        "-Xmx256m",
        "-XX:MaxMetaspaceSize=128m",
        "-XX:+ExitOnOutOfMemoryError",
        "-Dfile.encoding=UTF-8",
        "-Duser.language=en",
        "-Duser.country=US",
        "-Duser.timezone=UTC",
        "-cp",
        classpath,
        JAVA_MAIN,
        action,
    ]
    if path is not None:
        command.append(str(path))
    return command


def java_environment(jdk_root):
    return {
        "JAVA_HOME": str(jdk_root),
        "LANG": "C",
        "LC_ALL": "C",
        "TZ": "UTC",
    }


def validate_oracle_identity(document, descriptor, action):
    expected = {
        "name": PRODUCER,
        "version": descriptor["producer_version"],
        "commit": descriptor["source_revision"],
    }
    if document.get("oracle") != expected or document.get("action") != action:
        raise HarnessError("Parquet Java oracle identity differs")
    return None


def java_physical(physical):
    return "BINARY" if physical == "BYTE_ARRAY" else physical


def validate_audit(document, fixture, leaves, raw_columns, descriptor):
    expected_keys = {
        "oracle", "action", "created_by", "schema", "row_count",
        "columns", "row_groups",
    }
    if set(document) != expected_keys:
        raise HarnessError(f"Parquet Java audit keys differ: {fixture['id']}")
    validate_oracle_identity(document, descriptor, "audit")
    schema = document["schema"]
    if not isinstance(schema, list) or len(schema) != len(leaves):
        raise HarnessError(f"Parquet Java schema coverage differs: {fixture['id']}")
    for leaf, field in zip(leaves, schema):
        required = {"name", "physical_type", "maximum_definition_level",
            "maximum_repetition_level", "column_order"}
        if set(field) != required or len(leaf["path"]) != 1 or \
                field["name"] != leaf["path"][0] or \
                field["physical_type"] != \
                java_physical(leaf["leaf_schema"]["physical_type"]) or \
                not isinstance(field["maximum_definition_level"], int) or \
                isinstance(field["maximum_definition_level"], bool) or \
                field["maximum_definition_level"] < 0 or \
                field["maximum_repetition_level"] != 0 or \
                field["column_order"] not in (
                    "TYPE_DEFINED_ORDER", "UNDEFINED"):
            raise HarnessError(f"Parquet Java schema differs: {fixture['id']}")
    expected_rows = sum(int(raw_columns[(fixture["id"], group, 0)][
        "num_values"]) for group in range(fixture["row_group_count"]))
    if document["row_count"] != expected_rows or \
            not isinstance(document["columns"], list) or \
            len(document["columns"]) != len(leaves) or \
            any(not isinstance(column, list) or len(column) != expected_rows
                for column in document["columns"]):
        raise HarnessError(f"Parquet Java row coverage differs: {fixture['id']}")
    validate_audit_statistics(document, fixture, leaves, raw_columns)
    return None


def validate_audit_statistics(document, fixture, leaves, raw_columns):
    groups = document["row_groups"]
    if not isinstance(groups, list) or \
            len(groups) != fixture["row_group_count"]:
        raise HarnessError(
            f"Parquet Java row-group coverage differs: {fixture['id']}")
    for group_index, group in enumerate(groups):
        if set(group) != {"row_group", "row_count", "columns"} or \
                group["row_group"] != group_index or \
                not isinstance(group["columns"], list) or \
                len(group["columns"]) != len(leaves):
            raise HarnessError(
                f"Parquet Java row-group facts differ: {fixture['id']}")
        expected_rows = int(raw_columns[(fixture["id"], group_index, 0)][
            "num_values"])
        if group["row_count"] != expected_rows:
            raise HarnessError(
                f"Parquet Java row-group rows differ: {fixture['id']}")
        for leaf_index, (leaf, column) in enumerate(zip(
                leaves, group["columns"])):
            required = {"path", "physical_type", "statistics_class", "empty",
                "has_non_null_value", "num_nulls_set", "num_nulls",
                "min_hex", "max_hex", "should_ignore_statistics"}
            raw = raw_columns[(fixture["id"], group_index, leaf_index)]
            if set(column) != required or column["path"] != leaf["path"] or \
                    column["physical_type"] != \
                    java_physical(leaf["leaf_schema"]["physical_type"]) or \
                    not isinstance(column["statistics_class"], str) or \
                    not all(isinstance(column[field], bool) for field in (
                        "empty", "has_non_null_value", "num_nulls_set",
                        "should_ignore_statistics")):
                raise HarnessError(
                    f"Parquet Java statistics identity differs: {fixture['id']}")
            for field in ("min_hex", "max_hex"):
                value = column[field]
                if value is not None and (not isinstance(value, str) or
                        len(value) % 2 or HEX_PATTERN.fullmatch(value) is None):
                    raise HarnessError(
                        f"Parquet Java statistics bytes differ: {fixture['id']}")
            if column["num_nulls"] is not None and (
                    not isinstance(column["num_nulls"], int) or
                    isinstance(column["num_nulls"], bool) or
                    column["num_nulls"] < 0):
                raise HarnessError(
                    f"Parquet Java statistics count differs: {fixture['id']}")
            if raw["null_count"] is not None and column["num_nulls_set"] and \
                    column["num_nulls"] != int(raw["null_count"]):
                raise HarnessError(
                    f"Parquet Java null count differs: {fixture['id']}")
    return None


def parse_canonical_integer(value, label):
    if not isinstance(value, str) or re.fullmatch(r"-?(?:0|[1-9][0-9]*)",
            value) is None:
        raise HarnessError(f"Parquet Java {label} is not canonical")
    return int(value)


def canonical_value(value, leaf):
    if value is None:
        return None
    if not isinstance(value, dict) or set(value) != {"kind", "value"}:
        raise HarnessError("Parquet Java tagged value is invalid")
    schema = leaf["leaf_schema"]
    physical = schema["physical_type"]
    logical = schema["logical_type"]
    kind = value["kind"]
    encoded = value["value"]
    if physical == "BOOLEAN":
        if kind != "boolean" or not isinstance(encoded, bool):
            raise HarnessError("Parquet Java BOOLEAN representation differs")
        return encoded
    if physical in ("INT32", "INT64"):
        expected = "int32" if physical == "INT32" else "int64"
        if kind != expected:
            raise HarnessError("Parquet Java integer representation differs")
        integer = parse_canonical_integer(encoded, physical)
        if logical == "DECIMAL":
            return {"decimal_scale": schema["scale"],
                "unscaled": str(integer)}
        return integer
    if physical in ("FLOAT", "DOUBLE"):
        expected = "float32_bits" if physical == "FLOAT" else "float64_bits"
        digits = 8 if physical == "FLOAT" else 16
        if kind != expected or not isinstance(encoded, str) or \
                re.fullmatch(rf"[0-9a-f]{{{digits}}}", encoded) is None:
            raise HarnessError("Parquet Java floating representation differs")
        return {expected: encoded}
    if physical in ("BYTE_ARRAY", "FIXED_LEN_BYTE_ARRAY", "INT96"):
        expected = {"BYTE_ARRAY": "binary", "FIXED_LEN_BYTE_ARRAY": "fixed",
            "INT96": "int96"}[physical]
        if kind != expected or not isinstance(encoded, str) or \
                len(encoded) % 2 or HEX_PATTERN.fullmatch(encoded) is None:
            raise HarnessError("Parquet Java binary representation differs")
        raw = bytes.fromhex(encoded)
        if physical == "FIXED_LEN_BYTE_ARRAY" and \
                len(raw) != schema["type_length"]:
            raise HarnessError("Parquet Java fixed-width value differs")
        if physical == "INT96":
            if len(raw) != 12:
                raise HarnessError("Parquet Java INT96 width differs")
            nanoseconds = int.from_bytes(raw[:8], "little", signed=False)
            if nanoseconds >= 86_400_000_000_000:
                raise HarnessError("Parquet Java INT96 time-of-day differs")
            julian_day = int.from_bytes(raw[8:], "little", signed=False)
            epoch = (julian_day - 2_440_588) * 86_400_000_000_000 + \
                nanoseconds
            return {"timestamp_nanoseconds": str(epoch)}
        if logical == "DECIMAL":
            if not raw:
                raise HarnessError("Parquet Java DECIMAL bytes are empty")
            unscaled = int.from_bytes(raw, "big", signed=True)
            return {"decimal_scale": schema["scale"],
                "unscaled": str(unscaled)}
        if logical in ("JSON", "STRING", "ENUM"):
            try:
                return raw.decode("utf-8", "strict")
            except UnicodeError as error:
                raise HarnessError(
                    "Parquet Java logical string is invalid UTF-8") from error
        return {"bytes_hex": encoded}
    raise HarnessError(
        f"unsupported Parquet Java logical value: {physical}/{logical}")


def logical_observations(document, leaves):
    columns = []
    for leaf, values in zip(leaves, document["columns"]):
        columns.append({
            "logical_type": leaf["leaf_schema"]["logical_type"],
            "path": leaf["path"],
            "physical_type": leaf["leaf_schema"]["physical_type"],
            "values": [canonical_value(value, leaf) for value in values],
        })
    return [{
        "columns": columns,
        "contract": "n6-logical-values-v1",
        "row_count": document["row_count"],
    }]


def type_order_observations(document, case_id, raw_file, raw_columns):
    columns = [record for (current, _, _), record in raw_columns.items()
        if current == case_id]
    columns.sort(key=lambda record: (record["row_group"], record["leaf"]))
    observed_groups = document["row_groups"]
    if len(columns) != sum(len(group["columns"])
            for group in observed_groups):
        raise HarnessError("Parquet Java TYPE_ORDER topology differs")
    for raw in columns:
        group = observed_groups[raw["row_group"]]
        observed = group["columns"][raw["leaf"]]
        schema = document["schema"][raw["leaf"]]
        if schema["column_order"] != "TYPE_DEFINED_ORDER" or \
                raw["column_order"]["state"] != "TYPE_ORDER":
            raise HarnessError(
                "Parquet Java column order is not TYPE_ORDER")
        if observed["path"] != raw["path"] or \
                observed["physical_type"] != \
                java_physical(raw["leaf_schema"]["physical_type"]) or \
                group["row_count"] != int(raw["num_values"]):
            raise HarnessError("Parquet Java TYPE_ORDER facts differ")
    return [{"file": raw_file, "columns": columns}]


def compatibility_observations(document, fixture, leaves, raw_columns):
    columns = []
    expected_classes = {
        "INT32": "org.apache.parquet.column.statistics.IntStatistics",
        "INT64": "org.apache.parquet.column.statistics.LongStatistics",
        "BYTE_ARRAY": "org.apache.parquet.column.statistics.BinaryStatistics",
        "FIXED_LEN_BYTE_ARRAY":
            "org.apache.parquet.column.statistics.BinaryStatistics",
    }
    for group in document["row_groups"]:
        for leaf_index, (leaf, observed) in enumerate(zip(
                leaves, group["columns"])):
            raw = raw_columns[(fixture["id"], group["row_group"], leaf_index)]
            physical = leaf["leaf_schema"]["physical_type"]
            raw_min = raw["deprecated_min_hex"]
            raw_max = raw["deprecated_max_hex"]
            if observed["statistics_class"] != expected_classes[physical] or \
                    observed["empty"] or \
                    observed["has_non_null_value"] or \
                    not observed["num_nulls_set"] or \
                    observed["should_ignore_statistics"] or \
                    observed["min_hex"] is not None or \
                    observed["max_hex"] is not None or \
                    raw_min is None or raw_max is None or \
                    raw_min == raw_max or \
                    observed["num_nulls"] != int(raw["null_count"]):
                raise HarnessError(
                    f"Parquet Java legacy statistics differ: {fixture['id']}")
            columns.append({
                "exposed_has_non_null_value":
                    observed["has_non_null_value"],
                "exposed_max_hex": observed["max_hex"],
                "exposed_min_hex": observed["min_hex"],
                "num_nulls": observed["num_nulls"],
                "path": leaf["path"],
                "physical_type": physical,
                "raw_deprecated_max_hex": raw_max,
                "raw_deprecated_min_hex": raw_min,
                "row_group": group["row_group"],
                "producer_policy_ignored":
                    observed["should_ignore_statistics"],
                "statistics_class": observed["statistics_class"],
            })
    return [{
        "columns": columns,
        "contract": "n6-parquet-java-legacy-statistics-v1",
        "created_by": document["created_by"],
        "row_group_count": len(document["row_groups"]),
    }]


def validate_policy_document(document, descriptor):
    if set(document) != {"oracle", "action", "observations"}:
        raise HarnessError("Parquet Java policy keys differ")
    validate_oracle_identity(document, descriptor, "policy")
    observations = document["observations"]
    expected = expected_policy_observations()
    if not isinstance(observations, list) or len(observations) != len(expected):
        raise HarnessError("Parquet Java policy coverage differs")
    for observation, expected_observation in zip(observations, expected):
        if observation != expected_observation:
            raise HarnessError(
                "Parquet Java policy observation differs: "
                f"{expected_observation['scenario_id']}")
    return observations


def run_java(jdk_root, toolchain, runtime_root, action, path=None):
    command = java_command(jdk_root, toolchain, action, path)
    stdout, _ = run_bounded(command, java_environment(jdk_root), runtime_root)
    return load_oracle_json(stdout)


def generate(arguments):
    requested_repository = checked_directory(arguments.repository,
        "repository root")
    for path, label in ((arguments.manifest, "manifest"),
            (arguments.capabilities, "capabilities"),
            (arguments.fixtures, "fixtures"),
            (arguments.descriptor, "toolchain descriptor"),
            (arguments.raw_evidence, "normalized raw evidence")):
        checked_regular_file(path, label)
    context = common.build_context(arguments, PRODUCER, EVIDENCE_ID,
        draft_evidence=arguments.draft)
    repository = context["repository"]
    if repository != requested_repository:
        raise HarnessError("repository root changed during context construction")
    expected_descriptor = common.repository_input(repository,
        DESCRIPTOR_RELATIVE)
    if pathlib.Path(arguments.descriptor).resolve(strict=True) != \
            expected_descriptor:
        raise HarnessError(
            "descriptor path does not select the Parquet Java pin")
    descriptor = validate_descriptor(repository, context["descriptor"],
        context["authority_record"])
    validate_claim_scope(context["claims"])
    java_root = checked_directory(arguments.java_root,
        "Parquet Java authority root")
    jdk_root = checked_directory(arguments.jdk_root, "Parquet Java JDK root")
    validate_java_toolchain(descriptor, java_root, jdk_root)
    corpus_root = checked_directory(arguments.corpus_root, "fixture root")
    selected_cases = sorted({case_id for case_id, _ in context["claims"]
        if case_id != POLICY_CASE})
    expected_cases = EXPECTED_LOGICAL_CASES | \
        (EXPECTED_LEGACY_CASES - {POLICY_CASE}) | \
        {case_id for case_id, _ in EXPECTED_UNSUPPORTED}
    if any(capability == "wire.column-order.type"
            for _, capability in context["claims"]):
        expected_cases |= EXPECTED_TYPE_ORDER_CASES
    if set(selected_cases) != expected_cases:
        raise HarnessError("Parquet Java fixture coverage differs")
    total_bytes = sum(context["known"][case_id]["size"]
        for case_id in selected_cases)
    if total_bytes > context["limits"]["max_total_bytes"]:
        raise HarnessError("Parquet Java fixtures exceed the evidence limit")
    output = checked_output_path(arguments.output)
    try:
        output.relative_to(repository)
    except ValueError:
        pass
    else:
        declared_output = repository.joinpath(
            *pathlib.PurePosixPath(context["target_entry"]["file"]).parts)
        if output != declared_output:
            raise HarnessError(
                "repository output is not the declared evidence path")
    artifact_inputs = [common.repository_input(repository, item["file"])
        for item in descriptor["artifact"]]
    artifact_inputs.append(common.repository_input(repository,
        descriptor["harness_jar_file"]))
    common.reject_output_alias(output,
        [*context["protected_inputs"], *artifact_inputs],
        [corpus_root, java_root, jdk_root])
    unsupported_cases = {case_id for (case_id, _), status
        in context["claims"].items() if status == "unsupported"}
    records = [common.run_record(EVIDENCE_ID, PRODUCER,
        context["authority_record"], context["descriptor_sha256"],
        repository, context["manifest"], unsupported_cases,
        context["upstream_evidence"],
        input_hashes=context["input_hashes"])]
    snapshots = tempfile.TemporaryDirectory(prefix="parquet-n6-java-inputs-",
        dir=output.parent)
    runtime = tempfile.TemporaryDirectory(prefix="parquet-n6-java-runtime-",
        dir=output.parent)
    snapshot_root = pathlib.Path(snapshots.name)
    runtime_root = pathlib.Path(runtime.name)
    try:
        toolchain = snapshot_toolchain(repository, descriptor, snapshot_root)
        snapshot_paths = {}
        for case_id in selected_cases:
            fixture = context["known"][case_id]
            if case_id not in context["raw_files"]:
                raise HarnessError(f"raw file fact is absent: {case_id}")
            path = common.checked_file(corpus_root, fixture["file"],
                fixture["sha256"], fixture["size"], snapshot_root)
            if path.stat().st_size > context["limits"]["max_file_bytes"]:
                raise HarnessError(
                    f"Parquet Java fixture exceeds its limit: {case_id}")
            snapshot_paths[case_id] = path
        for path in snapshot_paths.values():
            os.chmod(path, 0o444)
        os.chmod(snapshot_root, 0o555)
        for case_id in selected_cases:
            fixture = context["known"][case_id]
            records.append(context["raw_files"][case_id])
            current_claims = sorted((capability, status)
                for (current, capability), status in context["claims"].items()
                if current == case_id)
            supported = any(status != "unsupported"
                for _, status in current_claims)
            document = None
            leaves = common.leaf_records(case_id, context["raw_columns"])
            if supported:
                document = run_java(jdk_root, toolchain, runtime_root,
                    "audit", snapshot_paths[case_id])
                validate_audit(document, fixture, leaves,
                    context["raw_columns"], descriptor)
                if document["created_by"] != \
                        context["raw_files"][case_id]["created_by"]:
                    raise HarnessError(
                        f"Parquet Java created_by differs: {case_id}")
            for capability, status in current_claims:
                if status == "unsupported":
                    records.append(common.case_result(case_id, capability,
                        fixture["digest_contract"], "UNSUPPORTED"))
                elif capability == "read.logical-values":
                    records.append(common.case_result(case_id, capability,
                        fixture["digest_contract"], "PASS",
                        logical_observations(document, leaves)))
                elif capability == "wire.column-order.type":
                    records.append(common.case_result(case_id, capability,
                        fixture["digest_contract"], "PASS",
                        type_order_observations(document, case_id,
                            context["raw_files"][case_id],
                            context["raw_columns"])))
                elif capability == "compat.legacy-statistics":
                    records.append(common.case_result(case_id, capability,
                        fixture["digest_contract"], "PASS",
                        compatibility_observations(document, fixture, leaves,
                            context["raw_columns"])))
                else:
                    raise HarnessError(
                        f"unhandled Parquet Java capability: {capability}")
        policy_document = run_java(jdk_root, toolchain, runtime_root, "policy")
        policy_observations = validate_policy_document(policy_document,
            descriptor)
        policy_case = context["known"][POLICY_CASE]
        expected_policy = policy_case["expected_sha256"][
            "compat.legacy-statistics"]
        records.append(common.case_result(POLICY_CASE,
            "compat.legacy-statistics", policy_case["digest_contract"],
            "PASS", policy_observations, expected_policy))
    finally:
        try:
            os.chmod(snapshot_root, 0o700)
        except FileNotFoundError:
            pass
        snapshots.cleanup()
        runtime.cleanup()
    expected_records = 1 + len(selected_cases) + len(context["claims"])
    if len(records) != expected_records:
        raise HarnessError("Parquet Java normalized record count differs")
    value = common.evidence_bytes(records,
        context["limits"]["max_file_bytes"],
        context["limits"]["max_line_bytes"],
        context["limits"]["max_records_per_input"])
    common.atomic_output(output, value, arguments.check)
    return value


def parser():
    result = argparse.ArgumentParser()
    result.add_argument("--repository", required=True)
    result.add_argument("--manifest", required=True)
    result.add_argument("--capabilities", required=True)
    result.add_argument("--fixtures", required=True)
    result.add_argument("--descriptor", required=True)
    result.add_argument("--raw-evidence", required=True)
    result.add_argument("--corpus-root", required=True)
    result.add_argument("--java-root", required=True)
    result.add_argument("--jdk-root", required=True)
    result.add_argument("--output", required=True)
    result.add_argument("--check", action="store_true")
    result.add_argument("--draft", action="store_true")
    return result


def main():
    try:
        generate(parser().parse_args())
    except HarnessError as error:
        print(f"parquet-java: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
