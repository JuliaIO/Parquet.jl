#!/usr/bin/env python3
import argparse
import datetime
import decimal
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

SCRIPT_DIRECTORY = pathlib.Path(__file__).resolve().parent
HARNESS_DIRECTORY = SCRIPT_DIRECTORY.parents[1] / "harnesses"
sys.path.insert(0, str(HARNESS_DIRECTORY))
from common import (HarnessError, atomic_output, build_context, case_result,
    checked_file, decimal_unscaled, evidence_bytes, leaf_records,
    regular_file_bytes, repository_input, reject_output_alias, run_record,
    sha256_bytes, sha256_file, verify_python_runtime)
sys.path = [entry for entry in sys.path
    if pathlib.Path(entry or ".").resolve() != HARNESS_DIRECTORY]


PRODUCER = "arrow-rs"
EVIDENCE_ID = "normalized-arrow-rs"
DESCRIPTOR_RELATIVE = "test/conformance/n6/oracles/arrow-rs/toolchain.toml"
IMAGE_STDOUT_LIMIT = 8 * 1024 * 1024
IMAGE_STDERR_LIMIT = 1024 * 1024
IMAGE_TIMEOUT_SECONDS = 45
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
EXPECTED_UNSUPPORTED = {
    ("apache-floating-orders-nan-count", "wire.column-order.ieee"),
    ("apache-floating-orders-nan-count", "wire.statistics.nan-count"),
}
EXPECTED_IMAGE_SOURCES = {
    "/opt/bootstrap/arrow-rs/.gitignore",
    "/opt/bootstrap/arrow-rs/Cargo.lock",
    "/opt/bootstrap/arrow-rs/Cargo.toml",
    "/opt/bootstrap/arrow-rs/README.md",
    "/opt/bootstrap/arrow-rs/UPSTREAM.toml",
    "/opt/bootstrap/arrow-rs/rust-toolchain.toml",
    "/opt/bootstrap/arrow-rs/scripts/build.sh",
    "/opt/bootstrap/arrow-rs/scripts/run.sh",
    "/opt/bootstrap/arrow-rs/scripts/test.sh",
    "/opt/bootstrap/arrow-rs/src/cases.rs",
    "/opt/bootstrap/arrow-rs/src/evidence.rs",
    "/opt/bootstrap/arrow-rs/src/main.rs",
}
EXPECTED_WRAPPERS = {
    "test/conformance/n6/harnesses/common.py",
    "test/conformance/n6/oracles/arrow-rs/build.sh",
    "test/conformance/n6/oracles/arrow-rs/check.sh",
    "test/conformance/n6/oracles/arrow-rs/metadata/Cargo.toml",
    "test/conformance/n6/oracles/arrow-rs/metadata/src/main.rs",
    "test/conformance/n6/oracles/arrow-rs/run.py",
    "test/conformance/n6/oracles/arrow-rs/run.sh",
    "test/conformance/n6/oracles/arrow-rs/runtests.py",
}
TIMESTAMP_PATTERN = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})"
    r"(?:\.(\d{1,9}))?$")
HEX_PATTERN = re.compile(r"^[0-9a-f]*$")


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
        raise HarnessError("Arrow Rust audit returned invalid JSON") from error
    if not isinstance(result, dict):
        raise HarnessError("Arrow Rust audit did not return a JSON object")
    return result


def load_arrow_json_row(value):
    try:
        result = json.loads(value, object_pairs_hook=unique_object,
            parse_constant=invalid_number, parse_float=decimal.Decimal,
            parse_int=canonical_integer)
    except (UnicodeError, json.JSONDecodeError, decimal.InvalidOperation) as error:
        raise HarnessError("Arrow Rust JSON row is invalid") from error
    if not isinstance(result, dict):
        raise HarnessError("Arrow Rust JSON row is not an object")
    return result


def run_bounded(command, maximum_stdout=IMAGE_STDOUT_LIMIT,
        maximum_stderr=IMAGE_STDERR_LIMIT,
        timeout_seconds=IMAGE_TIMEOUT_SECONDS):
    process = subprocess.Popen(command, stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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
                raise HarnessError("bounded command timed out")
            events = selector.select(remaining)
            if not events:
                raise HarnessError("bounded command timed out")
            for key, _ in events:
                chunk = os.read(key.fileobj.fileno(), 65536)
                if not chunk:
                    selector.unregister(key.fileobj)
                    continue
                name = key.data
                output[name].extend(chunk)
                if len(output[name]) > limits[name]:
                    raise HarnessError(f"bounded command {name} exceeds its limit")
        status = process.wait(timeout=max(0.1, deadline - time.monotonic()))
    except BaseException:
        process.kill()
        process.wait()
        raise
    finally:
        selector.close()
    if status != 0:
        diagnostic = bytes(output["stderr"]).decode("utf-8", "replace").strip()
        if len(diagnostic) > 1000:
            diagnostic = diagnostic[:1000] + "..."
        raise HarnessError(
            f"bounded command failed with status {status}: {diagnostic}")
    return bytes(output["stdout"]), bytes(output["stderr"])


def docker_base(arguments, descriptor):
    return [
        arguments.docker,
        "run",
        "--rm",
        "--pull",
        "never",
        "--network",
        "none",
        "--platform",
        descriptor["image_platform"],
        "--read-only",
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges",
        "--pids-limit",
        "64",
        "--memory",
        "512m",
        "--cpus",
        "1",
        "--user",
        "65534:65534",
        "--tmpfs",
        "/tmp:rw,nosuid,nodev,noexec,size=16m",
    ]


def docker_image_identity(arguments, descriptor):
    command = [arguments.docker, "image", "inspect", "--format",
        "{{.Id}}|{{.Architecture}}|{{.Os}}", descriptor["image_reference"]]
    stdout, _ = run_bounded(command, 4096, IMAGE_STDERR_LIMIT, 15)
    expected = (f"{descriptor['image_id']}|amd64|linux\n").encode("ascii")
    if stdout != expected:
        raise HarnessError("local Arrow Rust image identity differs")
    return None


def verify_image_files(arguments, descriptor):
    expected = {descriptor["binary_path"]: descriptor["binary_sha256"]}
    for item in descriptor["source"]:
        expected[item["path"]] = item["sha256"]
    command = docker_base(arguments, descriptor)
    command.extend(["--entrypoint", "/usr/bin/sha256sum",
        descriptor["image_id"]])
    command.extend(sorted(expected))
    stdout, _ = run_bounded(command, 65536)
    observed = {}
    try:
        for line in stdout.decode("ascii").splitlines():
            digest, path = line.split("  ", 1)
            if path in observed:
                raise HarnessError(f"duplicate image identity path: {path}")
            observed[path] = digest
    except (UnicodeError, ValueError) as error:
        raise HarnessError("image identity output is malformed") from error
    if observed != expected:
        raise HarnessError("Arrow Rust image file identities differ")
    return None


def validate_descriptor(repository, descriptor, authority_record):
    required = {
        "descriptor_version", "status", "producer", "producer_version",
        "source_revision", "image_reference", "image_id", "image_platform",
        "binary_path", "binary_sha256", "platform", "python_version",
        "python_distribution_url", "python_distribution_sha256",
        "python_tree_policy", "python_executable_sha256",
        "python_tree_sha256", "rust_toolchain",
        "rustc", "cargo", "metadata_binary_file",
        "metadata_binary_sha256", "metadata_binary_size", "source", "wrapper",
    }
    if set(descriptor) != required or descriptor["descriptor_version"] != 1:
        raise HarnessError("Arrow Rust toolchain descriptor has invalid keys")
    expected = {
        "producer": PRODUCER,
        "producer_version": authority_record["version"],
        "source_revision": authority_record["revision"],
        "image_platform": "linux/amd64",
        "platform": "macos-15-arm64",
        "python_version": "3.12.8",
        "rust_toolchain": "1.96.1",
        "rustc": "rustc 1.96.1 (31fca3adb 2026-06-26)",
        "cargo": "cargo 1.96.1 (356927216 2026-06-26)",
    }
    for field, value in expected.items():
        if descriptor[field] != value:
            raise HarnessError(f"Arrow Rust descriptor has stale {field}")
    if descriptor["status"] not in ("planned", "verified"):
        raise HarnessError("Arrow Rust descriptor has invalid status")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", descriptor["image_id"]):
        raise HarnessError("Arrow Rust descriptor image ID is invalid")
    if not re.fullmatch(r"[0-9a-f]{64}", descriptor["binary_sha256"]):
        raise HarnessError("Arrow Rust descriptor binary digest is invalid")
    if not re.fullmatch(r"[0-9a-f]{64}",
            descriptor["metadata_binary_sha256"]):
        raise HarnessError(
            "Arrow Rust descriptor metadata binary digest is invalid")
    if not isinstance(descriptor["metadata_binary_size"], int) or \
            isinstance(descriptor["metadata_binary_size"], bool) or \
            descriptor["metadata_binary_size"] <= 0:
        raise HarnessError(
            "Arrow Rust descriptor metadata binary size is invalid")
    if not re.fullmatch(r"[0-9a-f]{64}",
            descriptor["python_executable_sha256"]):
        raise HarnessError("Arrow Rust descriptor Python digest is invalid")
    if not re.fullmatch(r"[0-9a-f]{64}", descriptor["python_tree_sha256"]):
        raise HarnessError("Arrow Rust descriptor Python tree digest is invalid")
    if not descriptor["binary_path"].startswith("/"):
        raise HarnessError("Arrow Rust descriptor binary path is not absolute")
    paths = set()
    for group in (descriptor["source"], descriptor["wrapper"]):
        if not isinstance(group, list) or not group:
            raise HarnessError("Arrow Rust descriptor identity list is empty")
        for item in group:
            if set(item) != {"path", "sha256"} or item["path"] in paths:
                raise HarnessError("Arrow Rust descriptor identity is invalid")
            if not re.fullmatch(r"[0-9a-f]{64}", item["sha256"]):
                raise HarnessError("Arrow Rust descriptor digest is invalid")
            paths.add(item["path"])
    for item in descriptor["source"]:
        if not item["path"].startswith("/opt/bootstrap/arrow-rs/"):
            raise HarnessError("Arrow Rust source path is outside the image pin")
    if {item["path"] for item in descriptor["source"]} != \
            EXPECTED_IMAGE_SOURCES:
        raise HarnessError("Arrow Rust image source coverage differs")
    if {item["path"] for item in descriptor["wrapper"]} != EXPECTED_WRAPPERS:
        raise HarnessError("Arrow Rust wrapper coverage differs")
    for item in descriptor["wrapper"]:
        candidate = repository_input(repository, item["path"])
        if sha256_file(candidate) != item["sha256"]:
            raise HarnessError(f"Arrow Rust wrapper digest differs: {item['path']}")
    metadata_binary = repository_input(repository,
        descriptor["metadata_binary_file"])
    if metadata_binary.stat().st_size != descriptor["metadata_binary_size"] or \
            sha256_file(metadata_binary) != \
            descriptor["metadata_binary_sha256"]:
        raise HarnessError("Arrow Rust metadata binary identity differs")
    verify_python_runtime(descriptor)
    return descriptor


def audit_command(arguments, descriptor, corpus_root, relative):
    if any(character in str(corpus_root) for character in (",", ":", "\n", "\r")):
        raise HarnessError("corpus root cannot be encoded as a Docker mount")
    command = docker_base(arguments, descriptor)
    command.extend([
        "--mount",
        f"type=bind,source={corpus_root},target=/corpus,readonly",
        "--entrypoint",
        descriptor["binary_path"],
        descriptor["image_id"],
        "audit",
        "--input",
        "/corpus/" + relative,
    ])
    return command


def metadata_command(arguments, descriptor, corpus_root, relative,
        binary_name):
    if any(character in str(corpus_root)
            for character in (",", ":", "\n", "\r")):
        raise HarnessError("corpus root cannot be encoded as a Docker mount")
    if "/" in binary_name or binary_name in ("", ".", ".."):
        raise HarnessError("Arrow Rust metadata binary name is invalid")
    command = docker_base(arguments, descriptor)
    command.extend([
        "--mount",
        f"type=bind,source={corpus_root},target=/corpus,readonly",
        "--entrypoint",
        "/corpus/" + binary_name,
        descriptor["image_id"],
        "type-order",
        "--input",
        "/corpus/" + relative,
    ])
    return command


def snapshot_metadata_binary(repository, descriptor, root):
    source = repository_input(repository, descriptor["metadata_binary_file"])
    value = regular_file_bytes(source, descriptor["metadata_binary_size"],
        "Arrow Rust metadata binary")
    if len(value) != descriptor["metadata_binary_size"] or \
            sha256_bytes(value) != descriptor["metadata_binary_sha256"]:
        raise HarnessError("Arrow Rust metadata binary identity differs")
    destination = pathlib.Path(root) / ".arrow-rs-metadata"
    output = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o500)
    try:
        with os.fdopen(output, "wb") as stream:
            stream.write(value)
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        try:
            os.unlink(destination)
        except FileNotFoundError:
            pass
        raise
    return destination


def validate_tool_evidence(oracle, descriptor):
    expected = {
        "name": PRODUCER,
        "version": descriptor["producer_version"],
        "commit": descriptor["source_revision"],
        "rust_toolchain": descriptor["rust_toolchain"],
        "rustc": descriptor["rustc"],
        "cargo": descriptor["cargo"],
    }
    if oracle != expected:
        raise HarnessError("Arrow Rust audit tool identity differs")
    return None


def expected_arrow_type(leaf):
    schema = leaf["leaf_schema"]
    physical = schema["physical_type"]
    logical = schema["logical_type"]
    if logical == "DECIMAL":
        return f"Decimal128({schema['precision']}, {schema['scale']})"
    if logical == "JSON":
        return "Utf8"
    if logical == "FLOAT16":
        return "Float16"
    if physical == "BOOLEAN":
        return "Boolean"
    if physical == "INT32":
        return "Int32"
    if physical == "INT64":
        return "Int64"
    if physical == "FLOAT":
        return "Float32"
    if physical == "DOUBLE":
        return "Float64"
    if physical == "INT96":
        return "Timestamp(Nanosecond, None)"
    if physical == "BYTE_ARRAY":
        return "Binary"
    if physical == "FIXED_LEN_BYTE_ARRAY":
        return f"FixedSizeBinary({schema['type_length']})"
    raise HarnessError(f"unsupported Arrow Rust physical type: {physical}")


def validate_audit_columns(evidence, leaves, fixture, raw_columns):
    columns = evidence.get("columns")
    if not isinstance(columns, list) or len(columns) != len(leaves):
        raise HarnessError(f"Arrow Rust leaf count differs: {fixture['id']}")
    row_group_rows = None
    for leaf_ordinal, (leaf, column) in enumerate(zip(leaves, columns)):
        required_column = {
            "path", "physical_type", "maximum_definition_level",
            "maximum_repetition_level", "row_groups",
        }
        maximum_definition = column.get("maximum_definition_level")
        if set(column) != required_column or \
                not isinstance(maximum_definition, int) or \
                isinstance(maximum_definition, bool) or maximum_definition < 0 or \
                column.get("path") != leaf["path"] or \
                column.get("physical_type") != \
                leaf["leaf_schema"]["physical_type"]:
            raise HarnessError(f"Arrow Rust leaf identity differs: {fixture['id']}")
        if column.get("maximum_repetition_level") != 0:
            raise HarnessError(f"Arrow Rust claimed fixture is nested: {fixture['id']}")
        groups = column.get("row_groups")
        if not isinstance(groups, list) or \
                len(groups) != fixture["row_group_count"]:
            raise HarnessError(f"Arrow Rust row-group coverage differs: {fixture['id']}")
        current_rows = []
        for ordinal, group in enumerate(groups):
            rows = group.get("rows")
            required_group = {
                "row_group", "rows", "compression", "repetition",
                "definition", "dense_values", "pages",
            }
            raw = raw_columns.get((fixture["id"], ordinal, leaf_ordinal))
            if set(group) != required_group or raw is None or \
                    rows != int(raw["num_values"]) or \
                    group.get("row_group") != ordinal or \
                    not isinstance(rows, int) or isinstance(rows, bool) or rows < 0:
                raise HarnessError(f"Arrow Rust row-group identity differs: {fixture['id']}")
            repetition = group.get("repetition")
            definition = group.get("definition")
            dense = group.get("dense_values")
            if not all(isinstance(value, list)
                    for value in (repetition, definition, dense)) or \
                    len(repetition) != rows or len(definition) != rows or \
                    any(value != 0 for value in repetition) or \
                    any(not isinstance(value, int) or isinstance(value, bool) or
                        not 0 <= value <= maximum_definition
                        for value in definition) or \
                    len(dense) != sum(value == maximum_definition
                        for value in definition) or \
                    not isinstance(group.get("compression"), str) or \
                    not isinstance(group.get("pages"), list):
                raise HarnessError(f"Arrow Rust level evidence differs: {fixture['id']}")
            current_rows.append(rows)
        if row_group_rows is None:
            row_group_rows = current_rows
        elif row_group_rows != current_rows:
            raise HarnessError(f"Arrow Rust leaf row counts differ: {fixture['id']}")
    if evidence.get("rows") != sum(row_group_rows or []):
        raise HarnessError(f"Arrow Rust total row count differs: {fixture['id']}")
    return None


def validated_arrow_rows(evidence, leaves, case_id):
    arrow = evidence["arrow"]
    names = [leaf["path"][0] for leaf in leaves]
    if any(len(leaf["path"]) != 1 for leaf in leaves) or \
            len(set(names)) != len(names):
        raise HarnessError(f"Arrow Rust logical fixture is not flat: {case_id}")
    rows = []
    for canonical_row, encoded_row in zip(arrow["canonical_rows"],
            arrow["json_rows"]):
        if not isinstance(canonical_row, list) or \
                len(canonical_row) != len(leaves) or \
                not isinstance(encoded_row, str):
            raise HarnessError(f"Arrow Rust canonical row differs: {case_id}")
        row = load_arrow_json_row(encoded_row)
        if list(row) != names:
            raise HarnessError(f"Arrow Rust JSON field order differs: {case_id}")
        for index, entry in enumerate(canonical_row):
            if not isinstance(entry, dict) or set(entry) != {"field", "value"} or \
                    entry["field"] != names[index]:
                raise HarnessError(f"Arrow Rust canonical field differs: {case_id}")
        rows.append((canonical_row, row))
    return names, rows


def validate_audit(document, fixture, raw_columns, descriptor, audit_name):
    expected_root = {
        "evidence_version", "oracle", "action", "file_count",
        "supported_count", "unsupported_count", "files",
    }
    if set(document) != expected_root or document["evidence_version"] != 1 or \
            document["action"] != "audit" or document["file_count"] != 1 or \
            document["supported_count"] != 1 or \
            document["unsupported_count"] != 0:
        raise HarnessError(f"Arrow Rust audit envelope differs: {fixture['id']}")
    validate_tool_evidence(document["oracle"], descriptor)
    if not isinstance(document["files"], list) or len(document["files"]) != 1:
        raise HarnessError(f"Arrow Rust audit file coverage differs: {fixture['id']}")
    result = document["files"][0]
    if set(result) != {"status", "file", "evidence", "error"} or \
            result["status"] != "supported" or result["error"] is not None:
        raise HarnessError(f"Arrow Rust audit rejected fixture: {fixture['id']}")
    if result["file"] != audit_name or not isinstance(result["evidence"], dict):
        raise HarnessError(f"Arrow Rust audit filename differs: {fixture['id']}")
    evidence = result["evidence"]
    required = {
        "case_id", "file_name", "sha256", "file_bytes", "rows",
        "row_groups", "physical_schema", "columns", "arrow",
    }
    if set(evidence) != required or evidence["case_id"] != audit_name or \
            evidence["file_name"] != audit_name or \
            evidence["sha256"] != fixture["sha256"] or \
            evidence["file_bytes"] != fixture["size"] or \
            evidence["row_groups"] != fixture["row_group_count"] or \
            not isinstance(evidence["physical_schema"], str):
        raise HarnessError(f"Arrow Rust file facts differ: {fixture['id']}")
    leaves = leaf_records(fixture["id"], raw_columns)
    validate_audit_columns(evidence, leaves, fixture, raw_columns)
    arrow = evidence["arrow"]
    expected_arrow = {
        "status", "schema", "canonical_rows", "json_rows",
        "ordered_map_rows", "diagnostic",
    }
    if not isinstance(arrow, dict) or set(arrow) != expected_arrow or \
            arrow["status"] != "ok" or arrow["ordered_map_rows"] is not None or \
            arrow["diagnostic"] is not None:
        raise HarnessError(f"Arrow Rust high-level read differs: {fixture['id']}")
    rows = evidence["rows"]
    if not isinstance(rows, int) or isinstance(rows, bool) or rows < 0 or \
            not isinstance(arrow["canonical_rows"], list) or \
            not isinstance(arrow["json_rows"], list) or \
            len(arrow["canonical_rows"]) != rows or \
            len(arrow["json_rows"]) != rows:
        raise HarnessError(f"Arrow Rust row evidence differs: {fixture['id']}")
    schema = arrow["schema"]
    if not isinstance(schema, list) or len(schema) != len(leaves):
        raise HarnessError(f"Arrow Rust schema coverage differs: {fixture['id']}")
    for leaf, field, column in zip(leaves, schema, evidence["columns"]):
        if set(field) != {"name", "nullable", "data_type", "metadata"} or \
                field["name"] != leaf["path"][0] or \
                field["data_type"] != expected_arrow_type(leaf) or \
                field["nullable"] != \
                (column["maximum_definition_level"] > 0) or \
                not isinstance(field["metadata"], dict) or \
                any(not isinstance(key, str) or not isinstance(value, str)
                    for key, value in field["metadata"].items()):
            raise HarnessError(f"Arrow Rust Arrow schema differs: {fixture['id']}")
    validated_arrow_rows(evidence, leaves, fixture["id"])
    return evidence, leaves


def validate_metadata(document, fixture, descriptor, audit_name):
    expected = {
        "oracle", "action", "file_name", "file_bytes", "row_group_count",
        "leaf_count", "row_groups",
    }
    if not isinstance(document, dict) or set(document) != expected or \
            document["action"] != "type-order" or \
            document["file_name"] != audit_name or \
            document["file_bytes"] != fixture["size"] or \
            document["row_group_count"] != fixture["row_group_count"] or \
            document["leaf_count"] != fixture["leaf_count"]:
        raise HarnessError(
            f"Arrow Rust metadata file facts differ: {fixture['id']}")
    validate_tool_evidence(document["oracle"], descriptor)
    groups = document["row_groups"]
    if not isinstance(groups, list) or \
            len(groups) != fixture["row_group_count"]:
        raise HarnessError(
            f"Arrow Rust metadata row-group coverage differs: {fixture['id']}")
    for row_group, group in enumerate(groups):
        if not isinstance(group, dict) or set(group) != {
                "row_group", "row_count", "columns"} or \
                group["row_group"] != row_group or \
                not isinstance(group["row_count"], int) or \
                isinstance(group["row_count"], bool) or \
                group["row_count"] < 0 or \
                not isinstance(group["columns"], list) or \
                len(group["columns"]) != fixture["leaf_count"]:
            raise HarnessError(
                f"Arrow Rust metadata row group differs: {fixture['id']}")
        for column in group["columns"]:
            if not isinstance(column, dict) or set(column) != {
                    "path", "physical_type", "column_order", "num_values"} or \
                    not isinstance(column["path"], list) or \
                    not column["path"] or \
                    any(not isinstance(part, str) or not part
                        for part in column["path"]) or \
                    not isinstance(column["physical_type"], str) or \
                    column["column_order"] not in (
                        "TYPE_ORDER", "UNDEFINED", "UNKNOWN") or \
                    not isinstance(column["num_values"], int) or \
                    isinstance(column["num_values"], bool) or \
                    column["num_values"] < 0:
                raise HarnessError(
                    f"Arrow Rust metadata column differs: {fixture['id']}")
    return document


def type_order_observations(document, case_id, raw_file, raw_columns):
    columns = [record for (current, _, _), record in raw_columns.items()
        if current == case_id]
    columns.sort(key=lambda record: (record["row_group"], record["leaf"]))
    groups = document["row_groups"]
    if len(columns) != sum(len(group["columns"]) for group in groups):
        raise HarnessError("Arrow Rust TYPE_ORDER topology differs")
    for raw in columns:
        observed = groups[raw["row_group"]]["columns"][raw["leaf"]]
        if observed["column_order"] != "TYPE_ORDER" or \
                raw["column_order"]["state"] != "TYPE_ORDER":
            raise HarnessError("Arrow Rust column order is not TYPE_ORDER")
        if observed["path"] != raw["path"] or \
                observed["physical_type"] != \
                raw["leaf_schema"]["physical_type"] or \
                observed["num_values"] != int(raw["num_values"]):
            raise HarnessError("Arrow Rust TYPE_ORDER facts differ")
    return [{"file": raw_file, "columns": columns}]


def timestamp_nanoseconds(value):
    match = TIMESTAMP_PATTERN.fullmatch(value)
    if match is None:
        raise HarnessError(f"unsupported Arrow Rust timestamp: {value!r}")
    year, month, day, hour, minute, second = map(int, match.groups()[:6])
    base = datetime.datetime(year, month, day, hour, minute, second)
    epoch = datetime.datetime(1970, 1, 1)
    delta = base - epoch
    fraction = (match.group(7) or "").ljust(9, "0")
    return str((delta.days * 86400 + delta.seconds) * 1_000_000_000 +
        int(fraction or "0"))


def canonical_leaf_value(value, json_value, leaf, arrow_type):
    if value is None:
        if json_value is not None:
            raise HarnessError("Arrow Rust null representations disagree")
        return None
    schema = leaf["leaf_schema"]
    physical = schema["physical_type"]
    logical = schema["logical_type"]
    if logical == "DECIMAL":
        if not isinstance(value, dict) or \
                value.get("data_type") != arrow_type or \
                set(value) != {"data_type", "display"} or \
                not isinstance(value["display"], str):
            raise HarnessError("Arrow Rust DECIMAL representation differs")
        parsed = decimal.Decimal(value["display"])
        if decimal.Decimal(json_value) != parsed:
            raise HarnessError("Arrow Rust DECIMAL JSON representation differs")
        return {"decimal_scale": schema["scale"],
            "unscaled": decimal_unscaled(parsed, schema["scale"])}
    if physical in ("FLOAT", "DOUBLE"):
        digits = 8 if physical == "FLOAT" else 16
        if not isinstance(value, str) or not re.fullmatch(
                rf"0x[0-9a-f]{{{digits}}}", value):
            raise HarnessError("Arrow Rust floating representation differs")
        if not isinstance(json_value, (int, decimal.Decimal)) or \
                isinstance(json_value, bool):
            raise HarnessError("Arrow Rust floating JSON representation differs")
        return {"float32_bits" if physical == "FLOAT" else "float64_bits":
            value[2:]}
    if physical == "INT96":
        if not isinstance(value, dict) or set(value) != \
                {"data_type", "display"} or value["data_type"] != arrow_type or \
                not isinstance(value["display"], str) or \
                json_value != value["display"]:
            raise HarnessError("Arrow Rust INT96 representation differs")
        return {"timestamp_nanoseconds": timestamp_nanoseconds(value["display"])}
    if physical in ("BYTE_ARRAY", "FIXED_LEN_BYTE_ARRAY") and \
            logical not in ("JSON", "STRING", "ENUM"):
        if not isinstance(json_value, str) or len(json_value) % 2 or \
                HEX_PATTERN.fullmatch(json_value) is None:
            raise HarnessError("Arrow Rust binary representation differs")
        if physical == "FIXED_LEN_BYTE_ARRAY" and \
                len(json_value) != 2 * schema["type_length"]:
            raise HarnessError("Arrow Rust fixed binary width differs")
        return {"bytes_hex": json_value}
    if logical in ("JSON", "STRING", "ENUM"):
        if not isinstance(value, str) or json_value != value:
            raise HarnessError("Arrow Rust string representation differs")
        return value
    if physical == "BOOLEAN":
        if not isinstance(value, bool) or json_value is not value:
            raise HarnessError("Arrow Rust BOOLEAN representation differs")
        return value
    if physical in ("INT32", "INT64"):
        if not isinstance(value, int) or isinstance(value, bool) or \
                json_value != value:
            raise HarnessError("Arrow Rust integer representation differs")
        return value
    raise HarnessError(
        f"unsupported Arrow Rust logical value: {physical}/{logical}")


def logical_observations(evidence, leaves, case_id):
    arrow = evidence["arrow"]
    names, rows = validated_arrow_rows(evidence, leaves, case_id)
    columns = [[] for _ in leaves]
    arrow_types = [field["data_type"] for field in arrow["schema"]]
    for canonical_row, row in rows:
        for index, (entry, leaf, arrow_type) in enumerate(zip(canonical_row,
                leaves, arrow_types)):
            columns[index].append(canonical_leaf_value(entry["value"],
                row[names[index]], leaf, arrow_type))
    normalized = []
    for leaf, values in zip(leaves, columns):
        normalized.append({
            "logical_type": leaf["leaf_schema"]["logical_type"],
            "path": leaf["path"],
            "physical_type": leaf["leaf_schema"]["physical_type"],
            "values": values,
        })
    return [{
        "columns": normalized,
        "contract": "n6-logical-values-v1",
        "row_count": evidence["rows"],
    }]


def validate_claim_scope(claims):
    expected = {
        (case_id, "read.logical-values"): "planned"
        for case_id in EXPECTED_LOGICAL_CASES
    }
    expected.update({key: "unsupported" for key in EXPECTED_UNSUPPORTED})
    type_order = {(case_id, "wire.column-order.type")
        for case_id in EXPECTED_TYPE_ORDER_CASES}
    claimed_type_order = {key for key in claims
        if key[1] == "wire.column-order.type"}
    if claimed_type_order and claimed_type_order != type_order:
        raise HarnessError("Arrow Rust TYPE_ORDER capability scope differs")
    if claimed_type_order:
        expected.update({key: "planned" for key in type_order})
    if set(claims) != set(expected):
        raise HarnessError("Arrow Rust capability claim scope differs")
    for key, status in claims.items():
        if expected[key] == "planned" and status not in ("planned", "verified"):
            raise HarnessError(f"Arrow Rust positive claim status differs: {key}")
        if expected[key] == "unsupported" and status != "unsupported":
            raise HarnessError(f"Arrow Rust unsupported claim status differs: {key}")
    return None


def generate(arguments):
    requested_repository = checked_directory(arguments.repository,
        "repository root")
    for path, label in ((arguments.manifest, "manifest"),
            (arguments.capabilities, "capabilities"),
            (arguments.fixtures, "fixtures"),
            (arguments.descriptor, "toolchain descriptor"),
            (arguments.raw_evidence, "normalized raw evidence")):
        checked_regular_file(path, label)
    context = build_context(arguments, PRODUCER, EVIDENCE_ID,
        draft_evidence=arguments.draft)
    repository = context["repository"]
    if repository != requested_repository:
        raise HarnessError("repository root changed during context construction")
    manifest = context["manifest"]
    authority_record = context["authority_record"]
    known = context["known"]
    claims = context["claims"]
    raw_files = context["raw_files"]
    raw_columns = context["raw_columns"]
    limits = context["limits"]
    descriptor_sha256 = context["descriptor_sha256"]
    descriptor = context["descriptor"]
    expected_descriptor = repository_input(repository, DESCRIPTOR_RELATIVE)
    if pathlib.Path(arguments.descriptor).resolve(strict=True) != \
            expected_descriptor:
        raise HarnessError("descriptor path does not select the Arrow Rust pin")
    validate_descriptor(repository, descriptor, authority_record)
    validate_claim_scope(claims)
    corpus_root = checked_directory(arguments.corpus_root, "fixture root")
    selected_cases = sorted({case_id for case_id, _ in claims})
    expected_cases = EXPECTED_LOGICAL_CASES | \
        {case_id for case_id, _ in EXPECTED_UNSUPPORTED}
    type_order_enabled = any(capability == "wire.column-order.type"
        for _, capability in claims)
    if type_order_enabled:
        expected_cases |= EXPECTED_TYPE_ORDER_CASES
    if set(selected_cases) != expected_cases:
        raise HarnessError("Arrow Rust fixture coverage differs")
    total_bytes = sum(known[case_id]["size"] for case_id in selected_cases)
    if total_bytes > limits["max_total_bytes"]:
        raise HarnessError("Arrow Rust fixture bytes exceed the evidence limit")
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
    metadata_binary = repository_input(repository,
        descriptor["metadata_binary_file"])
    reject_output_alias(output,
        [*context["protected_inputs"], metadata_binary], [corpus_root])
    docker_image_identity(arguments, descriptor)
    verify_image_files(arguments, descriptor)
    unsupported_cases = {case_id for (case_id, _), status in claims.items()
        if status == "unsupported"}
    records = [run_record(EVIDENCE_ID, PRODUCER, authority_record,
        descriptor_sha256, repository, manifest, unsupported_cases,
        context["upstream_evidence"], input_hashes=context["input_hashes"])]
    snapshots = tempfile.TemporaryDirectory(prefix="parquet-n6-arrow-rs-",
        dir=output.parent)
    snapshot_root = pathlib.Path(snapshots.name)
    try:
        snapshot_paths = {}
        for case_id in selected_cases:
            fixture = known[case_id]
            if case_id not in raw_files:
                raise HarnessError(f"raw file fact is absent: {case_id}")
            path = checked_file(corpus_root, fixture["file"], fixture["sha256"],
                fixture["size"], snapshot_root)
            if path.stat().st_size > limits["max_file_bytes"]:
                raise HarnessError(
                    f"Arrow Rust fixture exceeds its limit: {case_id}")
            snapshot_paths[case_id] = path
        metadata_snapshot = snapshot_metadata_binary(repository, descriptor,
            snapshot_root)
        for path in snapshot_paths.values():
            os.chmod(path, 0o444)
        os.chmod(metadata_snapshot, 0o555)
        os.chmod(snapshot_root, 0o555)
        for case_id in selected_cases:
            fixture = known[case_id]
            path = snapshot_paths[case_id]
            current_claims = sorted((capability, status)
                    for (current, capability), status in claims.items()
                    if current == case_id)
            needs_audit = any(capability == "read.logical-values" or
                status == "unsupported" for capability, status in current_claims)
            evidence = None
            leaves = None
            if needs_audit:
                command = audit_command(arguments, descriptor, snapshot_root,
                    path.name)
                stdout, _ = run_bounded(command)
                document = load_oracle_json(stdout)
                evidence, leaves = validate_audit(document, fixture,
                    raw_columns, descriptor, path.name)
            metadata = None
            if any(capability == "wire.column-order.type"
                    for capability, _ in current_claims):
                command = metadata_command(arguments, descriptor,
                    snapshot_root, path.name, metadata_snapshot.name)
                stdout, _ = run_bounded(command)
                metadata = validate_metadata(load_oracle_json(stdout), fixture,
                    descriptor, path.name)
            records.append(raw_files[case_id])
            for capability, status in current_claims:
                if status == "unsupported":
                    records.append(case_result(case_id, capability,
                        fixture["digest_contract"], "UNSUPPORTED"))
                elif capability == "read.logical-values":
                    observations = logical_observations(evidence, leaves,
                        case_id)
                    records.append(case_result(case_id, capability,
                        fixture["digest_contract"], "PASS", observations))
                elif capability == "wire.column-order.type":
                    observations = type_order_observations(metadata, case_id,
                        raw_files[case_id], raw_columns)
                    records.append(case_result(case_id, capability,
                        fixture["digest_contract"], "PASS", observations))
                else:
                    raise HarnessError(
                        f"unhandled Arrow Rust capability: {capability}")
    finally:
        try:
            os.chmod(snapshot_root, 0o700)
        except FileNotFoundError:
            pass
        snapshots.cleanup()
    expected_records = 1 + len(selected_cases) + len(claims)
    if len(records) != expected_records:
        raise HarnessError("Arrow Rust normalized record count differs")
    value = evidence_bytes(records, limits["max_file_bytes"],
        limits["max_line_bytes"],
        limits["max_records_per_input"])
    atomic_output(output, value, arguments.check)
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
    result.add_argument("--output", required=True)
    result.add_argument("--docker", default="docker")
    result.add_argument("--check", action="store_true")
    result.add_argument("--draft", action="store_true")
    return result


def main():
    arguments = parser().parse_args()
    value = generate(arguments)
    mode = "checked" if arguments.check else "wrote"
    print(f"{EVIDENCE_ID}: {mode} {value.count(b'\n')} records and "
        f"{len(value)} bytes")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as error:
        print(f"N6 Arrow Rust harness failed: {error}", file=sys.stderr)
        sys.exit(1)
