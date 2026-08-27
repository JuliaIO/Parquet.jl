#!/usr/bin/env python3
import argparse
import contextlib
import hashlib
import jsonschema
import os
import pathlib
import re
import subprocess
import sys
import tempfile
import stat

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import common
import normalize_raw


EVIDENCE_ID = "normalized-independent-model"
PRODUCER = "n6-independent-model"
SEMANTIC_CAPABILITIES = {
    "semantic.type-order",
    "semantic.ieee-total-order",
    "semantic.logical-order",
    "semantic.count-state",
    "semantic.producer-trust",
}
BRIDGE_PATH = pathlib.Path(__file__).resolve().parent / "model_bridge.jl"
PRODUCER_DESCRIPTOR_PATH = pathlib.Path(__file__).resolve().parent / \
    "model-producer.toml"
MODEL_DIRECTORY = BRIDGE_PATH.parent.parent / "model"
MODEL_TEST_PATH = MODEL_DIRECTORY / "runtests.jl"
PRODUCER_FILE_LIMIT = 4 * 1024 * 1024
BRIDGE_INPUT_LIMIT = 16 * 1024 * 1024
BRIDGE_OUTPUT_LIMIT = 4 * 1024 * 1024
INT64_MAX = (1 << 63) - 1
BOUND_STATES = {"BOUND_ABSENT", "BOUND_UNKNOWN", "BOUND_KNOWN"}
EXACTNESS_STATES = {
    "EXACTNESS_UNKNOWN", "EXACTNESS_INEXACT", "EXACTNESS_EXACT"}
BOUND_REASONS = {
    "absent", "all_nan_type_order", "contradictory_bounds",
    "deprecated_order_mismatch", "ieee_bound_kind_contradiction",
    "invalid_logical", "known", "legacy_wrong_order",
    "missing_column_orders", "nan_type_order", "no_non_null_values",
    "over_limit", "parquet_251", "undefined_type_order",
    "unknown_column_order", "unproven_ieee_nan", "widened_zero",
}
OCCUPANCY_STATES = {
    "OCCUPANCY_UNKNOWN", "OCCUPANCY_EMPTY", "OCCUPANCY_ALL_NAN",
    "OCCUPANCY_HAS_NON_NAN",
}
FAMILIES = {"FAMILY_NONE", "FAMILY_MODERN", "FAMILY_DEPRECATED"}
COMPARATORS = {
    "COMPARATOR_SIGNED", "COMPARATOR_UNSIGNED", "COMPARATOR_UNSIGNED_BYTES",
    "COMPARATOR_DECIMAL", "COMPARATOR_BOOLEAN", "COMPARATOR_TYPE_FLOAT",
    "COMPARATOR_IEEE_FLOAT", "COMPARATOR_UNDEFINED",
}
TRUST_STATES = {"TRUST_TRUSTED", "TRUST_UNTRUSTED"}
TRUST_REASONS = {"no_bounds", "parquet_251", "legacy_wrong_order", "trusted"}
ALLOWED_JULIA_TOOLCHAINS = {
    "1.10.11": {
        "executable":
            "6f687953e48958fc6596962379691d1c8a1720d3a9ff39c1e3113888e43bd8ae",
        "runtime_tree":
            "c784c03af8ab52e48aa6f57ce8aa06a3c9160671054301ab6c3dfebf2e582525",
    },
    "1.12.6": {
        "executable":
            "9ad38bea81ecace044a4bdef2a0246dee94cb8a44c9420809cc00f9872651c64",
        "runtime_tree":
            "273ec71de498a36c77a7e4bb3af4a3f75c338bd1cfe255cab30805b6a2cda76e",
    },
}
JULIA_RUNTIME_MAX_ENTRIES = 10_000
JULIA_RUNTIME_MAX_FILE_BYTES = 1024 * 1024 * 1024
JULIA_RUNTIME_MAX_TOTAL_BYTES = 2 * 1024 * 1024 * 1024


def _token(value):
    if value is None:
        return "-"
    if isinstance(value, bool):
        return "1" if value else "0"
    return str(value)


def _hex_string(value):
    if value is None:
        return "-"
    return value.encode("utf-8").hex()


def _model_input(index, column, file_record):
    leaf = column["leaf_schema"]
    fields = (
        index,
        column["case_id"],
        column["row_group"],
        column["leaf"],
        file_record["leaf_count"],
        leaf["physical_type"],
        leaf["logical_type"],
        leaf["type_length"],
        leaf["bit_width"],
        leaf["is_signed"],
        leaf["precision"],
        leaf["time_unit"],
        column["num_values"],
        column["column_order"]["state"],
        file_record["created_by_present"],
        _hex_string(file_record["created_by"]),
        column["min_value_hex"],
        column["max_value_hex"],
        column["deprecated_min_hex"],
        column["deprecated_max_hex"],
        column["null_count"],
        column["nan_count"],
        column["distinct_count"],
        column["is_min_value_exact"],
        column["is_max_value_exact"],
    )
    tokens = [_token(value) for value in fields]
    if any("\t" in value or "\n" in value for value in tokens):
        raise common.EvidenceError("model bridge token contains a delimiter")
    return "\t".join(tokens)


def _parse_value(token):
    if token == "NONE":
        return None
    fields = token.split(":")
    kind = fields[0]
    if kind in ("SIGNED", "UNSIGNED") and len(fields) == 2:
        value = int(fields[1])
        if kind == "SIGNED" and not -(1 << 63) <= value <= INT64_MAX:
            raise common.EvidenceError("model returned an invalid signed value")
        if kind == "UNSIGNED" and not 0 <= value < 1 << 64:
            raise common.EvidenceError("model returned an invalid unsigned value")
        return {"kind": kind, "value": fields[1]}
    if kind == "BOOLEAN" and fields[1:] in (["0"], ["1"]):
        return {"kind": kind, "value": fields[1] == "1"}
    if kind in ("BYTES", "DECIMAL") and len(fields) == 2:
        if re.fullmatch(r"(?:[0-9a-f]{2})*", fields[1]) is None:
            raise common.EvidenceError("model returned invalid hexadecimal bytes")
        return {"kind": kind, "hex": fields[1]}
    if kind == "FLOAT" and len(fields) == 3:
        width = int(fields[1])
        if width not in (16, 32, 64) or len(fields[2]) != width // 4:
            raise common.EvidenceError("model bridge returned an invalid float")
        if re.fullmatch(r"[0-9a-f]+", fields[2]) is None:
            raise common.EvidenceError("model returned invalid float bits")
        return {"kind": kind, "width": width, "bits_hex": fields[2]}
    raise common.EvidenceError("model bridge returned an invalid value")


def _parse_bound(fields, offset):
    result = {
        "state": fields[offset],
        "reason": fields[offset + 1],
        "exactness": fields[offset + 2],
        "value": _parse_value(fields[offset + 3]),
    }
    if result["state"] not in BOUND_STATES or \
            result["reason"] not in BOUND_REASONS or \
            result["exactness"] not in EXACTNESS_STATES:
        raise common.EvidenceError("model bridge returned an invalid bound fact")
    known = result["state"] == "BOUND_KNOWN"
    if known == (result["value"] is None):
        raise common.EvidenceError("model bound state contradicts its value")
    return result


def _parse_ok(fields):
    if len(fields) != 21:
        raise common.EvidenceError("model bridge output field count is invalid")
    booleans = (fields[10], fields[12], fields[14])
    if any(value not in ("0", "1") for value in booleans):
        raise common.EvidenceError("model bridge returned an invalid count state")
    counts = [int(value) for value in (fields[11], fields[13], fields[15])]
    if any(not 0 <= value <= INT64_MAX for value in counts):
        raise common.EvidenceError("model bridge returned an invalid count")
    if any(fields[index] == "0" and fields[index + 1] != "0"
            for index in (10, 12, 14)):
        raise common.EvidenceError("unknown model count has a value")
    if fields[16] not in OCCUPANCY_STATES or fields[17] not in FAMILIES or \
            fields[18] not in COMPARATORS or fields[19] not in TRUST_STATES or \
            fields[20] not in TRUST_REASONS:
        raise common.EvidenceError("model bridge returned an invalid state")
    return {
        "outcome": "OK",
        "lower": _parse_bound(fields, 2),
        "upper": _parse_bound(fields, 6),
        "counts": {
            "null": {"known": fields[10] == "1", "value": fields[11]},
            "nan": {"known": fields[12] == "1", "value": fields[13]},
            "distinct": {"known": fields[14] == "1", "value": fields[15]},
        },
        "occupancy": fields[16],
        "family": fields[17],
        "comparator": fields[18],
        "trust": {"state": fields[19], "reason": fields[20]},
    }


def _parse_result(line):
    fields = line.split("\t")
    if len(fields) < 2:
        raise common.EvidenceError("model bridge returned a short line")
    index = fields[0]
    if fields[1] == "OK":
        return index, _parse_ok(fields)
    if fields[1] in ("FORMAT_ERROR", "ARGUMENT_ERROR") and len(fields) == 3:
        try:
            message = bytes.fromhex(fields[2]).decode("utf-8")
        except (ValueError, UnicodeError) as error:
            raise common.EvidenceError(
                "model bridge returned an invalid error message") from error
        return index, {"outcome": fields[1], "message": message}
    raise common.EvidenceError("model bridge returned an invalid outcome")


def _julia_runtime_tree_sha256(root):
    root = pathlib.Path(root)
    if root.resolve(strict=True) != root or root.is_symlink() or not root.is_dir():
        raise common.EvidenceError("Julia runtime root is not canonical")
    entries = []
    total_bytes = 0
    for directory, directories, files in os.walk(root, followlinks=False):
        base = pathlib.Path(directory)
        for name in directories + files:
            path = base / name
            metadata = path.lstat()
            if stat.S_ISLNK(metadata.st_mode) or stat.S_ISREG(metadata.st_mode):
                relative = path.relative_to(root).as_posix()
                if "\0" in relative or "\n" in relative:
                    raise common.EvidenceError("Julia runtime path is invalid")
                entries.append((relative, path, metadata))
                if stat.S_ISREG(metadata.st_mode):
                    if not 0 <= metadata.st_size <= JULIA_RUNTIME_MAX_FILE_BYTES:
                        raise common.EvidenceError(
                            "Julia runtime file has an invalid size")
                    total_bytes += metadata.st_size
            elif not stat.S_ISDIR(metadata.st_mode):
                raise common.EvidenceError("Julia runtime has a special file")
            if len(entries) > JULIA_RUNTIME_MAX_ENTRIES or \
                    total_bytes > JULIA_RUNTIME_MAX_TOTAL_BYTES:
                raise common.EvidenceError("Julia runtime tree exceeds its limit")
    digest = hashlib.sha256()
    for relative, path, metadata in sorted(entries):
        encoded = relative.encode("utf-8")
        if stat.S_ISLNK(metadata.st_mode):
            target = os.readlink(path)
            if "\0" in target or "\n" in target:
                raise common.EvidenceError("Julia runtime link is invalid")
            record = b"L\0" + encoded + b"\0" + target.encode("utf-8") + b"\n"
        else:
            flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
            nofollow = getattr(os, "O_NOFOLLOW", None)
            if nofollow is None:
                raise common.EvidenceError(
                    "platform cannot reject Julia runtime symlinks")
            descriptor = os.open(path, flags | nofollow)
            file_digest = hashlib.sha256()
            try:
                with os.fdopen(descriptor, "rb") as stream:
                    before = os.fstat(stream.fileno())
                    if not stat.S_ISREG(before.st_mode) or \
                            not 0 <= before.st_size <= \
                            JULIA_RUNTIME_MAX_FILE_BYTES:
                        raise common.EvidenceError(
                            "Julia runtime file has an invalid size")
                    for block in iter(lambda: stream.read(1024 * 1024), b""):
                        file_digest.update(block)
                    after = os.fstat(stream.fileno())
            except BaseException:
                try:
                    os.close(descriptor)
                except OSError:
                    pass
                raise
            identity = lambda value: (value.st_dev, value.st_ino,
                value.st_size, value.st_mtime_ns, value.st_ctime_ns)
            if identity(before) != identity(after):
                raise common.EvidenceError(
                    "Julia runtime file changed while hashed")
            record = b"F\0" + encoded + b"\0" + \
                file_digest.hexdigest().encode("ascii") + b"\n"
        digest.update(record)
    return digest.hexdigest()


def _julia_toolchain(command):
    if not isinstance(command, (list, tuple)) or len(command) != 1 or \
            not isinstance(command[0], str):
        raise common.EvidenceError(
            "Julia model command must be one absolute executable")
    executable = pathlib.Path(command[0])
    if not executable.is_absolute() or executable.resolve(strict=True) != executable:
        raise common.EvidenceError(
            "Julia model executable must be absolute and canonical")
    digest = common.sha256_file(executable, 2 * 1024 * 1024)
    matches = [version for version, expected in ALLOWED_JULIA_TOOLCHAINS.items()
        if digest == expected["executable"]]
    if len(matches) != 1:
        raise common.EvidenceError("Julia model executable is not pinned")
    version = matches[0]
    runtime_root = executable.parent.parent
    runtime_digest = _julia_runtime_tree_sha256(runtime_root)
    if runtime_digest != ALLOWED_JULIA_TOOLCHAINS[version]["runtime_tree"]:
        raise common.EvidenceError("Julia model runtime tree is not pinned")
    return executable, runtime_root, version, digest, runtime_digest


def run_model(columns, files, command, producer_root, run_suite=False):
    executable, runtime_root, expected_version, expected_digest, \
        expected_runtime_digest = _julia_toolchain(command)
    ordered = sorted(columns,
        key=lambda item: (item["case_id"], item["row_group"], item["leaf"]))
    keys = []
    lines = []
    for index, column in enumerate(ordered):
        case_id = column["case_id"]
        if case_id not in files:
            raise common.EvidenceError("model column lacks its file record")
        token = str(index)
        keys.append((case_id, column["row_group"], column["leaf"]))
        lines.append(_model_input(token, column, files[case_id]))
    payload = ("\n".join(lines) + "\n").encode("ascii")
    if not 0 < len(payload) <= BRIDGE_INPUT_LIMIT:
        raise common.EvidenceError("model bridge input exceeds its byte limit")
    bridge_path = pathlib.Path(producer_root) / \
        "test/conformance/n6/normalizers/model_bridge.jl"
    arguments = [str(executable),
        "--startup-file=no",
        "--history-file=no",
        "--compiled-modules=no",
        "--check-bounds=yes",
        "--project=@stdlib",
        str(bridge_path),
    ]
    if run_suite:
        arguments.append("--run-suite")
    environment = os.environ.copy()
    environment["JULIA_LOAD_PATH"] = "@stdlib"
    environment["JULIA_PROJECT"] = "@stdlib"
    environment["JULIA_NUM_THREADS"] = "1"
    with tempfile.TemporaryDirectory(prefix="parquet-n6-model-depot-") as depot:
        environment["JULIA_DEPOT_PATH"] = depot
        try:
            process = subprocess.run(arguments, input=payload,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120,
                check=False, env=environment)
        except (OSError, subprocess.TimeoutExpired) as error:
            raise common.EvidenceError(f"independent model failed: {error}") from error
    if len(process.stdout) > BRIDGE_OUTPUT_LIMIT or \
            len(process.stderr) > BRIDGE_OUTPUT_LIMIT:
        raise common.EvidenceError("independent model output exceeds its byte limit")
    if process.returncode != 0:
        detail = process.stderr.decode("utf-8", errors="replace")
        raise common.EvidenceError(
            f"independent model exited {process.returncode}: {detail}")
    try:
        text = process.stdout.decode("utf-8")
    except UnicodeError as error:
        raise common.EvidenceError("independent model output is not UTF-8") from error
    if not text.endswith("\n"):
        raise common.EvidenceError("independent model output lacks a final newline")
    output_lines = text[:-1].split("\n")
    if len(output_lines) != len(lines) + 1:
        raise common.EvidenceError("independent model output count is inconsistent")
    toolchain = output_lines[0].split("\t")
    if len(toolchain) != 3 or toolchain[0] != "TOOLCHAIN" or \
            (toolchain[1], toolchain[2]) != \
            (expected_version, expected_digest):
        raise common.EvidenceError("independent model toolchain line is invalid")
    if common.sha256_file(executable, 2 * 1024 * 1024) != expected_digest:
        raise common.EvidenceError("Julia model executable changed after use")
    if _julia_runtime_tree_sha256(runtime_root) != expected_runtime_digest:
        raise common.EvidenceError("Julia model runtime tree changed after use")
    results = {}
    for line in output_lines[1:]:
        index, result = _parse_result(line)
        try:
            position = int(index)
        except (ValueError, IndexError) as error:
            raise common.EvidenceError("independent model index is invalid") from error
        if not 0 <= position < len(keys):
            raise common.EvidenceError("independent model index is invalid")
        key = keys[position]
        if key in results:
            raise common.EvidenceError("independent model returned a duplicate result")
        results[key] = result
    if len(results) != len(keys):
        raise common.EvidenceError("independent model result coverage is incomplete")
    return expected_runtime_digest, results


def require_model_success(results):
    failures = sorted((key, value) for key, value in results.items()
        if value["outcome"] != "OK")
    if failures:
        key, result = failures[0]
        raise common.EvidenceError(
            f"independent model rejected normalized column {key}: "
            f"{result['outcome']}: {result['message']}")
    return None


def _verify_models(context):
    root = context["root"]
    expected_models = {
        "test/conformance/n6/model/N6StatisticsModel.jl",
        "test/conformance/n6/model/runtests.jl",
        "test/conformance/n6/model/cases.toml",
        "test/conformance/n6/model/README.md",
    }
    actual_models = {
        item["file"] for item in context["manifest"]["frozen_model"]}
    if actual_models != expected_models or len(actual_models) != len(
            context["manifest"]["frozen_model"]):
        raise common.EvidenceError("frozen independent model set is inconsistent")
    model_hashes = {}
    model_payloads = {}
    for item in context["manifest"]["frozen_model"]:
        path = root / item["file"]
        payload = common.read_file_bytes(path, PRODUCER_FILE_LIMIT)
        digest = common.sha256_bytes(payload)
        if digest != item["sha256"]:
            raise common.EvidenceError(
                f"frozen independent model hash mismatch: {item['file']}")
        model_hashes[item["file"]] = digest
        model_payloads[path] = payload
    descriptor_relative = context["manifest"].get(
        "model_producer_descriptor_file")
    descriptor_expected = context["manifest"].get(
        "model_producer_descriptor_sha256")
    expected_descriptor = \
        "test/conformance/n6/normalizers/model-producer.toml"
    if descriptor_relative != expected_descriptor or \
            not isinstance(descriptor_expected, str):
        raise common.EvidenceError("model producer descriptor is inconsistent")
    descriptor_path = root / descriptor_relative
    if descriptor_path != PRODUCER_DESCRIPTOR_PATH:
        raise common.EvidenceError("model producer descriptor path differs")
    descriptor_payload = common.read_file_bytes(descriptor_path, 2 * 1024 * 1024)
    descriptor_sha256 = common.sha256_bytes(descriptor_payload)
    if descriptor_sha256 != descriptor_expected:
        raise common.EvidenceError("model producer descriptor hash mismatch")
    descriptor = common.parse_toml_bytes(descriptor_payload, descriptor_path)
    model_payloads[descriptor_path] = descriptor_payload
    if set(descriptor) != {"descriptor_version", "producer", "file"} or \
            descriptor["descriptor_version"] != 1 or \
            descriptor["producer"] != PRODUCER or \
            not isinstance(descriptor["file"], list):
        raise common.EvidenceError("model producer descriptor header differs")
    expected_producer_files = {
        *expected_models,
        "test/conformance/n6/normalizers/common.py",
        "test/conformance/n6/normalizers/model_bridge.jl",
        "test/conformance/n6/normalizers/normalize_model.py",
        "test/conformance/n6/normalizers/normalize_raw.py",
    }
    producer_hashes = {}
    for item in descriptor["file"]:
        if not isinstance(item, dict) or set(item) != {"path", "sha256"} or \
                not isinstance(item["path"], str) or \
                not isinstance(item["sha256"], str):
            raise common.EvidenceError("model producer file entry differs")
        relative = pathlib.PurePosixPath(item["path"])
        if relative.is_absolute() or ".." in relative.parts or \
                relative.as_posix() != item["path"]:
            raise common.EvidenceError("model producer path is unsafe")
        path = root.joinpath(*relative.parts)
        payload = model_payloads.get(path)
        if payload is None:
            payload = common.read_file_bytes(path, PRODUCER_FILE_LIMIT)
            model_payloads[path] = payload
        digest = common.sha256_bytes(payload)
        if digest != item["sha256"]:
            raise common.EvidenceError(
                f"model producer file hash mismatch: {item['path']}")
        if item["path"] in producer_hashes:
            raise common.EvidenceError("model producer file is duplicated")
        producer_hashes[item["path"]] = digest
    if set(producer_hashes) != expected_producer_files:
        raise common.EvidenceError("model producer file set is inconsistent")
    if any(producer_hashes[path] != digest
            for path, digest in model_hashes.items()):
        raise common.EvidenceError("model producer and frozen model differ")
    authority = _authority(context)
    if authority["revision"] != descriptor_sha256:
        raise common.EvidenceError("model authority revision is inconsistent")
    toolchains = {item["id"]: item for item in context["manifest"]["toolchain"]}
    if len(toolchains) != len(context["manifest"]["toolchain"]):
        raise common.EvidenceError("manifest toolchain IDs are ambiguous")
    expected_hashes = set()
    for version, digests in ALLOWED_JULIA_TOOLCHAINS.items():
        identifier = "julia-" + ".".join(version.split(".")[:2])
        toolchain = toolchains.get(identifier)
        expected_artifacts = [{
            "name": f"julia-{version}-executable",
            "sha256": digests["executable"],
        }, {
            "name": f"julia-{version}-runtime-tree-sha256-v1",
            "sha256": digests["runtime_tree"],
        }]
        if toolchain is None or toolchain["status"] != "verified" or \
                toolchain["version"] != version or \
                toolchain["artifacts"] != expected_artifacts:
            raise common.EvidenceError(
                f"Julia model toolchain is inconsistent: {identifier}")
        expected_hashes.update(digests.values())
    if set(authority["toolchain_sha256"]) != expected_hashes or \
            len(authority["toolchain_sha256"]) != len(expected_hashes):
        raise common.EvidenceError("model authority toolchains are inconsistent")
    context["model_snapshots"] = {
        root / relative: digest for relative, digest in producer_hashes.items()}
    context["model_snapshots"][descriptor_path] = descriptor_sha256
    context["model_payloads"] = model_payloads
    return None


def _set_snapshot_modes(root, locked):
    root = pathlib.Path(root)
    directories = []
    for directory, names, files in os.walk(root, topdown=True,
            followlinks=False):
        path = pathlib.Path(directory)
        directories.append(path)
        for name in files:
            target = path / name
            if target.is_symlink():
                raise common.EvidenceError(
                    f"model snapshot file is a symbolic link: {target}")
            target.chmod(0o400 if locked else 0o600)
        for name in names:
            target = path / name
            if target.is_symlink():
                raise common.EvidenceError(
                    f"model snapshot directory is a symbolic link: {target}")
    for directory in reversed(directories):
        directory.chmod(0o500 if locked else 0o700)
    return None


@contextlib.contextmanager
def model_producer_snapshot(context):
    payloads = context.get("model_payloads")
    if not isinstance(payloads, dict) or not payloads:
        raise common.EvidenceError("model producer bytes are not authenticated")
    with tempfile.TemporaryDirectory(
            prefix="parquet-n6-model-producer-") as directory:
        snapshot_root = pathlib.Path(directory) / "producer"
        snapshot_root.mkdir(mode=0o700)
        for source, payload in payloads.items():
            try:
                relative = pathlib.Path(source).relative_to(context["root"])
            except ValueError as error:
                raise common.EvidenceError(
                    f"model producer path escapes the repository: {source}") from error
            target = snapshot_root / relative
            target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            with target.open("xb") as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
        _set_snapshot_modes(snapshot_root, True)
        try:
            yield snapshot_root
        finally:
            _set_snapshot_modes(snapshot_root, False)


def _semantic_cases(context):
    path = context["root"] / "test/conformance/n6/model/cases.toml"
    value = context["model_payloads"][path]
    cases = common.parse_toml_bytes(value, path)
    if cases.get("schema_version") != 1 or \
            not isinstance(cases.get("case_groups"), list):
        raise common.EvidenceError("semantic case manifest header differs")
    output = {}
    for case in cases["case_groups"]:
        required = {"id", "requirements", "capabilities", "digest_contract",
            "expected_sha256"}
        if not isinstance(case, dict) or set(case) != required:
            raise common.EvidenceError("semantic case fields differ")
        identifier = case["id"]
        if identifier in output:
            raise common.EvidenceError("semantic case ID is duplicated")
        output[identifier] = case
    return output


def _semantic_observations(case, context):
    snapshots = context["model_snapshots"]
    model_path = context["root"] / \
        "test/conformance/n6/model/N6StatisticsModel.jl"
    tests_path = context["root"] / "test/conformance/n6/model/runtests.jl"
    return [{
        "case_group": case["id"],
        "contract": "n6-independent-model-suite-v1",
        "model_sha256": snapshots[model_path],
        "requirements": case["requirements"],
        "runtests_sha256": snapshots[tests_path],
        "suite_status": "PASS",
    }]


def _raw_records(normalized_path, raw_path, context):
    limits = context["manifest"]["evidence_limits"]
    payload = common.read_file_bytes(normalized_path, limits["max_file_bytes"])
    expected = normalize_raw.render_raw_evidence(raw_path)
    if payload != expected:
        raise common.EvidenceError(
            "normalized raw input is not the deterministic frozen-corpus result")
    normalize_raw.validate_normalized_payload(payload, context)
    records = common.parse_jsonl_bytes(payload, normalized_path,
        limits["max_line_bytes"], limits["max_records_per_input"])
    for record in records:
        context["normalized_validator"].validate(record)
    return records, payload


def _raw_upstream(context, payload):
    entries = [item for section in ("frozen_evidence", "planned_evidence")
        for item in context["manifest"].get(section, [])
        if item["id"] == normalize_raw.EVIDENCE_ID]
    if len(entries) != 1:
        raise common.EvidenceError("normalized raw evidence entry is ambiguous")
    entry = entries[0]
    if entry.get("authority") != normalize_raw.PRODUCER or \
            entry.get("format") != "normalized-jsonl":
        raise common.EvidenceError("normalized raw evidence entry is inconsistent")
    return [{
        "evidence_id": normalize_raw.EVIDENCE_ID,
        "file": entry["file"],
        "sha256": common.sha256_bytes(payload),
    }]


def _authority(context):
    matches = [item for item in context["capabilities"]["authority"]
        if item["id"] == PRODUCER]
    if len(matches) != 1:
        raise common.EvidenceError("independent model authority is ambiguous")
    authority = matches[0]
    if authority["kind"] != "test-owned-independent-semantics" or \
            authority["version"] != "1":
        raise common.EvidenceError("independent model authority is inconsistent")
    return authority


def _model_claims(context):
    claims = {}
    for claim in _authority(context).get("claim", []):
        if claim["capability"] not in SEMANTIC_CAPABILITIES or \
                claim["status"] not in ("verified", "planned"):
            continue
        for case_id in claim["cases"]:
            key = (case_id, claim["capability"])
            if key in claims:
                raise common.EvidenceError(f"duplicate model claim: {key}")
            claims[key] = claim["status"]
    return claims


def _run_record(context, toolchain, upstream_evidence):
    authority = _authority(context)
    if toolchain not in authority["toolchain_sha256"]:
        raise common.EvidenceError(
            "independent model executable is not an allowed toolchain")
    manifest = context["manifest"]
    paths = context["paths"]
    return {
        "record": "run",
        "schema_version": 2,
        "evidence_id": EVIDENCE_ID,
        "producer": PRODUCER,
        "producer_version": authority["version"],
        "source_revision": authority["revision"],
        "plan_sha256": manifest["plan_sha256"],
        "capabilities_sha256": context["snapshots"][paths["capabilities"]],
        "fixture_manifest_sha256": context["snapshots"][paths["fixtures"]],
        "corpus_manifest_sha256": context["snapshots"][paths["corpus"]],
        "evidence_schema_sha256": context["snapshots"][paths["schema"]],
        "toolchain_sha256": toolchain,
        "upstream_evidence": upstream_evidence,
        "unsupported_cases": [],
    }


def _result(case, capability, observations, detail=None):
    digest = common.capability_digest(case["id"], capability, observations)
    expected = case.get("expected_sha256", {}).get(capability, digest)
    if digest != expected:
        raise common.EvidenceError(
            f"semantic case digest differs: {(case['id'], capability)}: "
            f"expected={expected}, actual={digest}")
    return {
        "record": "case_result",
        "schema_version": 2,
        "case_id": case["id"],
        "capability_id": capability,
        "digest_contract": case.get(
            "digest_contract", "n6-capability-result-sha256-v1"),
        "status": "PASS",
        "expected_sha256": expected,
        "actual_sha256": digest,
        "detail": detail or
            "Frozen independent model interpreted every normalized raw column in this case.",
    }


def render_model_evidence(normalized_path, raw_path, julia_command):
    context = normalize_raw.load_context()
    _verify_models(context)
    records, raw_payload = _raw_records(normalized_path, raw_path, context)
    files = {record["case_id"]: record for record in records
        if record["record"] == "file"}
    columns = [record for record in records
        if record["record"] == "column_statistics"]
    cases = [case for case in context["fixtures"]["fixture"]
        if case["source_kind"] == "apache-corpus"]
    if len(files) != context["raw_entry"]["case_count"] or \
            set(files) != {case["id"] for case in cases}:
        raise common.EvidenceError("normalized raw file coverage is incomplete")
    with model_producer_snapshot(context) as producer_root:
        toolchain, model_results = run_model(columns, files, julia_command,
            producer_root, run_suite=True)
    require_model_success(model_results)
    claims = _model_claims(context)
    semantic_cases = _semantic_cases(context)
    output = [_run_record(context, toolchain,
        _raw_upstream(context, raw_payload))]
    emitted_semantic = set()
    for case_id in sorted(semantic_cases):
        case = semantic_cases[case_id]
        for capability in case["capabilities"]:
            if (case_id, capability) not in claims:
                continue
            emitted_semantic.add((case_id, capability))
            output.append(_result(case, capability,
                _semantic_observations(case, context),
                "The frozen independent model suite passed this semantic case."))
    expected_semantic = {key for key in claims if key[0] in semantic_cases}
    if emitted_semantic != expected_semantic:
        raise common.EvidenceError("semantic model claim coverage is incomplete")
    for case in cases:
        case_id = case["id"]
        file_record = files[case_id]
        case_columns = sorted((record for record in columns
            if record["case_id"] == case_id),
            key=lambda record: (record["row_group"], record["leaf"]))
        output.append(file_record)
        output.extend(case_columns)
        observations = [{
            "file": file_record,
            "columns": [{
                "column": record,
                "model_result": model_results[(
                    case_id, record["row_group"], record["leaf"])],
            } for record in case_columns],
        }]
        capabilities = sorted(capability for capability in case["capabilities"]
            if (case_id, capability) in claims)
        output.extend(_result(case, capability, observations)
            for capability in capabilities)
    for record in output:
        context["normalized_validator"].validate(record)
    payload = common.jsonl_bytes(output)
    limits = context["manifest"]["evidence_limits"]
    if len(payload) > limits["max_file_bytes"] or \
            len(output) > limits["max_records_per_input"]:
        raise common.EvidenceError("model evidence exceeds a frozen limit")
    normalize_raw.validate_normalized_payload(payload, context, (raw_payload,))
    return payload


def parse_arguments(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True,
        help="deterministic normalized raw evidence")
    parser.add_argument("--raw-input", required=True,
        help="frozen gate-generated raw scanner output")
    parser.add_argument("--output", required=True)
    parser.add_argument("--julia-executable", required=True)
    parser.add_argument("--check", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    try:
        args = parse_arguments(argv)
        command = [args.julia_executable]
        payload = render_model_evidence(
            args.input, args.raw_input, command)
        context = normalize_raw.load_context()
        _verify_models(context)
        common.ensure_snapshots(context["snapshots"])
        common.ensure_snapshots(context["model_snapshots"])
        inputs = (args.input, args.raw_input, *context["paths"].values(),
            *context["model_snapshots"])
        common.publish_bytes(args.output, payload, args.check, inputs)
        action = "is fresh" if args.check else "written"
        print(f"N6 independent model evidence {action}: {args.output}")
        return 0
    except (common.EvidenceError, OSError, ValueError,
            jsonschema.ValidationError) as error:
        print(f"N6 model normalization failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
