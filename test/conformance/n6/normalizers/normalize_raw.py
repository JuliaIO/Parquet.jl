#!/usr/bin/env python3
import argparse
import json
import os
import pathlib
import re
import subprocess
import sys
import tempfile

import jsonschema

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import common


INT64_MAX = (1 << 63) - 1
INT32_MIN = -(1 << 31)
INT32_MAX = (1 << 31) - 1
FORMAT_COMMIT = "c47e2a66e88943fc46fde1b028a9432f14fdf5c0"
RAW_EVIDENCE_ID = "raw-java-apache-corpus"
EVIDENCE_ID = "normalized-raw-java-apache-corpus"
GENERATED_RAW_EVIDENCE_ID = "raw-java-generated"
GENERATED_EVIDENCE_ID = "normalized-raw-java-generated"
GENERATED_RAW_SHA256 = \
    "4f5e896a53c970cb3ae03876d40dafb202850929e4dc6b589aece474b0d61a25"
GENERATED_RAW_CASE_COUNT = 12
GENERATED_RAW_RECORD_COUNT = 12
PRODUCER = "n6-raw-java"
WIRE_CAPABILITIES = {
    "wire.column-order.type",
    "wire.column-order.ieee",
    "wire.column-order.empty",
    "wire.statistics.deprecated-bounds",
    "wire.statistics.modern-bounds",
    "wire.statistics.exactness",
    "wire.statistics.counts",
    "wire.statistics.nan-count",
}
CONVERTED_LOGICAL = {
    "UTF8": "STRING",
    "ENUM": "ENUM",
    "DECIMAL": "DECIMAL",
    "DATE": "DATE",
    "TIME_MILLIS": "TIME",
    "TIME_MICROS": "TIME",
    "TIMESTAMP_MILLIS": "TIMESTAMP",
    "TIMESTAMP_MICROS": "TIMESTAMP",
    "UINT_8": "INTEGER",
    "UINT_16": "INTEGER",
    "UINT_32": "INTEGER",
    "UINT_64": "INTEGER",
    "INT_8": "INTEGER",
    "INT_16": "INTEGER",
    "INT_32": "INTEGER",
    "INT_64": "INTEGER",
    "JSON": "JSON",
    "BSON": "BSON",
    "INTERVAL": "INTERVAL",
}
INTEGER_CONVERTED = {
    "UINT_8": (8, False),
    "UINT_16": (16, False),
    "UINT_32": (32, False),
    "UINT_64": (64, False),
    "INT_8": (8, True),
    "INT_16": (16, True),
    "INT_32": (32, True),
    "INT_64": (64, True),
}
TIME_CONVERTED = {
    "TIME_MILLIS": ("TIME", "MILLIS"),
    "TIME_MICROS": ("TIME", "MICROS"),
    "TIMESTAMP_MILLIS": ("TIMESTAMP", "MILLIS"),
    "TIMESTAMP_MICROS": ("TIMESTAMP", "MICROS"),
}
GROUP_TYPES = {"MAP", "MAP_KEY_VALUE", "LIST", "VARIANT"}
STATISTIC_FIELDS = (
    (1, "max", "deprecated_max_hex"),
    (2, "min", "deprecated_min_hex"),
    (3, "null_count", "null_count"),
    (4, "distinct_count", "distinct_count"),
    (5, "max_value", "max_value_hex"),
    (6, "min_value", "min_value_hex"),
    (7, "is_max_value_exact", "is_max_value_exact"),
    (8, "is_min_value_exact", "is_min_value_exact"),
    (9, "nan_count", "nan_count"),
)
CORPUS_LINE = re.compile(
    rb"([0-9a-f]{64})  (data/[A-Za-z0-9._+@=-]+(?:/[A-Za-z0-9._+@=-]+)*)\n")


def _presence(raw, name):
    value = raw[name]
    return value["value"] if value["present"] else None


def _required_converted(logical, bit_width, is_signed, time_unit):
    direct = {
        "STRING": "UTF8",
        "ENUM": "ENUM",
        "DECIMAL": "DECIMAL",
        "DATE": "DATE",
        "JSON": "JSON",
        "BSON": "BSON",
        "INTERVAL": "INTERVAL",
    }
    if logical in direct:
        return direct[logical]
    if logical == "INTEGER" and bit_width is not None and is_signed is not None:
        return ("INT_" if is_signed else "UINT_") + str(bit_width)
    if logical in ("TIME", "TIMESTAMP") and time_unit in ("MILLIS", "MICROS"):
        return logical + "_" + time_unit
    return None


def effective_leaf(raw):
    physical = raw["physical_type"]
    type_length = _presence(raw, "type_length")
    converted = _presence(raw, "converted_type")
    raw_scale = _presence(raw, "scale")
    raw_precision = _presence(raw, "precision")
    logical_union = raw["logical_type"]
    bit_width = None
    is_signed = None
    time_unit = None
    adjusted = None
    crs = None
    algorithm = None
    scale = None
    precision = None
    if logical_union["present"]:
        logical = logical_union["member"]
        if logical is None:
            raise common.EvidenceError(
                "future logical union member cannot be normalized")
        if logical in GROUP_TYPES:
            raise common.EvidenceError(
                f"group logical type is present on a leaf: {logical}")
        parameters = logical_union["parameters"]
        if logical == "INTEGER":
            bit_width = parameters["bit_width"]
            is_signed = parameters["is_signed"]
        elif logical == "DECIMAL":
            scale = parameters["scale"]
            precision = parameters["precision"]
            if raw_scale != scale or raw_precision != precision:
                raise common.EvidenceError(
                    "modern DECIMAL contradicts SchemaElement parameters")
        elif logical in ("TIME", "TIMESTAMP"):
            time_unit = parameters["unit"]
            adjusted = parameters["is_adjusted_to_utc"]
        elif logical == "GEOMETRY":
            crs = parameters["crs"]["value"] if \
                parameters["crs"]["present"] else None
        elif logical == "GEOGRAPHY":
            crs = parameters["crs"]["value"] if \
                parameters["crs"]["present"] else None
            algorithm = parameters["algorithm"]["value"] if \
                parameters["algorithm"]["present"] else None
    else:
        if converted in GROUP_TYPES:
            raise common.EvidenceError(
                f"group converted type is present on a leaf: {converted}")
        logical = CONVERTED_LOGICAL.get(converted, "NONE")
        if converted == "DECIMAL":
            if raw_scale is None or raw_precision is None:
                raise common.EvidenceError(
                    "legacy DECIMAL lacks precision or scale")
            scale = raw_scale
            precision = raw_precision
        elif converted in INTEGER_CONVERTED:
            bit_width, is_signed = INTEGER_CONVERTED[converted]
        elif converted in TIME_CONVERTED:
            expected, time_unit = TIME_CONVERTED[converted]
            if logical != expected:
                raise common.EvidenceError("legacy temporal type is inconsistent")
            adjusted = True
    if logical != "DECIMAL" and (raw_scale is not None or raw_precision is not None):
        raise common.EvidenceError(
            "non-DECIMAL leaf carries precision or scale")
    required = _required_converted(logical, bit_width, is_signed, time_unit)
    if converted != required:
        raise common.EvidenceError(
            "logical type lacks its exact compatible converted type")
    if type_length is not None and not INT32_MIN <= type_length <= INT32_MAX:
        raise common.EvidenceError("type_length is outside signed Int32")
    return {
        "physical_type": physical,
        "logical_type": logical,
        "converted_type": converted,
        "type_length": type_length,
        "precision": precision,
        "scale": scale,
        "bit_width": bit_width,
        "is_signed": is_signed,
        "time_unit": time_unit,
        "is_adjusted_to_utc": adjusted,
        "crs": crs,
        "geography_algorithm": algorithm,
    }


def normalize_order(raw):
    if raw is None:
        return {
            "state": "ABSENT",
            "field_id": None,
            "wire_type": None,
            "header_hex": None,
        }
    states = {
        "known": raw["member"],
        "unknown": "UNKNOWN",
        "wrong_type": "WRONG_TYPE",
        "empty": "EMPTY",
    }
    state = states.get(raw["state"])
    if state not in ("TYPE_ORDER", "IEEE_754_TOTAL_ORDER", "UNKNOWN",
            "WRONG_TYPE", "EMPTY"):
        raise common.EvidenceError("raw ColumnOrder state is inconsistent")
    return {
        "state": state,
        "field_id": raw["field_id"],
        "wire_type": raw["wire_type"],
        "header_hex": raw["header_hex"],
    }


def _load_json(payload, label):
    try:
        return json.loads(payload.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as error:
        raise common.EvidenceError(f"invalid JSON input: {label}: {error}") from error


def parse_corpus_manifest(payload):
    if not isinstance(payload, bytes) or not payload:
        raise common.EvidenceError("corpus manifest is empty or not bytes")
    lines = payload.splitlines(keepends=True)
    if len(lines) > 64:
        raise common.EvidenceError("corpus manifest exceeds its record limit")
    result = {}
    order = []
    for line in lines:
        match = CORPUS_LINE.fullmatch(line)
        if match is None:
            raise common.EvidenceError("corpus manifest row is not canonical")
        digest = match.group(1).decode("ascii")
        path = match.group(2).decode("ascii")
        if path in result:
            raise common.EvidenceError(f"duplicate corpus path: {path}")
        result[path] = digest
        order.append(path)
    if order != sorted(order):
        raise common.EvidenceError("corpus manifest paths are not sorted")
    return result


def load_context():
    root = common.repository_root()
    n6 = common.n6_root()
    paths = {
        "manifest": n6 / "manifest.toml",
        "capabilities": n6 / "capabilities.toml",
        "fixtures": n6 / "fixtures.toml",
        "corpus": n6 / "corpus-files.sha256",
        "schema": n6 / "evidence.schema.json",
        "raw_schema": n6 / "oracles" / "raw-java" / "evidence.schema.json",
    }
    payloads = {"manifest": common.read_file_bytes(paths["manifest"],
        2 * 1024 * 1024)}
    manifest = common.parse_toml_bytes(payloads["manifest"], paths["manifest"])
    common.verify_python_toolchain(manifest)
    plan = root / manifest["plan_file"]
    paths["plan"] = plan
    for name, maximum in (
            ("capabilities", 2 * 1024 * 1024),
            ("fixtures", 2 * 1024 * 1024),
            ("corpus", 64 * 1024),
            ("schema", 2 * 1024 * 1024),
            ("raw_schema", 2 * 1024 * 1024),
            ("plan", 2 * 1024 * 1024)):
        payloads[name] = common.read_file_bytes(paths[name], maximum)
    expected_hashes = {
        "capabilities": manifest["capabilities_sha256"],
        "fixtures": manifest["fixture_manifest_sha256"],
        "corpus": manifest["corpus_manifest_sha256"],
        "schema": manifest["evidence_schema_sha256"],
        "plan": manifest["plan_sha256"],
    }
    for name, expected in expected_hashes.items():
        if common.sha256_bytes(payloads[name]) != expected:
            raise common.EvidenceError(
                f"{name} hash does not match the manifest")
    capabilities = common.parse_toml_bytes(
        payloads["capabilities"], paths["capabilities"])
    fixtures = common.parse_toml_bytes(payloads["fixtures"], paths["fixtures"])
    if capabilities.get("plan_sha256") != manifest["plan_sha256"] or \
            capabilities.get("fixture_manifest") != paths["fixtures"].name or \
            capabilities.get("unsupported_is_pass") is not False:
        raise common.EvidenceError("capability matrix boundary is inconsistent")
    if fixtures.get("checksum_manifest") != paths["corpus"].name:
        raise common.EvidenceError("fixture corpus boundary is inconsistent")
    corpus = parse_corpus_manifest(payloads["corpus"])
    apache = [item for item in fixtures["fixture"]
        if item["source_kind"] == "apache-corpus"]
    if any(item["status"] != "verified" or \
            item["authority"] != fixtures["authority"] or \
            item["source_revision"] != fixtures["source_revision"]
            for item in apache):
        raise common.EvidenceError("Apache corpus fixture authority is inconsistent")
    fixture_corpus = {item["file"]: item["sha256"] for item in apache}
    if len(fixture_corpus) != len(apache) or corpus != fixture_corpus:
        raise common.EvidenceError("fixture files differ from the corpus manifest")
    raw_entries = [item for item in manifest["frozen_evidence"]
        if item["id"] == RAW_EVIDENCE_ID]
    if len(raw_entries) != 1:
        raise common.EvidenceError("raw evidence manifest entry is ambiguous")
    raw_entry = raw_entries[0]
    if raw_entry["status"] != "verified" or \
            raw_entry.get("storage") != "gate-generated":
        raise common.EvidenceError("raw evidence identity is not verified")
    if raw_entry["schema_file"] != \
            "test/conformance/n6/oracles/raw-java/evidence.schema.json":
        raise common.EvidenceError("raw evidence schema path is unexpected")
    if common.sha256_bytes(payloads["raw_schema"]) != raw_entry["schema_sha256"]:
        raise common.EvidenceError("raw schema hash does not match the manifest")
    raw_schema = _load_json(payloads["raw_schema"], paths["raw_schema"])
    normalized_schema = _load_json(payloads["schema"], paths["schema"])
    raw_class = jsonschema.validators.validator_for(raw_schema)
    raw_class.check_schema(raw_schema)
    normalized_class = jsonschema.validators.validator_for(normalized_schema)
    normalized_class.check_schema(normalized_schema)
    return {
        "root": root,
        "paths": paths,
        "manifest": manifest,
        "capabilities": capabilities,
        "fixtures": fixtures,
        "raw_entry": raw_entry,
        "snapshots": {paths[name]: common.sha256_bytes(payload)
            for name, payload in payloads.items()},
        "raw_validator": raw_class(raw_schema),
        "normalized_validator": normalized_class(normalized_schema),
    }


def _authority(context):
    matches = [item for item in context["capabilities"]["authority"]
        if item["id"] == PRODUCER]
    if len(matches) != 1:
        raise common.EvidenceError("raw authority is ambiguous")
    authority = matches[0]
    entry = context["raw_entry"]
    if authority["version"] != "parquet-2.13-raw-footer-v3" or \
            authority["revision"] != FORMAT_COMMIT or \
            entry["authority"] != PRODUCER or \
            authority["toolchain_sha256"] != [entry["toolchain_sha256"]]:
        raise common.EvidenceError("raw authority identity is inconsistent")
    return authority


def fixture_cases(fixtures, fixture_set):
    if fixture_set == "apache":
        return [item for item in fixtures["fixture"]
            if item["source_kind"] == "apache-corpus"]
    if fixture_set != "generated":
        raise common.EvidenceError(f"unknown fixture set: {fixture_set}")
    cases = []
    for source in fixtures["generated_case"]:
        if source["status"] != "verified" or \
                source["output_identity_status"] != "verified":
            raise common.EvidenceError(
                f"generated fixture identity is not verified: {source['id']}")
        case = dict(source)
        case["file"] = source["output_file"]
        case["sha256"] = source["output_sha256"]
        case["size"] = source["output_size"]
        cases.append(case)
    return cases


def _fixture_maps(fixtures, fixture_set):
    cases = fixture_cases(fixtures, fixture_set)
    by_name = {}
    ids = set()
    for case in cases:
        if case["id"] in ids:
            raise common.EvidenceError(f"duplicate fixture ID: {case['id']}")
        ids.add(case["id"])
        name = pathlib.PurePosixPath(case["file"]).name
        if name in by_name:
            raise common.EvidenceError(f"duplicate fixture basename: {name}")
        by_name[name] = case
    return cases, by_name


def validate_normalized_payload(payload, context, upstream_payloads=()):
    validator = context["root"] / "test" / "conformance" / "n6" / \
        "validate_evidence.py"
    temporaries = []
    for value in (*upstream_payloads, payload):
        with tempfile.NamedTemporaryFile(prefix="parquet-n6-normalized-",
                suffix=".jsonl", delete=False) as stream:
            temporary = pathlib.Path(stream.name)
            stream.write(value)
            stream.flush()
            os.fsync(stream.fileno())
        temporaries.append(temporary)
    try:
        arguments = [
            sys.executable,
            "-B",
            "-I",
            str(validator),
            "--schema", str(context["paths"]["schema"]),
            "--manifest", str(context["paths"]["manifest"]),
            "--capabilities", str(context["paths"]["capabilities"]),
            "--fixtures", str(context["paths"]["fixtures"]),
            *(str(temporary) for temporary in temporaries),
        ]
        process = subprocess.run(arguments, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, timeout=60, check=False)
        if len(process.stdout) > 64 * 1024 or len(process.stderr) > 64 * 1024:
            raise common.EvidenceError("normalized validator output exceeds its limit")
        if process.returncode != 0:
            detail = process.stderr.decode("utf-8", errors="replace")
            raise common.EvidenceError(
                f"normalized output failed semantic validation: {detail}")
    except (OSError, subprocess.TimeoutExpired) as error:
        raise common.EvidenceError(
            f"normalized validator could not run: {error}") from error
    finally:
        for temporary in temporaries:
            temporary.unlink(missing_ok=True)
    return None


def _check_leaf(raw, ordinal):
    if raw["ordinal"] != ordinal:
        raise common.EvidenceError("schema leaf ordinals are not contiguous")
    if not raw["path"]:
        raise common.EvidenceError("schema leaf path is empty")
    return None


def _orders(metadata, leaves):
    raw = metadata["column_orders"]
    if not raw["present"]:
        return [None for _ in leaves]
    if raw["count"] != len(leaves) or len(raw["values"]) != len(leaves):
        raise common.EvidenceError("ColumnOrder vector is not leaf aligned")
    orders = []
    for ordinal, (order, leaf) in enumerate(zip(raw["values"], leaves)):
        if order["ordinal"] != ordinal or order["schema_leaf_ordinal"] != ordinal:
            raise common.EvidenceError("ColumnOrder ordinals are not leaf aligned")
        if order["path"] != leaf["path"] or \
                order["physical_type"] != leaf["physical_type"] or \
                order["logical_type"] != leaf["logical_type"]["member"]:
            raise common.EvidenceError("ColumnOrder schema facts are inconsistent")
        orders.append(order)
    return orders


def normalize_statistics(raw):
    if len(raw["fields"]) != len(STATISTIC_FIELDS):
        raise common.EvidenceError("Statistics field count is inconsistent")
    output = {
        "has_statistics": raw["present"],
        "deprecated_min_hex": None,
        "deprecated_max_hex": None,
        "min_value_hex": None,
        "max_value_hex": None,
        "is_min_value_exact": None,
        "is_max_value_exact": None,
        "null_count": None,
        "distinct_count": None,
        "nan_count": None,
        "unknown_statistics_field_ids": [],
    }
    for field, expected in zip(raw["fields"], STATISTIC_FIELDS):
        field_id, name, target = expected
        if field["field_id"] != field_id or field["name"] != name:
            raise common.EvidenceError("Statistics field order is inconsistent")
        value = field["value"]
        if field["present"] and isinstance(value, dict):
            if value["byte_length"] * 2 != len(value["hex"]):
                raise common.EvidenceError("binary Statistics length is inconsistent")
            value = value["hex"]
        if field["present"] and target in (
                "null_count", "distinct_count", "nan_count"):
            if not isinstance(value, int) or isinstance(value, bool) or \
                    not -(1 << 63) <= value <= INT64_MAX:
                raise common.EvidenceError(
                    f"{target} is outside signed Int64")
            value = str(value)
        output[target] = value if field["present"] else None
    if not raw["present"] and any(value is not None
            for key, value in output.items()
            if key not in ("has_statistics", "unknown_statistics_field_ids")):
        raise common.EvidenceError("absent Statistics carries a value")
    return output


def _column_record(case, leaves, orders, row_group, column):
    ordinal = column["schema_leaf_ordinal"]
    if not column["metadata_present"] or not isinstance(ordinal, int) or \
            not 0 <= ordinal < len(leaves):
        raise common.EvidenceError("column metadata does not map to a schema leaf")
    leaf = leaves[ordinal]
    if column["ordinal"] != ordinal or column["path"] != leaf["path"] or \
            column["physical_type"] != leaf["physical_type"] or \
            column["logical_type"] != leaf["logical_type"]["member"]:
        raise common.EvidenceError("column and schema leaf facts are inconsistent")
    num_values = column["num_values"]
    if not isinstance(num_values, int) or isinstance(num_values, bool) or \
            not 0 <= num_values <= INT64_MAX:
        raise common.EvidenceError("column num_values is outside signed Int64")
    record = {
        "record": "column_statistics",
        "schema_version": 2,
        "case_id": case["id"],
        "file": case["file"],
        "row_group": row_group["ordinal"],
        "leaf": ordinal,
        "path": leaf["path"],
        "leaf_schema": effective_leaf(leaf),
        "column_order": normalize_order(orders[ordinal]),
        "num_values": str(num_values),
    }
    record.update(normalize_statistics(column["statistics"]))
    return record


def _normalize_case(case, raw):
    if raw["file"] != pathlib.PurePosixPath(case["file"]).name or \
            raw["file_sha256"] != case["sha256"] or \
            raw["file_size"] != case["size"]:
        raise common.EvidenceError(f"raw file identity mismatch: {case['id']}")
    metadata = raw["file_metadata"]
    leaves = metadata["schema_leaves"]
    if metadata["schema_leaf_count"] != len(leaves) or \
            len(leaves) != case["leaf_count"]:
        raise common.EvidenceError(f"raw leaf count mismatch: {case['id']}")
    for ordinal, leaf in enumerate(leaves):
        _check_leaf(leaf, ordinal)
    if len({tuple(leaf["path"]) for leaf in leaves}) != len(leaves):
        raise common.EvidenceError(f"raw leaf paths are not unique: {case['id']}")
    row_groups = metadata["row_groups"]
    if metadata["row_group_count"] != len(row_groups) or \
            len(row_groups) != case["row_group_count"]:
        raise common.EvidenceError(f"raw row-group count mismatch: {case['id']}")
    if metadata["num_rows"] != sum(group["num_rows"] for group in row_groups):
        raise common.EvidenceError(f"raw row counts are inconsistent: {case['id']}")
    orders = _orders(metadata, leaves)
    file_record = {
        "record": "file",
        "schema_version": 2,
        "case_id": case["id"],
        "file": case["file"],
        "sha256": case["sha256"],
        "size": case["size"],
        "footer_length": raw["footer_length"],
        "row_group_count": case["row_group_count"],
        "leaf_count": case["leaf_count"],
        "column_order_count": len(orders)
            if metadata["column_orders"]["present"] else None,
        "created_by_present": metadata["created_by"]["present"],
        "created_by": metadata["created_by"]["value"],
    }
    columns = []
    for ordinal, row_group in enumerate(row_groups):
        if row_group["ordinal"] != ordinal or \
                row_group["column_count"] != len(row_group["columns"]) or \
                len(row_group["columns"]) != len(leaves):
            raise common.EvidenceError(
                f"raw row-group topology mismatch: {case['id']}")
        mapped = [_column_record(case, leaves, orders,
            row_group, column) for column in row_group["columns"]]
        if sorted(record["leaf"] for record in mapped) != list(range(len(leaves))):
            raise common.EvidenceError(
                f"raw row-group leaves are incomplete: {case['id']}")
        columns.extend(sorted(mapped, key=lambda record: record["leaf"]))
    expected = 1 + len(columns)
    if case["normalized_record_count"] != expected:
        raise common.EvidenceError(
            f"normalized topology count mismatch: {case['id']}")
    return file_record, columns


def _run_record(context, evidence_id, unsupported_cases):
    authority = _authority(context)
    if len(authority["toolchain_sha256"]) != 1:
        raise common.EvidenceError("raw authority must bind one toolchain")
    manifest = context["manifest"]
    paths = context["paths"]
    return {
        "record": "run",
        "schema_version": 2,
        "evidence_id": evidence_id,
        "producer": PRODUCER,
        "producer_version": authority["version"],
        "source_revision": authority["revision"],
        "plan_sha256": manifest["plan_sha256"],
        "capabilities_sha256": context["snapshots"][paths["capabilities"]],
        "fixture_manifest_sha256": context["snapshots"][paths["fixtures"]],
        "corpus_manifest_sha256": context["snapshots"][paths["corpus"]],
        "evidence_schema_sha256": context["snapshots"][paths["schema"]],
        "toolchain_sha256": authority["toolchain_sha256"][0],
        "unsupported_cases": unsupported_cases,
    }


def _claims(context):
    authority = _authority(context)
    claims = {}
    for claim in authority.get("claim", []):
        if claim["capability"] not in WIRE_CAPABILITIES or \
                claim["status"] not in ("verified", "planned"):
            continue
        for case_id in claim["cases"]:
            key = (case_id, claim["capability"])
            if key in claims:
                raise common.EvidenceError(f"duplicate raw claim: {key}")
            claims[key] = claim["status"]
    return claims


def _unsupported_claims(context):
    claims = []
    seen = set()
    for claim in _authority(context).get("claim", []):
        if claim["status"] != "unsupported":
            continue
        for case_id in claim["cases"]:
            key = (case_id, claim["capability"])
            if key in seen:
                raise common.EvidenceError(
                    f"duplicate unsupported raw claim: {key}")
            seen.add(key)
            claims.append(key)
    return sorted(claims)


def _result(case, capability, file_record, columns):
    observations = [{
        "file": file_record,
        "columns": columns,
    }]
    digest = common.capability_digest(case["id"], capability, observations)
    return {
        "record": "case_result",
        "schema_version": 2,
        "case_id": case["id"],
        "capability_id": capability,
        "digest_contract": case.get(
            "digest_contract", "n6-capability-result-sha256-v1"),
        "status": "PASS",
        "expected_sha256": digest,
        "actual_sha256": digest,
        "detail": "Exact raw Parquet 2.13 wire facts normalized without semantic interpretation.",
    }


def _unsupported_result(case_id, capability):
    return {
        "record": "case_result",
        "schema_version": 2,
        "case_id": case_id,
        "capability_id": capability,
        "digest_contract": "n6-capability-result-sha256-v1",
        "status": "UNSUPPORTED",
        "expected_sha256": None,
        "actual_sha256": None,
        "detail": "The reviewed capability matrix marks this result unsupported.",
    }


def _generated_raw_entry(context):
    matches = [item for item in context["manifest"]["frozen_evidence"]
        if item["id"] == GENERATED_RAW_EVIDENCE_ID]
    if len(matches) > 1:
        raise common.EvidenceError("generated raw evidence entry is ambiguous")
    if not matches:
        return {
            "id": GENERATED_RAW_EVIDENCE_ID,
            "status": "verified",
            "authority": PRODUCER,
            "storage": "gate-generated",
            "schema_file":
                "test/conformance/n6/oracles/raw-java/evidence.schema.json",
            "schema_sha256": context["raw_entry"]["schema_sha256"],
            "toolchain_sha256": context["raw_entry"]["toolchain_sha256"],
            "case_count": GENERATED_RAW_CASE_COUNT,
            "record_count": GENERATED_RAW_RECORD_COUNT,
            "sha256": GENERATED_RAW_SHA256,
        }
    entry = matches[0]
    expected = {
        "status": "verified",
        "authority": PRODUCER,
        "storage": "gate-generated",
        "schema_file":
            "test/conformance/n6/oracles/raw-java/evidence.schema.json",
        "schema_sha256": context["raw_entry"]["schema_sha256"],
        "toolchain_sha256": context["raw_entry"]["toolchain_sha256"],
        "case_count": GENERATED_RAW_CASE_COUNT,
        "record_count": GENERATED_RAW_RECORD_COUNT,
        "sha256": GENERATED_RAW_SHA256,
    }
    if any(entry.get(field) != value for field, value in expected.items()):
        raise common.EvidenceError("generated raw evidence identity is inconsistent")
    return entry


def _profile(context, fixture_set):
    if fixture_set == "apache":
        return context["raw_entry"], EVIDENCE_ID
    if fixture_set == "generated":
        return _generated_raw_entry(context), GENERATED_EVIDENCE_ID
    raise common.EvidenceError(f"unknown fixture set: {fixture_set}")


def render_raw_evidence(raw_path, fixture_set="apache"):
    context = load_context()
    entry, evidence_id = _profile(context, fixture_set)
    limits = context["manifest"]["evidence_limits"]
    raw_payload = common.read_file_bytes(raw_path, limits["max_file_bytes"])
    if common.sha256_bytes(raw_payload) != entry["sha256"]:
        raise common.EvidenceError("raw evidence hash does not match its frozen identity")
    records = common.parse_jsonl_bytes(raw_payload, raw_path,
        limits["max_line_bytes"], entry["record_count"])
    if len(records) != entry["record_count"]:
        raise common.EvidenceError("raw evidence record count is incomplete")
    by_file = {}
    for record in records:
        context["raw_validator"].validate(record)
        if record["evidence_version"] != "parquet-2.13-raw-footer-v3" or \
                record["format_commit"] != FORMAT_COMMIT or \
                record["thrift_version"] != "0.23.0":
            raise common.EvidenceError("raw evidence version is unexpected")
        if record["file"] in by_file:
            raise common.EvidenceError(f"duplicate raw file: {record['file']}")
        by_file[record["file"]] = record
    cases, fixture_by_name = _fixture_maps(context["fixtures"], fixture_set)
    if set(by_file) != set(fixture_by_name):
        raise common.EvidenceError("raw evidence file set differs from the fixture corpus")
    if len(cases) != entry["case_count"]:
        raise common.EvidenceError("raw evidence case count differs from its identity")
    claims = _claims(context)
    unsupported = _unsupported_claims(context) if fixture_set == "apache" else []
    unsupported_cases = sorted({case_id for case_id, _ in unsupported})
    output = [_run_record(context, evidence_id, unsupported_cases)]
    for case in cases:
        raw = by_file[pathlib.PurePosixPath(case["file"]).name]
        file_record, columns = _normalize_case(case, raw)
        output.append(file_record)
        output.extend(columns)
        capabilities = sorted(capability for capability in case["capabilities"]
            if (case["id"], capability) in claims)
        output.extend(_result(case, capability, file_record, columns)
            for capability in capabilities)
    output.extend(_unsupported_result(case_id, capability)
        for case_id, capability in unsupported)
    for record in output:
        context["normalized_validator"].validate(record)
    payload = common.jsonl_bytes(output)
    if len(payload) > limits["max_file_bytes"]:
        raise common.EvidenceError("normalized raw evidence exceeds its byte limit")
    if len(output) > limits["max_records_per_input"]:
        raise common.EvidenceError("normalized raw evidence exceeds its record limit")
    validate_normalized_payload(payload, context)
    return payload


def parse_arguments(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--fixture-set", choices=("apache", "generated"),
        default="apache")
    parser.add_argument("--check", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    try:
        args = parse_arguments(argv)
        payload = render_raw_evidence(args.input, args.fixture_set)
        context = load_context()
        common.ensure_snapshots(context["snapshots"])
        sources = (pathlib.Path(__file__), pathlib.Path(common.__file__))
        inputs = (args.input, *sources, *context["paths"].values())
        common.publish_bytes(args.output, payload, args.check, inputs)
        action = "is fresh" if args.check else "written"
        print(f"N6 {args.fixture_set} normalized raw evidence {action}: "
            f"{args.output}")
        return 0
    except (common.EvidenceError, OSError, ValueError,
            jsonschema.ValidationError) as error:
        print(f"N6 raw normalization failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
