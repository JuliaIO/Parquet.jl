#!/usr/bin/env python3
import argparse
import copy
import hashlib
import io
import json
import os
import pathlib
import re
import stat
import sys
import tempfile
import tomllib

import jsonschema

INT64_MIN = -(1 << 63)
INT64_MAX = (1 << 63) - 1
INT32_MAX = (1 << 31) - 1
STATISTIC_FIELDS = (
    "deprecated_min_hex",
    "deprecated_max_hex",
    "min_value_hex",
    "max_value_hex",
    "is_min_value_exact",
    "is_max_value_exact",
    "null_count",
    "distinct_count",
    "nan_count",
)
COLUMN_EVIDENCE_CAPABILITIES = {
    "wire.column-order.type",
    "wire.column-order.ieee",
    "wire.column-order.empty",
    "wire.statistics.deprecated-bounds",
    "wire.statistics.modern-bounds",
    "wire.statistics.exactness",
    "wire.statistics.counts",
    "wire.statistics.nan-count",
    "semantic.type-order",
    "semantic.ieee-total-order",
    "semantic.logical-order",
    "semantic.count-state",
    "semantic.producer-trust",
    "write.type-order",
    "write.statistics-disabled",
    "write.statistics-limit",
    "compat.legacy-statistics",
}
COMPACT_TO_TTYPE = {
    1: 2,
    2: 2,
    3: 3,
    4: 6,
    5: 8,
    6: 10,
    7: 4,
    8: 11,
    9: 15,
    10: 14,
    11: 13,
    12: 12,
    13: 16,
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
GROUP_CONVERTED_TYPES = {"MAP", "MAP_KEY_VALUE", "LIST"}
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
    "TIME_MILLIS": "MILLIS",
    "TIME_MICROS": "MICROS",
    "TIMESTAMP_MILLIS": "MILLIS",
    "TIMESTAMP_MICROS": "MICROS",
}


class EvidenceError(ValueError):
    pass


CONTROL_FILE_LIMIT = 4 * 1024 * 1024


def metadata_identity(metadata):
    return (metadata.st_dev, metadata.st_ino, metadata.st_mode,
        metadata.st_nlink, metadata.st_size, metadata.st_mtime_ns,
        metadata.st_ctime_ns)


def stable_regular_bytes(path, maximum_bytes, label, *, allow_empty=True):
    path = pathlib.Path(path)
    metadata = path.lstat()
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise EvidenceError(f"{label} is not a regular file")
    minimum = 0 if allow_empty else 1
    if not minimum <= metadata.st_size <= maximum_bytes:
        raise EvidenceError(f"{label} has an invalid byte size")
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise EvidenceError(f"cannot open {label} safely") from error
    with os.fdopen(descriptor, "rb") as stream:
        opened = os.fstat(stream.fileno())
        if metadata_identity(opened) != metadata_identity(metadata):
            raise EvidenceError(f"{label} changed while it was opened")
        value = stream.read(maximum_bytes + 1)
        if len(value) > maximum_bytes or len(value) != opened.st_size or \
                stream.read(1):
            raise EvidenceError(f"{label} changed size while it was read")
        final = os.fstat(stream.fileno())
        if metadata_identity(final) != metadata_identity(opened):
            raise EvidenceError(f"{label} changed while it was read")
    current = path.lstat()
    if metadata_identity(current) != metadata_identity(opened):
        raise EvidenceError(f"{label} path changed while it was read")
    return value


def sha256_bytes(value):
    return hashlib.sha256(value).hexdigest()


def safe_relative(value):
    if not isinstance(value, str) or not value or "\\" in value:
        return False
    if any(not character.isascii() or not (
            character.isalnum() or character in "._+@=-/")
            for character in value):
        return False
    path = pathlib.PurePosixPath(value)
    return not path.is_absolute() and all(part not in ("", ".", "..")
        for part in path.parts) and path.as_posix() == value


def safe_evidence_file(value):
    prefix = "test/conformance/n6/evidence/"
    return safe_relative(value) and value.startswith(prefix) and \
        len(value) > len(prefix) and pathlib.PurePosixPath(value).name.endswith(
        ".jsonl") and pathlib.PurePosixPath(value).name != ".jsonl"


def manifest_evidence_entry(manifest, evidence_id):
    matches = []
    for section in ("frozen_evidence", "planned_evidence"):
        matches.extend(entry for entry in manifest.get(section, [])
            if entry["id"] == evidence_id)
    if len(matches) != 1:
        raise EvidenceError(
            f"manifest evidence entry is absent or ambiguous: {evidence_id}")
    return matches[0]


def repository_root(manifest_path):
    manifest_file = pathlib.Path(manifest_path).resolve(strict=True)
    try:
        root = manifest_file.parents[3]
    except IndexError as error:
        raise EvidenceError("manifest path is outside the N6 repository layout") from error
    if root.joinpath("test", "conformance", "n6", "manifest.toml") != manifest_file:
        raise EvidenceError("manifest path is outside the canonical N6 location")
    return root


def manifest_input_file(root, relative):
    if not safe_relative(relative):
        raise EvidenceError(f"manifest input path is unsafe: {relative}")
    candidate = root.joinpath(*pathlib.PurePosixPath(relative).parts)
    if not candidate.is_file():
        raise EvidenceError(f"manifest input file is absent: {relative}")
    current = root
    for part in pathlib.PurePosixPath(relative).parts:
        current = current / part
        if current.is_symlink():
            raise EvidenceError(f"manifest input contains a symbolic link: {relative}")
    try:
        candidate.resolve(strict=True).relative_to(root)
    except ValueError as error:
        raise EvidenceError(f"manifest input escapes the repository: {relative}") from error
    return candidate


def toolchain_status(manifest, digest):
    owners = [toolchain["status"] for toolchain in manifest["toolchain"]
        if any(artifact["sha256"] == digest for artifact in toolchain["artifacts"])]
    if len(owners) != 1:
        raise EvidenceError("producer toolchain digest has ambiguous manifest ownership")
    return owners[0]


def load_semantic_cases(root, manifest, capabilities, fixtures):
    relative = "test/conformance/n6/model/cases.toml"
    entries = [item for item in manifest["frozen_model"]
        if item["file"] == relative]
    if len(entries) != 1:
        raise EvidenceError("semantic case manifest is absent or ambiguous")
    path = manifest_input_file(root, relative)
    value = stable_regular_bytes(path, CONTROL_FILE_LIMIT,
        "N6 semantic case manifest", allow_empty=False)
    if sha256_bytes(value) != entries[0]["sha256"]:
        raise EvidenceError("semantic case manifest hash differs")
    try:
        model_cases = tomllib.loads(value.decode("utf-8"))
    except (UnicodeError, tomllib.TOMLDecodeError) as error:
        raise EvidenceError("semantic case manifest is invalid TOML") from error
    if model_cases.get("schema_version") != 1 or \
            not isinstance(model_cases.get("case_groups"), list):
        raise EvidenceError("semantic case manifest header differs")
    capability_ids = {item["id"] for item in capabilities["capability"]}
    contracts = set(fixtures["digest_contracts"])
    result = []
    identifiers = set()
    for case in model_cases["case_groups"]:
        required = {"id", "requirements", "capabilities", "digest_contract",
            "expected_sha256"}
        if not isinstance(case, dict) or set(case) != required:
            raise EvidenceError("semantic case fields differ")
        identifier = case["id"]
        if not isinstance(identifier, str) or \
                re.fullmatch(r"[a-z0-9]+(?:[._-][a-z0-9]+)*", identifier) is None or \
                identifier in identifiers:
            raise EvidenceError("semantic case ID is invalid or duplicated")
        identifiers.add(identifier)
        requirements = case["requirements"]
        case_capabilities = case["capabilities"]
        expected = case["expected_sha256"]
        if not isinstance(requirements, list) or not requirements or \
                any(not isinstance(item, str) or not item for item in requirements):
            raise EvidenceError(f"semantic case requirements differ: {identifier}")
        if not isinstance(case_capabilities, list) or \
                case_capabilities != sorted(set(case_capabilities)) or \
                any(item not in capability_ids for item in case_capabilities):
            raise EvidenceError(f"semantic case capabilities differ: {identifier}")
        if case["digest_contract"] not in contracts or \
                not isinstance(expected, dict) or \
                set(expected) != set(case_capabilities) or \
                any(not isinstance(digest, str) or
                    re.fullmatch(r"[0-9a-f]{64}", digest) is None
                    for digest in expected.values()):
            raise EvidenceError(f"semantic case digests differ: {identifier}")
        result.append(case)
    claimed = {}
    fixture_ids = {item["id"] for item in
        fixtures["fixture"] + fixtures["generated_case"]}
    for authority in capabilities["authority"]:
        for claim in authority.get("claim", []):
            for case_id in claim["cases"]:
                if case_id in fixture_ids:
                    continue
                key = (case_id, claim["capability"])
                claimed.setdefault(key, set()).add(authority["id"])
    declared = {(case["id"], capability)
        for case in result for capability in case["capabilities"]}
    if set(claimed) != declared:
        raise EvidenceError("semantic case capability scope differs from claims")
    return result


def validate_model_producer_binding(root, manifest, capabilities):
    expected_relative = \
        "test/conformance/n6/normalizers/model-producer.toml"
    relative = manifest.get("model_producer_descriptor_file")
    expected_sha256 = manifest.get("model_producer_descriptor_sha256")
    if relative != expected_relative or not isinstance(expected_sha256, str):
        raise EvidenceError("model producer descriptor declaration differs")
    descriptor_path = manifest_input_file(root, relative)
    payload = stable_regular_bytes(descriptor_path, CONTROL_FILE_LIMIT,
        "N6 model producer descriptor", allow_empty=False)
    descriptor_sha256 = sha256_bytes(payload)
    if descriptor_sha256 != expected_sha256:
        raise EvidenceError("model producer descriptor hash differs")
    try:
        descriptor = tomllib.loads(payload.decode("utf-8"))
    except (UnicodeError, tomllib.TOMLDecodeError) as error:
        raise EvidenceError("model producer descriptor is invalid TOML") from error
    if set(descriptor) != {"descriptor_version", "producer", "file"} or \
            descriptor["descriptor_version"] != 1 or \
            descriptor["producer"] != "n6-independent-model" or \
            not isinstance(descriptor["file"], list):
        raise EvidenceError("model producer descriptor header differs")
    expected_files = {
        "test/conformance/n6/model/N6StatisticsModel.jl",
        "test/conformance/n6/model/README.md",
        "test/conformance/n6/model/cases.toml",
        "test/conformance/n6/model/runtests.jl",
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
            raise EvidenceError("model producer file entry differs")
        file_path = manifest_input_file(root, item["path"])
        file_bytes = stable_regular_bytes(file_path, 32 * 1024 * 1024,
            "N6 model producer file", allow_empty=False)
        if sha256_bytes(file_bytes) != item["sha256"]:
            raise EvidenceError(
                f"model producer file hash differs: {item['path']}")
        if item["path"] in producer_hashes:
            raise EvidenceError("model producer file is duplicated")
        producer_hashes[item["path"]] = item["sha256"]
    if set(producer_hashes) != expected_files:
        raise EvidenceError("model producer file set differs")
    frozen_models = {item["file"]: item["sha256"]
        for item in manifest["frozen_model"]}
    if len(frozen_models) != len(manifest["frozen_model"]) or any(
            producer_hashes.get(path) != digest
            for path, digest in frozen_models.items()):
        raise EvidenceError("model producer and frozen model differ")
    authorities = [item for item in capabilities["authority"]
        if item["id"] == "n6-independent-model"]
    if len(authorities) != 1 or \
            authorities[0]["revision"] != descriptor_sha256:
        raise EvidenceError("model authority does not bind its producer")
    return None


def evidence_limits(manifest, fixtures, capabilities):
    limits = manifest["evidence_limits"]
    required = {
        "max_inputs", "max_file_bytes", "max_total_bytes", "max_line_bytes",
        "max_records_per_input", "max_records_total",
    }
    if set(limits) != required or any(type(limits[name]) is not int
            for name in required):
        raise EvidenceError("manifest evidence limits have invalid keys or types")
    if not (0 < limits["max_inputs"] <= len(capabilities["authority"])):
        raise EvidenceError("manifest evidence input limit is invalid")
    if not (0 < limits["max_line_bytes"] <= limits["max_file_bytes"] <=
            limits["max_total_bytes"]):
        raise EvidenceError("manifest evidence byte limits are invalid")
    maximum_records = 1 + sum(case["normalized_record_count"] +
        len(case["capabilities"]) for case in
        fixtures["fixture"] + fixtures["generated_case"])
    maximum_records += sum(len(case["capabilities"])
        for case in fixtures["semantic_case"])
    if limits["max_records_per_input"] != maximum_records:
        raise EvidenceError("manifest per-input record limit is not fixture-derived")
    if limits["max_records_total"] != maximum_records * limits["max_inputs"]:
        raise EvidenceError("manifest total record limit is not input-derived")
    return limits


def parse_int64(value, name):
    parsed = int(value)
    if parsed < INT64_MIN or parsed > INT64_MAX:
        raise EvidenceError(f"{name} is outside signed Int64")
    return parsed


def fixed_decimal_precision(type_length):
    # This is the Parquet formula floor(log10(2^(8*n - 1) - 1)). The fixed
    # decimal constant has enough digits to be exact over the signed i32 width
    # accepted by the pinned IDL.
    log10_2 = int(
        "3010299956639811952137388947244930267681898814621085413104274611271081892744")
    scale = 10 ** 76
    return ((8 * type_length - 1) * log10_2) // scale


def validate_leaf_annotations(leaf):
    physical = leaf["physical_type"]
    logical = leaf["logical_type"]
    converted = leaf["converted_type"]
    type_length = leaf["type_length"]
    bit_width = leaf["bit_width"]
    is_signed = leaf["is_signed"]
    time_unit = leaf["time_unit"]
    if type_length is not None and not -(1 << 31) <= type_length <= INT32_MAX:
        raise EvidenceError("leaf has an invalid signed-i32 type length")
    if physical == "FIXED_LEN_BYTE_ARRAY" and (
            type_length is None or type_length <= 0):
        raise EvidenceError("fixed leaf lacks a positive signed-i32 type length")
    if logical in ("MAP", "LIST", "VARIANT"):
        raise EvidenceError(f"group logical type {logical} is present on a leaf")
    if converted in GROUP_CONVERTED_TYPES:
        raise EvidenceError(f"group converted type {converted} is present on a leaf")
    if converted is not None:
        expected_logical = CONVERTED_LOGICAL.get(converted)
        if expected_logical is None or logical != expected_logical:
            raise EvidenceError("logical and converted types are inconsistent")

    required_converted = {
        "STRING": "UTF8",
        "ENUM": "ENUM",
        "DECIMAL": "DECIMAL",
        "DATE": "DATE",
        "JSON": "JSON",
        "BSON": "BSON",
        "INTERVAL": "INTERVAL",
    }.get(logical)
    if logical == "INTEGER" and bit_width is not None and is_signed is not None:
        required_converted = ("INT_" if is_signed else "UINT_") + str(bit_width)
    if logical in ("TIME", "TIMESTAMP") and time_unit in ("MILLIS", "MICROS"):
        required_converted = logical + "_" + time_unit
    if converted != required_converted:
        raise EvidenceError("logical type lacks its exact compatible converted type")
    return None


def validate_decimal_leaf(leaf):
    physical = leaf["physical_type"]
    logical = leaf["logical_type"]
    type_length = leaf["type_length"]
    precision = leaf["precision"]
    scale = leaf["scale"]
    decimal = logical == "DECIMAL"
    if decimal:
        if precision is None or scale is None or scale > precision:
            raise EvidenceError("DECIMAL lacks valid precision and scale")
        if physical == "INT32":
            maximum_precision = 9
        elif physical == "INT64":
            maximum_precision = 18
        elif physical == "FIXED_LEN_BYTE_ARRAY":
            maximum_precision = fixed_decimal_precision(type_length)
        elif physical == "BYTE_ARRAY":
            maximum_precision = None
        else:
            raise EvidenceError("DECIMAL has an invalid physical type")
        if maximum_precision is not None and precision > maximum_precision:
            raise EvidenceError("DECIMAL precision exceeds its physical type")
    elif precision is not None or scale is not None:
        raise EvidenceError("non-DECIMAL leaf carries decimal parameters")
    return None


def validate_integer_leaf(leaf):
    physical = leaf["physical_type"]
    logical = leaf["logical_type"]
    converted = leaf["converted_type"]
    bit_width = leaf["bit_width"]
    is_signed = leaf["is_signed"]
    integer = logical == "INTEGER"
    if integer:
        if bit_width is None or is_signed is None:
            raise EvidenceError("INTEGER lacks width or signedness")
        expected_physical = "INT64" if bit_width == 64 else "INT32"
        if physical != expected_physical:
            raise EvidenceError("INTEGER width contradicts its physical type")
        if converted in INTEGER_CONVERTED and (
                bit_width, is_signed) != INTEGER_CONVERTED[converted]:
            raise EvidenceError("INTEGER parameters contradict its converted type")
    elif bit_width is not None or is_signed is not None:
        raise EvidenceError("non-INTEGER leaf carries integer parameters")
    return None


def validate_temporal_leaf(leaf):
    physical = leaf["physical_type"]
    logical = leaf["logical_type"]
    converted = leaf["converted_type"]
    time_unit = leaf["time_unit"]
    adjusted = leaf["is_adjusted_to_utc"]
    temporal = logical in ("TIME", "TIMESTAMP")
    if temporal:
        if time_unit is None or adjusted is None:
            raise EvidenceError("temporal leaf lacks unit or UTC adjustment")
        if logical == "TIME":
            expected_physical = "INT32" if time_unit == "MILLIS" else "INT64"
        else:
            expected_physical = "INT64"
        if physical != expected_physical:
            raise EvidenceError("temporal unit contradicts its physical type")
        if converted in TIME_CONVERTED:
            if time_unit != TIME_CONVERTED[converted]:
                raise EvidenceError("temporal parameters contradict its converted type")
    elif time_unit is not None or adjusted is not None:
        raise EvidenceError("non-temporal leaf carries temporal parameters")
    return None


def validate_leaf_physical_type(leaf):
    physical = leaf["physical_type"]
    logical = leaf["logical_type"]
    converted = leaf["converted_type"]
    type_length = leaf["type_length"]
    physical_by_logical = {
        "STRING": "BYTE_ARRAY",
        "ENUM": "BYTE_ARRAY",
        "DATE": "INT32",
        "JSON": "BYTE_ARRAY",
        "BSON": "BYTE_ARRAY",
        "UUID": "FIXED_LEN_BYTE_ARRAY",
        "FLOAT16": "FIXED_LEN_BYTE_ARRAY",
        "INTERVAL": "FIXED_LEN_BYTE_ARRAY",
        "GEOMETRY": "BYTE_ARRAY",
        "GEOGRAPHY": "BYTE_ARRAY",
    }
    expected_physical = physical_by_logical.get(logical)
    if expected_physical is not None and physical != expected_physical:
        raise EvidenceError(f"{logical} has an invalid physical type")
    expected_lengths = {"UUID": 16, "FLOAT16": 2, "INTERVAL": 12}
    if logical in expected_lengths and type_length != expected_lengths[logical]:
        raise EvidenceError(f"{logical} has an invalid fixed width")
    if logical == "INTERVAL" and converted != "INTERVAL":
        raise EvidenceError("INTERVAL lacks its required converted type")
    if logical == "NONE" and converted is not None:
        raise EvidenceError("NONE logical type carries a converted annotation")
    return None


def validate_geospatial_leaf(leaf):
    logical = leaf["logical_type"]
    crs = leaf["crs"]
    geography_algorithm = leaf["geography_algorithm"]
    geospatial = logical in ("GEOMETRY", "GEOGRAPHY")
    if geospatial:
        if logical == "GEOMETRY" and geography_algorithm is not None:
            raise EvidenceError("GEOMETRY carries a geography algorithm")
    elif crs is not None or geography_algorithm is not None:
        raise EvidenceError("non-geospatial leaf carries geospatial parameters")
    return None


def validate_leaf_schema(leaf):
    validate_leaf_annotations(leaf)
    validate_decimal_leaf(leaf)
    validate_integer_leaf(leaf)
    validate_temporal_leaf(leaf)
    validate_leaf_physical_type(leaf)
    validate_geospatial_leaf(leaf)
    return None


def authority_map(capabilities):
    result = {}
    for authority in capabilities["authority"]:
        if authority["id"] in result:
            raise EvidenceError(f"duplicate authority {authority['id']}")
        result[authority["id"]] = authority
    return result


def case_map(fixtures):
    result = {}
    for fixture in fixtures["fixture"]:
        if fixture["id"] in result:
            raise EvidenceError(f"duplicate fixture {fixture['id']}")
        expected = 1 + fixture["row_group_count"] * fixture["leaf_count"]
        if fixture["normalized_record_count"] != expected:
            raise EvidenceError(f"invalid normalized record count for {fixture['id']}")
        result[fixture["id"]] = dict(fixture, generated=False, semantic=False,
            digest_contract=fixtures["default_digest_contract"])
    for generated in fixtures["generated_case"]:
        if generated["id"] in result:
            raise EvidenceError(f"duplicate case {generated['id']}")
        expected = 1 + generated["row_group_count"] * generated["leaf_count"]
        if generated["normalized_record_count"] != expected:
            raise EvidenceError(f"invalid normalized record count for {generated['id']}")
        result[generated["id"]] = dict(generated, generated=True, semantic=False,
            file=generated["output_file"])
    for semantic in fixtures["semantic_case"]:
        if semantic["id"] in result:
            raise EvidenceError(f"duplicate semantic case {semantic['id']}")
        result[semantic["id"]] = dict(semantic, generated=False, semantic=True)
    return result


def claim_statuses(authority):
    pairs = {}
    for claim in authority.get("claim", []):
        for case_id in claim["cases"]:
            key = (case_id, claim["capability"])
            if key in pairs:
                raise EvidenceError(f"duplicate authority claim {key}")
            pairs[key] = claim["status"]
    return pairs


def decode_compact_header(header):
    raw = bytes.fromhex(header)
    first = raw[0]
    compact_type = first & 0x0F
    delta = first >> 4
    if compact_type == 0:
        if raw != b"\x00":
            raise EvidenceError("STOP ColumnOrder header has trailing bytes")
        return None, None
    if compact_type not in COMPACT_TO_TTYPE:
        raise EvidenceError("ColumnOrder header has an invalid Compact-Thrift type")
    if delta:
        if len(raw) != 1:
            raise EvidenceError("delta ColumnOrder header has trailing bytes")
        field_id = delta
    else:
        encoded = raw[1:]
        if not encoded or len(encoded) > 3 or encoded[-1] & 0x80:
            raise EvidenceError("ColumnOrder field ID varint is incomplete")
        value = 0
        shift = 0
        for index, byte in enumerate(encoded):
            if index < len(encoded) - 1 and not byte & 0x80:
                raise EvidenceError("ColumnOrder field ID header has trailing bytes")
            value |= (byte & 0x7F) << shift
            shift += 7
        canonical = []
        remaining = value
        while True:
            byte = remaining & 0x7F
            remaining >>= 7
            canonical.append(byte | (0x80 if remaining else 0))
            if not remaining:
                break
        if bytes(canonical) != encoded:
            raise EvidenceError("ColumnOrder field ID uses a noncanonical varint")
        field_id = (value >> 1) ^ -(value & 1)
        if not -(1 << 15) <= field_id < (1 << 15):
            raise EvidenceError("ColumnOrder field ID is outside signed Int16")
    return field_id, COMPACT_TO_TTYPE[compact_type]


def validate_column_order(order):
    state = order["state"]
    field_id = order["field_id"]
    wire_type = order["wire_type"]
    header = order["header_hex"]
    if state == "ABSENT":
        if any(value is not None for value in (field_id, wire_type, header)):
            raise EvidenceError("absent ColumnOrder carries raw fields")
        return None
    if header is None:
        raise EvidenceError("present ColumnOrder lacks its raw header")
    decoded_field, decoded_type = decode_compact_header(header)
    if (field_id, wire_type) != (decoded_field, decoded_type):
        raise EvidenceError("ColumnOrder raw header contradicts its decoded fields")
    if state == "TYPE_ORDER":
        if (field_id, wire_type) != (1, 12):
            raise EvidenceError("TYPE_ORDER has an invalid raw header")
    elif state == "IEEE_754_TOTAL_ORDER":
        if (field_id, wire_type) != (2, 12):
            raise EvidenceError("IEEE order has an invalid raw header")
    elif state == "UNKNOWN":
        if field_id is None or field_id in (1, 2) or wire_type is None:
            raise EvidenceError("unknown ColumnOrder is not preserved")
    elif state == "WRONG_TYPE":
        if field_id not in (1, 2) or wire_type in (None, 12):
            raise EvidenceError("wrong-type ColumnOrder is inconsistent")
    elif state == "EMPTY":
        if field_id is not None or wire_type is not None or header != "00":
            raise EvidenceError("empty ColumnOrder carries a member")
    return None


def validate_column(record):
    num_values = parse_int64(record["num_values"], "num_values")
    if num_values < 0:
        raise EvidenceError("num_values is negative")
    counts = {}
    for name in ("null_count", "distinct_count", "nan_count"):
        value = record[name]
        counts[name] = None if value is None else parse_int64(value, name)
        if counts[name] is not None and not 0 <= counts[name] <= num_values:
            raise EvidenceError(f"{name} is outside the column value count")
    physical = record["leaf_schema"]["physical_type"]
    logical = record["leaf_schema"]["logical_type"]
    validate_leaf_schema(record["leaf_schema"])
    if counts["nan_count"] is not None and physical not in ("FLOAT", "DOUBLE") and logical != "FLOAT16":
        raise EvidenceError("nan_count is present on a non-floating leaf")
    if counts["null_count"] is not None and counts["nan_count"] is not None:
        if counts["null_count"] + counts["nan_count"] > num_values:
            raise EvidenceError("null_count plus nan_count exceeds num_values")
    if counts["null_count"] is not None and counts["distinct_count"] is not None:
        if counts["distinct_count"] > num_values - counts["null_count"]:
            raise EvidenceError("distinct_count exceeds non-null values")
    if not record["has_statistics"]:
        if any(record[name] is not None for name in STATISTIC_FIELDS):
            raise EvidenceError("statistics fields are present when has_statistics is false")
        if record["unknown_statistics_field_ids"]:
            raise EvidenceError("unknown statistics fields exist without Statistics")
    unknown = record["unknown_statistics_field_ids"]
    if unknown != sorted(unknown):
        raise EvidenceError("unknown statistics field IDs are not sorted")
    if any(field_id in range(1, 10) for field_id in unknown):
        raise EvidenceError("known statistics field ID is marked unknown")
    validate_column_order(record["column_order"])
    return None


def validate_record_schema(records, validator):
    if not records:
        raise EvidenceError("the first record must be run")
    for record in records:
        if not isinstance(record, dict):
            raise EvidenceError("every evidence record must be a JSON object")
        validator.validate(record)
    if records[0]["record"] != "run":
        raise EvidenceError("the first record must be run")
    if sum(record["record"] == "run" for record in records) != 1:
        raise EvidenceError("evidence must contain exactly one run record")
    return None


def validate_run_record(run, manifest, capabilities, fixtures, input_hashes,
        gate):
    for field, expected in input_hashes["run"].items():
        if run[field] != expected:
            raise EvidenceError(f"run {field} does not match the frozen input")
    authorities = authority_map(capabilities)
    if run["producer"] not in authorities:
        raise EvidenceError(f"unknown producer {run['producer']}")
    authority = authorities[run["producer"]]
    if run["producer_version"] != authority["version"]:
        raise EvidenceError("producer version does not match its authority")
    if run["source_revision"] != authority["revision"]:
        raise EvidenceError("producer revision does not match its authority")
    if run["toolchain_sha256"] not in authority["toolchain_sha256"]:
        raise EvidenceError("producer toolchain is not pinned by its authority")
    selected_toolchain_status = toolchain_status(manifest, run["toolchain_sha256"])
    if gate and selected_toolchain_status != "verified":
        raise EvidenceError("gate producer toolchain is not verified")
    claims = claim_statuses(authority)
    known_fixtures = case_map(fixtures)
    return claims, known_fixtures


def validate_file_record(record, fixture, files, gate):
    case_id = record["case_id"]
    if fixture["semantic"]:
        raise EvidenceError(f"semantic case has a file record: {case_id}")
    if case_id in files:
        raise EvidenceError(f"duplicate file record for {case_id}")
    if record["file"] != fixture["file"]:
        raise EvidenceError(f"file path mismatch for {case_id}")
    if not fixture["generated"]:
        if record["sha256"] != fixture["sha256"] or \
                record["size"] != fixture["size"]:
            raise EvidenceError(f"file identity mismatch for {case_id}")
    elif fixture["output_identity_status"] == "verified":
        if record["sha256"] != fixture["output_sha256"] or \
                record["size"] != fixture["output_size"]:
            raise EvidenceError(f"generated file identity mismatch for {case_id}")
    elif gate:
        raise EvidenceError(f"generated file identity is not verified: {case_id}")
    if record["footer_length"] > record["size"] - 12:
        raise EvidenceError(f"footer is not contained for {case_id}")
    if record["row_group_count"] != fixture["row_group_count"]:
        raise EvidenceError(f"row-group count mismatch for {case_id}")
    if record["leaf_count"] != fixture["leaf_count"]:
        raise EvidenceError(f"leaf count mismatch for {case_id}")
    if record["column_order_count"] not in (None, record["leaf_count"]):
        raise EvidenceError(f"column-order cardinality mismatch for {case_id}")
    files[case_id] = record
    return None


def validate_column_record(record, fixture, columns):
    case_id = record["case_id"]
    if fixture["semantic"]:
        raise EvidenceError(f"semantic case has a column record: {case_id}")
    key = (case_id, record["row_group"], record["leaf"])
    if key in columns:
        raise EvidenceError(f"duplicate column record {key}")
    if record["file"] != fixture["file"]:
        raise EvidenceError(f"column file mismatch for {case_id}")
    if record["row_group"] >= fixture["row_group_count"]:
        raise EvidenceError(f"row-group ordinal is outside {case_id}")
    if record["leaf"] >= fixture["leaf_count"]:
        raise EvidenceError(f"leaf ordinal is outside {case_id}")
    validate_column(record)
    columns[key] = record
    return None


def validate_case_result(record, fixture, results, claims, gate):
    case_id = record["case_id"]
    key = (case_id, record["capability_id"])
    if key in results:
        raise EvidenceError(f"duplicate case result {key}")
    if record["capability_id"] not in fixture["capabilities"]:
        raise EvidenceError(f"capability is outside fixture scope: {key}")
    if record["digest_contract"] != fixture["digest_contract"]:
        raise EvidenceError(f"digest contract is outside fixture scope: {key}")
    if key not in claims:
        raise EvidenceError(f"producer has no claim for capability: {key}")
    claim_status = claims[key]
    if record["status"] == "PASS" and \
            record["expected_sha256"] != record["actual_sha256"]:
        raise EvidenceError(f"passing digests differ for {key}")
    if fixture["semantic"] and record["status"] == "PASS" and \
            record["expected_sha256"] != fixture["expected_sha256"][
                record["capability_id"]]:
        raise EvidenceError(f"semantic case digest differs from its manifest: {key}")
    if record["status"] == "PASS" and claim_status not in ("verified", "planned"):
        raise EvidenceError(f"producer cannot pass its {claim_status} claim: {key}")
    if record["status"] == "UNSUPPORTED" and claim_status != "unsupported":
        raise EvidenceError(f"unsupported result is not reviewed: {key}")
    if record["status"] == "FAIL" and claim_status not in ("verified", "planned"):
        raise EvidenceError(f"producer cannot fail its {claim_status} claim: {key}")
    if gate and record["status"] == "PASS" and claim_status != "verified":
        raise EvidenceError(f"passing gate claim is not verified: {key}")
    if gate and record["status"] == "FAIL":
        raise EvidenceError(f"failed gate result: {key}")
    results[key] = record
    return None


def collect_evidence_records(records, known_fixtures, claims, gate):
    files = {}
    columns = {}
    results = {}
    for record in records[1:]:
        kind = record["record"]
        case_id = record["case_id"]
        if case_id not in known_fixtures:
            raise EvidenceError(f"unknown fixture {case_id}")
        fixture = known_fixtures[case_id]
        if kind == "file":
            validate_file_record(record, fixture, files, gate)
        elif kind == "column_statistics":
            validate_column_record(record, fixture, columns)
        elif kind == "case_result":
            validate_case_result(record, fixture, results, claims, gate)
        else:
            raise EvidenceError(f"unsupported evidence record kind: {kind}")
    return files, columns, results


def validate_case_columns(case_id, case_columns, file_record, fixture):
    expected_pairs = {
        (row_group, leaf)
        for row_group in range(fixture["row_group_count"])
        for leaf in range(fixture["leaf_count"])
    }
    actual_pairs = {(record["row_group"], record["leaf"])
        for record in case_columns}
    if actual_pairs != expected_pairs:
        raise EvidenceError(f"incomplete row-group and leaf coverage for {case_id}")
    orders_present = file_record["column_order_count"] is not None
    for record in case_columns:
        order_absent = record["column_order"]["state"] == "ABSENT"
        if orders_present == order_absent:
            raise EvidenceError(
                f"column-order vector presence contradicts leaf state for {case_id}")
    leaf_facts = {}
    leaf_orders = {}
    for record in case_columns:
        fact = (record["path"], record["leaf_schema"])
        previous = leaf_facts.setdefault(record["leaf"], fact)
        if previous != fact:
            raise EvidenceError(f"leaf schema changes across row groups for {case_id}")
        previous_order = leaf_orders.setdefault(record["leaf"],
            record["column_order"])
        if previous_order != record["column_order"]:
            raise EvidenceError(f"column order changes across row groups for {case_id}")
    paths = {tuple(fact[0]) for fact in leaf_facts.values()}
    if len(paths) != fixture["leaf_count"]:
        raise EvidenceError(f"leaf paths are not unique for {case_id}")
    return None


def validate_record_coverage(run, files, columns, results, known_fixtures):
    by_case = {}
    for key, record in columns.items():
        by_case.setdefault(key[0], []).append(record)
    for case_id, case_columns in by_case.items():
        if case_id not in files:
            raise EvidenceError(f"columns have no file record for {case_id}")
        validate_case_columns(case_id, case_columns, files[case_id],
            known_fixtures[case_id])
    for case_id in files:
        if case_id not in by_case and not any(key[0] == case_id for key in results):
            raise EvidenceError(f"file record has no evidence for {case_id}")
    for key, result in results.items():
        if key[0] not in files and not known_fixtures[key[0]]["semantic"]:
            raise EvidenceError(f"case result has no file record: {key}")
    unsupported_cases = {
        case_id for (case_id, _), record in results.items()
        if record["status"] == "UNSUPPORTED"
    }
    if run["unsupported_cases"] != sorted(unsupported_cases):
        raise EvidenceError("run unsupported_cases does not match case results")
    return by_case


def validate_records(records, validator, manifest, capabilities, fixtures,
        input_hashes, gate=False):
    validate_record_schema(records, validator)
    run = records[0]
    claims, known_fixtures = validate_run_record(run, manifest, capabilities,
        fixtures, input_hashes, gate)
    files, columns, results = collect_evidence_records(records, known_fixtures,
        claims, gate)
    by_case = validate_record_coverage(run, files, columns, results,
        known_fixtures)
    return {
        "run": run,
        "files": files,
        "columns": columns,
        "column_cases": set(by_case),
        "results": results,
        "passes": {key for key, record in results.items()
            if record["status"] == "PASS"},
    }


def check_evidence_file_metadata(path, label, limits):
    try:
        metadata = path.stat(follow_symlinks=False)
    except FileNotFoundError as error:
        raise EvidenceError(f"evidence file is absent: {label}") from error
    if not stat.S_ISREG(metadata.st_mode):
        raise EvidenceError(f"evidence is not a regular file: {label}")
    if not 0 < metadata.st_size <= limits["max_file_bytes"]:
        raise EvidenceError(f"evidence has an invalid byte size: {label}")
    return metadata.st_size


def checked_gate_input(relative, manifest, manifest_path, used_entries, limits):
    if not safe_evidence_file(relative):
        raise EvidenceError(f"gate evidence path is not canonical and relative: {relative}")
    frozen = [entry for entry in manifest["frozen_evidence"]
        if entry["file"] == relative]
    planned = [entry for entry in manifest["planned_evidence"]
        if entry["file"] == relative]
    if planned:
        raise EvidenceError(f"planned evidence cannot satisfy the gate: {relative}")
    if len(frozen) != 1:
        raise EvidenceError(f"gate evidence is undeclared or ambiguous: {relative}")
    entry = frozen[0]
    if entry["id"] in used_entries:
        raise EvidenceError(f"duplicate gate evidence entry: {entry['id']}")
    if entry["status"] != "verified" or entry["format"] != "normalized-jsonl":
        raise EvidenceError(f"gate evidence is not verified normalized JSONL: {relative}")
    if entry["schema_file"] != manifest["evidence_schema_file"] or \
            entry["fixture_manifest_file"] != manifest["fixture_manifest_file"]:
        raise EvidenceError(f"gate evidence uses the wrong frozen inputs: {relative}")
    if entry["schema_sha256"] != manifest["evidence_schema_sha256"]:
        raise EvidenceError(f"gate evidence uses the wrong schema hash: {relative}")
    root = repository_root(manifest_path)
    candidate = root.joinpath(*pathlib.PurePosixPath(relative).parts)
    current = root
    for part in pathlib.PurePosixPath(relative).parts:
        current = current / part
        if current.is_symlink():
            raise EvidenceError(f"gate evidence path contains a symbolic link: {relative}")
    try:
        candidate.resolve(strict=True).relative_to(root)
    except ValueError as error:
        raise EvidenceError(f"gate evidence path escapes the repository: {relative}") from error
    used_entries.add(entry["id"])
    return entry, candidate


def validate_gate_entry_coverage(manifest, used_entries):
    expected_entries = [entry["id"] for entry in manifest["frozen_evidence"]
        if entry["format"] == "normalized-jsonl"]
    expected = set(expected_entries)
    if len(expected) != len(expected_entries):
        raise EvidenceError("normalized frozen evidence IDs are ambiguous")
    if used_entries != expected:
        missing = sorted(expected - used_entries)
        extra = sorted(used_entries - expected)
        raise EvidenceError(
            "gate evidence set differs from normalized frozen evidence: "
            f"missing={missing}, extra={extra}")
    return None


def read_jsonl_bytes(value, label, limits):
    if len(value) > limits["max_file_bytes"]:
        raise EvidenceError(f"{label}: evidence file exceeds its byte limit")

    def unique_object(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise EvidenceError(f"{label}: duplicate JSON object key: {key}")
            value[key] = item
        return value

    def invalid_constant(value):
        raise EvidenceError(f"{label}: invalid JSON numeric constant: {value}")

    def invalid_float(value):
        raise EvidenceError(f"{label}: floating JSON number is forbidden: {value}")

    def canonical_integer(value):
        if value != "0" and (not value or value[0] == "0" or
                value.startswith("-0")):
            raise EvidenceError(f"{label}: noncanonical JSON integer: {value}")
        return int(value)

    records = []
    with io.BytesIO(value) as stream:
        line_number = 0
        while True:
            line = stream.readline(limits["max_line_bytes"] + 1)
            if not line:
                break
            line_number += 1
            if len(line) > limits["max_line_bytes"]:
                raise EvidenceError(
                    f"{label}:{line_number}: line exceeds its byte limit")
            if not line.endswith(b"\n"):
                raise EvidenceError(f"{label}:{line_number}: missing final newline")
            decoded = line.decode("utf-8")
            records.append(json.loads(decoded, object_pairs_hook=unique_object,
                parse_constant=invalid_constant, parse_float=invalid_float,
                parse_int=canonical_integer))
            if len(records) > limits["max_records_per_input"]:
                raise EvidenceError(
                    f"{label}: evidence file exceeds its record limit")
    return records


def read_jsonl(path, limits):
    value = stable_regular_bytes(path, limits["max_file_bytes"],
        f"evidence file {path}", allow_empty=False)
    return read_jsonl_bytes(value, str(path), limits)


def validate_comparison_groups(fixtures, passing_results):
    groups = {}
    for case in fixtures["generated_case"]:
        group = case["comparison_group"]
        if group:
            groups.setdefault(group, []).append(case)
    for group, cases in groups.items():
        capabilities = set(cases[0]["capabilities"])
        if any(set(case["capabilities"]) != capabilities for case in cases):
            raise EvidenceError(f"comparison group has inconsistent capabilities: {group}")
        for capability in capabilities:
            observations = []
            for case in cases:
                records = passing_results.get((case["id"], capability), [])
                if not records:
                    raise EvidenceError(
                        f"comparison group lacks a passing result: {(group, capability)}")
                observations.extend((record["digest_contract"],
                    record["actual_sha256"]) for _, record in records)
            if len(set(observations)) != 1:
                raise EvidenceError(
                    f"comparison group results disagree: {(group, capability)}")
    return None


def canonical_record(record):
    return json.dumps(record, ensure_ascii=True, separators=(",", ":"),
        sort_keys=True)


def validate_gate_bindings(args, manifest, input_hashes):
    root = repository_root(args.manifest)
    expected = {
        "schema": manifest["evidence_schema_file"],
        "capabilities": manifest["capabilities_file"],
        "fixtures": manifest["fixture_manifest_file"],
    }
    for argument, relative in expected.items():
        supplied = pathlib.Path(getattr(args, argument)).resolve(strict=True)
        frozen = root.joinpath(*pathlib.PurePosixPath(relative).parts)
        if supplied != frozen:
            raise EvidenceError(f"gate {argument} is not the manifest-pinned file")
    expected_hashes = {
        "schema": (input_hashes["run"]["evidence_schema_sha256"],
            manifest["evidence_schema_sha256"]),
        "capabilities": (input_hashes["run"]["capabilities_sha256"],
            manifest["capabilities_sha256"]),
        "fixtures": (input_hashes["run"]["fixture_manifest_sha256"],
            manifest["fixture_manifest_sha256"]),
    }
    for argument, (actual_hash, expected_hash) in expected_hashes.items():
        if actual_hash != expected_hash:
            raise EvidenceError(f"gate {argument} hash does not match the manifest")
    direct_inputs = {
        "plan": (input_hashes["run"]["plan_sha256"],
            manifest["plan_sha256"]),
        "corpus": (input_hashes["run"]["corpus_manifest_sha256"],
            manifest["corpus_manifest_sha256"]),
        "artifacts": (input_hashes["artifact_manifest_sha256"],
            manifest["artifact_manifest_sha256"]),
    }
    for name, (actual_hash, expected_hash) in direct_inputs.items():
        if actual_hash != expected_hash:
            raise EvidenceError(f"gate {name} hash does not match the manifest")
    return root


def base_self_test(manifest, capability_data, fixture_data, input_hashes):
    zero = "0" * 64
    producer = next(item for item in capability_data["authority"]
        if item["id"] == "n6-raw-java")
    if len(producer["toolchain_sha256"]) != 1:
        raise EvidenceError("raw self-test requires one pinned toolchain")
    fixture = next(item for item in fixture_data["fixture"] if item["id"] == "apache-binary")
    run = {
        "record": "run",
        "schema_version": 2,
        "evidence_id": "self-test",
        "producer": "n6-raw-java",
        "producer_version": producer["version"],
        "source_revision": producer["revision"],
        "plan_sha256": input_hashes["run"]["plan_sha256"],
        "capabilities_sha256": input_hashes["run"]["capabilities_sha256"],
        "fixture_manifest_sha256":
            input_hashes["run"]["fixture_manifest_sha256"],
        "corpus_manifest_sha256":
            input_hashes["run"]["corpus_manifest_sha256"],
        "evidence_schema_sha256":
            input_hashes["run"]["evidence_schema_sha256"],
        "toolchain_sha256": producer["toolchain_sha256"][0],
        "unsupported_cases": [],
    }
    file_record = {
        "record": "file",
        "schema_version": 2,
        "case_id": fixture["id"],
        "file": fixture["file"],
        "sha256": fixture["sha256"],
        "size": fixture["size"],
        "footer_length": 100,
        "row_group_count": 1,
        "leaf_count": 1,
        "column_order_count": 1,
        "created_by_present": False,
        "created_by": None,
    }
    column = {
        "record": "column_statistics",
        "schema_version": 2,
        "case_id": fixture["id"],
        "file": fixture["file"],
        "row_group": 0,
        "leaf": 0,
        "path": ["value"],
        "leaf_schema": {
            "physical_type": "BYTE_ARRAY",
            "logical_type": "NONE",
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
        },
        "column_order": {
            "state": "TYPE_ORDER",
            "field_id": 1,
            "wire_type": 12,
            "header_hex": "1c",
        },
        "num_values": "1",
        "has_statistics": False,
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
    result = {
        "record": "case_result",
        "schema_version": 2,
        "case_id": fixture["id"],
        "capability_id": "wire.column-order.type",
        "digest_contract": fixture_data["default_digest_contract"],
        "status": "PASS",
        "expected_sha256": zero,
        "actual_sha256": zero,
        "detail": "self-test",
    }
    return [run, file_record, column, result]


def validate_self_test_records(records, context, gate=False):
    return validate_records(records, *context, gate=gate)


def reject_self_test_records(records, context, gate=False):
    try:
        validate_self_test_records(records, context, gate)
    except (EvidenceError, jsonschema.ValidationError, ValueError):
        return None
    raise EvidenceError("an adversarial self-test record was accepted")


def reject_self_test_mutation(base, context, mutator):
    records = copy.deepcopy(base)
    mutator(records)
    reject_self_test_records(records, context)
    return None


def accept_self_test_leaf(base, context, fields):
    records = copy.deepcopy(base)
    records[2]["leaf_schema"].update(fields)
    validate_self_test_records(records, context)
    return None


def reject_self_test_leaf(base, context, fields):
    records = copy.deepcopy(base)
    records[2]["leaf_schema"].update(fields)
    reject_self_test_records(records, context)
    return None


def self_test_compact_headers():
    header_cases = {
        "00": (None, None),
        "1c": (1, 12),
        "2c": (2, 12),
        "15": (1, 8),
        "0cfeff03": (32767, 12),
        "0cffff03": (-32768, 12),
    }
    for header, expected in header_cases.items():
        if decode_compact_header(header) != expected:
            raise EvidenceError(f"Compact-Thrift header self-test failed: {header}")
    for header in ("0e", "0c", "0c8200", "1c00"):
        try:
            decode_compact_header(header)
        except EvidenceError:
            pass
        else:
            raise EvidenceError(f"invalid Compact-Thrift header passed: {header}")
    return None


def self_test_valid_leaves(base, context):
    valid_leaves = [
        {"physical_type": "BYTE_ARRAY", "logical_type": "STRING",
            "converted_type": "UTF8"},
        {"physical_type": "INT32", "logical_type": "DECIMAL",
            "converted_type": "DECIMAL", "precision": 9, "scale": 2},
        {"physical_type": "INT32", "logical_type": "INTEGER",
            "converted_type": "UINT_16", "bit_width": 16,
            "is_signed": False},
        {"physical_type": "INT32", "logical_type": "TIME",
            "converted_type": "TIME_MILLIS", "time_unit": "MILLIS",
            "is_adjusted_to_utc": True},
        {"physical_type": "INT32", "logical_type": "TIME",
            "converted_type": "TIME_MILLIS", "time_unit": "MILLIS",
            "is_adjusted_to_utc": False},
        {"physical_type": "INT64", "logical_type": "TIMESTAMP",
            "converted_type": "TIMESTAMP_MICROS", "time_unit": "MICROS",
            "is_adjusted_to_utc": False},
        {"physical_type": "INT64", "logical_type": "TIMESTAMP",
            "time_unit": "NANOS", "is_adjusted_to_utc": False},
        {"physical_type": "FIXED_LEN_BYTE_ARRAY", "logical_type": "UUID",
            "type_length": 16},
        {"physical_type": "FIXED_LEN_BYTE_ARRAY", "logical_type": "FLOAT16",
            "type_length": 2},
        {"physical_type": "FIXED_LEN_BYTE_ARRAY", "logical_type": "INTERVAL",
            "converted_type": "INTERVAL", "type_length": 12},
        {"physical_type": "BYTE_ARRAY", "logical_type": "GEOGRAPHY",
            "crs": "OGC:CRS84", "geography_algorithm": "SPHERICAL"},
        {"physical_type": "INT96", "logical_type": "UNKNOWN"},
        {"physical_type": "BYTE_ARRAY", "type_length": -1},
    ]
    for fields in valid_leaves:
        accept_self_test_leaf(base, context, fields)
    return None


def self_test_multileaf_coverage(base, context, fixtures):
    multi_fixture = next(item for item in fixtures["fixture"]
        if item["id"] == "apache-alltypes-dictionary")
    multi = [copy.deepcopy(base[0]), copy.deepcopy(base[1])]
    multi[1].update({
        "case_id": multi_fixture["id"],
        "file": multi_fixture["file"],
        "sha256": multi_fixture["sha256"],
        "size": multi_fixture["size"],
        "row_group_count": multi_fixture["row_group_count"],
        "leaf_count": multi_fixture["leaf_count"],
        "column_order_count": None,
    })
    for leaf in range(multi_fixture["leaf_count"]):
        column = copy.deepcopy(base[2])
        column.update({
            "case_id": multi_fixture["id"],
            "file": multi_fixture["file"],
            "leaf": leaf,
            "path": [f"value_{leaf}"],
        })
        column["column_order"] = {
            "state": "ABSENT",
            "field_id": None,
            "wire_type": None,
            "header_hex": None,
        }
        multi.append(column)
    validate_self_test_records(multi, context)
    duplicate_path = copy.deepcopy(multi)
    duplicate_path[3]["path"] = duplicate_path[2]["path"]
    reject_self_test_records(duplicate_path, context)
    return None


def self_test_multirow_coverage(base, context, fixtures):
    multi_row_fixture = next(item for item in fixtures["fixture"]
        if item["id"] == "apache-floating-orders-nan-count")
    multi_row = [copy.deepcopy(base[0]), copy.deepcopy(base[1])]
    multi_row[1].update({
        "case_id": multi_row_fixture["id"],
        "file": multi_row_fixture["file"],
        "sha256": multi_row_fixture["sha256"],
        "size": multi_row_fixture["size"],
        "row_group_count": multi_row_fixture["row_group_count"],
        "leaf_count": multi_row_fixture["leaf_count"],
        "column_order_count": multi_row_fixture["leaf_count"],
    })
    for row_group in range(multi_row_fixture["row_group_count"]):
        for leaf in range(multi_row_fixture["leaf_count"]):
            column = copy.deepcopy(base[2])
            column.update({
                "case_id": multi_row_fixture["id"],
                "file": multi_row_fixture["file"],
                "row_group": row_group,
                "leaf": leaf,
                "path": [f"value_{leaf}"],
            })
            multi_row.append(column)
    validate_self_test_records(multi_row, context)
    inconsistent_order = copy.deepcopy(multi_row)
    inconsistent_order[-1]["column_order"] = {
        "state": "IEEE_754_TOTAL_ORDER",
        "field_id": 2,
        "wire_type": 12,
        "header_hex": "2c",
    }
    reject_self_test_records(inconsistent_order, context)
    return None


def self_test_column_rejections(base, context):
    reject = lambda mutator: reject_self_test_mutation(base, context, mutator)
    reject(lambda records: records[2].__setitem__("num_values", None))
    reject(lambda records: records[2].__setitem__("num_values", "0001"))
    reject(lambda records: records[2].__setitem__("num_values", str(1 << 63)))
    reject(lambda records: records[2].__setitem__("nan_count", "0"))
    reject(lambda records: records[2].__setitem__("min_value_hex", "00"))
    reject(lambda records: records[2]["column_order"].__setitem__("field_id", 2))
    reject(lambda records: records[2]["column_order"].__setitem__(
        "header_hex", "1c00"))
    reject(lambda records: records[2].__setitem__(
        "unknown_statistics_field_ids", [3, 3]))
    reject(lambda records: records[2].__setitem__(
        "unknown_statistics_field_ids", [9]))
    reject(lambda records: records[1].__setitem__("file", "/absolute.parquet"))
    reject(lambda records: records[1].__setitem__("file", "data/./binary.parquet"))
    reject(lambda records: records[1].__setitem__(
        "footer_length", records[1]["size"]))
    reject(lambda records: records[1].__setitem__("footer_length", 0))
    reject(lambda records: records[1].__setitem__("column_order_count", 2))
    reject(lambda records: records[1].__setitem__("column_order_count", None))
    reject(lambda records: records[2]["column_order"].update({
        "state": "ABSENT", "field_id": None, "wire_type": None,
        "header_hex": None}))
    reject(lambda records: records[2]["leaf_schema"].update({
        "physical_type": "INT32", "logical_type": "FLOAT16"}))
    return None


def self_test_leaf_rejections(base, context):
    reject = lambda fields: reject_self_test_leaf(base, context, fields)
    reject({"physical_type": "FIXED_LEN_BYTE_ARRAY", "type_length": None})
    reject({"physical_type": "FIXED_LEN_BYTE_ARRAY", "type_length": 0})
    reject({"logical_type": "STRING", "converted_type": None})
    reject({"physical_type": "INT32", "logical_type": "STRING",
        "converted_type": "UTF8"})
    reject({"logical_type": "ENUM", "converted_type": "UTF8"})
    reject({"physical_type": "INT32", "logical_type": "DECIMAL",
        "converted_type": "DECIMAL", "precision": 10, "scale": 2})
    reject({"physical_type": "BYTE_ARRAY", "logical_type": "DECIMAL",
        "converted_type": "DECIMAL", "precision": 2, "scale": 3})
    reject({"precision": 2, "scale": 0})
    reject({"physical_type": "INT32", "logical_type": "INTEGER",
        "converted_type": "UINT_16", "bit_width": 32, "is_signed": False})
    reject({"physical_type": "INT32", "logical_type": "INTEGER",
        "converted_type": "UINT_64", "bit_width": 64, "is_signed": False})
    reject({"physical_type": "INT64", "logical_type": "TIME",
        "converted_type": "TIME_MILLIS", "time_unit": "MICROS",
        "is_adjusted_to_utc": True})
    reject({"physical_type": "BYTE_ARRAY", "logical_type": "GEOMETRY",
        "geography_algorithm": "SPHERICAL"})
    reject({"crs": "OGC:CRS84"})
    reject({"physical_type": "FIXED_LEN_BYTE_ARRAY", "logical_type": "UUID",
        "type_length": 15})
    reject({"physical_type": "FIXED_LEN_BYTE_ARRAY", "logical_type": "INTERVAL",
        "type_length": 12})
    reject({"logical_type": "MAP", "converted_type": "MAP"})
    return None


def self_test_record_rejections(base, context, capabilities):
    zero = "0" * 64
    reject = lambda mutator: reject_self_test_mutation(base, context, mutator)
    reject(lambda records: records.insert(0, records.pop(1)))
    reject(lambda records: records[3].__setitem__("status", "UNSUPPORTED"))
    reject(lambda records: records[0].__setitem__("producer_version", "wrong"))
    reject(lambda records: records[0].__setitem__("source_revision", "wrong"))
    reject(lambda records: records[0].__setitem__("toolchain_sha256", zero))
    reject(lambda records: records[0].__setitem__("manifest_sha256", zero))
    reject(lambda records: records[0].__setitem__("artifact_manifest_sha256", zero))
    reject(lambda records: records[3].__setitem__("capability_id", "read.logical-values"))
    no_file = copy.deepcopy(base)
    pyarrow = next(item for item in capabilities["authority"]
        if item["id"] == "pyarrow")
    no_file[0].update({
        "producer": pyarrow["id"],
        "producer_version": pyarrow["version"],
        "source_revision": pyarrow["revision"],
        "toolchain_sha256": pyarrow["toolchain_sha256"][0],
    })
    no_file[3]["capability_id"] = "read.logical-values"
    reject_self_test_records([no_file[0], no_file[3]], context)
    planned_capabilities = copy.deepcopy(capabilities)
    raw = next(item for item in planned_capabilities["authority"]
        if item["id"] == "n6-raw-java")
    claim = next(item for item in raw["claim"]
        if item["capability"] == "wire.column-order.type" and
        "apache-binary" in item["cases"])
    claim["status"] = "planned"
    planned_context = (context[0], context[1], planned_capabilities,
        context[3], context[4])
    reject_self_test_records(base, planned_context, gate=True)
    reject_self_test_records([[]] + copy.deepcopy(base[1:]), context)
    malformed_later = copy.deepcopy(base)
    malformed_later[2] = []
    reject_self_test_records(malformed_later, context)
    return None


def self_test_semantic_case(base, context, capabilities, fixtures):
    authority = next(item for item in capabilities["authority"]
        if item["id"] == "n6-independent-model")
    case = next(item for item in fixtures["semantic_case"]
        if item["id"] == "plain-bound-decoding")
    capability = "semantic.type-order"
    digest = case["expected_sha256"][capability]
    run = copy.deepcopy(base[0])
    run.update({
        "evidence_id": "semantic-self-test",
        "producer": authority["id"],
        "producer_version": authority["version"],
        "source_revision": authority["revision"],
        "toolchain_sha256": authority["toolchain_sha256"][0],
    })
    result = {
        "record": "case_result",
        "schema_version": 2,
        "case_id": case["id"],
        "capability_id": capability,
        "digest_contract": case["digest_contract"],
        "status": "PASS",
        "expected_sha256": digest,
        "actual_sha256": digest,
        "detail": "semantic self-test",
    }
    records = [run, result]
    validate_self_test_records(records, context)
    validate_self_test_records(records, context, gate=True)
    wrong = copy.deepcopy(records)
    wrong[1]["expected_sha256"] = "1" * 64
    wrong[1]["actual_sha256"] = "1" * 64
    reject_self_test_records(wrong, context)
    with_file = copy.deepcopy(records)
    file_record = copy.deepcopy(base[1])
    file_record["case_id"] = case["id"]
    with_file.insert(1, file_record)
    reject_self_test_records(with_file, context)
    return None


def self_test_gate_bindings(manifest_path, manifest, capabilities, fixtures):
    normalized = [entry for section in ("frozen_evidence", "planned_evidence")
        for entry in manifest[section] if entry["format"] == "normalized-jsonl"]
    if not normalized:
        raise EvidenceError("normalized evidence is absent from the manifest")
    seed = copy.deepcopy(normalized[0])
    synthetic = copy.deepcopy(manifest)
    synthetic["frozen_evidence"] = [entry
        for entry in synthetic["frozen_evidence"]
        if entry["format"] != "normalized-jsonl"]
    synthetic["planned_evidence"] = []
    planned = copy.deepcopy(seed)
    planned["status"] = "planned"
    synthetic["planned_evidence"].append(planned)
    try:
        checked_gate_input(planned["file"], synthetic, manifest_path, set(),
            evidence_limits(synthetic, fixtures, capabilities))
    except EvidenceError:
        pass
    else:
        raise EvidenceError("planned evidence passed the gate binding self-test")
    if safe_relative("evidence/\nunsafe.jsonl") or safe_relative("évidence.jsonl"):
        raise EvidenceError("unsafe relative evidence path passed self-test")
    if safe_evidence_file("test/conformance/n6/README.md") or \
            safe_evidence_file("test/conformance/n6/evidence/.jsonl"):
        raise EvidenceError("unsafe evidence exclusion path passed self-test")
    synthetic["planned_evidence"] = []
    entry = copy.deepcopy(seed)
    entry.update({
        "status": "verified",
        "storage": "checked-in",
        "schema_sha256": synthetic["evidence_schema_sha256"],
    })
    synthetic["frozen_evidence"].append(entry)
    limits = evidence_limits(synthetic, fixtures, capabilities)
    used_entries = set()
    checked_gate_input(entry["file"], synthetic, manifest_path, used_entries,
        limits)
    try:
        checked_gate_input(entry["file"], synthetic, manifest_path,
            used_entries, limits)
    except EvidenceError:
        pass
    else:
        raise EvidenceError("duplicate frozen evidence passed its self-test")
    validate_gate_entry_coverage(synthetic, used_entries)
    try:
        validate_gate_entry_coverage(synthetic, set())
    except EvidenceError:
        pass
    else:
        raise EvidenceError("omitted frozen evidence passed its self-test")
    return None


def self_test_jsonl(manifest, capabilities, fixtures):
    limits = evidence_limits(manifest, fixtures, capabilities)
    with tempfile.TemporaryDirectory() as directory:
        temporary = pathlib.Path(directory)

        def reject_jsonl(name, payload, changed_limits=None):
            path = temporary / name
            path.write_bytes(payload)
            try:
                read_jsonl(path, changed_limits or limits)
            except (EvidenceError, json.JSONDecodeError, UnicodeError):
                return None
            raise EvidenceError("an adversarial JSONL self-test passed")

        valid = temporary / "valid.jsonl"
        valid.write_bytes(b"{}\n")
        if read_jsonl(valid, limits) != [{}]:
            raise EvidenceError("valid JSONL self-test failed")
        linked = temporary / "linked.jsonl"
        linked.symlink_to(valid)
        try:
            read_jsonl(linked, limits)
        except EvidenceError:
            pass
        else:
            raise EvidenceError("symbolic-link evidence passed its self-test")
        empty = temporary / "empty.jsonl"
        empty.write_bytes(b"")
        try:
            check_evidence_file_metadata(empty, "empty.jsonl", limits)
        except EvidenceError:
            pass
        else:
            raise EvidenceError("empty evidence passed its metadata self-test")
        tiny_file = dict(limits, max_file_bytes=2)
        try:
            check_evidence_file_metadata(valid, "valid.jsonl", tiny_file)
        except EvidenceError:
            pass
        else:
            raise EvidenceError("oversized evidence passed its metadata self-test")
        reject_jsonl("duplicate.jsonl", b'{"a":1,"a":2}\n')
        reject_jsonl("constant.jsonl", b'{"a":NaN}\n')
        reject_jsonl("float.jsonl", b'{"a":0.0}\n')
        reject_jsonl("negative-zero.jsonl", b'{"a":-0}\n')
        reject_jsonl("newline.jsonl", b"{}")
        short_line = dict(limits, max_line_bytes=2)
        reject_jsonl("line.jsonl", b"{} \n", short_line)
        one_record = dict(limits, max_records_per_input=1)
        reject_jsonl("records.jsonl", b"{}\n{}\n", one_record)
    return None


def self_test_upstream_evidence():
    raw_entry = {"id": "raw", "file":
        "test/conformance/n6/evidence/raw.jsonl"}
    derived_entry = {"id": "derived", "file":
        "test/conformance/n6/evidence/derived.jsonl",
        "upstream_evidence": ["raw"]}
    raw = {
        "entry": raw_entry,
        "run": {"evidence_id": "raw"},
        "sha256": "1" * 64,
    }
    derived = {
        "entry": derived_entry,
        "run": {
            "evidence_id": "derived",
            "upstream_evidence": [{
                "evidence_id": "raw",
                "file": raw_entry["file"],
                "sha256": raw["sha256"],
            }],
        },
        "sha256": "2" * 64,
    }
    inputs = {"raw": raw, "derived": derived}
    validate_upstream_evidence(inputs)
    for field, value in (("file", "test/conformance/n6/evidence/wrong.jsonl"),
            ("sha256", "3" * 64), ("evidence_id", "absent")):
        mutated = copy.deepcopy(inputs)
        mutated["derived"]["run"]["upstream_evidence"][0][field] = value
        try:
            validate_upstream_evidence(mutated)
        except EvidenceError:
            pass
        else:
            raise EvidenceError(
                f"invalid upstream {field} passed its self-test")
    cyclic = copy.deepcopy(inputs)
    cyclic["raw"]["entry"]["upstream_evidence"] = ["derived"]
    cyclic["raw"]["run"]["upstream_evidence"] = [{
        "evidence_id": "derived",
        "file": derived_entry["file"],
        "sha256": derived["sha256"],
    }]
    try:
        validate_upstream_evidence(cyclic)
    except EvidenceError:
        pass
    else:
        raise EvidenceError("cyclic upstream evidence passed its self-test")
    return None


def run_self_test(validator, manifest_path, manifest, capabilities, fixtures,
        input_hashes):
    context = (validator, manifest, capabilities, fixtures, input_hashes)
    base = base_self_test(manifest, capabilities, fixtures, input_hashes)
    self_test_compact_headers()
    validate_self_test_records(base, context)
    self_test_valid_leaves(base, context)
    self_test_multileaf_coverage(base, context, fixtures)
    self_test_multirow_coverage(base, context, fixtures)
    self_test_column_rejections(base, context)
    self_test_leaf_rejections(base, context)
    self_test_record_rejections(base, context, capabilities)
    self_test_semantic_case(base, context, capabilities, fixtures)
    self_test_gate_bindings(manifest_path, manifest, capabilities, fixtures)
    self_test_jsonl(manifest, capabilities, fixtures)
    self_test_upstream_evidence()
    return None


def parse_arguments(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--capabilities", required=True)
    parser.add_argument("--fixtures", required=True)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--gate", action="store_true")
    parser.add_argument("evidence", nargs="*")
    return parser.parse_args(argv)


def load_validation_inputs(args):
    manifest_bytes = stable_regular_bytes(args.manifest, CONTROL_FILE_LIMIT,
        "N6 manifest", allow_empty=False)
    manifest = tomllib.loads(manifest_bytes.decode("utf-8"))
    root = repository_root(args.manifest)
    schema_bytes = stable_regular_bytes(args.schema, CONTROL_FILE_LIMIT,
        "N6 evidence schema", allow_empty=False)
    capabilities_bytes = stable_regular_bytes(args.capabilities,
        CONTROL_FILE_LIMIT, "N6 capabilities", allow_empty=False)
    fixtures_bytes = stable_regular_bytes(args.fixtures, CONTROL_FILE_LIMIT,
        "N6 fixtures", allow_empty=False)

    def unique_object(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise EvidenceError(f"N6 evidence schema has duplicate key {key}")
            value[key] = item
        return value

    schema = json.loads(schema_bytes, object_pairs_hook=unique_object)
    validator_class = jsonschema.validators.validator_for(schema)
    validator_class.check_schema(schema)
    validator = validator_class(schema)
    capabilities = tomllib.loads(capabilities_bytes.decode("utf-8"))
    fixtures = tomllib.loads(fixtures_bytes.decode("utf-8"))
    validate_model_producer_binding(root, manifest, capabilities)
    fixtures["semantic_case"] = load_semantic_cases(root, manifest,
        capabilities, fixtures)
    plan_bytes = stable_regular_bytes(manifest_input_file(root,
        manifest["plan_file"]), CONTROL_FILE_LIMIT, "N6 plan", allow_empty=False)
    corpus_bytes = stable_regular_bytes(manifest_input_file(root,
        manifest["corpus_manifest_file"]), CONTROL_FILE_LIMIT,
        "N6 corpus manifest", allow_empty=False)
    artifacts_bytes = stable_regular_bytes(manifest_input_file(root,
        manifest["artifact_manifest_file"]), CONTROL_FILE_LIMIT,
        "N6 artifact manifest", allow_empty=False)
    input_hashes = {
        "run": {
            "plan_sha256": sha256_bytes(plan_bytes),
            "capabilities_sha256": sha256_bytes(capabilities_bytes),
            "fixture_manifest_sha256": sha256_bytes(fixtures_bytes),
            "corpus_manifest_sha256": sha256_bytes(corpus_bytes),
            "evidence_schema_sha256": sha256_bytes(schema_bytes),
        },
        "artifact_manifest_sha256": sha256_bytes(artifacts_bytes),
        "manifest_sha256": sha256_bytes(manifest_bytes),
    }
    limits = evidence_limits(manifest, fixtures, capabilities)
    return validator, manifest, capabilities, fixtures, limits, input_hashes


def resolve_evidence_input(argument, args, manifest, used_entries, limits):
    if args.gate:
        return checked_gate_input(argument, manifest, args.manifest,
            used_entries, limits)
    return None, pathlib.Path(argument)


def validate_frozen_run(entry, run, records, validated):
    if entry is None:
        return None
    if run["evidence_id"] != entry["id"]:
        raise EvidenceError("run evidence ID does not match its frozen entry")
    if run["producer"] != entry["authority"]:
        raise EvidenceError("run producer does not match its frozen entry")
    if run["toolchain_sha256"] != entry["toolchain_sha256"]:
        raise EvidenceError("run toolchain does not match its frozen entry")
    if len(records) != entry["record_count"]:
        raise EvidenceError("record count does not match its frozen entry")
    if len(validated["files"]) != entry["case_count"]:
        raise EvidenceError("case count does not match its frozen entry")
    return None


def collect_evidence_facts(validated, entry, records, evidence_ids, producers,
        passing_results, file_facts, column_facts, column_cases):
    run = validated["run"]
    if run["evidence_id"] in evidence_ids:
        raise EvidenceError(f"duplicate run evidence ID: {run['evidence_id']}")
    evidence_ids.add(run["evidence_id"])
    coverage = producers.setdefault(run["producer"], {
        "files": set(), "columns": set(), "results": set()})
    current = {
        "files": set(validated["files"]),
        "columns": set(validated["columns"]),
        "results": set(validated["results"]),
    }
    for kind, keys in current.items():
        overlap = coverage[kind] & keys
        if overlap:
            raise EvidenceError(
                f"producer evidence coverage overlaps for {kind}: {sorted(overlap)}")
        coverage[kind].update(keys)
    validate_frozen_run(entry, run, records, validated)
    for case_id, record in validated["files"].items():
        file_facts.setdefault(case_id, set()).add(canonical_record(record))
    for key, record in validated["columns"].items():
        column_facts.setdefault(key, set()).add(canonical_record(record))
    column_cases.update(validated["column_cases"])
    for key in validated["passes"]:
        passing_results.setdefault(key, []).append(
            (run["producer"], validated["results"][key]))
    return None


def validate_upstream_evidence(evidence_inputs):
    graph = {}
    for evidence_id, current in evidence_inputs.items():
        expected = current["entry"].get("upstream_evidence", [])
        bindings = current["run"].get("upstream_evidence", [])
        actual = [binding["evidence_id"] for binding in bindings]
        if len(actual) != len(set(actual)):
            raise EvidenceError(f"duplicate upstream evidence binding: {evidence_id}")
        if sorted(actual) != sorted(expected):
            raise EvidenceError(
                f"upstream evidence set differs from its manifest: {evidence_id}")
        graph[evidence_id] = set(actual)
        for binding in bindings:
            upstream_id = binding["evidence_id"]
            if upstream_id not in evidence_inputs:
                raise EvidenceError(
                    f"upstream evidence input is absent: {upstream_id}")
            upstream = evidence_inputs[upstream_id]
            if binding["file"] != upstream["entry"]["file"]:
                raise EvidenceError(
                    f"upstream evidence path differs: {upstream_id}")
            if binding["sha256"] != upstream["sha256"]:
                raise EvidenceError(
                    f"upstream evidence digest differs: {upstream_id}")
    visiting = set()
    visited = set()

    def visit(evidence_id):
        if evidence_id in visiting:
            raise EvidenceError("upstream evidence graph has a cycle")
        if evidence_id in visited:
            return
        visiting.add(evidence_id)
        for upstream_id in graph[evidence_id]:
            visit(upstream_id)
        visiting.remove(evidence_id)
        visited.add(evidence_id)

    for evidence_id in graph:
        visit(evidence_id)
    return None


def validate_authority_coverage(producers, capabilities):
    authorities = authority_map(capabilities)
    for producer, coverage in producers.items():
        claims = claim_statuses(authorities[producer])
        expected = {key for key, status in claims.items()
            if status != "not_assessed"}
        if coverage["results"] != expected:
            missing = sorted(expected - coverage["results"])
            extra = sorted(coverage["results"] - expected)
            raise EvidenceError(
                f"producer claim coverage differs for {producer}: "
                f"missing={missing}, extra={extra}")
    return None


def validate_evidence_inputs(args, validator, manifest, capabilities, fixtures,
        limits, input_hashes):
    if len(args.evidence) > limits["max_inputs"]:
        raise EvidenceError("too many evidence inputs")
    used_entries = set()
    evidence_ids = set()
    producers = {}
    evidence_inputs = {}
    passing_results = {}
    file_facts = {}
    column_facts = {}
    column_cases = set()
    total_bytes = 0
    total_records = 0
    for evidence_argument in args.evidence:
        entry, evidence_path = resolve_evidence_input(evidence_argument, args,
            manifest, used_entries, limits)
        evidence_bytes = stable_regular_bytes(evidence_path,
            limits["max_file_bytes"], f"evidence input {evidence_argument}",
            allow_empty=False)
        evidence_sha256 = sha256_bytes(evidence_bytes)
        if entry is not None and evidence_sha256 != entry["sha256"]:
            raise EvidenceError(
                f"gate evidence hash does not match its manifest: {evidence_argument}")
        total_bytes += len(evidence_bytes)
        if total_bytes > limits["max_total_bytes"]:
            raise EvidenceError("evidence inputs exceed their total byte limit")
        records = read_jsonl_bytes(evidence_bytes, evidence_argument, limits)
        total_records += len(records)
        if total_records > limits["max_records_total"]:
            raise EvidenceError("evidence inputs exceed their total record limit")
        validated = validate_records(records, validator, manifest, capabilities,
            fixtures, input_hashes, args.gate)
        run = validated["run"]
        declared_entry = entry or manifest_evidence_entry(manifest,
            run["evidence_id"])
        if run["evidence_id"] != declared_entry["id"] or \
                run["producer"] != declared_entry["authority"]:
            raise EvidenceError("run identity differs from its manifest entry")
        evidence_inputs[run["evidence_id"]] = {
            "entry": declared_entry,
            "run": run,
            "sha256": evidence_sha256,
        }
        collect_evidence_facts(validated, entry, records, evidence_ids,
            producers, passing_results, file_facts, column_facts, column_cases)
    if args.gate:
        validate_gate_entry_coverage(manifest, used_entries)
    validate_upstream_evidence(evidence_inputs)
    if args.gate:
        validate_authority_coverage(producers, capabilities)
    return passing_results, file_facts, column_facts, column_cases


def validate_cross_producer_facts(passing_results, file_facts, column_facts,
        column_cases, fixtures):
    semantic_ids = {case["id"] for case in fixtures["semantic_case"]}
    for case_id, facts in file_facts.items():
        if len(facts) != 1:
            raise EvidenceError(f"producers disagree on file facts: {case_id}")
    for key, facts in column_facts.items():
        if len(facts) != 1:
            raise EvidenceError(f"producers disagree on column facts: {key}")
    for key, producer_records in passing_results.items():
        observations = {(record["digest_contract"], record["actual_sha256"])
            for _, record in producer_records}
        if len(observations) != 1:
            raise EvidenceError(f"passing producers disagree: {key}")
        if key[1] in COLUMN_EVIDENCE_CAPABILITIES and \
                key[0] not in column_cases and key[0] not in semantic_ids:
            raise EvidenceError(f"column capability has no normalized facts: {key}")
    return None


def required_gate_results(fixtures, capabilities):
    declared = {
        (case["id"], capability)
        for case in fixtures["fixture"] + fixtures["generated_case"]
        for capability in case["capabilities"]
    }
    statuses = {}
    for authority in capabilities["authority"]:
        for claim in authority.get("claim", []):
            for case_id in claim["cases"]:
                statuses.setdefault((case_id, claim["capability"]), set()).add(
                    claim["status"])
    unreviewed = sorted(declared - set(statuses))
    if unreviewed:
        raise EvidenceError(f"fixture capabilities lack reviewed claims: {unreviewed}")
    positive = {key for key, values in statuses.items()
        if values & {"verified", "planned"}}
    return declared & positive


def self_test_required_gate_results():
    fixtures = {"fixture": [{"id": "case", "capabilities": ["cap"]}],
        "generated_case": []}
    capabilities = {"authority": [{"claim": [{"capability": "cap",
        "status": "unsupported", "cases": ["case"]}]}]}
    if required_gate_results(fixtures, capabilities):
        raise EvidenceError("unsupported-only claim became a required pass")
    capabilities["authority"][0]["claim"][0]["status"] = "verified"
    if required_gate_results(fixtures, capabilities) != {("case", "cap")}:
        raise EvidenceError("verified claim did not become a required pass")
    capabilities["authority"][0]["claim"] = []
    try:
        required_gate_results(fixtures, capabilities)
    except EvidenceError:
        pass
    else:
        raise EvidenceError("unreviewed fixture capability passed its self-test")
    return None


def validate_gate_results(fixtures, capabilities, passing_results):
    required = required_gate_results(fixtures, capabilities)
    missing = sorted(required - set(passing_results))
    if missing:
        raise EvidenceError(f"missing passing capability evidence: {missing}")
    validate_comparison_groups(fixtures, passing_results)
    return None


def main(argv=None):
    args = parse_arguments(argv)
    validator, manifest, capabilities, fixtures, limits, input_hashes = \
        load_validation_inputs(args)
    if args.gate:
        validate_gate_bindings(args, manifest, input_hashes)
    if args.self_test:
        run_self_test(validator, args.manifest, manifest, capabilities, fixtures,
            input_hashes)
        self_test_required_gate_results()
    passing, files, columns, column_cases = validate_evidence_inputs(args,
        validator, manifest, capabilities, fixtures, limits, input_hashes)
    validate_cross_producer_facts(passing, files, columns, column_cases,
        fixtures)
    if args.gate:
        validate_gate_results(fixtures, capabilities, passing)
    print("N6 normalized evidence validation passed.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, OSError, UnicodeError, jsonschema.ValidationError) as error:
        print(f"N6 evidence validation failed: {error}", file=sys.stderr)
        raise SystemExit(1)
