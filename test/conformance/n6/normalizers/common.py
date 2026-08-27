#!/usr/bin/env python3
import hashlib
import importlib.metadata
import json
import os
import pathlib
import stat
import sys
import tempfile
import tomllib


class EvidenceError(ValueError):
    pass


def _open_regular(path, maximum):
    path = pathlib.Path(path)
    if not isinstance(maximum, int) or isinstance(maximum, bool) or maximum < 1:
        raise ValueError("file byte limit must be a positive integer")
    nofollow = getattr(os, "O_NOFOLLOW", None)
    if nofollow is None:
        raise EvidenceError("this platform cannot reject symbolic-link inputs")
    flags = os.O_RDONLY | nofollow
    flags |= getattr(os, "O_CLOEXEC", 0)
    try:
        descriptor = os.open(path, flags)
    except FileNotFoundError as error:
        raise EvidenceError(f"input is absent: {path}") from error
    except OSError as error:
        raise EvidenceError(f"input cannot be opened safely: {path}: {error}") from error
    metadata = os.fstat(descriptor)
    if not stat.S_ISREG(metadata.st_mode):
        os.close(descriptor)
        raise EvidenceError(f"input is not a regular file: {path}")
    if not 0 < metadata.st_size <= maximum:
        os.close(descriptor)
        raise EvidenceError(f"input has an invalid byte size: {path}")
    return descriptor, metadata


def _identity(metadata):
    return (metadata.st_dev, metadata.st_ino, metadata.st_size,
        metadata.st_mtime_ns, metadata.st_ctime_ns)


def read_file_bytes(path, maximum):
    path = pathlib.Path(path)
    descriptor, before = _open_regular(path, maximum)
    try:
        with os.fdopen(descriptor, "rb") as stream:
            payload = stream.read(maximum + 1)
            after = os.fstat(stream.fileno())
    except BaseException:
        try:
            os.close(descriptor)
        except OSError:
            pass
        raise
    if len(payload) > maximum or len(payload) != before.st_size:
        raise EvidenceError(f"input changed size while read: {path}")
    if _identity(before) != _identity(after):
        raise EvidenceError(f"input changed while read: {path}")
    return payload


def _unique_object(path):
    def hook(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise EvidenceError(f"{path}: duplicate JSON key: {key}")
            value[key] = item
        return value
    return hook


def _invalid_constant(path):
    def reject(value):
        raise EvidenceError(f"{path}: invalid JSON constant: {value}")
    return reject


def _invalid_float(path):
    def reject(value):
        raise EvidenceError(f"{path}: floating JSON number is forbidden: {value}")
    return reject


def _canonical_integer(path):
    def parse(value):
        if value != "0" and (value.startswith("0") or value.startswith("-0")):
            raise EvidenceError(f"{path}: noncanonical JSON integer: {value}")
        return int(value)
    return parse


def parse_jsonl_bytes(payload, label, max_line_bytes, max_records):
    if not isinstance(payload, bytes):
        raise TypeError("JSONL payload must be bytes")
    if not payload:
        raise EvidenceError(f"{label}: JSONL payload is empty")
    if not isinstance(max_line_bytes, int) or isinstance(max_line_bytes, bool) or \
            max_line_bytes < 1:
        raise ValueError("line byte limit must be a positive integer")
    if not isinstance(max_records, int) or isinstance(max_records, bool) or \
            max_records < 1:
        raise ValueError("record limit must be a positive integer")
    path = pathlib.Path(label)
    records = []
    start = 0
    while start < len(payload):
        stop = payload.find(b"\n", start, start + max_line_bytes + 1)
        number = len(records) + 1
        if stop < 0:
            remaining = len(payload) - start
            if remaining >= max_line_bytes:
                raise EvidenceError(f"{path}:{number}: line exceeds its byte limit")
            raise EvidenceError(f"{path}:{number}: missing final newline")
        line = payload[start:stop + 1]
        if len(line) > max_line_bytes:
            raise EvidenceError(f"{path}:{number}: line exceeds its byte limit")
        try:
            decoded = line.decode("utf-8")
            record = json.loads(decoded,
                object_pairs_hook=_unique_object(path),
                parse_constant=_invalid_constant(path),
                parse_float=_invalid_float(path),
                parse_int=_canonical_integer(path))
        except (UnicodeError, json.JSONDecodeError) as error:
            raise EvidenceError(f"{path}:{number}: invalid JSON: {error}") from error
        records.append(record)
        if len(records) > max_records:
            raise EvidenceError(f"{path}: record count exceeds its limit")
        start = stop + 1
    return records


def read_jsonl(path, max_file_bytes, max_line_bytes, max_records):
    payload = read_file_bytes(path, max_file_bytes)
    return parse_jsonl_bytes(payload, path, max_line_bytes, max_records)


def _forbid_float(value):
    if isinstance(value, float):
        raise EvidenceError("floating JSON numbers are forbidden")
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise EvidenceError("JSON object keys must be strings")
            _forbid_float(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _forbid_float(item)
    return None


def canonical_json(value):
    _forbid_float(value)
    return json.dumps(value, ensure_ascii=False, allow_nan=False,
        separators=(",", ":"), sort_keys=True)


def jsonl_bytes(records):
    return b"".join((canonical_json(record) + "\n").encode("utf-8")
        for record in records)


def sha256_bytes(value):
    return hashlib.sha256(value).hexdigest()


def sha256_file(path, maximum=32 * 1024 * 1024):
    path = pathlib.Path(path)
    descriptor, before = _open_regular(path, maximum)
    digest = hashlib.sha256()
    try:
        with os.fdopen(descriptor, "rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(block)
            after = os.fstat(stream.fileno())
    except BaseException:
        try:
            os.close(descriptor)
        except OSError:
            pass
        raise
    if _identity(before) != _identity(after):
        raise EvidenceError(f"input changed while hashed: {path}")
    return digest.hexdigest()


def ensure_snapshots(snapshots):
    for path, expected in snapshots.items():
        if sha256_file(path) != expected:
            raise EvidenceError(f"input changed after its snapshot: {path}")
    return None


def capability_digest(case_id, capability_id, observations):
    envelope = {
        "capability_id": capability_id,
        "case_id": case_id,
        "observations": observations,
    }
    return sha256_bytes(canonical_json(envelope).encode("utf-8"))


def verify_python_toolchain(manifest):
    matches = [item for item in manifest["toolchain"]
        if item["id"] == "jsonschema-validator"]
    if len(matches) != 1 or matches[0]["status"] != "verified":
        raise EvidenceError("JSON Schema toolchain is not verified")
    toolchain = matches[0]
    artifacts = {item["name"]: item["sha256"]
        for item in toolchain["artifacts"]}
    if len(artifacts) != len(toolchain["artifacts"]):
        raise EvidenceError("JSON Schema toolchain artifacts are ambiguous")
    expected = artifacts.get("cpython-executable")
    executable = pathlib.Path(sys.executable).resolve(strict=True)
    if expected is None or sha256_file(executable) != expected:
        raise EvidenceError("Python executable hash is not pinned")
    versions = {
        "jsonschema": "4.26.0",
        "attrs": "25.4.0",
        "jsonschema-specifications": "2025.9.1",
        "referencing": "0.37.0",
        "rpds-py": "0.30.0",
    }
    try:
        actual = {name: importlib.metadata.version(name) for name in versions}
    except importlib.metadata.PackageNotFoundError as error:
        raise EvidenceError(f"pinned Python package is absent: {error}") from error
    if actual != versions:
        raise EvidenceError("Python package versions do not match the pin")
    return None


def _reject_output_alias(output, inputs):
    output = pathlib.Path(output)
    output_resolved = output.resolve(strict=False)
    for input_path in inputs:
        input_path = pathlib.Path(input_path)
        if output_resolved == input_path.resolve(strict=True):
            raise EvidenceError(f"output aliases an input: {output}")
        if output.exists() and os.path.samefile(output, input_path):
            raise EvidenceError(f"output aliases an input: {output}")
    return None


def _regular_output(path):
    try:
        metadata = pathlib.Path(path).stat(follow_symlinks=False)
    except FileNotFoundError:
        return False
    if not stat.S_ISREG(metadata.st_mode):
        raise EvidenceError(f"output is not a regular file: {path}")
    return True


def publish_bytes(path, payload, check, inputs):
    path = pathlib.Path(path)
    if not isinstance(payload, bytes):
        raise TypeError("output payload must be bytes")
    parent = path.parent
    if not parent.is_dir() or parent.is_symlink():
        raise EvidenceError(f"output parent is not a real directory: {parent}")
    _reject_output_alias(path, inputs)
    exists = _regular_output(path)
    if check:
        if not exists:
            raise EvidenceError(f"freshness output is absent: {path}")
        if read_file_bytes(path, max(1, len(payload))) != payload:
            raise EvidenceError(f"freshness output differs: {path}")
        return None
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=parent)
    temporary = pathlib.Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            os.fchmod(stream.fileno(), 0o644)
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        _regular_output(path)
        os.replace(temporary, path)
        directory = os.open(parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    return None


def parse_toml_bytes(payload, label):
    try:
        return tomllib.loads(payload.decode("utf-8"))
    except (UnicodeError, tomllib.TOMLDecodeError) as error:
        raise EvidenceError(f"invalid TOML input: {label}: {error}") from error


def load_toml(path, maximum=2 * 1024 * 1024):
    return parse_toml_bytes(read_file_bytes(path, maximum), path)


def normalizer_root():
    return pathlib.Path(__file__).resolve(strict=True).parent


def n6_root():
    return normalizer_root().parent


def repository_root():
    root = n6_root().parents[2]
    expected = root / "test" / "conformance" / "n6"
    if expected != n6_root():
        raise EvidenceError("normalizer is outside the canonical repository layout")
    return root
