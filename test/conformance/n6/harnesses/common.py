#!/usr/bin/env python3
import datetime
import decimal
import hashlib
import importlib
import io
import json
import os
import pathlib
import platform
import re
import stat
import struct
import sys
import tempfile
import tomllib
import zipfile
from contextlib import contextmanager


class HarnessError(Exception):
    pass


_CONTROL_FILE_LIMIT = 4 * 1024 * 1024
_READ_CHUNK = 1024 * 1024
_WHEEL_FILE_LIMIT = 128 * 1024 * 1024
_WHEEL_MEMBER_LIMIT = 256 * 1024 * 1024
_WHEEL_TOTAL_LIMIT = 512 * 1024 * 1024
_WHEEL_MEMBER_COUNT_LIMIT = 4096
_AUTHENTICATED_SNAPSHOT_ENV = "PARQUET_N6_AUTHENTICATED_SNAPSHOT"


def sha256_bytes(value):
    return hashlib.sha256(value).hexdigest()


def _metadata_identity(metadata):
    return (metadata.st_dev, metadata.st_ino, metadata.st_mode,
        metadata.st_nlink, metadata.st_size, metadata.st_mtime_ns,
        metadata.st_ctime_ns)


@contextmanager
def stable_regular_file(path, maximum_bytes, label):
    path = pathlib.Path(path)
    metadata = path.lstat()
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise HarnessError(f"{label} is not a regular file")
    if not 0 <= metadata.st_size <= maximum_bytes:
        raise HarnessError(f"{label} has an invalid size")
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise HarnessError(f"cannot open {label} safely") from error
    stream = os.fdopen(descriptor, "rb")
    try:
        opened = os.fstat(stream.fileno())
        if _metadata_identity(opened) != _metadata_identity(metadata):
            raise HarnessError(f"{label} changed while it was opened")
        yield stream, opened
        final = os.fstat(stream.fileno())
        if _metadata_identity(final) != _metadata_identity(opened):
            raise HarnessError(f"{label} changed while it was read")
    finally:
        stream.close()


def regular_file_bytes(path, maximum_bytes, label):
    with stable_regular_file(path, maximum_bytes, label) as (stream, metadata):
        value = stream.read(maximum_bytes + 1)
        if len(value) > maximum_bytes or len(value) != metadata.st_size:
            raise HarnessError(f"{label} changed size while it was read")
        if stream.read(1):
            raise HarnessError(f"{label} exceeds its byte limit")
    return value


def sha256_file(path, maximum_bytes=None):
    path = pathlib.Path(path)
    metadata = path.lstat()
    limit = metadata.st_size if maximum_bytes is None else maximum_bytes
    digest = hashlib.sha256()
    with stable_regular_file(path, limit, "hashed input") as (stream, opened):
        total = 0
        while chunk := stream.read(_READ_CHUNK):
            total += len(chunk)
            if total > limit:
                raise HarnessError("hashed input exceeds its byte limit")
            digest.update(chunk)
        if total != opened.st_size:
            raise HarnessError("hashed input changed size while it was read")
    return digest.hexdigest()


def canonical_json(value, *, ascii_only=False):
    return json.dumps(value, allow_nan=False, ensure_ascii=ascii_only,
        separators=(",", ":"), sort_keys=True)


def observation_digest(case_id, capability_id, observations):
    envelope = {
        "capability_id": capability_id,
        "case_id": case_id,
        "observations": observations,
    }
    return sha256_bytes(canonical_json(envelope).encode("utf-8"))


def safe_relative(value):
    if not value or "\\" in value or value.startswith("/"):
        return False
    parts = value.split("/")
    if any(part in ("", ".", "..") for part in parts):
        return False
    return all(character.isascii() and (character.isalnum() or
        character in "_ .+@=-/".replace(" ", "")) for character in value)


def checked_file(root, relative, expected_sha256, expected_size,
        snapshot_directory):
    if not safe_relative(relative):
        raise HarnessError(f"unsafe fixture path: {relative!r}")
    root_input = pathlib.Path(root)
    if root_input.is_symlink():
        raise HarnessError("fixture root is a symbolic link")
    root = root_input.resolve(strict=True)
    candidate = root.joinpath(*pathlib.PurePosixPath(relative).parts)
    current = root
    for part in pathlib.PurePosixPath(relative).parts:
        current = current / part
        if current.is_symlink():
            raise HarnessError(f"fixture path contains a symbolic link: {relative}")
    metadata = candidate.lstat()
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise HarnessError(f"fixture is not a regular file: {relative}")
    resolved = candidate.resolve(strict=True)
    try:
        resolved.relative_to(root)
    except ValueError as error:
        raise HarnessError(f"fixture escapes its root: {relative}") from error
    value = regular_file_bytes(resolved, expected_size,
        f"fixture {relative}")
    if len(value) != expected_size:
        raise HarnessError(f"fixture size differs: {relative}")
    if sha256_bytes(value) != expected_sha256:
        raise HarnessError(f"fixture digest differs: {relative}")
    snapshot_root = pathlib.Path(snapshot_directory)
    snapshot_metadata = snapshot_root.lstat()
    if stat.S_ISLNK(snapshot_metadata.st_mode) or \
            not stat.S_ISDIR(snapshot_metadata.st_mode):
        raise HarnessError("fixture snapshot root is not a directory")
    descriptor, snapshot = tempfile.mkstemp(prefix="fixture-", suffix=".parquet",
        dir=snapshot_root)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(value)
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        try:
            os.unlink(snapshot)
        except FileNotFoundError:
            pass
        raise
    return pathlib.Path(snapshot)


def load_toml_snapshot(path, label="TOML input"):
    source = regular_file_bytes(path, _CONTROL_FILE_LIMIT, label)
    try:
        value = tomllib.loads(source.decode("utf-8"))
    except (UnicodeError, tomllib.TOMLDecodeError) as error:
        raise HarnessError(f"{label} is invalid") from error
    return value, source, sha256_bytes(source)


def load_toml(path):
    value, _, _ = load_toml_snapshot(path)
    return value


def tree_sha256(root):
    root_input = pathlib.Path(root)
    if root_input.is_symlink():
        raise HarnessError("Python runtime root is a symbolic link")
    root = root_input.resolve(strict=True)
    entries = []
    for directory, directories, files in os.walk(root, followlinks=False):
        for name in directories + files:
            path = pathlib.Path(directory) / name
            if path.is_file() or path.is_symlink():
                entries.append(path.relative_to(root).as_posix())
    digest = hashlib.sha256()
    for relative in sorted(entries):
        path = root.joinpath(*pathlib.PurePosixPath(relative).parts)
        if path.is_symlink():
            row = b"L\0" + relative.encode("utf-8") + b"\0" + \
                os.readlink(path).encode("utf-8") + b"\n"
        else:
            row = b"F\0" + relative.encode("utf-8") + b"\0" + \
                sha256_file(path).encode("ascii") + b"\n"
        digest.update(row)
    return digest.hexdigest()


def runtime_tree_policy_violations(root):
    root = pathlib.Path(root).resolve(strict=True)
    violations = []
    for directory, directories, files in os.walk(root, followlinks=False):
        for name in directories:
            if name in ("site-packages", "__pycache__"):
                path = pathlib.Path(directory, name).relative_to(root)
                violations.append(path.as_posix())
        for name in files:
            if name.endswith(".pyc"):
                path = pathlib.Path(directory, name).relative_to(root)
                violations.append(path.as_posix())
        if len(violations) >= 16:
            break
    return sorted(violations)


def _wheel_entry_path(info):
    name = info.filename
    relative = name[:-1] if name.endswith("/") else name
    if not safe_relative(relative):
        raise HarnessError(f"wheel member path is unsafe: {name!r}")
    parts = pathlib.PurePosixPath(relative).parts
    if parts[0].endswith(".data"):
        raise HarnessError("wheel .data installation schemes are unsupported")
    mode = info.external_attr >> 16
    kind = stat.S_IFMT(mode)
    expected_kinds = (0, stat.S_IFDIR) if info.is_dir() else (0, stat.S_IFREG)
    if kind not in expected_kinds:
        raise HarnessError(f"wheel member is not a regular file: {name}")
    if info.flag_bits & 1:
        raise HarnessError(f"wheel member is encrypted: {name}")
    if info.compress_type not in (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED):
        raise HarnessError(f"wheel member compression is unsupported: {name}")
    if not 0 <= info.file_size <= _WHEEL_MEMBER_LIMIT:
        raise HarnessError(f"wheel member exceeds its size limit: {name}")
    return parts


def _wheel_descriptor_entry(descriptor):
    wheels = descriptor.get("wheels")
    if not isinstance(wheels, list) or len(wheels) != 1:
        raise HarnessError("Python descriptor must name exactly one wheel")
    entry = wheels[0]
    if not isinstance(entry, dict) or set(entry) != {"name", "sha256"} or \
            not isinstance(entry["name"], str) or \
            not isinstance(entry["sha256"], str) or \
            pathlib.PurePosixPath(entry["name"]).name != entry["name"] or \
            not safe_relative(entry["name"]) or len(entry["sha256"]) != 64 or \
            any(character not in "0123456789abcdef"
                for character in entry["sha256"]):
        raise HarnessError("Python descriptor wheel entry is invalid")
    return entry


@contextmanager
def wheel_import_root(wheel_path, descriptor):
    entry = _wheel_descriptor_entry(descriptor)
    wheel = pathlib.Path(wheel_path)
    if wheel.name != entry["name"]:
        raise HarnessError("wheel filename differs from its descriptor")
    value = regular_file_bytes(wheel, _WHEEL_FILE_LIMIT, "wheel input")
    if sha256_bytes(value) != entry["sha256"]:
        raise HarnessError("wheel digest differs from its descriptor")
    try:
        archive = zipfile.ZipFile(io.BytesIO(value))
    except zipfile.BadZipFile as error:
        raise HarnessError("wheel input is not a valid ZIP archive") from error
    with archive:
        infos = archive.infolist()
        if not 0 < len(infos) <= _WHEEL_MEMBER_COUNT_LIMIT:
            raise HarnessError("wheel member count exceeds its limit")
        names = [info.filename for info in infos]
        if len(names) != len(set(names)):
            raise HarnessError("wheel contains duplicate member names")
        paths = [_wheel_entry_path(info) for info in infos]
        if sum(info.file_size for info in infos) > _WHEEL_TOTAL_LIMIT:
            raise HarnessError("wheel expands beyond its total size limit")
        dist_info = {parts[0] for parts in paths
            if parts[0].endswith(".dist-info")}
        if len(dist_info) != 1:
            raise HarnessError("wheel dist-info directory is ambiguous")
        required = {
            next(iter(dist_info)) + "/RECORD",
            next(iter(dist_info)) + "/WHEEL",
        }
        if not required.issubset(names):
            raise HarnessError("wheel metadata files are incomplete")
        with tempfile.TemporaryDirectory(prefix="parquet-n6-wheel-") as directory:
            root = pathlib.Path(directory).resolve(strict=True)
            for info, parts in zip(infos, paths):
                destination = root.joinpath(*parts)
                if info.is_dir():
                    destination.mkdir(parents=True, exist_ok=True, mode=0o700)
                    continue
                destination.parent.mkdir(parents=True, exist_ok=True,
                    mode=0o700)
                flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
                if hasattr(os, "O_NOFOLLOW"):
                    flags |= os.O_NOFOLLOW
                descriptor_fd = os.open(destination, flags, 0o600)
                total = 0
                try:
                    with os.fdopen(descriptor_fd, "wb") as output, \
                            archive.open(info, "r") as source:
                        while chunk := source.read(_READ_CHUNK):
                            total += len(chunk)
                            if total > info.file_size:
                                raise HarnessError(
                                    f"wheel member changed size: {info.filename}")
                            output.write(chunk)
                        output.flush()
                        os.fsync(output.fileno())
                except BaseException:
                    try:
                        destination.unlink()
                    except FileNotFoundError:
                        pass
                    raise
                if total != info.file_size:
                    raise HarnessError(
                        f"wheel member changed size: {info.filename}")
            yield root


def _wheel_owned_modules(root):
    owned = set()
    for path in pathlib.Path(root).iterdir():
        if path.is_dir():
            if path.name.isidentifier():
                owned.add(path.name)
            continue
        if path.is_file() and path.name.endswith((".py", ".pyc", ".so", ".pyd")):
            name = path.name.split(".", 1)[0]
            if name.isidentifier():
                owned.add(name)
    return owned


def _owned_module_name(name, owned):
    return any(name == top or name.startswith(top + ".") for top in owned)


def _verify_module_origin(name, module, root):
    origins = []
    module_file = getattr(module, "__file__", None)
    if module_file is not None:
        origins.append(module_file)
    specification = getattr(module, "__spec__", None)
    specification_origin = getattr(specification, "origin", None)
    if specification_origin is not None and specification_origin not in origins:
        origins.append(specification_origin)
    locations = getattr(specification, "submodule_search_locations", None)
    if not origins and locations is not None:
        origins.extend(locations)
    if not origins:
        raise HarnessError(f"wheel module has no file origin: {name}")
    for source in origins:
        if not isinstance(source, (str, os.PathLike)) or source in (
                "built-in", "frozen"):
            raise HarnessError(f"wheel module has an invalid origin: {name}")
        try:
            pathlib.Path(source).resolve(strict=True).relative_to(root)
        except (OSError, ValueError) as error:
            raise HarnessError(
                f"module did not load from the pinned wheel: {name}") from error
    return


def _verify_owned_module_origins(owned, root):
    loaded = {name: module for name, module in sys.modules.items()
        if _owned_module_name(name, owned)}
    for name, module in loaded.items():
        _verify_module_origin(name, module, root)
    return


@contextmanager
def exact_wheel_module(wheel_path, descriptor, module_name, version):
    with wheel_import_root(wheel_path, descriptor) as root:
        owned = _wheel_owned_modules(root)
        if module_name not in owned:
            raise HarnessError(f"wheel does not own its requested module: {module_name}")
        preloaded = sorted(name for name in sys.modules
            if _owned_module_name(name, owned))
        if preloaded:
            raise HarnessError(
                f"wheel module is already loaded: {preloaded[0]}")
        sys.path.insert(0, str(root))
        try:
            module = importlib.import_module(module_name)
            _verify_owned_module_origins(owned, root)
            if getattr(module, "__version__", None) != version:
                raise HarnessError(f"wheel module version differs: {module_name}")
            yield module
        finally:
            try:
                _verify_owned_module_origins(owned, root)
            finally:
                try:
                    sys.path.remove(str(root))
                except ValueError:
                    pass
                for name in [name for name in sys.modules
                        if _owned_module_name(name, owned)]:
                    del sys.modules[name]
                importlib.invalidate_caches()


def verify_python_runtime(descriptor):
    if sys.implementation.name != "cpython":
        raise HarnessError("Python runtime is not CPython")
    expected_version = tuple(int(part) for part in
        descriptor["python_version"].split("."))
    if len(expected_version) != 3 or sys.version_info[:3] != expected_version:
        raise HarnessError("Python runtime version differs")
    if not sys.dont_write_bytecode:
        raise HarnessError("Python runtime must use -B")
    if not sys.flags.isolated:
        raise HarnessError("Python runtime must use -I")
    if not sys.flags.no_site:
        raise HarnessError("Python runtime must use -S")
    distribution_url = descriptor.get("python_distribution_url")
    if not isinstance(distribution_url, str) or not distribution_url.startswith(
            "https://github.com/astral-sh/python-build-standalone/"
            "releases/download/") or not distribution_url.endswith(".tar.gz") or \
            any(character.isspace() for character in distribution_url):
        raise HarnessError("Python distribution URL is invalid")
    distribution_sha256 = descriptor.get("python_distribution_sha256")
    if not isinstance(distribution_sha256, str) or re.fullmatch(
            r"[0-9a-f]{64}", distribution_sha256) is None:
        raise HarnessError("Python distribution digest is invalid")
    if descriptor.get("python_tree_policy") != \
            "extract-strip-site-packages-bytecode-v1":
        raise HarnessError("Python runtime tree policy differs")
    violations = runtime_tree_policy_violations(sys.base_prefix)
    if violations:
        raise HarnessError(
            f"Python runtime tree violates its clean policy: {violations[0]}")
    executable = pathlib.Path(sys.executable).resolve(strict=True)
    if sha256_file(executable) != descriptor["python_executable_sha256"]:
        raise HarnessError("Python executable digest differs")
    runtime_root = pathlib.Path(sys.base_prefix)
    if tree_sha256(runtime_root) != descriptor["python_tree_sha256"]:
        raise HarnessError("Python runtime tree digest differs")
    if any("site-packages" in pathlib.PurePath(entry).parts for entry in sys.path):
        raise HarnessError("Python runtime loaded a site-packages path")
    system = platform.system()
    machine = platform.machine()
    macos = platform.mac_ver()[0].split(".", 1)[0]
    actual_platform = f"macos-{macos}-{machine}" if system == "Darwin" else \
        f"{system.lower()}-{machine}"
    if actual_platform != descriptor["platform"]:
        raise HarnessError("Python runtime platform differs")
    return


def authority(capabilities, producer):
    matches = [item for item in capabilities["authority"]
        if item["id"] == producer]
    if len(matches) != 1:
        raise HarnessError(f"authority is absent or ambiguous: {producer}")
    return matches[0]


def fixture_map(fixtures):
    output = {}
    for source in fixtures["fixture"]:
        item = dict(source)
        item["digest_contract"] = fixtures["default_digest_contract"]
        item["generated"] = False
        item["semantic"] = False
        if item["id"] in output:
            raise HarnessError(f"duplicate fixture ID: {item['id']}")
        output[item["id"]] = item
    for source in fixtures["generated_case"]:
        item = dict(source)
        item["file"] = item["output_file"]
        item["generated"] = True
        item["semantic"] = False
        if item["id"] in output:
            raise HarnessError(f"duplicate fixture ID: {item['id']}")
        output[item["id"]] = item
    return output


def semantic_case_map(repository, manifest, capabilities, fixtures):
    relative = "test/conformance/n6/model/cases.toml"
    entries = [item for item in manifest["frozen_model"]
        if item["file"] == relative]
    if len(entries) != 1:
        raise HarnessError("semantic case manifest is absent or ambiguous")
    path = repository_input(repository, relative)
    value, _, digest = load_toml_snapshot(path, "semantic case manifest")
    if digest != entries[0]["sha256"]:
        raise HarnessError("semantic case manifest digest differs")
    if value.get("schema_version") != 1 or \
            not isinstance(value.get("case_groups"), list):
        raise HarnessError("semantic case manifest header differs")
    capability_ids = {item["id"] for item in capabilities["capability"]}
    contracts = set(fixtures["digest_contracts"])
    output = {}
    for source in value["case_groups"]:
        required = {"id", "requirements", "capabilities", "digest_contract",
            "expected_sha256"}
        if not isinstance(source, dict) or set(source) != required:
            raise HarnessError("semantic case fields differ")
        identifier = source["id"]
        case_capabilities = source["capabilities"]
        expected = source["expected_sha256"]
        if not isinstance(identifier, str) or re.fullmatch(
                r"[a-z0-9]+(?:[._-][a-z0-9]+)*", identifier) is None or \
                identifier in output:
            raise HarnessError("semantic case ID is invalid or duplicated")
        if not isinstance(source["requirements"], list) or \
                not source["requirements"] or any(not isinstance(item, str) or
                not item for item in source["requirements"]):
            raise HarnessError(f"semantic requirements differ: {identifier}")
        if not isinstance(case_capabilities, list) or \
                case_capabilities != sorted(set(case_capabilities)) or \
                any(item not in capability_ids for item in case_capabilities):
            raise HarnessError(f"semantic capabilities differ: {identifier}")
        if source["digest_contract"] not in contracts or \
                not isinstance(expected, dict) or \
                set(expected) != set(case_capabilities) or \
                any(not isinstance(digest, str) or
                    re.fullmatch(r"[0-9a-f]{64}", digest) is None
                    for digest in expected.values()):
            raise HarnessError(f"semantic digests differ: {identifier}")
        item = dict(source)
        item["generated"] = False
        item["semantic"] = True
        output[identifier] = item
    return output, path


def selected_claims(authority_record):
    output = {}
    for claim in authority_record["claim"]:
        if claim["status"] == "not_assessed":
            continue
        if claim["status"] not in ("planned", "verified", "unsupported"):
            raise HarnessError(f"invalid claim status: {claim['status']}")
        for case_id in claim["cases"]:
            key = (case_id, claim["capability"])
            if key in output:
                raise HarnessError(f"duplicate authority claim: {key}")
            output[key] = claim["status"]
    return output


def load_raw_facts(path, maximum_bytes, maximum_line_bytes, maximum_records):
    path = pathlib.Path(path)
    files = {}
    columns = {}
    run = None
    digest = hashlib.sha256()

    def unique_object(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise HarnessError(f"duplicate raw evidence key: {key}")
            value[key] = item
        return value

    def invalid_number(value):
        raise HarnessError(f"invalid JSON number: {value}")

    def canonical_integer(value):
        if value != "0" and (value.startswith("0") or value.startswith("-0")):
            raise HarnessError(f"noncanonical JSON integer: {value}")
        return int(value)

    with stable_regular_file(path, maximum_bytes,
            "normalized raw evidence") as (stream, metadata):
        if metadata.st_size == 0:
            raise HarnessError("normalized raw evidence is empty")
        line_number = 0
        total = 0
        while True:
            line = stream.readline(maximum_line_bytes + 1)
            if not line:
                break
            line_number += 1
            if line_number > maximum_records:
                raise HarnessError("normalized raw evidence has too many records")
            if len(line) > maximum_line_bytes:
                raise HarnessError(
                    f"normalized raw evidence line {line_number} is too large")
            if not line.endswith(b"\n"):
                raise HarnessError(
                    f"normalized raw evidence line {line_number} has no LF")
            total += len(line)
            if total > maximum_bytes:
                raise HarnessError("normalized raw evidence exceeds its byte limit")
            digest.update(line)
            try:
                record = json.loads(line, object_pairs_hook=unique_object,
                    parse_constant=invalid_number, parse_float=invalid_number,
                    parse_int=canonical_integer)
            except (json.JSONDecodeError, UnicodeError) as error:
                raise HarnessError(
                    f"raw evidence line {line_number} is invalid JSON") from error
            if not isinstance(record, dict):
                raise HarnessError(
                    f"raw evidence line {line_number} is not an object")
            expected = canonical_json(record, ascii_only=True).encode("utf-8") + \
                b"\n"
            if line != expected:
                raise HarnessError(
                    f"raw evidence line {line_number} is not canonical JSONL")
            kind = record.get("record")
            if kind == "run":
                if line_number != 1 or run is not None:
                    raise HarnessError("raw run record is absent or misplaced")
                run = record
            elif kind == "file":
                case_id = record["case_id"]
                if case_id in files:
                    raise HarnessError(f"duplicate raw file fact: {case_id}")
                files[case_id] = record
            elif kind == "column_statistics":
                key = (record["case_id"], record["row_group"], record["leaf"])
                if key in columns:
                    raise HarnessError(f"duplicate raw column fact: {key}")
                columns[key] = record
            elif kind != "case_result":
                raise HarnessError(
                    f"unknown raw evidence record on line {line_number}")
        if total != metadata.st_size:
            raise HarnessError(
                "normalized raw evidence changed size while it was read")
    if run is None or run.get("producer") != "n6-raw-java" or \
            run.get("schema_version") != 2:
        raise HarnessError("normalized raw evidence has the wrong run record")
    return run, files, columns, digest.hexdigest()


def leaf_records(case_id, raw_columns):
    selected = [record for (current, row_group, _), record in raw_columns.items()
        if current == case_id and row_group == 0]
    selected.sort(key=lambda record: record["leaf"])
    if [record["leaf"] for record in selected] != list(range(len(selected))):
        raise HarnessError(f"raw leaf ordinals are incomplete: {case_id}")
    return selected


def epoch_nanoseconds(value):
    if value.tzinfo is None:
        epoch = datetime.datetime(1970, 1, 1)
    else:
        value = value.astimezone(datetime.timezone.utc)
        epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
    delta = value - epoch
    return ((delta.days * 86400 + delta.seconds) * 1_000_000_000 +
        delta.microseconds * 1000)


def decimal_unscaled(value, scale):
    if not value.is_finite():
        raise HarnessError("DECIMAL value is not finite")
    parts = value.as_tuple()
    coefficient = 0
    for digit in parts.digits:
        coefficient = coefficient * 10 + digit
    shift = parts.exponent + scale
    if shift >= 0:
        coefficient *= 10 ** shift
    else:
        divisor = 10 ** -shift
        coefficient, remainder = divmod(coefficient, divisor)
        if remainder != 0:
            raise HarnessError("DECIMAL value does not fit its declared scale")
    if parts.sign:
        coefficient = -coefficient
    return str(coefficient)


def canonical_value(value, leaf_schema):
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, bytes):
        return {"bytes_hex": value.hex()}
    if isinstance(value, str):
        return value
    if isinstance(value, decimal.Decimal):
        scale = leaf_schema["scale"]
        if scale is None:
            raise HarnessError("DECIMAL value has no declared scale")
        return {"decimal_scale": scale,
            "unscaled": decimal_unscaled(value, scale)}
    if isinstance(value, datetime.datetime):
        return {"timestamp_nanoseconds": str(epoch_nanoseconds(value))}
    if isinstance(value, datetime.date):
        return {"date_days": (value - datetime.date(1970, 1, 1)).days}
    if isinstance(value, datetime.time):
        nanoseconds = ((value.hour * 3600 + value.minute * 60 + value.second) *
            1_000_000_000 + value.microsecond * 1000)
        return {"time_nanoseconds": str(nanoseconds)}
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        physical = leaf_schema["physical_type"]
        if physical == "FLOAT":
            return {"float32_bits": struct.pack(">f", value).hex()}
        if physical == "DOUBLE":
            return {"float64_bits": struct.pack(">d", value).hex()}
        raise HarnessError(f"floating value has physical type {physical}")
    raise HarnessError(f"unsupported logical value type: {type(value).__name__}")


def logical_observations(case_id, raw_columns, columns, row_count):
    leaves = leaf_records(case_id, raw_columns)
    if len(columns) != len(leaves):
        raise HarnessError(f"decoded leaf count differs: {case_id}")
    normalized = []
    for leaf, values in zip(leaves, columns):
        if len(values) != row_count:
            raise HarnessError(f"decoded column length differs: {case_id}")
        normalized.append({
            "logical_type": leaf["leaf_schema"]["logical_type"],
            "path": leaf["path"],
            "physical_type": leaf["leaf_schema"]["physical_type"],
            "values": [canonical_value(value, leaf["leaf_schema"])
                for value in values],
        })
    return [{
        "columns": normalized,
        "contract": "n6-logical-values-v1",
        "row_count": row_count,
    }]


def frozen_input_paths(root, manifest):
    paths = {
        "plan_sha256": manifest["plan_file"],
        "capabilities_sha256": manifest["capabilities_file"],
        "fixture_manifest_sha256": manifest["fixture_manifest_file"],
        "corpus_manifest_sha256": manifest["corpus_manifest_file"],
        "evidence_schema_sha256": manifest["evidence_schema_file"],
    }
    return {field: repository_input(root, relative)
        for field, relative in paths.items()}


def frozen_input_hashes(root, manifest, known_hashes=None):
    known_hashes = {} if known_hashes is None else known_hashes
    return {field: known_hashes[field] if field in known_hashes else
        sha256_file(path) for field, path in
        frozen_input_paths(root, manifest).items()}


def run_record(evidence_id, producer, authority_record, descriptor_sha256,
        root, manifest, unsupported_cases, upstream_evidence,
        input_hashes=None):
    record = {
        "record": "run",
        "schema_version": 2,
        "evidence_id": evidence_id,
        "producer": producer,
        "producer_version": authority_record["version"],
        "source_revision": authority_record["revision"],
        "toolchain_sha256": descriptor_sha256,
        "unsupported_cases": sorted(unsupported_cases),
        "upstream_evidence": [upstream_evidence],
    }
    hashes = frozen_input_hashes(root, manifest) if input_hashes is None else \
        input_hashes
    record.update(hashes)
    return record


def case_result(case_id, capability_id, contract, status, observations=None,
        expected_sha256=None):
    if status == "UNSUPPORTED":
        return {
            "record": "case_result",
            "schema_version": 2,
            "case_id": case_id,
            "capability_id": capability_id,
            "digest_contract": contract,
            "status": status,
            "expected_sha256": None,
            "actual_sha256": None,
            "detail": "The reviewed capability matrix marks this result unsupported.",
        }
    if status != "PASS":
        raise HarnessError(f"unsupported requested result status: {status}")
    actual = observation_digest(case_id, capability_id, observations)
    expected = actual if expected_sha256 is None else expected_sha256
    if not isinstance(expected, str) or re.fullmatch(
            r"[0-9a-f]{64}", expected) is None:
        raise HarnessError("expected capability digest is invalid")
    result_status = "PASS" if actual == expected else "FAIL"
    return {
        "record": "case_result",
        "schema_version": 2,
        "case_id": case_id,
        "capability_id": capability_id,
        "digest_contract": contract,
        "status": result_status,
        "expected_sha256": expected,
        "actual_sha256": actual,
        "detail": "Observed values match the frozen canonical digest."
            if result_status == "PASS" else
            "Observed values differ from the frozen canonical digest.",
    }


def evidence_bytes(records, maximum_bytes, maximum_line_bytes, maximum_records):
    if not 0 < len(records) <= maximum_records:
        raise HarnessError("evidence record count exceeds its limit")
    output = bytearray()
    for record in records:
        line = canonical_json(record, ascii_only=True).encode("utf-8") + b"\n"
        if len(line) > maximum_line_bytes:
            raise HarnessError("evidence line exceeds its byte limit")
        output.extend(line)
        if len(output) > maximum_bytes:
            raise HarnessError("evidence output exceeds its byte limit")
    return bytes(output)


def output_path(path, create_parents):
    requested = pathlib.Path(os.path.abspath(path))
    if create_parents:
        requested.parent.mkdir(parents=True, exist_ok=True)
    elif not requested.parent.is_dir():
        raise HarnessError("evidence output parent does not exist")
    parent = requested.parent.resolve(strict=True)
    if not parent.is_dir():
        raise HarnessError("evidence output parent is not a directory")
    path = parent / requested.name
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return path
    if stat.S_ISLNK(metadata.st_mode):
        raise HarnessError("evidence output is a symbolic link")
    if not stat.S_ISREG(metadata.st_mode):
        raise HarnessError("evidence output is not a regular file")
    return path


def manifest_output_path(repository, target_entry, requested):
    relative = target_entry.get("file")
    if not isinstance(relative, str) or not safe_relative(relative):
        raise HarnessError("target evidence file is invalid")
    root = pathlib.Path(repository).resolve(strict=True)
    expected = root.joinpath(*pathlib.PurePosixPath(relative).parts)
    current = root
    for part in pathlib.PurePosixPath(relative).parts[:-1]:
        current = current / part
        metadata = current.lstat()
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            raise HarnessError("target evidence parent is not a safe directory")
    destination = pathlib.Path(os.path.abspath(requested))
    if destination != expected:
        raise HarnessError("evidence output does not select the manifest target")
    return expected


def atomic_output(path, value, check):
    path = output_path(path, not check)
    if check:
        try:
            actual = regular_file_bytes(path, len(value), "evidence output")
        except (FileNotFoundError, HarnessError) as error:
            raise HarnessError("evidence output is stale") from error
        if actual != value:
            raise HarnessError("evidence output is stale")
        return
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.",
        dir=path.parent)
    try:
        os.fchmod(descriptor, 0o644)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(value)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def repository_input(repository, relative):
    if not safe_relative(relative):
        raise HarnessError(f"unsafe repository input path: {relative!r}")
    root = pathlib.Path(repository).resolve(strict=True)
    candidate = root.joinpath(*pathlib.PurePosixPath(relative).parts)
    current = root
    for part in pathlib.PurePosixPath(relative).parts:
        current = current / part
        if current.is_symlink():
            raise HarnessError(
                f"repository input contains a symbolic link: {relative}")
    if not candidate.is_file():
        raise HarnessError(f"repository input is not a regular file: {relative}")
    return candidate


def evidence_entry(manifest, evidence_id):
    matches = []
    for section in ("frozen_evidence", "planned_evidence"):
        matches.extend(item for item in manifest.get(section, [])
            if item["id"] == evidence_id)
    if len(matches) != 1:
        raise HarnessError(
            f"evidence entry is absent or ambiguous: {evidence_id}")
    return matches[0]


def authenticated_source_overrides(repository, descriptor, descriptor_path,
        executed_harness):
    snapshot_value = os.environ.get(_AUTHENTICATED_SNAPSHOT_ENV)
    if snapshot_value is None:
        raise HarnessError("authenticated source snapshot is absent")
    snapshot = pathlib.Path(snapshot_value).resolve(strict=True)
    descriptor_path = pathlib.Path(descriptor_path).resolve(strict=True)
    harness_path = pathlib.Path(executed_harness).resolve(strict=True)
    common_path = pathlib.Path(__file__).resolve(strict=True)
    if descriptor_path != snapshot / "descriptor.toml" or \
            harness_path.parent != snapshot or common_path != snapshot / \
            "common.py":
        raise HarnessError("authenticated source snapshot paths differ")
    harness_relative = descriptor.get("harness_file")
    if not isinstance(harness_relative, str) or harness_path.name != \
            pathlib.PurePosixPath(harness_relative).name:
        raise HarnessError("authenticated harness source differs")
    common_relative = (pathlib.PurePosixPath(harness_relative).parent /
        "common.py").as_posix()
    matches = [item for item in descriptor.get("support_files", [])
        if isinstance(item, dict) and item.get("file") == common_relative]
    if len(matches) != 1:
        raise HarnessError("authenticated common source is not declared")
    repository_input(repository, descriptor["test_file"])
    return {
        harness_relative: harness_path,
        common_relative: common_path,
    }


def descriptor_repository_inputs(repository, descriptor,
        source_overrides=None):
    source_overrides = {} if source_overrides is None else source_overrides
    declared = []
    if "harness_file" in descriptor:
        declared.append((descriptor["harness_file"],
            descriptor.get("harness_sha256")))
    if "test_file" in descriptor:
        declared.append((descriptor["test_file"],
            descriptor.get("test_sha256")))
    for item in descriptor.get("support_files", []):
        declared.append((item.get("file"), item.get("sha256")))
    for item in descriptor.get("wrapper", []):
        declared.append((item.get("path"), item.get("sha256")))
    paths = []
    for relative, expected in declared:
        if not isinstance(relative, str) or not isinstance(expected, str):
            raise HarnessError("descriptor source binding is incomplete")
        path = source_overrides.get(relative)
        if path is None:
            path = repository_input(repository, relative)
        else:
            path = pathlib.Path(path).resolve(strict=True)
        if sha256_file(path) != expected:
            raise HarnessError(f"descriptor source digest differs: {relative}")
        paths.append(path)
    return paths


def verify_python_descriptor(descriptor, repository, producer,
        source_overrides=None, descriptor_inputs=None):
    if descriptor.get("authority") != producer:
        raise HarnessError("Python descriptor authority differs")
    if descriptor.get("harness_status") != descriptor.get("status"):
        raise HarnessError("Python descriptor harness status differs")
    required = ("python_version", "python_executable_sha256",
        "python_tree_sha256", "python_distribution_url",
        "python_distribution_sha256", "python_tree_policy", "platform",
        "harness_file", "harness_sha256", "test_file", "test_sha256",
        "support_files", "wheels")
    if any(field not in descriptor for field in required):
        raise HarnessError("Python descriptor is incomplete")
    if descriptor_inputs is None:
        descriptor_repository_inputs(repository, descriptor, source_overrides)
    _wheel_descriptor_entry(descriptor)
    verify_python_runtime(descriptor)
    return


def reject_output_alias(output, inputs, protected_roots):
    destination = pathlib.Path(os.path.abspath(output)).resolve(strict=False)
    for source in inputs:
        resolved_source = pathlib.Path(source).resolve(strict=True)
        same_file = destination.exists() and os.path.samefile(destination,
            resolved_source)
        if destination == resolved_source or same_file:
            raise HarnessError("evidence output aliases an input")
    for source in protected_roots:
        root = pathlib.Path(source).resolve(strict=True)
        try:
            destination.relative_to(root)
        except ValueError:
            continue
        raise HarnessError("evidence output is inside a protected input root")


def _verify_evidence_bindings(manifest, producer, evidence_id, raw_entry,
        raw_evidence_sha256, draft):
    raw_id = "normalized-raw-java-apache-corpus"
    if raw_entry.get("authority") != "n6-raw-java" or \
            raw_entry.get("format") != "normalized-jsonl":
        raise HarnessError("normalized raw evidence declaration differs")
    raw_status = raw_entry.get("status")
    if raw_status == "verified":
        if raw_entry.get("sha256") != raw_evidence_sha256:
            raise HarnessError("normalized raw evidence digest differs")
    elif not draft or raw_status != "planned" or "sha256" in raw_entry:
        raise HarnessError("normalized raw evidence is not frozen")
    target = evidence_entry(manifest, evidence_id)
    if target.get("authority") != producer or \
            target.get("format") != "normalized-jsonl":
        raise HarnessError("target evidence declaration differs")
    if target.get("upstream_evidence") != [raw_id]:
        raise HarnessError("target evidence upstream declaration differs")
    if draft:
        if target.get("status") not in ("planned", "verified"):
            raise HarnessError("target evidence draft status differs")
    elif target.get("status") != "verified":
        raise HarnessError("target evidence is not frozen")
    return target


def build_context(arguments, producer, evidence_id,
        allow_unpinned_descriptor=False, draft_evidence=False,
        source_overrides=None, executed_harness=None):
    repository_input_path = pathlib.Path(arguments.repository)
    if repository_input_path.is_symlink():
        raise HarnessError("repository root is a symbolic link")
    repository = repository_input_path.resolve(strict=True)
    manifest_path = pathlib.Path(arguments.manifest).resolve(strict=True)
    expected_manifest = repository_input(repository,
        "test/conformance/n6/manifest.toml")
    if manifest_path != expected_manifest:
        raise HarnessError("manifest path does not select the repository input")
    manifest, _, _ = load_toml_snapshot(expected_manifest, "N6 manifest")
    expected_capabilities = repository_input(repository,
        manifest["capabilities_file"])
    expected_fixtures = repository_input(repository,
        manifest["fixture_manifest_file"])
    if pathlib.Path(arguments.capabilities).resolve(strict=True) != \
            expected_capabilities:
        raise HarnessError("capability path does not select the frozen input")
    if pathlib.Path(arguments.fixtures).resolve(strict=True) != expected_fixtures:
        raise HarnessError("fixture path does not select the frozen input")
    capabilities, _, capabilities_sha256 = load_toml_snapshot(
        expected_capabilities, "capability manifest")
    fixtures, _, fixtures_sha256 = load_toml_snapshot(expected_fixtures,
        "fixture manifest")
    authority_record = authority(capabilities, producer)
    descriptor_path = pathlib.Path(arguments.descriptor).resolve(strict=True)
    descriptor, _, descriptor_sha256 = load_toml_snapshot(descriptor_path,
        "Python toolchain descriptor")
    if executed_harness is not None:
        if source_overrides is not None:
            raise HarnessError("authenticated source overrides are ambiguous")
        source_overrides = authenticated_source_overrides(repository, descriptor,
            descriptor_path, executed_harness)
    authorized = authority_record["toolchain_sha256"]
    if allow_unpinned_descriptor:
        if authorized or descriptor.get("status") != "planned":
            raise HarnessError("draft descriptor mode is not authorized")
    elif descriptor_sha256 not in authorized:
        raise HarnessError("descriptor digest is not authorized")
    descriptor_inputs = descriptor_repository_inputs(repository, descriptor,
        source_overrides)
    known = fixture_map(fixtures)
    semantic, semantic_path = semantic_case_map(repository, manifest,
        capabilities, fixtures)
    overlap = set(known) & set(semantic)
    if overlap:
        raise HarnessError(f"semantic cases overlap fixtures: {sorted(overlap)}")
    known.update(semantic)
    claims = selected_claims(authority_record)
    unknown = sorted({case_id for case_id, _ in claims} - set(known))
    if unknown:
        raise HarnessError(f"authority claims unknown cases: {unknown}")
    out_of_scope = sorted((case_id, capability)
        for case_id, capability in claims
        if capability not in known[case_id]["capabilities"])
    if out_of_scope:
        raise HarnessError(f"authority claims out-of-scope cases: {out_of_scope}")
    limits = manifest["evidence_limits"]
    raw_entry = evidence_entry(manifest, "normalized-raw-java-apache-corpus")
    raw_evidence_path = repository_input(repository, raw_entry["file"])
    if pathlib.Path(arguments.raw_evidence).resolve(strict=True) != \
            raw_evidence_path:
        raise HarnessError("raw evidence path does not select the frozen input")
    raw_run, raw_files, raw_columns, raw_evidence_sha256 = load_raw_facts(
        raw_evidence_path,
        limits["max_file_bytes"], limits["max_line_bytes"],
        limits["max_records_per_input"])
    target_entry = _verify_evidence_bindings(manifest, producer, evidence_id,
        raw_entry, raw_evidence_sha256,
        allow_unpinned_descriptor or draft_evidence)
    target_output = manifest_output_path(repository, target_entry,
        arguments.output)
    hashes = frozen_input_hashes(repository, manifest, {
        "capabilities_sha256": capabilities_sha256,
        "fixture_manifest_sha256": fixtures_sha256,
    })
    for field, digest in hashes.items():
        if manifest.get(field) != digest:
            raise HarnessError(f"manifest has stale {field}")
        if raw_run.get(field) != digest:
            raise HarnessError(f"normalized raw evidence has stale {field}")
    raw_authority = authority(capabilities, "n6-raw-java")
    if raw_run.get("evidence_id") != "normalized-raw-java-apache-corpus" or \
            raw_run.get("producer_version") != raw_authority["version"] or \
            raw_run.get("source_revision") != raw_authority["revision"] or \
            raw_run.get("toolchain_sha256") not in \
            raw_authority["toolchain_sha256"]:
        raise HarnessError("normalized raw evidence has the wrong authority")
    upstream = {
        "evidence_id": raw_run["evidence_id"],
        "file": raw_entry["file"],
        "sha256": raw_evidence_sha256,
    }
    frozen_paths = frozen_input_paths(repository, manifest)
    protected_inputs = [expected_manifest, descriptor_path, raw_evidence_path,
        semantic_path, *frozen_paths.values(), *descriptor_inputs]
    return {
        "repository": repository,
        "manifest": manifest,
        "fixtures": fixtures,
        "authority_record": authority_record,
        "known": known,
        "claims": claims,
        "raw_files": raw_files,
        "raw_columns": raw_columns,
        "limits": limits,
        "descriptor": descriptor,
        "descriptor_sha256": descriptor_sha256,
        "raw_evidence_path": raw_evidence_path,
        "target_entry": target_entry,
        "output_path": target_output,
        "input_hashes": hashes,
        "source_overrides": {} if source_overrides is None else source_overrides,
        "descriptor_inputs": descriptor_inputs,
        "upstream_evidence": upstream,
        "protected_inputs": protected_inputs,
    }
