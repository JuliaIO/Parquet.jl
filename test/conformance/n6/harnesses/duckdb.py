#!/usr/bin/env python3
import argparse
import hashlib
import os
import pathlib
import stat
import subprocess
import sys
import tempfile
import tomllib


_SNAPSHOT_ENV = "PARQUET_N6_AUTHENTICATED_SNAPSHOT"
_SNAPSHOT_DESCRIPTOR = "descriptor.toml"
_SOURCE_LIMIT = 4 * 1024 * 1024


def _source_identity(metadata):
    return (metadata.st_dev, metadata.st_ino, metadata.st_mode,
        metadata.st_nlink, metadata.st_size, metadata.st_mtime_ns,
        metadata.st_ctime_ns)


def _source_bytes(path, label):
    path = pathlib.Path(path)
    metadata = path.lstat()
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode) or \
            not 0 <= metadata.st_size <= _SOURCE_LIMIT:
        raise RuntimeError(f"{label} is not a bounded regular file")
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags)
    with os.fdopen(descriptor, "rb") as stream:
        opened = os.fstat(stream.fileno())
        if _source_identity(opened) != _source_identity(metadata):
            raise RuntimeError(f"{label} changed while it was opened")
        value = stream.read(_SOURCE_LIMIT + 1)
        final = os.fstat(stream.fileno())
    if len(value) != opened.st_size or len(value) > _SOURCE_LIMIT or \
            _source_identity(final) != _source_identity(opened) or \
            _source_identity(path.lstat()) != _source_identity(opened):
        raise RuntimeError(f"{label} changed while it was read")
    return value


def _named_argument(arguments, name):
    matches = [index for index, value in enumerate(arguments) if value == name]
    if len(matches) != 1 or matches[0] + 1 >= len(arguments):
        raise RuntimeError(f"bootstrap argument is absent or repeated: {name}")
    return arguments[matches[0] + 1], matches[0] + 1


def _repository_source(repository, relative, label):
    if not isinstance(relative, str) or "\\" in relative:
        raise RuntimeError(f"{label} path is invalid")
    pure = pathlib.PurePosixPath(relative)
    if pure.is_absolute() or any(part in ("", ".", "..") for part in pure.parts):
        raise RuntimeError(f"{label} path is invalid")
    current = repository
    for part in pure.parts:
        current = current / part
        metadata = current.lstat()
        if stat.S_ISLNK(metadata.st_mode):
            raise RuntimeError(f"{label} path contains a symbolic link")
    if not current.is_file():
        raise RuntimeError(f"{label} is not a regular file")
    return current


def _write_snapshot(path, value):
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags, 0o400)
    os.fchmod(descriptor, 0o400)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(value)
        stream.flush()
        os.fsync(stream.fileno())
    return


def _snapshot_sources(arguments, harness_file):
    repository_value, _ = _named_argument(arguments, "--repository")
    descriptor_value, descriptor_index = _named_argument(arguments,
        "--descriptor")
    repository_input = pathlib.Path(repository_value)
    repository_metadata = repository_input.lstat()
    if stat.S_ISLNK(repository_metadata.st_mode) or \
            not stat.S_ISDIR(repository_metadata.st_mode):
        raise RuntimeError("repository root is not a regular directory")
    repository = repository_input.resolve(strict=True)
    descriptor_source = _source_bytes(descriptor_value,
        "Python toolchain descriptor")
    try:
        descriptor = tomllib.loads(descriptor_source.decode("utf-8"))
    except (UnicodeError, tomllib.TOMLDecodeError) as error:
        raise RuntimeError("Python toolchain descriptor is invalid") from error
    harness = _repository_source(repository, descriptor.get("harness_file"),
        "harness source")
    if harness.resolve(strict=True) != pathlib.Path(harness_file).resolve(
            strict=True):
        raise RuntimeError("executed harness does not match its descriptor")
    common_relative = (pathlib.PurePosixPath(
        descriptor["harness_file"]).parent / "common.py").as_posix()
    support = [item for item in descriptor.get("support_files", [])
        if isinstance(item, dict) and item.get("file") == common_relative]
    if len(support) != 1 or not isinstance(support[0].get("sha256"), str):
        raise RuntimeError("common source binding is absent or ambiguous")
    common = _repository_source(repository, common_relative, "common source")
    harness_source = _source_bytes(harness, "harness source")
    common_source = _source_bytes(common, "common source")
    if hashlib.sha256(harness_source).hexdigest() != \
            descriptor.get("harness_sha256"):
        raise RuntimeError("harness source digest differs")
    if hashlib.sha256(common_source).hexdigest() != support[0]["sha256"]:
        raise RuntimeError("common source digest differs")
    return (descriptor_source, descriptor_index, harness_source,
        common_source)


def _verify_snapshot_child(arguments, harness_file):
    root_input = pathlib.Path(os.environ[_SNAPSHOT_ENV])
    root_metadata = root_input.lstat()
    if stat.S_ISLNK(root_metadata.st_mode) or \
            not stat.S_ISDIR(root_metadata.st_mode) or \
            stat.S_IMODE(root_metadata.st_mode) != 0o500:
        raise RuntimeError("authenticated source snapshot is not read-only")
    root = root_input.resolve(strict=True)
    descriptor_value, _ = _named_argument(arguments, "--descriptor")
    descriptor_path = root / _SNAPSHOT_DESCRIPTOR
    harness_path = root / pathlib.Path(harness_file).name
    common_path = root / "common.py"
    if pathlib.Path(descriptor_value).resolve(strict=True) != descriptor_path or \
            pathlib.Path(harness_file).resolve(strict=True) != harness_path:
        raise RuntimeError("authenticated source snapshot paths differ")
    if {path.name for path in root.iterdir()} != {
            descriptor_path.name, harness_path.name, common_path.name}:
        raise RuntimeError("authenticated source snapshot contents differ")
    for path in (descriptor_path, harness_path, common_path):
        metadata = path.lstat()
        if stat.S_ISLNK(metadata.st_mode) or \
                not stat.S_ISREG(metadata.st_mode) or \
                stat.S_IMODE(metadata.st_mode) != 0o400:
            raise RuntimeError("authenticated source snapshot file is writable")
    descriptor_source = _source_bytes(descriptor_path,
        "snapshotted Python toolchain descriptor")
    try:
        descriptor = tomllib.loads(descriptor_source.decode("utf-8"))
    except (UnicodeError, tomllib.TOMLDecodeError) as error:
        raise RuntimeError("snapshotted descriptor is invalid") from error
    support = [item for item in descriptor.get("support_files", [])
        if isinstance(item, dict) and pathlib.PurePosixPath(
            item.get("file", "")).name == "common.py"]
    if len(support) != 1 or hashlib.sha256(_source_bytes(harness_path,
            "snapshotted harness source")).hexdigest() != \
            descriptor.get("harness_sha256") or hashlib.sha256(_source_bytes(
            common_path, "snapshotted common source")).hexdigest() != \
            support[0].get("sha256"):
        raise RuntimeError("authenticated source snapshot digest differs")
    return


# The outer process uses only the standard library. It authenticates and freezes
# harness/common bytes before the isolated child imports common or reads evidence.
def _authenticated_launch(arguments, harness_file):
    if _SNAPSHOT_ENV in os.environ:
        _verify_snapshot_child(arguments, harness_file)
        return None
    sources = _snapshot_sources(arguments, harness_file)
    descriptor_source, descriptor_index, harness_source, common_source = sources
    with tempfile.TemporaryDirectory(prefix="parquet-n6-sources-") as directory:
        root = pathlib.Path(directory).resolve(strict=True)
        descriptor_path = root / _SNAPSHOT_DESCRIPTOR
        harness_path = root / pathlib.Path(harness_file).name
        _write_snapshot(descriptor_path, descriptor_source)
        _write_snapshot(harness_path, harness_source)
        _write_snapshot(root / "common.py", common_source)
        os.chmod(root, 0o500)
        child_arguments = list(arguments[1:])
        child_arguments[descriptor_index - 1] = str(descriptor_path)
        environment = os.environ.copy()
        environment[_SNAPSHOT_ENV] = str(root)
        try:
            result = subprocess.run([sys.executable, "-I", "-B", "-S",
                str(harness_path), *child_arguments], check=False,
                env=environment)
        finally:
            os.chmod(root, 0o700)
        return result.returncode


if __name__ == "__main__":
    try:
        _launch_status = _authenticated_launch(sys.argv, __file__)
    except Exception as error:
        print(f"N6 DuckDB bootstrap failed: {error}", file=sys.stderr)
        sys.exit(1)
    if _launch_status is not None:
        sys.exit(_launch_status)

SCRIPT_DIRECTORY = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIRECTORY))
from common import (HarnessError, atomic_output, build_context, case_result,
    checked_file, evidence_bytes, exact_wheel_module, leaf_records,
    logical_observations,
    reject_output_alias, run_record, verify_python_descriptor)
sys.path = [entry for entry in sys.path
    if pathlib.Path(entry or ".").resolve() != SCRIPT_DIRECTORY]


PRODUCER = "duckdb"
EVIDENCE_ID = "normalized-duckdb"


def decoded_columns(connection, path, case_id, raw_columns):
    result = connection.execute("SELECT * FROM read_parquet(?)", [str(path)])
    names = [description[0] for description in result.description]
    rows = result.fetchall()
    leaves = leaf_records(case_id, raw_columns)
    expected = [record["path"][0] for record in leaves]
    if names != expected:
        raise HarnessError(f"DuckDB column order differs: {case_id}")
    columns = [[row[index] for row in rows] for index in range(len(names))]
    return columns, len(rows)


def metadata_observations(connection, path):
    selected = [
        "row_group_id", "row_group_num_rows", "row_group_num_columns",
        "row_group_bytes", "column_id", "file_offset", "num_values",
        "path_in_schema", "type", "stats_min", "stats_max",
        "stats_null_count", "stats_distinct_count", "stats_min_value",
        "stats_max_value", "compression", "encodings", "index_page_offset",
        "dictionary_page_offset", "data_page_offset", "total_compressed_size",
        "total_uncompressed_size", "bloom_filter_offset",
        "bloom_filter_length", "min_is_exact", "max_is_exact",
        "row_group_compressed_bytes",
    ]
    query = "SELECT " + ",".join(selected) + (
        " FROM parquet_metadata(?) ORDER BY row_group_id, column_id")
    rows = connection.execute(query, [str(path)]).fetchall()
    observations = [dict(zip(selected, row)) for row in rows]
    return [{
        "contract": "n6-duckdb-parquet-metadata-v1",
        "rows": observations,
    }]


def generate(arguments, context, duckdb_runtime):
    repository = context["repository"]
    manifest = context["manifest"]
    authority_record = context["authority_record"]
    known = context["known"]
    claims = context["claims"]
    raw_files = context["raw_files"]
    raw_columns = context["raw_columns"]
    limits = context["limits"]
    descriptor_sha256 = context["descriptor_sha256"]
    unsupported = {case_id for (case_id, _), status in claims.items()
        if status == "unsupported"}
    records = [run_record(EVIDENCE_ID, PRODUCER, authority_record,
        descriptor_sha256, repository, manifest, unsupported,
        context["upstream_evidence"], input_hashes=context["input_hashes"])]
    by_case = {}
    for (case_id, capability), status in claims.items():
        by_case.setdefault(case_id, []).append((capability, status))
    connection = duckdb_runtime.connect(":memory:")
    try:
        with tempfile.TemporaryDirectory(prefix="parquet-n6-duckdb-") as snapshots:
            for case_id in sorted(by_case):
                fixture = known[case_id]
                if case_id not in raw_files:
                    raise HarnessError(f"raw file fact is absent: {case_id}")
                path = checked_file(arguments.corpus_root, fixture["file"],
                    fixture["sha256"], fixture["size"], snapshots)
                records.append(raw_files[case_id])
                logical = None
                metadata = None
                for capability, status in sorted(by_case[case_id]):
                    if status == "unsupported":
                        result_status = "UNSUPPORTED"
                        observations = None
                    elif capability == "read.logical-values":
                        if logical is None:
                            columns, rows = decoded_columns(connection, path,
                                case_id, raw_columns)
                            logical = logical_observations(case_id, raw_columns,
                                columns, rows)
                        result_status = "PASS"
                        observations = logical
                    elif capability == "read.parquet-metadata-view":
                        if metadata is None:
                            metadata = metadata_observations(connection, path)
                        result_status = "PASS"
                        observations = metadata
                    else:
                        raise HarnessError(
                            f"unhandled DuckDB capability: {capability}")
                    records.append(case_result(case_id, capability,
                        fixture["digest_contract"], result_status,
                        observations))
    finally:
        connection.close()
    return evidence_bytes(records, limits["max_file_bytes"],
        limits["max_line_bytes"], limits["max_records_per_input"])


def parser():
    result = argparse.ArgumentParser()
    result.add_argument("--repository", required=True)
    result.add_argument("--manifest", required=True)
    result.add_argument("--capabilities", required=True)
    result.add_argument("--fixtures", required=True)
    result.add_argument("--descriptor", required=True)
    result.add_argument("--raw-evidence", required=True)
    result.add_argument("--wheel", required=True)
    result.add_argument("--corpus-root", required=True)
    result.add_argument("--output", required=True)
    result.add_argument("--check", action="store_true")
    result.add_argument("--draft", action="store_true")
    return result


def main():
    arguments = parser().parse_args()
    context = build_context(arguments, PRODUCER, EVIDENCE_ID,
        draft_evidence=arguments.draft, executed_harness=__file__)
    verify_python_descriptor(context["descriptor"], context["repository"],
        PRODUCER, context["source_overrides"], context["descriptor_inputs"])
    reject_output_alias(context["output_path"],
        [*context["protected_inputs"], arguments.wheel],
        [arguments.corpus_root])
    with exact_wheel_module(arguments.wheel, context["descriptor"],
            "duckdb", context["authority_record"]["version"]) as runtime:
        value = generate(arguments, context, runtime)
    atomic_output(context["output_path"], value, arguments.check)
    print(f"{EVIDENCE_ID}: {len(value)} bytes")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as error:
        print(f"N6 DuckDB harness failed: {error}", file=sys.stderr)
        sys.exit(1)
