"""Interop between Parquet.jl and pyarrow, Polars, and DuckDB, with interop.jl.

    python interop.py write OUT   each tool writes OUT/<tool>/<case>.parquet (a dataset
                                  directory OUT/<tool>/<case> for partitioned cases) and
                                  the values it holds as OUT/<tool>/<case>.json
    python interop.py read DIR    each tool reads every DIR/<case>.parquet (or dataset
                                  directory DIR/<case>) that Parquet.jl wrote and must
                                  find the values in DIR/<case>.json

Values compare exactly in one canonical JSON form, which interop.jl also builds: integers
"i:<n>", floats by bit pattern, temporal values as integer nanoseconds ("ts:<utc>:<n>"
for timestamps), decimals as "dec:<unscaled>e<scale>", bytes as hex, lists as arrays,
structs as {"struct": [[name, value], ...]}, and maps as {"map": [[key, value], ...]}.
"""
import json
import sys
from datetime import date, time, datetime, timezone
from decimal import Context, Decimal
from pathlib import Path

import duckdb
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

UNIT_NS = {"s": 10**9, "ms": 10**6, "us": 10**3, "ns": 1}
CONTEXT = Context(prec=100)
KEYS = ["k", "g"]  # partition columns of the partitioned cases


def canon(arr):
    """The canonical values of a pyarrow array."""
    if isinstance(arr, pa.ChunkedArray):
        return [value for chunk in arr.chunks for value in canon(chunk)]
    t = arr.type
    if pa.types.is_dictionary(t):
        return canon(arr.dictionary_decode())
    valid = arr.is_valid().to_pylist()

    def each(form, values=None):
        values = arr.to_pylist() if values is None else values
        return [form(v) if ok else None for v, ok in zip(values, valid)]

    if pa.types.is_null(t):
        return [None] * len(arr)
    if pa.types.is_boolean(t):
        return arr.to_pylist()
    if pa.types.is_integer(t):
        return each(lambda v: "i:%d" % v)
    if pa.types.is_floating(t):
        width = t.bit_width
        bits = arr.view({16: pa.uint16(), 32: pa.uint32(), 64: pa.uint64()}[width])
        return each(lambda v: "f%d:%0*x" % (width, width // 4, v), bits.to_pylist())
    if pa.types.is_string(t) or pa.types.is_large_string(t) or pa.types.is_string_view(t):
        return each(lambda v: "s:" + v)
    if (pa.types.is_binary(t) or pa.types.is_large_binary(t) or pa.types.is_binary_view(t)
            or pa.types.is_fixed_size_binary(t)):
        return each(lambda v: "b:" + v.hex())
    if pa.types.is_date32(t):
        return each(lambda v: "date:%d" % v, arr.view(pa.int32()).to_pylist())
    if pa.types.is_time(t):
        scale = UNIT_NS[t.unit]
        ticks = arr.view(pa.int32() if t.bit_width == 32 else pa.int64())
        return each(lambda v: "time:%d" % (v * scale), ticks.to_pylist())
    if pa.types.is_timestamp(t):
        scale, utc = UNIT_NS[t.unit], "true" if t.tz is not None else "false"
        return each(lambda v: "ts:%s:%d" % (utc, v * scale), arr.view(pa.int64()).to_pylist())
    if pa.types.is_decimal(t):
        return each(lambda v: "dec:%de%d" % (int(v.scaleb(t.scale, CONTEXT)), t.scale))
    if pa.types.is_map(t):
        offsets = arr.offsets.to_pylist()
        keys, items = canon(arr.values.field(0)), canon(arr.values.field(1))
        return [{"map": [[keys[k], items[k]] for k in range(offsets[i], offsets[i + 1])]}
                if ok else None for i, ok in enumerate(valid)]
    if pa.types.is_list(t) or pa.types.is_large_list(t):
        offsets, values = arr.offsets.to_pylist(), canon(arr.values)
        return [values[offsets[i]:offsets[i + 1]] if ok else None for i, ok in enumerate(valid)]
    if pa.types.is_struct(t):
        names = [t.field(k).name for k in range(t.num_fields)]
        fields = [canon(child) for child in arr.flatten()]
        return [{"struct": [[name, field[i]] for name, field in zip(names, fields)]}
                if ok else None for i, ok in enumerate(valid)]
    raise TypeError("no canonical form for %s" % t)


def canon_table(table):
    return {"names": table.column_names,
            "columns": {name: canon(table.column(name)) for name in table.column_names}}


def sort_rows(values):
    """Dataset rows in `id` order, since readers interleave files differently."""
    order = sorted(range(len(values["columns"]["id"])),
                   key=lambda i: int(values["columns"]["id"][i][2:]))
    values["columns"] = {name: [column[i] for i in order]
                         for name, column in values["columns"].items()}
    return values


def normalize(expected, actual):
    """Polars has no map type and reads a MAP as a list of key/value structs."""
    if isinstance(expected, dict) and "map" in expected and isinstance(actual, list):
        return {"map": [[entry["struct"][0][1], entry["struct"][1][1]] for entry in actual]}
    if isinstance(expected, list) and isinstance(actual, list) and len(expected) == len(actual):
        return [normalize(e, a) for e, a in zip(expected, actual)]
    if isinstance(expected, dict) and isinstance(actual, dict) and expected.keys() == actual.keys():
        return {k: normalize(expected[k], actual[k]) for k in expected}
    return actual


def difference(expected, actual):
    """A description of the first difference between two canonical tables, or None."""
    if expected["names"] != actual["names"]:
        return "columns %s, expected %s" % (actual["names"], expected["names"])
    for name in expected["names"]:
        want = expected["columns"][name]
        got = normalize(want, actual["columns"][name])
        if len(got) != len(want):
            return "column %s has %d rows, expected %d" % (name, len(got), len(want))
        for row, (w, g) in enumerate(zip(want, got)):
            if w != g:
                return "column %s row %d is %s, expected %s" % (name, row + 1, g, w)
    return None


# The cases each tool writes. Every tool starts from the same Arrow tables; the expected
# values are the ones the tool holds before writing.

def flat():
    return pa.table({
        "i8": pa.array([1, -128, 127, None], pa.int8()),
        "i16": pa.array([1, -32768, 32767, None], pa.int16()),
        "i32": pa.array([1, -2**31, 2**31 - 1, None], pa.int32()),
        "i64": pa.array([1, -2**63, 2**63 - 1, None], pa.int64()),
        "u8": pa.array([0, 255, 1, None], pa.uint8()),
        "u16": pa.array([0, 65535, 1, None], pa.uint16()),
        "u32": pa.array([0, 2**32 - 1, 1, None], pa.uint32()),
        "u64": pa.array([0, 2**64 - 1, 1, None], pa.uint64()),
        "f32": pa.array([1.5, float("nan"), -0.0, None], pa.float32()),
        "f64": pa.array([1.5, float("-inf"), -0.0, None], pa.float64()),
        "b": pa.array([True, False, None, True]),
        "s": pa.array(["a", "héllo ✓", "", None]),
        "bin": pa.array([b"\x00\xff", b"", None, b"x"]),
    })


def nested(maps=True):
    struct = pa.struct([("x", pa.int64()), ("y", pa.string())])
    columns = {
        "l": pa.array([[1, 2], [], None, [None, 3]], pa.list_(pa.int64())),
        "ll": pa.array([[[1], []], None, [[2, None]], []], pa.list_(pa.list_(pa.int64()))),
        "st": pa.array([{"x": 1, "y": "a"}, None, {"x": None, "y": "b"}, {"x": 3, "y": None}],
                       struct),
        "ls": pa.array([[{"x": 1, "y": "p"}], [], None, [None, {"x": 2, "y": None}]],
                       pa.list_(struct)),
        "sl": pa.array([{"x": 1, "v": [1.5]}, {"x": None, "v": None}, None, {"x": 2, "v": []}],
                       pa.struct([("x", pa.int32()), ("v", pa.list_(pa.float64()))])),
    }
    if maps:
        columns["m"] = pa.array([[("a", 1), ("b", None)], [], None, [("c", 3)]],
                                pa.map_(pa.string(), pa.int64()))
    return pa.table(columns)


def temporal():
    utc = timezone.utc
    return pa.table({
        "date": pa.array([date(1970, 1, 1), date(1969, 12, 31), date(2024, 2, 29), None]),
        "t_ms": pa.array([time(0), time(23, 59, 59, 999000), time(12), None], pa.time32("ms")),
        "t_us": pa.array([time(0), time(23, 59, 59, 999999), time(12), None], pa.time64("us")),
        "ts_ms": pa.array([0, -1, 1_700_000_000_123, None], pa.timestamp("ms")),
        "ts_us": pa.array([0, -1, 1_700_000_000_123_456, None], pa.timestamp("us")),
        "ts_ns": pa.array([0, -1, 1_700_000_000_123_456_789, None], pa.timestamp("ns")),
        "tz_ms": pa.array([0, -1, 1_700_000_000_123, None], pa.timestamp("ms", tz="UTC")),
        "tz_us": pa.array([datetime(2024, 2, 29, 12, 30, tzinfo=utc), None,
                           datetime(1969, 12, 31, 23, 59, 59, 999999, tzinfo=utc),
                           datetime(1970, 1, 1, tzinfo=utc)], pa.timestamp("us", tz="UTC")),
    })


def decimal():
    return pa.table({
        "d9": pa.array([Decimal("1234567.89"), Decimal("-0.01"), None, Decimal("0")],
                       pa.decimal128(9, 2)),
        "d18": pa.array([Decimal("12345678901234.5678"), Decimal("-1"), Decimal("0.0001"),
                         None], pa.decimal128(18, 4)),
        "d38": pa.array([Decimal("1234567890123456789012345678.9012345678"), None,
                         Decimal("-0.0000000001"), Decimal("0")], pa.decimal128(38, 10)),
    })


def dictionary():
    words = ["alpha", "beta", "alpha", None, "gamma", "alpha"] * 200
    return pa.table({
        "cat": pa.array(words).dictionary_encode(),
        "rep": pa.array(words),
        "num": pa.array([1, 2, 1, None, 3, 1] * 200, pa.int64()),
    })


def partitioned():
    rows = 60
    return pa.table({
        "id": pa.array(range(rows), pa.int64()),
        "k": pa.array([i % 2 for i in range(rows)], pa.int64()),
        "g": pa.array([["a", "b c", "d"][i % 3] for i in range(rows)]),
        "v": pa.array([None if i % 5 == 0 else i * 1.25 for i in range(rows)], pa.float64()),
        "s": pa.array([None if i % 4 == 0 else "row %d" % i for i in range(rows)]),
    })


CASES = {"flat": flat, "nested": nested, "temporal": temporal, "decimal": decimal,
         "dictionary": dictionary}


def dataset_values(table):
    """A partitioned table as readers return it: file columns, then partition keys."""
    names = [n for n in table.column_names if n not in KEYS] + KEYS
    return sort_rows(canon_table(table.select(names)))


def save(path, values):
    path.write_text(json.dumps(values))


def write_pyarrow(out):
    for name, case in CASES.items():
        table = case()
        pq.write_table(table, out / (name + ".parquet"))
        save(out / (name + ".json"), canon_table(table))
        # Data page V2, zstd, and several row groups and pages.
        pq.write_table(table, out / (name + "_v2.parquet"), data_page_version="2.0",
                       compression="zstd", row_group_size=3, data_page_size=64)
        save(out / (name + "_v2.json"), canon_table(table))
    table = decimal()
    pq.write_table(table, out / "decimal_int.parquet", store_decimal_as_integer=True)
    save(out / "decimal_int.json", canon_table(table))
    table = partitioned()
    pq.write_to_dataset(table, out / "partitioned", partition_cols=KEYS)
    save(out / "partitioned.json", dataset_values(table))


def write_polars(out):
    for name, case in CASES.items():
        frame = pl.from_arrow(case(maps=False) if name == "nested" else case())
        frame.write_parquet(out / (name + ".parquet"))
        save(out / (name + ".json"), canon_table(frame.to_arrow()))
    frame = pl.from_arrow(partitioned())
    frame.write_parquet(out / "partitioned", partition_by=KEYS)
    # Polars keeps the partition columns in the files, so they stay in place.
    save(out / "partitioned.json", sort_rows(canon_table(frame.to_arrow())))


def write_duckdb(out):
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    for name, case in list(CASES.items()) + [("partitioned", partitioned)]:
        source = case()  # noqa: F841 (DuckDB scans the local variable `source`)
        held = con.sql("SELECT * FROM source").to_arrow_table()
        if name == "partitioned":
            con.execute("COPY (SELECT * FROM source) TO '%s' (FORMAT parquet, PARTITION_BY (%s))"
                        % (out / name, ", ".join(KEYS)))
            save(out / (name + ".json"), dataset_values(held))
        else:
            con.execute("COPY (SELECT * FROM source) TO '%s' (FORMAT parquet)"
                        % (out / (name + ".parquet")))
            save(out / (name + ".json"), canon_table(held))


def write(out):
    for tool, writer in (("pyarrow", write_pyarrow), ("polars", write_polars),
                         ("duckdb", write_duckdb)):
        directory = Path(out) / tool
        directory.mkdir(parents=True, exist_ok=True)
        writer(directory)
        print("%s wrote %d cases" % (tool, len(list(directory.glob("*.json")))))


# Readers of the files Parquet.jl wrote.

def read_pyarrow(path):
    if path.is_dir():
        return pq.read_table(path, partitioning="hive")
    return pq.read_table(path)


def read_polars(path):
    if path.is_dir():
        return pl.read_parquet(str(path / "**" / "*.parquet"), hive_partitioning=True).to_arrow()
    return pl.read_parquet(path).to_arrow()


def read_duckdb(path):
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    if path.is_dir():
        source = "read_parquet('%s/**/*.parquet', hive_partitioning = true)" % path
    else:
        source = "read_parquet('%s')" % path
    return con.sql("SELECT * FROM " + source).to_arrow_table()


def read(directory):
    failures = 0
    for spec in sorted(Path(directory).glob("*.json")):
        expected = json.loads(spec.read_text())
        path = spec.with_suffix("") if spec.with_suffix("").is_dir() else spec.with_suffix(".parquet")
        for tool, reader in (("pyarrow", read_pyarrow), ("polars", read_polars),
                             ("duckdb", read_duckdb)):
            try:
                actual = canon_table(reader(path))
                if path.is_dir():
                    actual = sort_rows(actual)
                    # Readers order the partition columns their own way (DuckDB by name).
                    if sorted(actual["names"]) == sorted(expected["names"]):
                        actual["names"] = expected["names"]
                problem = difference(expected, json.loads(json.dumps(actual)))
            except Exception as err:  # noqa: BLE001 (report every reader failure)
                problem = "%s: %s" % (type(err).__name__, err)
            failures += problem is not None
            print("%-4s %-8s %s%s" % ("FAIL" if problem else "ok", tool, path.name,
                                      ": " + problem if problem else ""))
    return failures


if __name__ == "__main__":
    command, target = sys.argv[1:3]
    if command == "write":
        write(target)
    elif read(target):
        sys.exit(1)
