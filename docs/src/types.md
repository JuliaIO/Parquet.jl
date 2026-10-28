# Type mapping

## Parquet to Julia

A column's element type comes from its Parquet type and annotation. An optional column's
elements are `Union{Missing,T}`.

| Parquet type | Julia element type |
|:--|:--|
| BOOLEAN, INT32, INT64, FLOAT, DOUBLE | `Bool`, `Int32`, `Int64`, `Float32`, `Float64` |
| INT(8-64, signed or not), FLOAT16 | `Int8`-`UInt64`, `Float16` |
| STRING, ENUM | `DataStrings.DataString` (the column is a `DataStrings.StringVector`) |
| BYTE_ARRAY | `DataStrings.DataBytes` (the column is a `DataStrings.BytesVector`) |
| FIXED_LEN_BYTE_ARRAY(N) | `NTuple{N,UInt8}`; `DataStrings.DataBytes` when N > 256 |
| UUID | `UUIDs.UUID` |
| DECIMAL(p, s), p <= 76 | `DataDecimals.Decimal{p,s,T}`, stored in `Int32`, `Int64`, `Int128`, or `DataDecimals.Int256` |
| DECIMAL(p, s), p > 76 | [`Parquet.Decimal`](@ref) |
| DATE, TIME(unit) | `Dates.Date`, `Dates.Time` |
| TIMESTAMP(unit) | `Durations.Timestamp{unit}` (`Millisecond`, `Microsecond`, `Nanosecond`) |
| TIMESTAMP(unit), adjusted to UTC | `Durations.ZonedTimestamp{unit,:UTC}`, or the time zone the ARROW:schema gives |
| INT64 that the ARROW:schema calls a duration | `Dates.Second`, `Millisecond`, `Microsecond`, or `Nanosecond` |
| INT96 | `Durations.Timestamp{Nanosecond}`, or the resolution `int96` gives |
| INTERVAL | `Durations.Duration` |
| JSON, BSON | [`Parquet.JSONValue`](@ref), [`Parquet.BSONValue`](@ref) |
| UNKNOWN (a null column) | `Missing` |
| VARIANT, GEOMETRY, GEOGRAPHY | their storage: VARIANT's `metadata` and `value` bytes (and any shredded `typed_value`), WKB bytes |
| LIST, MAP, groups | list, map, and struct rows ([Nested columns](@ref)) |

- A `DataString` refers to the column's decoded bytes rather than to a copy per value;
  `String(value)` makes one. Strings of up to 12 bytes are stored in the value itself.
- pyarrow, Polars, and other Arrow-based writers store their Arrow schema in the file's
  `ARROW:schema` metadata. A read takes the time zones of UTC-adjusted timestamps and the
  units of durations from it, including inside nested columns; the Parquet schema decides
  everything else, and an `ARROW:schema` that does not match the file is ignored.
- A `Durations.Timestamp` compares with `Dates.DateTime`, so
  `Parquet.colcmp(>=, Parquet.col(:pickup), DateTime(2025, 1, 1))` filters on it. A
  filter also compares such a literal with a UTC timestamp column, as a UTC time
  ([Choose columns and rows](@ref)).
- INT96 values (Spark, Hive, and Impala write them by default) are not adjusted to UTC, as
  pyarrow reads them. A value outside the nanosecond range (years 1677 to 2262), such as
  Spark's `9999-12-31` placeholder, throws `Parquet.UnsupportedFeatureError`; read such
  files with `int96=Dates.Microsecond`, Spark's own precision.
- An INTERVAL value with more than 2^31 - 1 months or days throws
  `Parquet.UnsupportedFeatureError`, and so does a TIME of 24:00:00, which `Dates.Time`
  cannot hold; `drop` the column to read the rest.
- Parquet has no union type: Arrow writers refuse to store Arrow unions in Parquet, so a
  file never holds one.

```@example types
using Parquet, Tables, Dates, DataDecimals

io = IOBuffer()
Parquet.write(io, (when=[DateTime(2025, 1, 1, 12)], day=[Date(2025, 1, 1)],
    price=[DataDecimals.Decimal{9,2}("12.50")], tag=["a"]))
table = Parquet.Table(take!(io))
Tables.schema(table)
```

## Julia to Parquet

Each column's element type decides its Parquet type. Values read back as the same type
unless the table says otherwise.

| Julia element type | Parquet column | Reads back as |
|:--|:--|:--|
| `Bool`, `Int8`-`Int64`, `UInt8`-`UInt64`, `Float16`, `Float32`, `Float64` | BOOLEAN, INT32/INT64 (with an INT annotation below 32 bits and for unsigned types), FLOAT16, FLOAT, DOUBLE | the same type |
| `AbstractString`, `Symbol`, `Char` | STRING | `DataStrings.DataString` |
| `Vector{UInt8}`, `DataStrings.DataBytes` | BYTE_ARRAY | `DataStrings.DataBytes` |
| `NTuple{N,UInt8}` | FIXED_LEN_BYTE_ARRAY(N) | `NTuple{N,UInt8}` |
| `UUID` | UUID | `UUID` |
| `Date`, `Time` | DATE, TIME(NANOS) | `Date`, `Time` |
| `DateTime` | TIMESTAMP(MILLIS) | `Durations.Timestamp{Millisecond}` |
| `Durations.Timestamp{P}` | TIMESTAMP in unit `P` (`Second` as milliseconds) | `Durations.Timestamp{P}` |
| `Durations.ZonedTimestamp{P,Z}` | TIMESTAMP adjusted to UTC; the zone goes in the ARROW:schema | `Durations.ZonedTimestamp{P,Z}` |
| `Nanosecond`-`Second`; `Minute`-`Week` | INT64 count of the unit; of seconds | the same type; `Second` |
| `Month`, `Year`, `Durations.Duration` | INTERVAL | `Durations.Duration` |
| `DataDecimals.Decimal{P,S}` | DECIMAL(P,S) on INT32, INT64, or FIXED_LEN_BYTE_ARRAY | the same type |
| `PooledArray`, `CategoricalArray` values | the type of the pool's values, dictionary encoded from the pool | plain values |
| `AbstractVector`, `NTuple` | LIST | `AbstractVector` rows |
| `NamedTuple` | group | `NamedTuple` rows |
| `AbstractDict`, `AbstractVector{<:Pair}` | MAP | `AbstractVector{<:Pair}` rows |
| `Missing` only | a null column | `Missing` |

STRING and ENUM values must be valid UTF-8: a string that is not fails the write with an
error naming its column and row. Write binary data as `Vector{UInt8}` values instead.

Nested values nest in any combination, with `missing` allowed at every level: a missing
list, an empty list, and a list holding `missing` stay distinct. Map keys cannot be
missing. A tuple's elements need one type up to `missing`; use a `NamedTuple` for a
struct of mixed types. Maps keep their entries in order, duplicate keys included. A table
read by `Parquet.Table` writes back without copying its nested rows.

A column whose element type is abstract or a `Union` of several types, such as a
`Vector{Any}`, is written as the one type its values share: numbers are promoted when
every value converts exactly, strings become STRING, and containers are promoted element
by element. So is a column of containers that leave their element types open, such as
`[[1, 2], []]` (a `Vector{Vector{Any}}`) or `Dict{String,Any}` rows. Other mixes,
`nothing` values, and `Int128` or `BigInt` columns are errors that name the column.
`Parquet.LogicalColumn` selects an annotation a Julia type cannot say, such as ENUM or a
TIME unit.

A `Durations.Duration` is written as INTERVAL when its components are nonnegative, its
time component is a whole number of milliseconds, and each component fits UInt32. INT96
is not written (the format deprecates it). Rewriting a `Parquet.Table` that has an
INT96 column reports an unsupported feature; write its columns instead to store them as
INT64 timestamps.

## Rewriting a table

Writing a `Parquet.Table` keeps its stored scalar annotations and file key-value
metadata. A Tables.jl copy of its columns has only their Julia types, so some
annotations change on the way back, while the values stay exact:

- An ENUM column reads as strings and is written back as STRING.
- A millisecond or microsecond TIME column reads as `Dates.Time` and is written back as
  TIME with nanosecond units.
- A `Vector{UInt8}` column is written as BYTE_ARRAY; a list of `UInt8` elements is
  written as a LIST of 8-bit integers.

[`Parquet.LogicalColumn`](@ref) selects the exact annotation when it matters, and supplies
a schema for empty and all-null columns. [`Parquet.JSONValue`](@ref) and
[`Parquet.BSONValue`](@ref) keep encoded documents as they are (`string(value)` is a JSON
document's text), and [`Parquet.Decimal`](@ref) holds DECIMAL values wider than 76
digits.

```@example types
io = IOBuffer()
Parquet.write(io, (level=Parquet.LogicalColumn(["low", "high"], :enum),
    at=Parquet.LogicalColumn([Time(9), Time(17)], :time; unit=:millis, adjusted=false)))
md = Parquet.metadata(Parquet.File(take!(io)))
[(element.name, element.converted_type) for element in md.schema[2:end]]
```
