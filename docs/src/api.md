# API reference

```@meta
CurrentModule = Parquet
```

## Reading and writing

```@docs
Parquet.write
```

## Logical values

```@docs
LogicalColumn
Timestamp
JSONValue
BSONValue
Interval
```

DECIMAL columns use [Decimals.jl](https://github.com/quinnj/Decimals.jl). A column
annotated `DECIMAL(precision, scale)` reads as `Decimals.Decimal{precision,scale,T}`,
where `T` is the narrowest signed integer that holds `10^precision - 1`: `Int32` up to
precision 9, `Int64` up to 18, `Int128` up to 38, and `BitIntegers.Int256` up to 76.
The value is isbits, so a decimal column is a dense `Vector`. Writing a
`Decimals.Decimal` column selects INT32, INT64, or the minimal
FIXED_LEN_BYTE_ARRAY width from the same precision. Precision above 76 is not
supported in either direction.
