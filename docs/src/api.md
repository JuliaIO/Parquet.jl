# API reference

```@meta
CurrentModule = Parquet
```

## Reading and writing

```@docs
Parquet.Table
Parquet.File
Parquet.Dataset
Parquet.read
Parquet.metadata
Parquet.statistics
Parquet.write
Parquet.Writer
Parquet.Limits
```

## Filters

```@docs
Parquet.col
Parquet.colcmp
Parquet.colin
Parquet.isnull
```

## Sources

```@docs
Parquet.AbstractSource
Parquet.sourcelength
Parquet.readrange
Parquet.concurrentreads
```

## Errors

```@docs
Parquet.FormatError
Parquet.UnsupportedFeatureError
Parquet.LimitError
```

## Nested values

```@docs
Parquet.ListValue
Parquet.MapValue
Parquet.StructValue
Parquet.ListVector
Parquet.StructVector
Parquet.MapVector
```

## Logical values

```@docs
LogicalColumn
Decimal
JSONValue
BSONValue
```

## Deprecated

The 0.8 names, still exported until 2.0 ([Migrating from 0.8](@ref)).

```@docs
read_parquet
write_parquet
```
