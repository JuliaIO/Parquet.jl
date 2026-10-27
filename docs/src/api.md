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

## Logical values

```@docs
LogicalColumn
Decimal
JSONValue
BSONValue
```
