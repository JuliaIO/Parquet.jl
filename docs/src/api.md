```@meta
CurrentModule = Parquet
```

# API
```@index
Pages = ["api.md"]
```

## Basic Usage
```@docs
Parquet.File
Parquet.Table
Parquet.Dataset
read_parquet
write_parquet
```

## Low-level Usage
```@docs
Page
PageLRU
TablePartition
TablePartitions
ColCursor
BatchedColumnsCursor
DatasetPartitions
Schema
```
