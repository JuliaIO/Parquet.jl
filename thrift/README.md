# Vendored Parquet IDL

`parquet.thrift` is the unmodified Apache Parquet format definition from
[apache/parquet-format](https://github.com/apache/parquet-format) release 2.13.0,
peeled source commit `c47e2a66e88943fc46fde1b028a9432f14fdf5c0`
(`src/main/thrift/parquet.thrift`, git blob `fe259d61bc470ade78bad48f5223a82598b91b59`).
It is licensed under the Apache License 2.0; the license header is preserved in the file.

`generate.jl` is a pure-Julia generator that turns the IDL into
`src/metadata/parquet.jl`, the immutable `Parquet.Metadata` structs decoded and encoded
by the Compact Protocol runtime in `src/thrift.jl`. No external Thrift compiler is used.

```bash
julia thrift/generate.jl          # regenerate src/metadata/parquet.jl
julia thrift/generate.jl --check  # fail when the checked-in file is stale
```

Generated code conventions:

- Every struct keeps unrecognized fields (including extension field 32767) in
  `unknown_fields::Tuple{Vararg{Thrift.RawField}}` with their verbatim header and payload
  bytes, and re-emits them in encounter order.
- Enums are modules with an `Int32` wrapper type `T`; unknown values round-trip unchanged.
- Unions are structs whose members are all optional; decoding and construction reject more
  than one member, and a single unknown member is preserved.
- Field names that are Julia keywords get a trailing underscore (`type` becomes `type_`);
  the comment next to each field records the Thrift id, requiredness, type, and name.
