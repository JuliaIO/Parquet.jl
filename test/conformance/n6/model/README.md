# Independent N6 statistics model

This directory contains the frozen test-only semantic model for N6-A. Its authority is
`docs/dev/n6-statistics-plan.md` at SHA-256
`15adf34af765a3300d8a49ced73532ed02b7e4edee5764453c58e53fcf12c304`.

The module owns its metadata-like types, PLAIN bound decoding, logical validation,
decimal comparison, producer parser and trust policy, count state machine, bound-family
selection, TYPE_ORDER compatibility, and raw-bit IEEE total order. It does not load the
Parquet package. It does not call a production decoder or comparator. The tests scan the
Julia files in this directory to keep that boundary explicit.

`LeafSpec` represents a leaf from an already validated Parquet schema. The model does
not validate whether a logical type and physical type may be paired in a schema. It
validates statistics widths, values, order, counts, and trust after that schema gate.

Producer semantic versions must contain `major.minor.patch`. Apache Arrow's policy
parser defaults omitted minor or patch components to zero. This model deliberately does
not. Partial versions such as `1`, `1.2`, `1.3`, and `1.10` have no usable version. A
recognized parquet-cpp or parquet-mr producer with such a version is conservatively in
the affected range for the old non-signed-order rule.

The compatibility parsers are separate. `parse_created_by` follows pinned parquet-java
and rejects bare application tokens. The Arrow old-order parser additionally recognizes
bare `parquet-cpp` and `parquet-mr` as those applications with no usable version. It does
not recognize other bare text. PARQUET-251 uses only the parquet-java parser, so every
bare or wholly unparsable value is untrusted on byte-array leaves. PARQUET-251 runs
before the Arrow equality exception. These policies may discard bounds, but cannot
create a false exclusion.

When `num_values` is zero, occupancy is empty without requiring optional count fields.
Any present bounds then become unknown.

Run the model directly from the repository root:

```sh
julia --startup-file=no test/conformance/n6/model/runtests.jl
```

`cases.toml` freezes the format, parquet-java, and Arrow policy source pins. It also
freezes the coverage groups, limit rules, and exhaustive Float16 order digest. The
digest covers every UInt16 bit pattern serialized in little-endian order after IEEE
total-order sorting.

This model interprets already-extracted statistics fields and computes independent
extrema from raw scalar values. It does not decode Compact Thrift or inspect Parquet
files. The separately pinned non-Julia raw 2.13 scanner owns that evidence. This model
also does not authorize pruning, ColumnIndex content, an oracle image, or
`oracles.lock`.
