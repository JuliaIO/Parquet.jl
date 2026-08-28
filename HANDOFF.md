# Parquet.jl `rewrite/1.0` handoff

Last updated: 2026-08-27

Read this file before continuing the rewrite. It separates implemented code,
verified evidence, planned scope, and release authority.

## Branch state

- Local and remote branch: `rewrite/1.0`
- Baseline: `95a3037aa1e643370f04c4d2393c2e925a7d3115`
- Baseline package: Parquet.jl 0.8.6
- Rewrite package version: `1.0.0-DEV`
- Stable format target: Apache Parquet 2.13.0
- Pinned format source: `c47e2a66e88943fc46fde1b028a9432f14fdf5c0`
- Pinned corpus: `09f3cdbde45302f0f0c689c950e465e98a9df960`
- Current disposition: preproduction
- Pull request: none
- Release authority: none

The branch preserves the registered package UUID and Git history. It removes the old
PAR2 runtime and replaces it with a new pure Julia implementation.

## Authority boundary

`test/conformance/n6/manifest.toml` is authoritative for the exact N6 evidence scope.
It intentionally contains:

```toml
status = "preproduction"
supported_platforms = ["macos-15-arm64"]
publication_authorized = false
oracle_lock_authorized = false
```

Do not call this branch release-ready. Do not publish an oracle lock, tag a release,
or change either authorization flag until the remaining provenance and platform gates
are complete and the user gives explicit approval.

The requested Claude Fable 5 Max review did not run. Claude Code reported an
insufficient credit balance. No Claude agreement or implementation approval exists.
Independent Codex agents performed the confirmed adversarial reviews. The final N6
review found no P0, P1, or P2 issue in its reviewed preproduction scope.

## What changed

The rewrite added these working layers:

- bounded source ownership and footer framing;
- a pure Julia Thrift Compact Protocol runtime;
- generated Parquet 2.13.0 metadata types;
- physical and logical schema validation;
- Dremel levels and nested vectors;
- PLAIN, RLE, dictionary, delta, and BYTE_STREAM_SPLIT encodings;
- Data Page V1 and V2 framing with CRC checks;
- UNCOMPRESSED, SNAPPY, GZIP, BROTLI, ZSTD, and LZ4 paths;
- scalar logical types and bounded JSON, BSON, and decimal validation;
- recursive nested reading and writing slices;
- statistics, producer-trust, page-index, and offset-index slices;
- a Tables.jl facade and namespaced writer API;
- resource-limit, mutation, corpus, interoperability, and evidence tests;
- Documenter documentation and CI workflows.

The package has no exports. The actual public declaration is in `src/Parquet.jl`.
The current public names are `BSONValue`, `Decimal`, `File`, `Interval`, `JSONValue`,
`Limits`, `LogicalColumn`, `Table`, `Timestamp`, `close!`, and `write`.

The public API and complete feature descriptions in `docs/dev/roadmap.md` are target
design. They are not proof that the corresponding module exists. For example,
`Dataset`, scan pushdown, bloom filters, encryption, Variant, and geospatial modules
are still target work.

## Repository map

| Area | Start here | Main verification |
| --- | --- | --- |
| Contribution rules | `AGENTS.md`, `SKILL.md` | Review every changed file against both |
| Architecture and gates | `docs/dev/architecture.md`, `docs/dev/roadmap.md` | `test/conformance/features.toml` |
| Source ownership | `src/source.jl`, `src/footer.jl` | `test/source.jl`, `test/footer.jl`, `test/limits.jl` |
| Metadata | `src/thrift.jl`, `src/metadata/parquet.jl` | `test/thrift.jl`, `test/metadata.jl`, `test/generator.jl` |
| Schema and nesting | `src/schema.jl`, `src/nested_schema.jl`, `src/dremel.jl` | `test/nested_schema.jl`, `test/nested_reader.jl`, `test/nested_table.jl` |
| Encodings | `src/plain.jl`, `src/rle.jl`, `src/delta.jl`, `src/bss.jl` | Matching files under `test/` |
| Pages and codecs | `src/page.jl`, `src/codecs.jl`, `src/checksum.jl` | `test/page.jl`, `test/codecs.jl`, `test/checksum.jl` |
| Logical values | `src/logical*.jl` | `test/logical*.jl` |
| Statistics and indexes | `src/statistics.jl`, `src/page_index.jl` | `test/statistics.jl`, `test/write_statistics.jl`, `test/write_offset_index.jl` |
| Writer | `src/write*.jl` | `test/write*.jl` |
| Tables facade | `src/table.jl`, `src/nested_table.jl` | `test/table.jl`, `test/nested_table.jl` |
| N5 conformance | `test/conformance/n5/` | `test/conformance/n5/runtests.jl` |
| N6 evidence gate | `test/conformance/n6/README.md`, `test/conformance/n6/manifest.toml` | `test/conformance/n6/runtests.jl` |
| User documentation | `README.md`, `docs/src/` | `docs/make.jl` |

## Verified evidence

The final preproduction verification on 2026-08-24 recorded:

- full package suites passed on Julia 1.10.11 and 1.12.6;
- N6 external gate passed 5,117 of 5,117 checks;
- the independent model passed 578 of 578 checks;
- N6 static lanes passed 5,063 of 5,063 checks on both Julia versions;
- the Python harness passed 23 of 23 checks;
- normalizer tests passed 26 of 26 checks;
- all 69 pinned artifact hashes passed;
- strict docs, doctests, and public API checks passed;
- the final source composite hash was
  `cdb21788f1c7c4d29e567681ecd851fb6dcdbe115bb29243e006cecb35080c05`.

Before the branch push on 2026-08-27, `Pkg.test("Parquet")` passed again on
Julia 1.10.11 and 1.12.6 from temporary resolved environments. These reruns included
the local N5 and N6 harness tests. Corpus-only tests had the expected skips described
below.

Exact N6 file identities at that gate:

- `test/conformance/n6/runtests.jl`:
  `b0708ac70a942093a631e849a2442169fd64376908b5e2b979055cb51fb7a3eb`
- `test/conformance/n6/manifest.toml`:
  `f6761f7c13a80688e64651918aa9db1c46452a8f1c99a2c597a859ff4a82b2bf`
- `test/conformance/n6/artifacts.sha256`:
  `dd5bf9b64b843597eabbec70b611bc7c39341ccce847b66279e25aca96fce4e6`

The ordinary corpus tests skip corpus-only cases when `test/parquet-testing` is
absent. The exact N6 run used the authenticated external corpus. Keep that distinction
in all reports.

## Common validation

From the repository root:

```sh
julia +1.10.11 --project=. --startup-file=no --history-file=no test/runtests.jl
julia +1.12.6 --project=. --startup-file=no --history-file=no test/runtests.jl
PARQUET_N6_GATE=0 julia +1.12.6 --project=. --startup-file=no --history-file=no test/conformance/n6/runtests.jl
julia +1.12.6 --project=docs --startup-file=no --history-file=no docs/make.jl
git diff --check
```

The exact N6 external gate needs authenticated source trees, runtime archives, wheels,
the JDK, the raw Java download cache, and Docker. Its complete environment contract is
in `test/conformance/n6/README.md`. Do not replace it with ambient Python, Java, Julia,
or package installations.

The CI workflow runs package and N5 tests on Linux, macOS, and Windows. It also has
macOS ARM64 N6 static lanes. A branch-only push does not run the current push workflow,
because push events are limited to `master`. A pull request would run CI, but no pull
request was requested for this handoff.

## Known remaining work

1. Reconcile implementation, tests, docs, and `test/conformance/features.toml`.
   The ledger remains conservative: stages 1 through 4 are `in_progress`, and stages
   5 through 8 are `planned`. Promote a row only after its full evidence contract
   passes.
2. Complete the target-only modules: bloom filters, residual-safe scan pushdown,
   modular encryption, Variant, geospatial support, and datasets.
3. Prove PyArrow and DuckDB source-to-wheel provenance. Their official wheel bytes and
   runtime behavior are verified, but both source entries remain `planned` in the N6
   manifest.
4. Expand the exact gate beyond macOS 15 ARM64. Run clean Linux, Windows, other macOS,
   Julia nightly, bounds, reverse-dependency, PkgEval, performance, and allocation
   qualification.
5. Rebuild and review all current user-facing support statements. `README.md` and some
   roadmap current-state paragraphs understate later nested, statistics, and index
   slices. Treat tests and frozen evidence as facts until the text is reconciled.
6. Keep publication and oracle locking disabled until every release gate is complete.

## Out of scope

These are decided, not pending. Do not reopen them without a concrete user need.

- LZO. Every available implementation is GPL-2, so an MIT core cannot depend on one.
  A file that uses it reports an unsupported feature.
- INT96. Deprecated in the format and not supported in either direction.
- Writing the deprecated LZ4 codec and the deprecated BIT_PACKED encoding. Both remain
  readable, because files in the wild use them; new files use LZ4_RAW and RLE.

Complete coverage of the format is explicitly not a goal. No mainstream implementation
has it, and the practical target is interoperability with the implementations people
actually use.

## Safe continuation order

1. Start from a clean clone of `origin/rewrite/1.0`.
2. Read `AGENTS.md`, this handoff, `docs/dev/architecture.md`, and the relevant plan.
3. Confirm the branch tip and N6 hashes before changing source.
4. Select one conservative feature-ledger row.
5. Add valid, invalid, resource-limit, and independent interoperability evidence.
6. Run focused tests, both supported Julia suites, static N6, docs, and the relevant
   external oracle.
7. Update a feature status only when the complete evidence contract is satisfied.
8. Report package, CI, review, provenance, and release readiness as separate gates.

## Local checkout note

This working directory can contain ignored N5/N6 build caches, downloaded toolchains,
and compiled oracle output under `test/conformance/`. They are not part of the branch.
A fresh clone reconstructs only the checked-in sources, fixtures, manifests, and
normalized evidence. The committed branch must not contain a root `Manifest.toml`,
`docs/Manifest.toml`, `docs/build`, Python bytecode, private keys, or access tokens.
