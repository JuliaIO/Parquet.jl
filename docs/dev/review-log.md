# Architecture review log

The 1.0 plan and implementation received primary Codex work and independent Codex
reviews. A Claude Fable 5 Max review was requested, but the account reported
insufficient credit and no response was obtained. Actor-specific Claude attributions
in earlier drafts were not verified and have been replaced with role-based wording.

## Plan round 1

Codex proposed the layered reader and writer design, strict resource limits, a small
namespaced API, a generated metadata layer, and release gates based on independent
implementations.

The initial adversarial plan review required these material changes:

- Unknown enums, union members, page types, and fields must survive a metadata round trip.
- Page headers are authoritative. Footer encoding and offset lists are hints.
- `PLAIN_DICTIONARY` remains a first-class read path.
- Statistics pruning needs type-specific trust rules and an exact residual filter.
- Data Page V2, encryption modules, Variant, and geospatial data need separate gates.
- Corpus files with malformed metadata must fail safely instead of selecting a fallback
  type or allocating from unchecked counts.

Codex accepted these changes and added them to the roadmap.

## Plan round 2

Codex found three stable-format features that the first converged draft had deferred:

- LZO is not deprecated in 2.13.0. Version 1.0 must read and write it.
- Complete Variant support includes shredded Variant writing.
- Complete GEOGRAPHY writing includes antimeridian-aware bounding boxes where
  `xmin > xmax` is valid.

The second plan review accepted all three corrections. The reviews also found that LZO needs an
external parquet-java plus hadoop-lzo oracle because Arrow-family implementations do
not provide that evidence.

There is no remaining architecture disagreement. The roadmap records the combined
scope. An agreement on scope is not evidence that an implementation stage is complete.

## Implementation review

A delegated implementation pass added the Compact Protocol runtime and generated metadata types. Codex
found an evaluation-order error in binary-field skipping, missing exact raw-header
preservation, unsafe container arithmetic, weak Boolean validation, keyword collisions,
and incomplete union checks. The implementation pass fixed these items before the module was accepted.

The same loop applies to later delegated modules. Each module needs direct tests, corpus
evidence, an independent oracle where possible, and a separate Codex review.

A later delegated pass implemented DELTA_BINARY_PACKED, DELTA_LENGTH_BYTE_ARRAY,
DELTA_BYTE_ARRAY, and BYTE_STREAM_SPLIT. Codex found unchecked miniblock layout
allocation, unchecked physical widths, unsafe position arithmetic, and missing page
bounds. The pass fixed these items. Codex then found that the stable specification permits
arbitrary width bytes for unused final miniblocks. The pass moved validation to used
miniblocks and added `0xff` compatibility cases. Both Julia 1.10 and current stable pass
the focused tests. Separate CodecZlib and CodecZstd environments pass the compressed
corpus oracles. These encoding features remain in progress until they are integrated
with the page reader and writer.

A delegated pass then implemented bounded Data Page V1 framing and flat column decoding. The
slice verifies page CRCs, requires exact payload consumption, supports required and
optional PLAIN physical values, and rejects unsupported compression, dictionary
encoding, repetition, and encryption. Corpus tests cover valid and corrupted checksum
pages, nullable pages, fixed byte arrays, and seeded mutations.

Codex reviewed and integrated that decoder with a small Tables.jl facade and a
one-row-group PLAIN V1 writer. The review added checked row and byte totals, string and
page size checks, logical STRING materialization, exact footer consumption,
and end-to-end corpus tests. PyArrow, DuckDB, and Parquet2 independently read the
writer output. This vertical slice remains in progress because it does not yet cover
all Data Page V1 encodings, codecs, nested levels, statistics, or multi-row-group
writing.

In a second decoder review, Codex found a tautological corpus assertion, an incorrect
INDEX_PAGE rejection, and unsafe handling of contradictory dictionary offsets. The pass
replaced the assertion with exact PyArrow-backed footer facts, confirmed that reference
readers skip framed index pages, and derived chunk-start rules from the specification
plus two real writer quirks in parquet-testing. The final range checks reject negative
or contradictory offsets, bound every nonempty chunk before the footer, and pass 3,000
seeded hostile-metadata cases without raw arithmetic errors.

A delegated pass then implemented the bounded compression layer for UNCOMPRESSED, SNAPPY, GZIP,
BROTLI, ZSTD, LZ4_RAW, and deprecated LZ4 input. Codex integrated exact-size page
decompression and writer compression, added adaptive dictionary read and write paths,
and corrected row-group compressed and uncompressed totals. Legacy PLAIN_DICTIONARY,
modern RLE_DICTIONARY, omitted dictionary offsets, PLAIN fallback pages, and hostile
indices have direct coverage. Constant dictionary indices use RLE runs; this both
reduces size and avoids a Parquet2 failure on a legal zero-width bit-packed run.
PyArrow, DuckDB, and Parquet2 read constant and multi-entry files emitted with every
writable codec. LZO remains blocked because the available package and binary are GPL-2;
the MIT core does not add them.

An adversarial integration audit found that parquet-cpp rejects dictionary-encoded
BOOLEAN columns even though the format permits them. The adaptive writer now keeps
BOOLEAN data PLAIN, while the reader retains BOOLEAN dictionary compatibility. The same
audit confirmed all 60 regenerated writer cases across PyArrow and 6,000 seeded corrupt
files. A follow-up review added the complete Hadoop BlockCompressorStream grammar,
including multi-chunk blocks and the four-byte empty marker, while retaining parquet-cpp's
repeated-pair and raw-block fallbacks. Adaptive dictionary limits now fall back to an
already-valid PLAIN candidate, and byte-array dictionary counts are checked against the
available payload before pointer-array allocation.

Codex then connected the existing value kernels to the flat page reader and added Data
Page V2 framing. An adversarial review confirmed that V2 keeps repetition and
definition streams uncompressed and without length prefixes, compresses only the value
section, defaults an absent `is_compressed` field to true, and checks the CRC across the
complete stored payload. The review also found a real corpus compatibility case: a flat
column can carry a redundant zero-width repetition stream. The reader now validates and
exactly consumes that stream instead of requiring it to be absent.

The V2 writer omits flat repetition bytes and uses adaptive value compression. It keeps
compressed bytes only when they are smaller, writes raw empty or incompressible values
with `is_compressed=false`, and reports V2 page types in encoding statistics. Dictionary
pages remain whole-page compressed, while V2 dictionary indexes follow the value-section
rule. The same review exposed a nine-byte deprecated-LZ4 encoding of an empty block; the
compatibility decoder now accepts it without weakening truncated-stream checks.

Pinned Apache fixtures cover empty compressed pages, redundant zero-width levels,
Boolean RLE, all delta encodings, BYTE_STREAM_SPLIT, concatenated GZIP members, and V2
dictionary data. PyArrow 25.0.1 and DuckDB 1.4.1 read the emitted compressed,
adaptive-uncompressed, levels-only, and dictionary V2 cases. The complete pinned-corpus
suite passes on Julia 1.10 and current stable. V2 remains marked in progress until nested
levels and every writer encoding are implemented.

The next delegated writer pass connected the remaining kernels to complete flat V1 and
V2 files. A private explicit path now emits DELTA_BINARY_PACKED,
DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY, BYTE_STREAM_SPLIT, and Boolean RLE. It
encodes only present values, retains the Boolean value-stream length prefix in V2,
deduplicates RLE in footer encoding lists, and rejects every invalid physical-type pair
before it writes a page. Empty and all-null cases match the canonical empty streams.

Codex kept this selector private because one global encoding cannot represent a mixed
table well. Public per-column selection remains an API decision. Julia 1.10 and current
stable pass the writer matrix. PyArrow 25.0.1 and DuckDB 1.4.1 read 60 exact-value files
covering both page versions, all five non-PLAIN encoding families, all six writable
codecs, and page checksums. LZO remains a Stage 3 gap.

The fixed-width writer pass maps `NTuple{N,UInt8}` table columns to
FIXED_LEN_BYTE_ARRAY(N). The type carries the width for empty and all-null columns.
PLAIN, DELTA_BYTE_ARRAY, BYTE_STREAM_SPLIT, and adaptive dictionary output preserve
the schema width, optional levels, and footer encoding metadata. The high-level
`Parquet.Table` facade retains the file-provided width in an internal runtime-width
vector, so a read-write cycle cannot silently turn values into variable BYTE_ARRAY
columns or create an untrusted width-dependent Julia type. PyArrow 25.0.1 reads 48
exact-value files across both page versions, PLAIN, DELTA_BYTE_ARRAY,
BYTE_STREAM_SPLIT, adaptive dictionary output, all six writable codecs, and verified
checksums. DuckDB 1.4.1 reads the 36 non-BYTE_STREAM_SPLIT files; that DuckDB release
rejects the format-2.11 extension of BYTE_STREAM_SPLIT to FIXED_LEN_BYTE_ARRAY.
An adversarial review found that constructing `NTuple{type_length,UInt8}` from file
metadata could spend seconds specializing a huge type even for a zero-row file. Schema
parsing now applies the string-byte limit first, and the table facade carries untrusted
widths only as runtime data. Writer input can still use statically sized tuples.

The public writer now accepts a table-wide or name-based `encoding` policy without
adding another public type. Pair, NamedTuple, and dictionary forms override selected
columns; unlisted columns retain the existing PLAIN or adaptive-dictionary default.
The `:dictionary` policy selects the same size-checked dictionary path. Policy keys
are matched exactly after String conversion, and unknown or duplicate normalized
names and invalid physical-type pairs fail before any output is published. Footer
encoding lists, page encoding statistics, and dictionary offsets continue to describe
the pages actually written rather than the requested policy. The focused writer suite
passes 1,023 checks on Julia 1.10 and current stable. PyArrow 25.0.1 and DuckDB 1.4.1
read 12 mixed-policy files with exact values across both page versions and all six
writable codecs. PyArrow also verified every page checksum and the fixed-width schema.

The first Stage 4 slice separates physical page decoding from nested assembly through
an internal `LeafStream` of repetition levels, definition levels, and dense present
values. V1 decodes repetition levels before definition levels and permits a later page
to begin inside a row. V2 requires every page to begin at repetition level zero and
checks `num_rows` and `num_nulls` against the decoded streams. Flat `readcolumn` remains
an adapter over the same boundary. Focused tests cover RLE and deprecated BIT_PACKED
levels, split pages, dictionary pages, every current value encoding, exact stream
consumption, hostile counts, and malformed starts on Julia 1.10 and current stable.

The logical layer gives modern STRING and DATE annotations precedence over legacy
ConvertedType fields, validates their physical types, and preserves unsupported modern
annotations as physical data. The high-level table assembles canonical optional lists
and the compatible Apache `item` leaf spelling. The writer emits the canonical
three-level `list.element` schema and exact levels for null lists, empty lists, null
elements, and present DATE values. `ColumnMetaData.num_values` records seven leaf
entries for the five-row acceptance example, while the V2 page records five rows and
four null leaf entries.

The pinned Apache `list_columns.parquet` fixture passes. Julia reads nine independently
generated PyArrow and DuckDB files across V1, V2, PLAIN, dictionary, uncompressed,
Snappy, and Zstd forms. PyArrow 25.0.1 and DuckDB 1.5.5 read Julia scalar DATE and
DATE-list files using PLAIN, DELTA_BINARY_PACKED, and dictionary value streams in both
page versions, with page-checksum verification in PyArrow. A fresh Claude Fable 5
maximum-reasoning implementation audit could not start because Claude Code reported an
insufficient credit balance. No new Claude implementation approval is claimed. The
earlier architecture agreement still applies, and the wider logical and nested stages
remain in progress.

The next Stage 4 pass implemented the remaining stable scalar logical annotations.
TIME covers milliseconds, microseconds, and nanoseconds with a strict one-day range.
TIMESTAMP retains exact microsecond and nanosecond ticks and the `isAdjustedToUTC` flag.
INTEGER covers all signed and unsigned 8-, 16-, 32-, and 64-bit forms. DECIMAL converts
INT32, INT64, BYTE_ARRAY, and FIXED_LEN_BYTE_ARRAY with exact precision checks and
big-endian two's-complement bytes. UUID, FLOAT16, ENUM, JSON, BSON, legacy INTERVAL, and
UNKNOWN have validated physical mappings. Modern annotations take precedence over
conflicting legacy annotations. Unsupported future annotations remain physical values.

An integration review found that inferring a new schema from materialized Julia values
would lose source-only details. The table writer now retains an existing scalar leaf
schema, including TIME units, timestamp UTC flags, ENUM identity, declared decimal
precision, and unknown modern annotations. Plain high-level values use documented
defaults only where the Julia type does not carry those details. Bounded validators
check full RFC 8259 JSON syntax and BSON 1.1 document structure without materializing
an object tree. BSON payloads remain opaque after validation.

The complete test suite passes on Julia 1.10 and current stable, with zero detected
method ambiguities. The pinned Apache logical corpus passes exact value checks.
PyArrow 25.0.1 reads Julia temporal, integer, decimal, UUID, FLOAT16, JSON, BSON, and
INTERVAL output and Julia reads PyArrow V1 and V2 logical fixtures exactly. DuckDB
1.5.5 reads the Julia temporal, integer, decimal, and UUID cases, and Julia reads its
corresponding output. DuckDB rejects BSON ConvertedType metadata in the combined binary
fixture, so that form uses PyArrow as the independent reader.

A fresh Claude Fable 5 maximum-reasoning audit was requested for this integrated pass.
Claude Code returned `Credit balance is too low`, so the audit did not run and no fresh
Claude approval is claimed. An independent Codex agent performed the adversarial code
review instead. It found contradictory metadata on schema-bearing rewrites, weak JSON
and BSON validation, quadratic binary DECIMAL conversion, and silent fallback for an
unknown timestamp unit. The implementation now canonicalizes all known outgoing scalar
annotations, validates both document grammars, uses bounded bulk BigInt conversion, and
reports future time units as unsupported. The logical-types ledger remains
`in_progress`: recursive and legacy nested forms, structs, maps, VARIANT, and geospatial
data still remain.

The public, namespaced `Parquet.LogicalColumn` wrapper closes the explicit-authoring
gap for ENUM, every TIME and TIMESTAMP unit and adjustment flag, and DECIMAL precision
and scale. It retains these parameters for empty and all-null columns. PyArrow 25.0.1
confirmed exact schemas and values for all of these combinations, including nanosecond
timestamps. DuckDB 1.5.5 accepted every file and confirmed the schema and values at the
precision it exposes; its timestamp conversion truncates nanoseconds to microseconds.

A read-only follow-up review found no remaining P0 or P1 issue. It found three bounded
P2 allocation paths: tagged JSON and BSON writes copied bytes before validation, fixed
DECIMAL vector conversion allocated output before checking its schema width, and BSON
array validation created one decimal key string per element. Validation now precedes
the required successful-write copy, fixed widths are preflighted before vector
allocation, and array keys are parsed directly from ASCII bytes. Rejected large
JSON/BSON inputs allocate 128/112 bytes in the regression probes, one million-row fixed
DECIMAL schema rejections allocate under 1 KiB, and a 4,096-element BSON array validates
with 80 bytes of allocation on Julia 1.10 and current stable.
A second read-only adversarial pass found no remaining P0, P1, or P2 issue in these
fixes. Its 100,000-element BSON case also validated with 80 bytes of allocation.

Final `Pkg.test()` runs pass on Julia 1.10 and current stable against pinned
parquet-testing commit `09f3cdbde45302f0f0c689c950e465e98a9df960`. Recursive
ambiguity checks find zero ambiguities, and `git diff --check` passes. These results
close this scalar implementation slice only. They do not close Stage 4 or the 1.0
release gates.
