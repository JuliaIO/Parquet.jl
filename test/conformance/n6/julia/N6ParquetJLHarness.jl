module N6ParquetJLHarness

using Dates
using Parquet
using SHA
using TOML

const N6_ROOT = normpath(joinpath(@__DIR__, ".."))
const REPO_ROOT = normpath(joinpath(N6_ROOT, "..", "..", ".."))
const MODEL_FILE = joinpath(N6_ROOT, "model", "N6StatisticsModel.jl")
const FROZEN_MODEL_SHA256 =
    "32c090ed6e0c6af49eabf3f96afc6e17dff630c4e89372367e693202e87c4262"

function _statidentity(value)
    return (value.device, value.inode, value.mode, value.nlink, value.size,
        value.mtime, value.ctime)
end

function _stablefilebytes(path::String, maximum::Int64, label::String)
    maximum >= 0 || throw(ArgumentError("$label byte limit is negative"))
    maximum < typemax(Int) || throw(ArgumentError("$label byte limit is too large"))
    islink(path) && throw(ArgumentError("$label is a symlink"))
    before = lstat(path)
    isfile(before) || throw(ArgumentError("$label is not a regular file"))
    before.size <= maximum || throw(ArgumentError("$label exceeds its byte limit"))
    return open(path, "r") do stream
        opened = stat(stream)
        _statidentity(opened) == _statidentity(before) || throw(ArgumentError(
            "$label changed while it was opened"))
        bytes = read(stream, Int(maximum) + 1)
        length(bytes) <= maximum || throw(ArgumentError(
            "$label exceeds its byte limit"))
        length(bytes) == opened.size && eof(stream) || throw(ArgumentError(
            "$label changed size while it was read"))
        final = stat(stream)
        current = lstat(path)
        isfile(current) && !islink(path) || throw(ArgumentError(
            "$label changed type while it was read"))
        _statidentity(final) == _statidentity(opened) &&
            _statidentity(current) == _statidentity(opened) ||
            throw(ArgumentError("$label changed while it was read"))
        return bytes
    end
end

const FROZEN_MODEL_BYTES = _stablefilebytes(MODEL_FILE, Int64(1024 * 1024),
    "frozen N6 model source")
bytes2hex(SHA.sha256(FROZEN_MODEL_BYTES)) == FROZEN_MODEL_SHA256 ||
    error("frozen N6 model source digest differs")
Base.include_string(@__MODULE__, String(copy(FROZEN_MODEL_BYTES)), MODEL_FILE)

const Model = N6StatisticsModel
const MD = Parquet.Metadata
const TH = Parquet.Thrift
const PRODUCER_DESCRIPTOR_FILE =
    joinpath(N6_ROOT, "julia", "parquet-jl-producer.toml")
const PRODUCER_DESCRIPTOR_BYTES = _stablefilebytes(PRODUCER_DESCRIPTOR_FILE,
    Int64(4 * 1024 * 1024), "Parquet.jl producer descriptor")
const PRODUCER_DESCRIPTOR_SHA256 =
    bytes2hex(SHA.sha256(PRODUCER_DESCRIPTOR_BYTES))
const PRODUCER_DESCRIPTOR = TOML.parse(String(copy(PRODUCER_DESCRIPTOR_BYTES)))
const FIXTURES_FILE = joinpath(N6_ROOT, "fixtures.toml")
const CAPABILITIES_FILE = joinpath(N6_ROOT, "capabilities.toml")
const MANIFEST_FILE = joinpath(N6_ROOT, "manifest.toml")
const EVIDENCE_SCHEMA_FILE = joinpath(N6_ROOT, "evidence.schema.json")
const CORPUS_MANIFEST_FILE = joinpath(N6_ROOT, "corpus-files.sha256")
const PLAN_FILE = joinpath(REPO_ROOT, "docs", "dev", "n6-statistics-plan.md")
const EVIDENCE_FILE = joinpath(N6_ROOT, "evidence", "parquet-jl.normalized.jsonl")
const CANONICAL_TOOLCHAIN_SHA256 = PRODUCER_DESCRIPTOR_SHA256
const CANONICAL_WRITER_EXECUTABLE_SHA256 =
    PRODUCER_DESCRIPTOR["julia_executable_sha256"]
const CANONICAL_SOURCE_REVISION =
    PRODUCER_DESCRIPTOR["source_composite_sha256"]
const CANONICAL_WRITER_VERSION = v"1.12.6"
const MAX_DECLARATION_BYTES = Int64(1024 * 1024)
const MAX_GENERATED_BYTES = Int64(32 * 1024 * 1024)
const MAX_ROWS = 64
const GENERATED_CASE_COUNT = 12
const EXPECTED_PROFILES = Dict(
    "julia-writer-type-order" => ("writer-type-order-v1", Int64(6001), "type-order"),
    "julia-writer-ieee-order" => ("writer-ieee-order-v1", Int64(6002), "ieee-order"),
    "julia-writer-undefined-order" => ("writer-undefined-order-v1", Int64(6003), "undefined-order"),
    "julia-writer-statistics-disabled" => ("writer-statistics-disabled-v1", Int64(6004), "statistics-disabled"),
    "julia-writer-oversized-bounds" => ("writer-oversized-bounds-v1", Int64(6005), "one-byte-over"),
    "julia-writer-nested-row-groups" => ("writer-nested-row-groups-v1", Int64(6006), "nested-row-groups"),
    "julia-reader-untrusted-producer" => ("reader-untrusted-producer-v1", Int64(6007), "parquet-mr-1.7.0"),
    "julia-reader-no-pruning-absent" => ("reader-no-pruning-v1", Int64(6008), "absent"),
    "julia-reader-no-pruning-trusted" => ("reader-no-pruning-v1", Int64(6008), "trusted"),
    "julia-reader-no-pruning-untrusted" => ("reader-no-pruning-v1", Int64(6008), "producer-untrusted"),
    "julia-reader-no-pruning-oversized" => ("reader-no-pruning-v1", Int64(6008), "oversized"),
    "julia-reader-no-pruning-invalid" => ("reader-no-pruning-v1", Int64(6008), "semantically-unusable"),
)

mutable struct SeedStream
    state::UInt64
end

function SeedStream(seed::Integer)
    seed >= 0 || throw(ArgumentError("generator seed must be nonnegative"))
    return SeedStream(xor(UInt64(seed), 0x9e3779b97f4a7c15))
end

function nextseed!(stream::SeedStream)
    stream.state = stream.state * 0x5851f42d4c957f2d + 0x14057b7ef767814f
    return stream.state
end

function seedbyte!(stream::SeedStream)
    return UInt8(nextseed!(stream) >> 56)
end

struct ProfileOutput
    table::Any
    bytes::Vector{UInt8}
    statistics_enabled::Bool
    statistics_limit::Int64
    comparison_bytes::Union{Nothing,Vector{UInt8}}
end

struct CheckedCase
    declaration::Dict{String,Any}
    bytes::Vector{UInt8}
    file_record::Dict{String,Any}
    column_records::Vector{Dict{String,Any}}
    logical_values_sha256::String
    metadata_sha256::String
    assertion_facts::Dict{String,Any}
    no_pruning_sha256::Union{Nothing,String}
end

struct HarnessOutput
    files::Vector{Pair{String,Vector{UInt8}}}
    evidence::Vector{UInt8}
    checked::Vector{CheckedCase}
end

mutable struct TraceSource <: Parquet.AbstractSource
    bytes::Vector{UInt8}
    reads::Vector{Tuple{Int64,Int64}}
end

function Parquet.sourcelength(source::TraceSource)
    return Int64(length(source.bytes))
end

function Parquet.readrange(source::TraceSource, offset::Integer, count::Integer)
    offset64 = Int64(offset)
    count64 = Int64(count)
    offset64 >= 0 || throw(ArgumentError("trace source offset is negative"))
    count64 >= 0 || throw(ArgumentError("trace source count is negative"))
    stop = Base.checked_add(offset64, count64)
    stop <= length(source.bytes) || throw(BoundsError(source.bytes,
        (offset64, count64)))
    push!(source.reads, (offset64, count64))
    first = Int(offset64) + 1
    return @view source.bytes[first:(first + Int(count64) - 1)]
end

function Parquet.close!(::TraceSource)
    return
end

function filehash(path::AbstractString; maximum::Int64=MAX_DECLARATION_BYTES,
        label::String="hashed input")
    return bytehash(_stablefilebytes(String(path), maximum, label))
end

function bytehash(bytes::AbstractVector{UInt8})
    return bytes2hex(SHA.sha256(bytes))
end

function _hex4(value::UInt32)
    return lowercase(string(value; base=16, pad=8))
end

function _hex8(value::UInt64)
    return lowercase(string(value; base=16, pad=16))
end

function _jsonstring!(output::IO, value::AbstractString)
    write(output, UInt8('"'))
    for character in value
        character == '"' && (write(output, "\\\""); continue)
        character == '\\' && (write(output, "\\\\"); continue)
        character == '\b' && (write(output, "\\b"); continue)
        character == '\f' && (write(output, "\\f"); continue)
        character == '\n' && (write(output, "\\n"); continue)
        character == '\r' && (write(output, "\\r"); continue)
        character == '\t' && (write(output, "\\t"); continue)
        if Int(character) < 0x20
            write(output, "\\u", lowercase(string(Int(character); base=16, pad=4)))
        else
            write(output, string(character))
        end
    end
    write(output, UInt8('"'))
    return
end

function _canonicaljson!(output::IO, value)
    value === nothing && (write(output, "null"); return)
    value isa Bool && (write(output, value ? "true" : "false"); return)
    value isa Integer && !(value isa Bool) && (write(output, string(value)); return)
    value isa AbstractFloat && throw(ArgumentError(
        "floating JSON numbers are forbidden in N6 evidence"))
    value isa AbstractString && (_jsonstring!(output, value); return)
    if value isa AbstractDict
        keys_ = sort!(String[String(key) for key in keys(value)])
        length(keys_) == length(unique(keys_)) || throw(ArgumentError(
            "canonical JSON object has duplicate string keys"))
        write(output, UInt8('{'))
        for (index, key) in enumerate(keys_)
            index == 1 || write(output, UInt8(','))
            _jsonstring!(output, key)
            write(output, UInt8(':'))
            _canonicaljson!(output, value[key])
        end
        write(output, UInt8('}'))
        return
    end
    if value isa Union{Tuple,AbstractVector}
        write(output, UInt8('['))
        for (index, item) in enumerate(value)
            index == 1 || write(output, UInt8(','))
            _canonicaljson!(output, item)
        end
        write(output, UInt8(']'))
        return
    end
    throw(ArgumentError("unsupported canonical JSON value $(typeof(value))"))
end

function canonicaljson(value)
    output = IOBuffer()
    _canonicaljson!(output, value)
    return String(take!(output))
end

function canonicalhash(value)
    return bytehash(codeunits(canonicaljson(value)))
end

function _typedinteger(value::Integer)
    return Dict{String,Any}(
        "type" => string(nameof(typeof(value))),
        "value" => string(value),
    )
end

function _logicalvalue(value)
    value === missing && return nothing
    value isa Bool && return value
    value isa Integer && return _typedinteger(value)
    value isa Float16 && return Dict{String,Any}(
        "bits" => lowercase(string(reinterpret(UInt16, value); base=16, pad=4)),
        "type" => "Float16")
    value isa Float32 && return Dict{String,Any}(
        "bits" => _hex4(reinterpret(UInt32, value)), "type" => "Float32")
    value isa Float64 && return Dict{String,Any}(
        "bits" => _hex8(reinterpret(UInt64, value)), "type" => "Float64")
    value isa AbstractString && return String(value)
    value isa Parquet.JSONValue && return Dict{String,Any}(
        "json_hex" => bytes2hex(value.bytes))
    value isa Parquet.BSONValue && return Dict{String,Any}(
        "bson_hex" => bytes2hex(value.bytes))
    value isa Parquet.Interval && return Dict{String,Any}(
        "days" => string(value.days), "milliseconds" => string(value.milliseconds),
        "months" => string(value.months))
    value isa Parquet.DataDecimals.AbstractDecimal && return Dict{String,Any}(
        "scale" => string(Parquet.DataDecimals.scale(value)),
        "unscaled" => string(Parquet.DataDecimals.unscaled(value)))
    value isa Parquet.Decimal && return Dict{String,Any}(
        "scale" => string(value.scale), "unscaled" => string(value.unscaled))
    value isa Date && return Dict{String,Any}(
        "date_days" => string(Dates.value(value)))
    value isa DateTime && return Dict{String,Any}(
        "datetime_millis" => string(Dates.value(value)))
    value isa Time && return Dict{String,Any}(
        "time_nanos" => string(Dates.value(value)))
    value isa Parquet.Timestamp && return Dict{String,Any}(
        "adjusted" => value.is_adjusted_to_utc,
        "ticks" => string(value.ticks), "type" => string(typeof(value)))
    value isa Parquet.StructValue && return Dict{String,Any}(
        String(value.names[index]) => _logicalvalue(value[index])
        for index in 1:length(value))
    value isa NamedTuple && return Dict{String,Any}(
        String(name) => _logicalvalue(getproperty(value, name)) for name in keys(value))
    value isa Tuple && all(item -> item isa UInt8, value) &&
        return Dict{String,Any}("bytes_hex" => bytes2hex(UInt8[value...]))
    value isa Tuple && return Any[_logicalvalue(item) for item in value]
    value isa Pair && return Dict{String,Any}(
        "key" => _logicalvalue(first(value)), "value" => _logicalvalue(last(value)))
    value isa AbstractVector{UInt8} && return Dict{String,Any}(
        "bytes_hex" => bytes2hex(value))
    value isa AbstractVector && return Any[_logicalvalue(item) for item in value]
    value isa AbstractDict && return Any[_logicalvalue(pair) for pair in value]
    throw(ArgumentError("unsupported logical value $(typeof(value))"))
end

function logicalrows(columns::NamedTuple)
    names = keys(columns)
    values = Base.values(columns)
    rows = isempty(values) ? 0 : length(first(values))
    all(column -> length(column) == rows, values) || throw(ArgumentError(
        "logical columns have different lengths"))
    output = []
    sizehint!(output, rows)
    for row in 1:rows
        push!(output, Dict{String,Any}(
            String(name) => _logicalvalue(column[row])
            for (name, column) in zip(names, values)))
    end
    return output
end

function checkeddeclarations(fixtures::Dict{String,Any})
    declarations = fixtures["generated_case"]
    length(declarations) == GENERATED_CASE_COUNT || throw(ArgumentError(
        "N6 harness requires exactly $GENERATED_CASE_COUNT generated cases"))
    ids = String[declaration["id"] for declaration in declarations]
    Set(ids) == Set(keys(EXPECTED_PROFILES)) || throw(ArgumentError(
        "generated case IDs differ from the reviewed profile set"))
    for declaration in declarations
        id = declaration["id"]
        declaration["output_file"] == "generated/$id.parquet" ||
            throw(ArgumentError("generated output path differs for $id"))
        expected = EXPECTED_PROFILES[id]
        actual = (declaration["generator_profile"], declaration["generator_seed"],
            declaration["variant_id"])
        actual == expected || throw(ArgumentError(
            "generated identity differs for $id"))
        declaration["output_identity_status"] in ("planned", "verified") ||
            throw(ArgumentError("unsupported output identity status for $id"))
        declaration["authority"] == "parquet-jl" || throw(ArgumentError(
            "generated case authority differs for $id"))
        declaration["row_group_count"] >= 0 || throw(ArgumentError(
            "negative row-group count for $id"))
        declaration["leaf_count"] >= 0 || throw(ArgumentError(
            "negative leaf count for $id"))
    end
    outputs = String[declaration["output_file"] for declaration in declarations]
    length(unique(outputs)) == length(outputs) || throw(ArgumentError(
        "generated case output paths are not unique"))
    return fixtures, declarations
end

function _writerbytes(table; statistics::Bool=true, limit::Int64=4096,
        pageversion::Symbol=:v2, rowgroupsize::Int=3)
    rows = isempty(values(table)) ? 0 : length(first(values(table)))
    rows <= MAX_ROWS || throw(ArgumentError("generator row count exceeds $MAX_ROWS"))
    limits = Parquet.Limits(max_statistics_value_bytes=limit,
        max_materialized_bytes=256 * 1024 * 1024,
        max_string_bytes=16 * 1024 * 1024,
        max_container_elements=100_000)
    bytes = Parquet._encodefile(table; checksum=false, dictionary=false,
        codec=:uncompressed, pageversion=pageversion, encoding=:plain,
        rowgroupsize=rowgroupsize, pagesize=nothing, pageindex=true,
        statistics=statistics, limits=limits)
    length(bytes) <= MAX_GENERATED_BYTES || throw(ArgumentError(
        "generated Parquet output exceeds $MAX_GENERATED_BYTES bytes"))
    return bytes
end

function _typeordertable(seed::Int64)
    stream = SeedStream(seed)
    marker = Int32(seedbyte!(stream))
    signed = Union{Missing,Int32}[-1000 - marker, missing, 9, 400, -7, 1200 + marker]
    unsigned = UInt32[0, 0x80000000, typemax(UInt32), UInt32(seedbyte!(stream)), 1, 9]
    raw = Union{Missing,Vector{UInt8}}[
        UInt8[0x00, seedbyte!(stream)], missing, UInt8[0xff], UInt8[],
        UInt8[0x7f, 0x00], UInt8[0x80],
    ]
    text = Union{Missing,String}["z", missing, "a", "aa", "a\0b", "zz"]
    decimals = Union{Missing,Parquet.Decimal}[
        Parquet.Decimal(-90001, 2), missing, Parquet.Decimal(0, 2),
        Parquet.Decimal(12345, 2), Parquet.Decimal(-1, 2),
        Parquet.Decimal(99999, 2),
    ]
    decimal = Parquet.LogicalColumn(decimals, :decimal; precision=9, scale=2)
    dates = Union{Missing,Date}[
        Date(1900, 1, 1), missing, Date(1970, 1, 1), Date(2000, 2, 29),
        Date(1969, 12, 31), Date(2100, 1, 1),
    ]
    flag = Union{Missing,Bool}[false, missing, true, true, false, true]
    fixed = NTuple{3,UInt8}[(0x00, 0xff, seedbyte!(stream)), (0x10, 0x00, 0x00),
        (0xff, 0x00, 0x00), (0x01, 0x02, 0x03), (0x7f, 0xff, 0xff),
        (0x80, 0x00, 0x00)]
    return (; signed, unsigned, raw, text, decimal, dates, flag, fixed)
end

function _ieeetable(seed::Int64)
    stream = SeedStream(seed)
    payload16 = UInt16(seedbyte!(stream) & 0x3f) | 0x0001
    payload32 = UInt32(seedbyte!(stream)) | UInt32(1)
    payload64 = UInt64(seedbyte!(stream)) | UInt64(1)
    half = Union{Missing,Float16}[
        missing,
        reinterpret(Float16, UInt16(0x8000)),
        reinterpret(Float16, UInt16(0x0000)),
        reinterpret(Float16, UInt16(0xbc00)),
        reinterpret(Float16, UInt16(0x7c00)),
        reinterpret(Float16, UInt16(0x7e00 | payload16)),
        reinterpret(Float16, UInt16(0xfc01)),
        reinterpret(Float16, UInt16(0x7c01)),
        reinterpret(Float16, UInt16(0x7e10)),
        reinterpret(Float16, UInt16(0xfe20)),
        reinterpret(Float16, UInt16(0x7d55)),
        reinterpret(Float16, UInt16(0xfd23)),
    ]
    single = Union{Missing,Float32}[
        missing,
        reinterpret(Float32, UInt32(0x80000000)),
        reinterpret(Float32, UInt32(0x00000000)),
        reinterpret(Float32, UInt32(0xbf800000)),
        reinterpret(Float32, UInt32(0x7f800000)),
        reinterpret(Float32, UInt32(0x7fc00000 | payload32)),
        reinterpret(Float32, UInt32(0xff800001)),
        reinterpret(Float32, UInt32(0x7f800001)),
        reinterpret(Float32, UInt32(0x7fc12345)),
        reinterpret(Float32, UInt32(0xffc54321)),
        reinterpret(Float32, UInt32(0x7fa00011)),
        reinterpret(Float32, UInt32(0x7fe22222)),
    ]
    double = Union{Missing,Float64}[
        missing,
        reinterpret(Float64, UInt64(0x8000000000000000)),
        reinterpret(Float64, UInt64(0x0000000000000000)),
        reinterpret(Float64, UInt64(0xbff0000000000000)),
        reinterpret(Float64, UInt64(0x7ff0000000000000)),
        reinterpret(Float64, UInt64(0x7ff8000000000000) | payload64),
        reinterpret(Float64, UInt64(0xfff0000000000001)),
        reinterpret(Float64, UInt64(0x7ff0000000000001)),
        reinterpret(Float64, UInt64(0x7ff8000000012345)),
        reinterpret(Float64, UInt64(0xfff8000000000022)),
        reinterpret(Float64, UInt64(0x7ff4000000000033)),
        reinterpret(Float64, UInt64(0x7ff2000000000044)),
    ]
    return (; half, single, double)
end

function _undefinedtable(seed::Int64)
    stream = SeedStream(seed)
    marker = UInt32(seedbyte!(stream))
    first = Parquet.Interval[
        Parquet.Interval(0, 0, marker), Parquet.Interval(1, 2, 3),
        Parquet.Interval(9, 8, 7), Parquet.Interval(0, 1, 0),
    ]
    second = Union{Missing,Parquet.Interval}[
        missing, Parquet.Interval(marker, 0, 1), missing, Parquet.Interval(4, 5, 6),
    ]
    unknown = Missing[missing, missing, missing, missing]
    return (; first, second, unknown)
end

function _disabledtable(seed::Int64)
    stream = SeedStream(seed)
    number = Union{Missing,Int32}[
        Int32(seedbyte!(stream)), missing, -3, 7, 0, 99]
    text = Union{Missing,String}["left", missing, "right", "", "middle", "tail"]
    return (; number, text)
end

function _oversizedtable(seed::Int64)
    stream = SeedStream(seed)
    low = String(vcat(UInt8['a'], fill(UInt8('x'), 4095), seedbyte!(stream) & 0x0f | 0x30))
    high = String(vcat(UInt8['z'], fill(UInt8('y'), 4095), seedbyte!(stream) & 0x0f | 0x40))
    return (value=String[low, high, low],)
end

function _nestedtable(seed::Int64)
    stream = SeedStream(seed)
    element = Union{Missing,Int32}
    rowtype = NamedTuple{(:id,:items),Tuple{Int32,Union{Missing,Vector{element}}}}
    rows = Union{Missing,rowtype}[
        missing,
        rowtype((Int32(seedbyte!(stream)), missing)),
        rowtype((Int32(2), element[])),
        rowtype((Int32(3), element[missing, -1, 8])),
        rowtype((Int32(4), element[9])),
        rowtype((Int32(5), element[missing, 10])),
    ]
    tag = Union{Missing,String}[missing, "a", "b", "c", missing, "z"]
    flag = Bool[false, true, false, true, true, false]
    return (; tag, rows, flag)
end

function _untrustedtable(seed::Int64)
    stream = SeedStream(seed)
    binary = Union{Missing,String}[
        "z", missing, "a", "middle", "tail", string("seed-", seedbyte!(stream))]
    signed = Union{Missing,Int32}[-10, missing, 9, 100, -99, Int32(seedbyte!(stream))]
    return (; binary, signed)
end

function _nopruningtable(seed::Int64)
    stream = SeedStream(seed)
    marker = Int(seedbyte!(stream))
    json = Union{Missing,Parquet.JSONValue}[
        Parquet.JSONValue(codeunits("{\"value\":$marker}")),
        missing,
        Parquet.JSONValue(codeunits("[1,2,3]")),
        Parquet.JSONValue(codeunits("{\"nested\":true}")),
        Parquet.JSONValue(codeunits("null")),
        Parquet.JSONValue(codeunits("\"tail\"")),
    ]
    return (; json)
end

function _columnmetadata(metadata::MD.ColumnMetaData,
        statistics::Union{Nothing,MD.Statistics})
    return MD.ColumnMetaData(
        type_=metadata.type_, encodings=metadata.encodings,
        path_in_schema=metadata.path_in_schema, codec=metadata.codec,
        num_values=metadata.num_values,
        total_uncompressed_size=metadata.total_uncompressed_size,
        total_compressed_size=metadata.total_compressed_size,
        key_value_metadata=metadata.key_value_metadata,
        data_page_offset=metadata.data_page_offset,
        index_page_offset=metadata.index_page_offset,
        dictionary_page_offset=metadata.dictionary_page_offset,
        statistics=statistics, encoding_stats=metadata.encoding_stats,
        bloom_filter_offset=metadata.bloom_filter_offset,
        bloom_filter_length=metadata.bloom_filter_length,
        size_statistics=metadata.size_statistics,
        geospatial_statistics=metadata.geospatial_statistics,
        unknown_fields=metadata.unknown_fields)
end

function _columnchunk(chunk::MD.ColumnChunk, metadata::MD.ColumnMetaData)
    return MD.ColumnChunk(
        file_path=chunk.file_path, file_offset=chunk.file_offset,
        meta_data=metadata, offset_index_offset=chunk.offset_index_offset,
        offset_index_length=chunk.offset_index_length,
        column_index_offset=chunk.column_index_offset,
        column_index_length=chunk.column_index_length,
        crypto_metadata=chunk.crypto_metadata,
        encrypted_column_metadata=chunk.encrypted_column_metadata,
        unknown_fields=chunk.unknown_fields)
end

function _rowgroup(group::MD.RowGroup, columns::Vector{MD.ColumnChunk})
    return MD.RowGroup(columns=columns, total_byte_size=group.total_byte_size,
        num_rows=group.num_rows, sorting_columns=group.sorting_columns,
        file_offset=group.file_offset,
        total_compressed_size=group.total_compressed_size,
        ordinal=group.ordinal, unknown_fields=group.unknown_fields)
end

function _filemetadata(metadata::MD.FileMetaData,
        rowgroups::Vector{MD.RowGroup}, createdby::Union{Nothing,String},
        orders::Union{Nothing,Vector{MD.ColumnOrder}})
    return MD.FileMetaData(version=metadata.version, schema=metadata.schema,
        num_rows=metadata.num_rows, row_groups=rowgroups,
        key_value_metadata=metadata.key_value_metadata, created_by=createdby,
        column_orders=orders,
        encryption_algorithm=metadata.encryption_algorithm,
        footer_signing_key_metadata=metadata.footer_signing_key_metadata,
        unknown_fields=metadata.unknown_fields)
end

function _decodefooter(bytes::Vector{UInt8})
    file = Parquet.File(bytes)
    try
        metadata = TH.decode(copy(file.footer.bytes), MD.FileMetaData)
        return metadata, Int(file.footer.offset), Int(file.footer.length)
    finally
        close(file)
    end
end

function _replacefooter(bytes::Vector{UInt8}, metadata::MD.FileMetaData)
    _, offset, _ = _decodefooter(bytes)
    prefix = copy(@view bytes[1:offset])
    footer = TH.encode(metadata)
    length(footer) <= typemax(UInt32) || throw(ArgumentError(
        "generated footer exceeds UInt32"))
    output = vcat(prefix, footer)
    Parquet._writelittle!(output, UInt32(length(footer)))
    append!(output, Parquet.PARQUET_MAGIC)
    length(output) <= MAX_GENERATED_BYTES || throw(ArgumentError(
        "mutated output exceeds $MAX_GENERATED_BYTES bytes"))
    return output
end

function _mapstatistics(transform::Function, metadata::MD.FileMetaData)
    groups = MD.RowGroup[]
    sizehint!(groups, length(metadata.row_groups))
    for (groupindex, group) in enumerate(metadata.row_groups)
        columns = MD.ColumnChunk[]
        sizehint!(columns, length(group.columns))
        for (leafindex, chunk) in enumerate(group.columns)
            column = something(chunk.meta_data)
            statistics = transform(column.statistics, groupindex, leafindex)
            push!(columns, _columnchunk(chunk,
                _columnmetadata(column, statistics)))
        end
        push!(groups, _rowgroup(group, columns))
    end
    return groups
end

function _deprecatedstatistics(statistics::Union{Nothing,MD.Statistics})
    statistics === nothing && return nothing
    return MD.Statistics(max=statistics.max_value, min=statistics.min_value,
        null_count=statistics.null_count,
        distinct_count=statistics.distinct_count,
        nan_count=statistics.nan_count,
        unknown_fields=statistics.unknown_fields)
end

function _untrustedmutation(bytes::Vector{UInt8}, declaration)
    metadata, _, _ = _decodefooter(bytes)
    groups = _mapstatistics(metadata) do statistics, _, leafindex
        return leafindex == 2 ? _deprecatedstatistics(statistics) : statistics
    end
    createdby = declaration["mutation"]["created_by"]
    updated = _filemetadata(metadata, groups, createdby, metadata.column_orders)
    return _replacefooter(bytes, updated)
end

function _nopruneabsent(metadata::MD.FileMetaData)
    groups = _mapstatistics(metadata) do _, _, _
        return nothing
    end
    return _filemetadata(metadata, groups, metadata.created_by, nothing)
end

function _nopruneoversized(metadata::MD.FileMetaData, bytes::Int64)
    raw = fill(UInt8('x'), Int(bytes))
    groups = _mapstatistics(metadata) do statistics, _, _
        statistics === nothing && throw(AssertionError(
            "no-pruning base statistics are absent"))
        return MD.Statistics(null_count=statistics.null_count,
            distinct_count=statistics.distinct_count,
            min_value=copy(raw), max_value=copy(raw),
            is_min_value_exact=true, is_max_value_exact=true,
            nan_count=statistics.nan_count,
            unknown_fields=statistics.unknown_fields)
    end
    return _filemetadata(metadata, groups, metadata.created_by,
        metadata.column_orders)
end

function _nopruneinvalid(metadata::MD.FileMetaData, raw::Vector{UInt8})
    groups = _mapstatistics(metadata) do statistics, _, _
        statistics === nothing && throw(AssertionError(
            "no-pruning base statistics are absent"))
        return MD.Statistics(null_count=statistics.null_count,
            distinct_count=statistics.distinct_count,
            min_value=copy(raw), max_value=statistics.max_value,
            is_min_value_exact=true,
            is_max_value_exact=statistics.is_max_value_exact,
            nan_count=statistics.nan_count,
            unknown_fields=statistics.unknown_fields)
    end
    return _filemetadata(metadata, groups, metadata.created_by,
        metadata.column_orders)
end

function _nopruningmutation(bytes::Vector{UInt8}, declaration)
    metadata, _, _ = _decodefooter(bytes)
    mutation = declaration["mutation"]
    state = mutation["statistics_state"]
    updated = if state == "absent"
        _nopruneabsent(metadata)
    elseif state in ("trusted", "producer-untrusted")
        _filemetadata(metadata, metadata.row_groups, mutation["created_by"],
            metadata.column_orders)
    elseif state == "oversized"
        _nopruneoversized(metadata, mutation["bound_bytes"])
    elseif state == "semantically-unusable"
        mutation["field"] == "min_value" || throw(ArgumentError(
            "unsupported no-pruning invalid field"))
        _nopruneinvalid(metadata, hex2bytes(mutation["value_hex"]))
    else
        throw(ArgumentError("unsupported no-pruning state $state"))
    end
    return _replacefooter(bytes, updated)
end

function generateprofile(declaration::Dict{String,Any})
    profile = declaration["generator_profile"]
    seed = declaration["generator_seed"]
    if profile == "writer-type-order-v1"
        table = _typeordertable(seed)
        return ProfileOutput(table, _writerbytes(table; pageversion=:v1), true,
            Int64(4096), nothing)
    elseif profile == "writer-ieee-order-v1"
        table = _ieeetable(seed)
        return ProfileOutput(table, _writerbytes(table; pageversion=:v2,
            rowgroupsize=6), true,
            Int64(4096), nothing)
    elseif profile == "writer-undefined-order-v1"
        table = _undefinedtable(seed)
        return ProfileOutput(table, _writerbytes(table; pageversion=:v1,
            rowgroupsize=4), true, Int64(4096), nothing)
    elseif profile == "writer-statistics-disabled-v1"
        table = _disabledtable(seed)
        enabled = _writerbytes(table; statistics=true, pageversion=:v2)
        disabled = _writerbytes(table; statistics=false, pageversion=:v2)
        return ProfileOutput(table, disabled, false, Int64(4096), enabled)
    elseif profile == "writer-oversized-bounds-v1"
        table = _oversizedtable(seed)
        return ProfileOutput(table, _writerbytes(table; limit=4096,
            pageversion=:v2, rowgroupsize=3), true, Int64(4096), nothing)
    elseif profile == "writer-nested-row-groups-v1"
        table = _nestedtable(seed)
        return ProfileOutput(table, _writerbytes(table; pageversion=:v2), true,
            Int64(4096), nothing)
    elseif profile == "reader-untrusted-producer-v1"
        table = _untrustedtable(seed)
        base = _writerbytes(table; pageversion=:v1)
        return ProfileOutput(table, _untrustedmutation(base, declaration), true,
            Int64(4096), nothing)
    elseif profile == "reader-no-pruning-v1"
        table = _nopruningtable(seed)
        base = _writerbytes(table; pageversion=:v2)
        return ProfileOutput(table, _nopruningmutation(base, declaration), true,
            Int64(4096), nothing)
    end
    throw(ArgumentError("unsupported generator profile $profile"))
end

function _require(condition::Bool, message::AbstractString)
    condition || throw(AssertionError(String(message)))
    return
end

function _enumname(value)
    for (candidate, name) in TH.enumnames(typeof(value))
        candidate == value.value && return String(name)
    end
    throw(ArgumentError("unknown $(typeof(value)) value $(value.value)"))
end

function _logicalname(element::MD.SchemaElement)
    kind = Parquet._logicalkind(element)
    kind === nothing && return "NONE"
    kind isa Parquet._IntegerLogicalKind && return "INTEGER"
    kind isa Parquet._TimeLogicalKind && return "TIME"
    kind isa Parquet._TimestampLogicalKind && return "TIMESTAMP"
    kind === :string && return "STRING"
    kind === :enum && return "ENUM"
    kind === :decimal && return "DECIMAL"
    kind === :date && return "DATE"
    kind === :unknown && return "UNKNOWN"
    kind === :json && return "JSON"
    kind === :bson && return "BSON"
    kind === :uuid && return "UUID"
    kind === :float16 && return "FLOAT16"
    kind === :interval && return "INTERVAL"
    return uppercase(String(kind))
end

function _timeunitname(unit::UInt8)
    unit == Parquet._TEMPORAL_MILLIS && return "MILLIS"
    unit == Parquet._TEMPORAL_MICROS && return "MICROS"
    unit == Parquet._TEMPORAL_NANOS && return "NANOS"
    throw(ArgumentError("unknown temporal unit $unit"))
end

function _geospatialparameters(element::MD.SchemaElement)
    logical = element.logicalType
    logical === nothing && return nothing, nothing
    if logical.GEOMETRY !== nothing
        return logical.GEOMETRY.crs, nothing
    elseif logical.GEOGRAPHY !== nothing
        algorithm = logical.GEOGRAPHY.algorithm
        return logical.GEOGRAPHY.crs,
            algorithm === nothing ? nothing : _enumname(algorithm)
    end
    return nothing, nothing
end

function normalizeleaf(element::MD.SchemaElement)
    kind = Parquet._logicalkind(element)
    logical = _logicalname(element)
    precision = nothing
    scale = nothing
    bitwidth = nothing
    signed = nothing
    timeunit = nothing
    adjusted = nothing
    if kind === :decimal
        decimal = Parquet._decimalparameters(element)
        precision = Int(first(decimal))
        scale = Int(last(decimal))
    elseif kind isa Parquet._IntegerLogicalKind
        bitwidth = Int(kind.bitwidth)
        signed = kind.signed
    elseif kind isa Union{Parquet._TimeLogicalKind,Parquet._TimestampLogicalKind}
        timeunit = _timeunitname(kind.unit)
        adjusted = kind.is_adjusted_to_utc
    end
    crs, algorithm = _geospatialparameters(element)
    return Dict{String,Any}(
        "physical_type" => _enumname(element.type_),
        "logical_type" => logical,
        "converted_type" => element.converted_type === nothing ? nothing :
            _enumname(element.converted_type),
        "type_length" => element.type_length === nothing ? nothing :
            Int(element.type_length),
        "precision" => precision,
        "scale" => scale,
        "bit_width" => bitwidth,
        "is_signed" => signed,
        "time_unit" => timeunit,
        "is_adjusted_to_utc" => adjusted,
        "crs" => crs,
        "geography_algorithm" => algorithm,
    )
end

function normalizeorder(order::Union{Nothing,MD.ColumnOrder})
    order === nothing && return Dict{String,Any}(
        "state" => "ABSENT", "field_id" => nothing,
        "wire_type" => nothing, "header_hex" => nothing)
    encoded = TH.encode(order)
    if order.TYPE_ORDER !== nothing
        _require(encoded == UInt8[0x1c, 0x00, 0x00],
            "TYPE_ORDER does not use the canonical Compact-Thrift bytes")
        return Dict{String,Any}(
            "state" => "TYPE_ORDER", "field_id" => 1,
            "wire_type" => 12, "header_hex" => "1c")
    elseif order.IEEE_754_TOTAL_ORDER !== nothing
        _require(encoded == UInt8[0x2c, 0x00, 0x00],
            "IEEE_754_TOTAL_ORDER does not use the canonical Compact-Thrift bytes")
        return Dict{String,Any}(
            "state" => "IEEE_754_TOTAL_ORDER", "field_id" => 2,
            "wire_type" => 12, "header_hex" => "2c")
    end
    throw(AssertionError("generated ColumnOrder has no reviewed union member"))
end

function _nullableinteger(value::Union{Nothing,Integer})
    return value === nothing ? nothing : string(Int64(value))
end

function _nullablehex(value::Union{Nothing,AbstractVector{UInt8}})
    return value === nothing ? nothing : bytes2hex(value)
end

function normalizecolumn(caseid::String, relative::String, rowgroup::Int,
        leafindex::Int, node::Parquet.SchemaNode, column::MD.ColumnMetaData,
        order::Union{Nothing,MD.ColumnOrder})
    statistics = column.statistics
    unknown = statistics === nothing ? Int[] :
        sort!(unique!(Int[field.id for field in statistics.unknown_fields]))
    return Dict{String,Any}(
        "record" => "column_statistics",
        "schema_version" => 2,
        "case_id" => caseid,
        "file" => relative,
        "row_group" => rowgroup - 1,
        "leaf" => leafindex - 1,
        "path" => copy(node.path),
        "leaf_schema" => normalizeleaf(node.element),
        "column_order" => normalizeorder(order),
        "num_values" => string(column.num_values),
        "has_statistics" => statistics !== nothing,
        "deprecated_min_hex" => statistics === nothing ? nothing :
            _nullablehex(statistics.min),
        "deprecated_max_hex" => statistics === nothing ? nothing :
            _nullablehex(statistics.max),
        "min_value_hex" => statistics === nothing ? nothing :
            _nullablehex(statistics.min_value),
        "max_value_hex" => statistics === nothing ? nothing :
            _nullablehex(statistics.max_value),
        "is_min_value_exact" => statistics === nothing ? nothing :
            statistics.is_min_value_exact,
        "is_max_value_exact" => statistics === nothing ? nothing :
            statistics.is_max_value_exact,
        "null_count" => statistics === nothing ? nothing :
            _nullableinteger(statistics.null_count),
        "distinct_count" => statistics === nothing ? nothing :
            _nullableinteger(statistics.distinct_count),
        "nan_count" => statistics === nothing ? nothing :
            _nullableinteger(statistics.nan_count),
        "unknown_statistics_field_ids" => unknown,
    )
end

const MODEL_PHYSICAL_TYPES = Dict(
    "BOOLEAN" => Model.PHYSICAL_BOOLEAN,
    "INT32" => Model.PHYSICAL_INT32,
    "INT64" => Model.PHYSICAL_INT64,
    "INT96" => Model.PHYSICAL_INT96,
    "FLOAT" => Model.PHYSICAL_FLOAT,
    "DOUBLE" => Model.PHYSICAL_DOUBLE,
    "BYTE_ARRAY" => Model.PHYSICAL_BYTE_ARRAY,
    "FIXED_LEN_BYTE_ARRAY" => Model.PHYSICAL_FIXED_LEN_BYTE_ARRAY,
)

const MODEL_LOGICAL_TYPES = Dict(
    "NONE" => Model.LOGICAL_NONE,
    "STRING" => Model.LOGICAL_STRING,
    "ENUM" => Model.LOGICAL_ENUM,
    "JSON" => Model.LOGICAL_JSON,
    "BSON" => Model.LOGICAL_BSON,
    "UUID" => Model.LOGICAL_UUID,
    "DECIMAL" => Model.LOGICAL_DECIMAL,
    "DATE" => Model.LOGICAL_DATE,
    "TIME" => Model.LOGICAL_TIME,
    "TIMESTAMP" => Model.LOGICAL_TIMESTAMP,
    "FLOAT16" => Model.LOGICAL_FLOAT16,
    "INTERVAL" => Model.LOGICAL_INTERVAL,
    "UNKNOWN" => Model.LOGICAL_UNKNOWN,
    "VARIANT" => Model.LOGICAL_VARIANT,
    "GEOMETRY" => Model.LOGICAL_GEOMETRY,
    "GEOGRAPHY" => Model.LOGICAL_GEOGRAPHY,
    "LIST" => Model.LOGICAL_LIST,
    "MAP" => Model.LOGICAL_MAP,
)

function modelspec(leaf::Dict{String,Any})
    logicalname = leaf["logical_type"]
    logical = if logicalname == "INTEGER"
        leaf["is_signed"] ? Model.LOGICAL_SIGNED_INTEGER :
            Model.LOGICAL_UNSIGNED_INTEGER
    else
        MODEL_LOGICAL_TYPES[logicalname]
    end
    timeunit = leaf["time_unit"] == "MILLIS" ? Model.TIME_MILLIS :
        leaf["time_unit"] == "MICROS" ? Model.TIME_MICROS :
        leaf["time_unit"] == "NANOS" ? Model.TIME_NANOS : nothing
    return Model.LeafSpec(MODEL_PHYSICAL_TYPES[leaf["physical_type"]];
        logical=logical, type_length=leaf["type_length"],
        bit_width=leaf["bit_width"], precision=leaf["precision"],
        time_unit=timeunit)
end

function modelstatistics(statistics::Union{Nothing,MD.Statistics})
    statistics === nothing && return Model.RawStatistics()
    return Model.RawStatistics(
        modern_lower=statistics.min_value === nothing ? nothing :
            copy(statistics.min_value),
        modern_upper=statistics.max_value === nothing ? nothing :
            copy(statistics.max_value),
        deprecated_lower=statistics.min === nothing ? nothing :
            copy(statistics.min),
        deprecated_upper=statistics.max === nothing ? nothing :
            copy(statistics.max),
        null_count=statistics.null_count,
        nan_count=statistics.nan_count,
        distinct_count=statistics.distinct_count,
        lower_exact=statistics.is_min_value_exact,
        upper_exact=statistics.is_max_value_exact,
    )
end

function modelorders(orders::Union{Nothing,Vector{MD.ColumnOrder}})
    orders === nothing && return nothing
    output = Model.DeclaredOrder[]
    sizehint!(output, length(orders))
    for order in orders
        if order.TYPE_ORDER !== nothing
            push!(output, Model.ORDER_TYPE)
        elseif order.IEEE_754_TOTAL_ORDER !== nothing
            push!(output, Model.ORDER_IEEE)
        else
            push!(output, Model.ORDER_FUTURE)
        end
    end
    return output
end

function _modelboundstate(state::Model.BoundState)
    state == Model.BOUND_ABSENT && return :absent
    state == Model.BOUND_UNKNOWN && return :unknown
    state == Model.BOUND_KNOWN && return :known
    throw(ArgumentError("unknown model bound state $state"))
end

function _modelexactness(exactness::Model.Exactness)
    exactness == Model.EXACTNESS_UNKNOWN && return :unknown
    exactness == Model.EXACTNESS_INEXACT && return :inexact
    exactness == Model.EXACTNESS_EXACT && return :exact
    throw(ArgumentError("unknown model exactness $exactness"))
end

function _modelfamily(family::Model.BoundFamily)
    family == Model.FAMILY_NONE && return :none
    family == Model.FAMILY_MODERN && return :modern
    family == Model.FAMILY_DEPRECATED && return :deprecated
    throw(ArgumentError("unknown model family $family"))
end

function _modeltrust(state::Model.TrustState)
    state == Model.TRUST_TRUSTED && return :trusted
    state == Model.TRUST_UNTRUSTED && return :untrusted
    throw(ArgumentError("unknown model trust state $state"))
end

function _modelcomparison(comparison::Model.ComparatorKind)
    comparison == Model.COMPARATOR_SIGNED && return :signed
    comparison == Model.COMPARATOR_UNSIGNED && return :unsigned
    comparison == Model.COMPARATOR_UNSIGNED_BYTES && return :unsigned_bytes
    comparison == Model.COMPARATOR_DECIMAL && return :decimal
    comparison == Model.COMPARATOR_BOOLEAN && return :boolean
    comparison == Model.COMPARATOR_TYPE_FLOAT && return :floating
    comparison == Model.COMPARATOR_IEEE_FLOAT && return :ieee_total_order
    comparison == Model.COMPARATOR_UNDEFINED && return :undefined
    throw(ArgumentError("unknown model comparator $comparison"))
end

function _modeldeclared(order::Union{Nothing,Model.DeclaredOrder})
    order === nothing && return :absent
    order == Model.ORDER_TYPE && return :type_order
    order == Model.ORDER_IEEE && return :ieee_total_order
    order == Model.ORDER_FUTURE && return :unknown
    throw(ArgumentError("unknown model declared order $order"))
end

function _modelordercomparison(spec::Model.LeafSpec,
        order::Union{Nothing,Model.DeclaredOrder})
    order === nothing && return :undefined
    order == Model.ORDER_FUTURE && return :undefined
    order == Model.ORDER_IEEE && return :ieee_total_order
    order == Model.ORDER_TYPE || throw(ArgumentError(
        "unknown model declared order $order"))
    return _modelcomparison(Model._typecomparator(spec))
end

function _modeloccupancy(occupancy::Model.OccupancyState)
    occupancy == Model.OCCUPANCY_UNKNOWN && return :unknown
    occupancy == Model.OCCUPANCY_EMPTY && return :no_non_null
    occupancy == Model.OCCUPANCY_ALL_NAN && return :all_nan
    occupancy == Model.OCCUPANCY_HAS_NON_NAN && return :has_non_nan
    throw(ArgumentError("unknown model occupancy $occupancy"))
end

function _productionboundreason(reason::Symbol)
    reason === :valid && return :known
    reason === :invalid_value && return :invalid_logical
    reason in (:parquet_cpp_pre_1_3, :parquet_mr_pre_1_10) &&
        return :legacy_wrong_order
    reason === :missing_order && return :missing_column_orders
    reason === :unknown_order && return :unknown_column_order
    reason === :undefined_order && return :undefined_type_order
    reason === :no_non_null && return :no_non_null_values
    reason === :ieee_bound_kind && return :ieee_bound_kind_contradiction
    return reason
end

function _productiontrustreason(reason::Symbol)
    reason in (:parquet_cpp_pre_1_3, :parquet_mr_pre_1_10) &&
        return :legacy_wrong_order
    reason === :affected_equal_bounds && return :trusted
    return reason
end

function _modeladjustment(reason::Symbol, name::String)
    reason === :widened_zero || return :none
    name == "lower" && return :negative_zero
    name == "upper" && return :positive_zero
    throw(ArgumentError("unknown statistic bound name $name"))
end

function _comparecount(model::Model.CountFact, production, name::String)
    expectedstate = model.known ? :known : :absent
    _require(production.state == expectedstate,
        "$name state differs from the independent model")
    expected = model.known ? model.value : nothing
    _require(production.value == expected,
        "$name value differs from the independent model")
    return
end

function comparefacts(model::Model.StatisticsResult, production,
        spec::Model.LeafSpec, order::Union{Nothing,Model.DeclaredOrder})
    _require(production.family == _modelfamily(model.family),
        "statistics family differs from the independent model")
    _require(production.order.declared == _modeldeclared(order),
        "statistics declared order differs from the independent model")
    _require(production.order.comparison == _modelordercomparison(spec, order),
        "statistics declared-order comparison differs from the independent model")
    expectedcomparison = _modelcomparison(model.comparator)
    _require(production.comparison == expectedcomparison,
        "statistics effective comparator $(production.comparison) differs from " *
            "the independent model $expectedcomparison")
    _require(production.occupancy == _modeloccupancy(model.occupancy),
        "statistics occupancy differs from the independent model")
    _require(production.trust.state == _modeltrust(model.trust.state),
        "statistics trust differs from the independent model")
    _require(_productiontrustreason(production.trust.reason) == model.trust.reason,
        "statistics trust reason differs from the independent model")
    for (name, modeled, actual) in (("lower", model.lower, production.lower),
            ("upper", model.upper, production.upper))
        _require(actual.state == _modelboundstate(modeled.state),
            "$name bound state differs from the independent model")
        _require(actual.raw == modeled.raw,
            "$name raw bound differs from the independent model")
        _require(actual.exactness == _modelexactness(modeled.exactness),
            "$name exactness differs from the independent model")
        _require(_productionboundreason(actual.reason) == modeled.reason,
            "$name reason differs from the independent model")
        _require(actual.adjustment == _modeladjustment(modeled.reason, name),
            "$name adjustment differs from the independent model")
    end
    _comparecount(model.null_count, production.null_count, "null_count")
    _comparecount(model.nan_count, production.nan_count, "nan_count")
    _comparecount(model.distinct_count, production.distinct_count,
        "distinct_count")
    return
end

function _littlebytes(value::T) where {T<:Unsigned}
    output = Vector{UInt8}(undef, sizeof(T))
    for index in eachindex(output)
        output[index] = UInt8(value & T(0xff))
        value >>= 8
    end
    return output
end

function physicalraw(value, element::MD.SchemaElement)
    physical = element.type_
    physical == MD.Type.BOOLEAN && return UInt8[value ? 0x01 : 0x00]
    physical == MD.Type.INT32 && return _littlebytes(
        reinterpret(UInt32, value::Int32))
    physical == MD.Type.INT64 && return _littlebytes(
        reinterpret(UInt64, value::Int64))
    physical == MD.Type.FLOAT && return _littlebytes(
        reinterpret(UInt32, value::Float32))
    physical == MD.Type.DOUBLE && return _littlebytes(
        reinterpret(UInt64, value::Float64))
    physical == MD.Type.INT96 && return Vector{UInt8}(value)
    physical in (MD.Type.BYTE_ARRAY, MD.Type.FIXED_LEN_BYTE_ARRAY) &&
        return Vector{UInt8}(value)
    throw(ArgumentError("unsupported physical statistics value $physical"))
end

function _samemodelvalue(left::Model.ModelValue, right::Model.ModelValue)
    typeof(left) === typeof(right) || return false
    left isa Model.SignedValue && return left.value == right.value
    left isa Model.UnsignedValue && return left.value == right.value
    left isa Model.BooleanValue && return left.value == right.value
    left isa Model.ByteValue && return left.value == right.value
    left isa Model.DecimalValue && return left.value == right.value
    left isa Model.FloatValue && return left.width == right.width &&
        left.bits == right.bits
    throw(ArgumentError("unsupported independent-model value $(typeof(left))"))
end

function _summarycomparator(spec::Model.LeafSpec,
        order::Union{Nothing,MD.ColumnOrder})
    order === nothing && return Model.COMPARATOR_UNDEFINED
    order.IEEE_754_TOTAL_ORDER !== nothing && return Model.COMPARATOR_IEEE_FLOAT
    order.TYPE_ORDER !== nothing && return Model._typecomparator(spec)
    return Model.COMPARATOR_UNDEFINED
end

function _checksummary(caseid::String, spec::Model.LeafSpec,
        stream::Parquet.LeafStream, element::MD.SchemaElement,
        order::Union{Nothing,MD.ColumnOrder}, result::Model.StatisticsResult,
        statistics::Union{Nothing,MD.Statistics}, limit::Int64)
    rawvalues = Vector{UInt8}[physicalraw(value, element) for value in stream.values]
    comparator = _summarycomparator(spec, order)
    comparator == Model.COMPARATOR_UNDEFINED && return
    summary = Model.summarize_raw_values(spec, rawvalues, comparator)
    if statistics !== nothing && statistics.nan_count !== nothing
        _require(summary.nan_count == statistics.nan_count,
            "nan_count differs from the independent raw-value summary")
    end
    startswith(caseid, "julia-reader-no-pruning-") && return
    lowerraw = result.lower.raw
    upperraw = result.upper.raw
    if lowerraw === nothing || upperraw === nothing
        _require(lowerraw === nothing && upperraw === nothing,
            "writer emitted only one statistics bound")
        if caseid == "julia-writer-oversized-bounds"
            _require(summary.lower isa Model.ByteValue &&
                summary.upper isa Model.ByteValue,
                "oversized variable summary has the wrong model type")
            _require(!Model.writer_bounds_allowed(summary.lower.value,
                summary.upper.value, Model.ModelLimits(
                    max_statistics_value_bytes=limit)),
                "one-byte-over summary unexpectedly fits the statistics limit")
        end
        return
    end
    expectedlower = Model._summaryvalue(lowerraw, spec)
    expectedupper = Model._summaryvalue(upperraw, spec)
    _require(summary.lower !== nothing && summary.upper !== nothing,
        "emitted bounds exist for an empty independent summary")
    _require(_samemodelvalue(summary.lower, expectedlower),
        "emitted lower bound is not the independent raw-value extremum")
    _require(_samemodelvalue(summary.upper, expectedupper),
        "emitted upper bound is not the independent raw-value extremum")
    values = Model.ModelValue[Model._summaryvalue(raw, spec) for raw in rawvalues]
    hasnonnull = any(value -> !(value isa Model.FloatValue) ||
        !Model.float_isnan(value), values)
    for value in values
        candidate = !(value isa Model.FloatValue) ||
            !Model.float_isnan(value) ||
            (comparator == Model.COMPARATOR_IEEE_FLOAT && !hasnonnull)
        candidate || continue
        _require(Model.contains_value(summary, value, comparator),
            "independent extrema do not contain a contributing value")
    end
    return
end

function _assertpageboundaries(file::Parquet.File, metadata::MD.FileMetaData)
    for group in metadata.row_groups
        for chunk in group.columns
            _require(chunk.column_index_offset === nothing &&
                chunk.column_index_length === nothing,
                "generated N6 fixture contains ColumnIndex content")
            column = something(chunk.meta_data)
            start, stop = Parquet._chunkrange(column, file.footer.offset)
            position = start
            while position < stop
                frame = Parquet.readpage(file.source, position, stop,
                    Parquet.Limits())
                kind = Parquet.pagekind(frame)
                if kind === :data_v1
                    _require(frame.header.data_page_header.statistics === nothing,
                        "generated V1 page header contains statistics")
                elseif kind === :data_v2
                    _require(frame.header.data_page_header_v2.statistics === nothing,
                        "generated V2 page header contains statistics")
                end
                next = Parquet.pageend(frame)
                _require(next > position,
                    "generated page walk did not advance")
                position = next
            end
            _require(position == stop,
                "generated page walk missed the column boundary")
        end
    end
    return
end

function _bodybytes(bytes::Vector{UInt8})
    _, offset, _ = _decodefooter(bytes)
    return copy(@view bytes[1:offset])
end

function _materializedrows(bytes::Vector{UInt8})
    table = Parquet.Table(bytes)
    try
        return logicalrows(table.columns)
    finally
        close(table)
    end
end

function _tracedmaterialization(bytes::Vector{UInt8})
    source = TraceSource(bytes, Tuple{Int64,Int64}[])
    file = Parquet.File(source)
    footerstart = file.footer.offset
    try
        TH.decode(copy(file.footer.bytes), MD.FileMetaData)
    finally
        close(file)
    end
    empty!(source.reads)
    table = Parquet.Table(source)
    rows = try
        logicalrows(table.columns)
    finally
        close(table)
    end
    trace = IOBuffer()
    count = 0
    for (offset, length_) in source.reads
        readstop = Base.checked_add(offset, length_)
        clippedstart = max(offset, Int64(4))
        clippedstop = min(readstop, footerstart)
        clippedstop > clippedstart || continue
        write(trace, "offset=", string(clippedstart), ";length=",
            string(clippedstop - clippedstart), "\n")
        count += 1
    end
    tracehash = bytehash(take!(trace))
    bodyhash = bytehash(@view bytes[1:Int(footerstart)])
    logicalhash = canonicalhash(rows)
    contract = "body_sha256=$bodyhash\n" *
        "logical_values_sha256=$logicalhash\n" *
        "range_trace_sha256=$tracehash\n" *
        "read_count=$count\n"
    return bytehash(codeunits(contract)), bodyhash, logicalhash, tracehash, count
end

function _createdbyrecord(metadata::MD.FileMetaData)
    return metadata.created_by !== nothing, metadata.created_by
end

function _filerecord(declaration::Dict{String,Any}, bytes::Vector{UInt8},
        metadata::MD.FileMetaData, footerlength::Int64, leafcount::Int)
    present, createdby = _createdbyrecord(metadata)
    orders = metadata.column_orders
    return Dict{String,Any}(
        "record" => "file",
        "schema_version" => 2,
        "case_id" => declaration["id"],
        "file" => declaration["output_file"],
        "sha256" => bytehash(bytes),
        "size" => length(bytes),
        "footer_length" => Int(footerlength),
        "row_group_count" => length(metadata.row_groups),
        "leaf_count" => leafcount,
        "column_order_count" => orders === nothing ? nothing : length(orders),
        "created_by_present" => present,
        "created_by" => createdby,
    )
end

function _orderstate(order::Union{Nothing,MD.ColumnOrder})
    order === nothing && return :absent
    order.TYPE_ORDER !== nothing && return :type
    order.IEEE_754_TOTAL_ORDER !== nothing && return :ieee
    return :future
end

function _assertmodernstatistics(statistics::MD.Statistics)
    _require(statistics.min === nothing && statistics.max === nothing,
        "Julia writer emitted deprecated min/max statistics")
    if statistics.min_value === nothing || statistics.max_value === nothing
        _require(statistics.min_value === nothing && statistics.max_value === nothing,
            "Julia writer emitted only one modern bound")
        _require(statistics.is_min_value_exact === nothing &&
            statistics.is_max_value_exact === nothing,
            "omitted modern bounds retain exactness flags")
    else
        _require(statistics.is_min_value_exact === true &&
            statistics.is_max_value_exact === true,
            "Julia writer modern bounds are not marked exact")
    end
    return
end

function _floatlogicalbits(value::Float16)
    return UInt64(reinterpret(UInt16, value))
end

function _floatlogicalbits(value::Float32)
    return UInt64(reinterpret(UInt32, value))
end

function _floatlogicalbits(value::Float64)
    return reinterpret(UInt64, value)
end

function _assertieeeprofile(profile::ProfileOutput,
        interpretations::Vector{Any})
    for column in values(profile.table)
        mixed = collect(skipmissing(column[1:6]))
        allnan = collect(skipmissing(column[7:12]))
        _require(count(ismissing, column[1:6]) == 1,
            "IEEE mixed group does not contain one null")
        _require(any(value -> iszero(value) && signbit(value), mixed) &&
            any(value -> iszero(value) && !signbit(value), mixed),
            "IEEE mixed group does not contain both signed zeros")
        _require(any(value -> isfinite(value) && !iszero(value), mixed),
            "IEEE mixed group does not contain a finite nonzero value")
        _require(any(isinf, mixed) && any(isnan, mixed),
            "IEEE mixed group does not contain infinity and NaN")
        _require(length(allnan) == 6 && all(isnan, allnan),
            "IEEE all-NaN group contains a non-NaN value")
        _require(length(unique(_floatlogicalbits.(allnan))) == 6,
            "IEEE all-NaN group lacks distinct raw payloads")
    end
    for item in interpretations
        expectednulls = item.group == 1 ? Int64(1) : Int64(0)
        expectednans = item.group == 1 ? Int64(1) : Int64(6)
        _require(item.statistics.null_count == expectednulls &&
            item.statistics.nan_count == expectednans,
            "IEEE group count state differs from its profile")
    end
    return
end

function _profileassertions(declaration::Dict{String,Any},
        profile::ProfileOutput, metadata::MD.FileMetaData,
        interpretations::Vector{Any})
    caseid = declaration["id"]
    orders = metadata.column_orders
    states = orders === nothing ? Symbol[] : _orderstate.(orders)
    statistics = Any[item.statistics for item in interpretations]
    if startswith(caseid, "julia-writer-") && profile.statistics_enabled
        _require(orders !== nothing && length(orders) == declaration["leaf_count"],
            "statistics-enabled writer output lacks leaf-aligned column_orders")
        for value in statistics
            value === nothing || _assertmodernstatistics(value)
        end
    end
    if caseid == "julia-writer-type-order"
        _require(all(==( :type), states),
            "type-order fixture has a non-TYPE_ORDER leaf")
    elseif caseid == "julia-writer-ieee-order"
        _require(all(==( :ieee), states),
            "IEEE fixture has a non-IEEE leaf")
        _require(all(item -> item.statistics.nan_count !== nothing,
            interpretations), "IEEE fixture omits nan_count")
        _assertieeeprofile(profile, interpretations)
    elseif caseid == "julia-writer-undefined-order"
        _require(all(==( :type), states),
            "undefined-order fixture lacks TYPE_ORDER placeholders")
        _require(all(item -> item.statistics !== nothing &&
            item.statistics.min_value === nothing &&
            item.statistics.max_value === nothing, interpretations),
            "undefined-order fixture contains a bound")
    elseif caseid == "julia-writer-statistics-disabled"
        _require(orders === nothing,
            "statistics=false emitted column_orders")
        _require(all(isnothing, statistics),
            "statistics=false emitted row-group statistics")
        comparison = something(profile.comparison_bytes)
        _require(_bodybytes(profile.bytes) == _bodybytes(comparison),
            "statistics=false changed pre-footer bytes")
    elseif caseid == "julia-writer-oversized-bounds"
        _require(states == [:type],
            "oversized fixture lacks TYPE_ORDER")
        _require(all(item -> item.statistics !== nothing &&
            item.statistics.null_count !== nothing &&
            item.statistics.min_value === nothing &&
            item.statistics.max_value === nothing, interpretations),
            "oversized fixture did not retain counts while omitting both bounds")
    elseif caseid == "julia-writer-nested-row-groups"
        _require(all(==( :type), states),
            "nested fixture has a non-TYPE_ORDER leaf")
        _require(any(item -> item.column.num_values !=
            metadata.row_groups[item.group].num_rows, interpretations),
            "nested fixture does not prove leaf-entry counts")
    elseif caseid == "julia-reader-untrusted-producer"
        for item in interpretations
            if item.leaf == 1
                _require(item.model.trust.state == Model.TRUST_UNTRUSTED,
                    "PARQUET-251 string bounds were not suppressed")
                _require(item.model.lower.state == Model.BOUND_UNKNOWN &&
                    item.model.upper.state == Model.BOUND_UNKNOWN,
                    "untrusted string bounds remained known")
            else
                _require(item.model.family == Model.FAMILY_DEPRECATED,
                    "legacy signed fixture did not select deprecated bounds")
                _require(item.model.trust.state == Model.TRUST_TRUSTED,
                    "valid deprecated signed bounds became untrusted")
            end
        end
    elseif caseid == "julia-reader-no-pruning-absent"
        _require(orders === nothing && all(isnothing, statistics),
            "absent no-pruning fixture retains statistics metadata")
    elseif caseid == "julia-reader-no-pruning-trusted"
        _require(all(item -> item.model.trust.state == Model.TRUST_TRUSTED &&
            item.model.lower.state == Model.BOUND_KNOWN &&
            item.model.upper.state == Model.BOUND_KNOWN, interpretations),
            "trusted no-pruning fixture lacks known bounds")
    elseif caseid == "julia-reader-no-pruning-untrusted"
        _require(all(item -> item.model.trust.state == Model.TRUST_UNTRUSTED &&
            item.model.lower.state == Model.BOUND_UNKNOWN &&
            item.model.upper.state == Model.BOUND_UNKNOWN, interpretations),
            "producer-untrusted no-pruning fixture retained known bounds")
    elseif caseid == "julia-reader-no-pruning-oversized"
        _require(all(item -> item.model.lower.state == Model.BOUND_UNKNOWN &&
            item.model.upper.state == Model.BOUND_UNKNOWN, interpretations),
            "oversized no-pruning bounds remained known")
    elseif caseid == "julia-reader-no-pruning-invalid"
        _require(all(item -> item.model.lower.state == Model.BOUND_UNKNOWN,
            interpretations),
            "semantically invalid JSON lower bound remained known")
    end
    families = String[string(item.production.family) for item in interpretations]
    trusts = String[string(item.production.trust.state) for item in interpretations]
    return Dict{String,Any}(
        "body_sha256" => bytehash(_bodybytes(profile.bytes)),
        "column_orders" => String[string(state) for state in states],
        "statistics_families" => families,
        "trust_states" => trusts,
    )
end

function checkcase(declaration::Dict{String,Any}, profile::ProfileOutput)
    bytes = profile.bytes
    length(bytes) <= MAX_GENERATED_BYTES || throw(ArgumentError(
        "generated case exceeds the file-size limit"))
    relative = declaration["output_file"]
    caseid = declaration["id"]
    expectedrows = logicalrows(profile.table)
    expectedcanonical = canonicaljson(expectedrows)
    file = Parquet.File(bytes)
    metadata = nothing
    schema = nothing
    footerbytes = UInt8[]
    footerlength = Int64(0)
    records = Dict{String,Any}[]
    interpretations = []
    try
        footerbytes = copy(file.footer.bytes)
        footerlength = file.footer.length
        metadata = TH.decode(footerbytes, MD.FileMetaData)
        _require(TH.encode(metadata) == footerbytes,
            "decoded generated footer does not round-trip byte-for-byte")
        schema = Parquet.Schema(metadata)
        _require(metadata.num_rows == length(expectedrows),
            "generated file row count differs from its profile")
        _require(length(metadata.row_groups) == declaration["row_group_count"],
            "generated row-group topology differs from fixtures.toml")
        _require(length(schema.leaves) == declaration["leaf_count"],
            "generated leaf topology differs from fixtures.toml")
        _assertpageboundaries(file, metadata)
        orders = metadata.column_orders
        modeledorders = modelorders(orders)
        for (groupindex, group) in enumerate(metadata.row_groups)
            _require(length(group.columns) == length(schema.leaves),
                "generated row group is not leaf aligned")
            for leafindex in eachindex(schema.leaves)
                node = schema.leaves[leafindex]
                chunk = group.columns[leafindex]
                column = something(chunk.meta_data)
                order = orders === nothing ? nothing : orders[leafindex]
                record = normalizecolumn(caseid, relative, groupindex,
                    leafindex, node, column, order)
                push!(records, record)
                spec = modelspec(record["leaf_schema"])
                modeled = Model.interpret_statistics(spec, column.num_values,
                    modelstatistics(column.statistics), modeledorders;
                    leaf_index=leafindex, leaf_count=length(schema.leaves),
                    created_by=metadata.created_by,
                    limits=Model.ModelLimits(max_statistics_value_bytes=
                        profile.statistics_limit))
                production = Parquet._statisticsfacts(schema, leafindex,
                    metadata.created_by, orders, column;
                    limits=Parquet.Limits(max_statistics_value_bytes=
                        profile.statistics_limit))
                modeledorder = modeledorders === nothing ? nothing :
                    modeledorders[leafindex]
                comparefacts(modeled, production, spec, modeledorder)
                stream = Parquet.readleafstream(file, metadata, schema,
                    groupindex, leafindex; expected_rows=group.num_rows)
                _require(length(stream) == column.num_values,
                    "physical leaf entry count differs from num_values")
                if column.statistics !== nothing
                    _require(column.statistics.null_count ==
                        column.num_values - length(stream.values),
                        "null_count differs from entry count minus dense count")
                end
                _checksummary(caseid, spec, stream, node.element, order,
                    modeled, column.statistics, profile.statistics_limit)
                push!(interpretations, (group=groupindex, leaf=leafindex,
                    node=node, column=column, statistics=column.statistics,
                    model=modeled, production=production,
                    dense_count=length(stream.values),
                    entry_count=length(stream)))
            end
        end
    finally
        close(file)
    end
    actualrows = _materializedrows(bytes)
    actualcanonical = canonicaljson(actualrows)
    _require(actualcanonical == expectedcanonical,
        "generated fixture logical values changed after round trip")
    assertions = _profileassertions(declaration, profile, metadata,
        interpretations)
    assertions["logical_values_sha256"] = bytehash(codeunits(actualcanonical))
    assertions["metadata_sha256"] = bytehash(footerbytes)
    no_pruning = nothing
    if !isempty(declaration["comparison_group"])
        no_pruning, bodyhash, logicalhash, tracehash, readcount =
            _tracedmaterialization(bytes)
        _require(logicalhash == assertions["logical_values_sha256"],
            "traced and ordinary logical materialization differ")
        assertions["body_sha256"] = bodyhash
        assertions["range_trace_sha256"] = tracehash
        assertions["read_count"] = readcount
    end
    file_record = _filerecord(declaration, bytes, metadata, footerlength,
        length(schema.leaves))
    return CheckedCase(declaration, bytes, file_record, records,
        assertions["logical_values_sha256"], assertions["metadata_sha256"],
        assertions, no_pruning)
end

function _parquetjlauthority(capabilities::Dict{String,Any})
    for authority in capabilities["authority"]
        authority["id"] == "parquet-jl" && return authority
    end
    throw(ArgumentError("capabilities.toml lacks the parquet-jl authority"))
end

function _reviewedclaimpairs(capabilities::Dict{String,Any})
    authority = _parquetjlauthority(capabilities)
    pairs = Set{Tuple{String,String}}()
    for claim in authority["claim"]
        claim["status"] in ("planned", "verified") || continue
        capability = claim["capability"]
        for caseid in claim["cases"]
            pair = (caseid, capability)
            pair in pairs && throw(ArgumentError(
                "duplicate parquet-jl claim $pair"))
            push!(pairs, pair)
        end
    end
    return authority, pairs
end

function _standarddigest(case::CheckedCase, capability::String)
    observations = Any[
        Dict{String,Any}("file_sha256" => case.file_record["sha256"]),
        Dict{String,Any}("metadata_sha256" => case.metadata_sha256),
        Dict{String,Any}("logical_values_sha256" =>
            case.logical_values_sha256),
        Dict{String,Any}("assertions" => case.assertion_facts),
    ]
    envelope = Dict{String,Any}(
        "capability_id" => capability,
        "case_id" => case.declaration["id"],
        "observations" => observations,
    )
    return canonicalhash(envelope)
end

function _caseresult(case::CheckedCase, capability::String)
    contract = case.declaration["digest_contract"]
    digest = if contract == "n6-no-pruning-trace-sha256-v1"
        something(case.no_pruning_sha256)
    elseif contract == "n6-capability-result-sha256-v1"
        _standarddigest(case, capability)
    else
        throw(ArgumentError("unsupported digest contract $contract"))
    end
    return Dict{String,Any}(
        "record" => "case_result",
        "schema_version" => 2,
        "case_id" => case.declaration["id"],
        "capability_id" => capability,
        "digest_contract" => contract,
        "status" => "PASS",
        "expected_sha256" => digest,
        "actual_sha256" => digest,
        "detail" => "Parquet.jl N6 harness assertion passed",
    )
end

function _runrecord(authority::Dict{String,Any}, inputs::Dict{String,Vector{UInt8}})
    CANONICAL_TOOLCHAIN_SHA256 in authority["toolchain_sha256"] ||
        throw(ArgumentError("Parquet.jl producer descriptor is not pinned"))
    authority["revision"] == CANONICAL_SOURCE_REVISION ||
        throw(ArgumentError("Parquet.jl source composite is not pinned"))
    return Dict{String,Any}(
        "record" => "run",
        "schema_version" => 2,
        "evidence_id" => "normalized-parquet-jl",
        "producer" => authority["id"],
        "producer_version" => authority["version"],
        "source_revision" => authority["revision"],
        "plan_sha256" => bytehash(inputs[PLAN_FILE]),
        "capabilities_sha256" => bytehash(inputs[CAPABILITIES_FILE]),
        "fixture_manifest_sha256" => bytehash(inputs[FIXTURES_FILE]),
        "corpus_manifest_sha256" => bytehash(inputs[CORPUS_MANIFEST_FILE]),
        "evidence_schema_sha256" => bytehash(inputs[EVIDENCE_SCHEMA_FILE]),
        "toolchain_sha256" => CANONICAL_TOOLCHAIN_SHA256,
        "unsupported_cases" => String[],
    )
end

function _evidencebytes(records::Vector{Dict{String,Any}})
    output = IOBuffer()
    for record in records
        line = canonicaljson(record)
        ncodeunits(line) <= 1024 * 1024 || throw(ArgumentError(
            "normalized evidence line exceeds one MiB"))
        write(output, line, '\n')
    end
    bytes = take!(output)
    length(bytes) <= 32 * 1024 * 1024 || throw(ArgumentError(
        "normalized evidence exceeds 32 MiB"))
    return bytes
end

function _assertcomparisongroup(checked::Vector{CheckedCase})
    grouped = CheckedCase[case for case in checked if
        case.declaration["comparison_group"] == "julia-reader-no-pruning-v1"]
    length(grouped) == 5 || throw(AssertionError(
        "no-pruning comparison group does not have five variants"))
    digests = Set(something(case.no_pruning_sha256) for case in grouped)
    length(digests) == 1 || throw(AssertionError(
        "no-pruning variants have different exact digest contracts"))
    fields = ("body_sha256", "logical_values_sha256",
        "range_trace_sha256", "read_count")
    for field in fields
        values = Set(case.assertion_facts[field] for case in grouped)
        length(values) == 1 || throw(AssertionError(
            "no-pruning variants differ in $field"))
    end
    return
end

function _saferepositoryfile(relative::String)
    isabspath(relative) && throw(ArgumentError(
        "frozen model path is absolute: $relative"))
    occursin('\\', relative) && throw(ArgumentError(
        "frozen model path contains a backslash: $relative"))
    parts = split(relative, '/')
    any(part -> isempty(part) || part in (".", ".."), parts) &&
        throw(ArgumentError("frozen model path is unsafe: $relative"))
    path = normpath(joinpath(REPO_ROOT, parts...))
    startswith(relpath(path, REPO_ROOT), "..") && throw(ArgumentError(
        "frozen model path escapes the repository: $relative"))
    current = REPO_ROOT
    for part in parts
        current = joinpath(current, part)
        islink(current) && throw(ArgumentError(
            "frozen model path contains a symlink: $relative"))
    end
    return path
end

function _checkmodelpins(manifest::Dict{String,Any})
    entries = manifest["frozen_model"]
    isempty(entries) && throw(ArgumentError("manifest has no frozen N6 model"))
    seen = Set{String}()
    for entry in entries
        relative = entry["file"]
        relative in seen && throw(ArgumentError(
            "duplicate frozen model path: $relative"))
        push!(seen, relative)
        path = _saferepositoryfile(relative)
        bytes = _stablefilebytes(path, MAX_DECLARATION_BYTES,
            "frozen model input $relative")
        bytehash(bytes) == entry["sha256"] || throw(ArgumentError(
            "frozen model input digest differs: $relative"))
        path == MODEL_FILE && bytes != FROZEN_MODEL_BYTES && throw(ArgumentError(
            "included N6 model bytes differ from the checked snapshot"))
    end
    modelrelative = replace(relpath(MODEL_FILE, REPO_ROOT),
        Base.Filesystem.path_separator => '/')
    modelrelative in seen || throw(ArgumentError(
        "manifest does not pin the included N6 model source"))
    only(entry for entry in entries if entry["file"] == modelrelative)["sha256"] ==
        FROZEN_MODEL_SHA256 || throw(ArgumentError(
            "manifest model source hash differs from the bootstrap pin"))
    return
end

function _checkedinputs()
    paths = (PLAN_FILE, CAPABILITIES_FILE, FIXTURES_FILE, MANIFEST_FILE,
        EVIDENCE_SCHEMA_FILE, CORPUS_MANIFEST_FILE)
    inputs = Dict{String,Vector{UInt8}}()
    for path in paths
        inputs[path] = _stablefilebytes(path, MAX_DECLARATION_BYTES,
            "required N6 input $path")
    end
    manifest = TOML.parse(String(copy(inputs[MANIFEST_FILE])))
    fixtures = TOML.parse(String(copy(inputs[FIXTURES_FILE])))
    capabilities = TOML.parse(String(copy(inputs[CAPABILITIES_FILE])))
    _checkmodelpins(manifest)
    return inputs, manifest, fixtures, capabilities
end

function buildharness()
    inputs, _, fixtures, capabilities = _checkedinputs()
    _, declarations = checkeddeclarations(fixtures)
    authority, pairs = _reviewedclaimpairs(capabilities)
    checked = CheckedCase[]
    files = Pair{String,Vector{UInt8}}[]
    for declaration in declarations
        profile = generateprofile(declaration)
        case = checkcase(declaration, profile)
        if declaration["output_identity_status"] == "verified"
            _require(case.file_record["sha256"] == declaration["output_sha256"] &&
                case.file_record["size"] == declaration["output_size"],
                "generated identity differs from the verified declaration for " *
                    declaration["id"])
        end
        push!(checked, case)
        push!(files, declaration["output_file"] => case.bytes)
    end
    _assertcomparisongroup(checked)
    records = Dict{String,Any}[_runrecord(authority, inputs)]
    results = 0
    for case in checked
        push!(records, case.file_record)
        append!(records, case.column_records)
        declared = Set(String.(case.declaration["capabilities"]))
        for capability in case.declaration["capabilities"]
            pair = (case.declaration["id"], capability)
            pair in pairs || continue
            capability in declared || throw(AssertionError(
                "reviewed capability is outside the generated declaration"))
            push!(records, _caseresult(case, capability))
            results += 1
        end
    end
    length(checked) == 12 || throw(AssertionError(
        "N6 harness did not build 12 generated cases"))
    sum(length(case.column_records) for case in checked) == 52 ||
        throw(AssertionError("N6 harness did not build 52 column records"))
    results == 31 || throw(AssertionError(
        "N6 harness did not build 31 reviewed case results"))
    length(records) == 96 || throw(AssertionError(
        "N6 harness did not build 96 normalized evidence records"))
    return HarnessOutput(files, _evidencebytes(records), checked)
end

function _safeoutput(relative::String)
    occursin(r"^generated/[A-Za-z0-9._+@=-]+$", relative) ||
        throw(ArgumentError("unsafe generated output path $relative"))
    target = normpath(joinpath(N6_ROOT, split(relative, '/')...))
    dirname(target) == joinpath(N6_ROOT, "generated") ||
        throw(ArgumentError("generated output escapes its declared directory"))
    return target
end

function _safedirectory(path::String, create::Bool)
    parent = dirname(path)
    parent in (joinpath(N6_ROOT, "generated"),
        joinpath(N6_ROOT, "evidence")) || throw(ArgumentError(
        "output directory is outside the N6 harness boundary"))
    islink(parent) && throw(ArgumentError("output directory is a symlink"))
    if ispath(parent)
        isdir(parent) || throw(ArgumentError("output parent is not a directory"))
    elseif create
        mkdir(parent)
    else
        throw(ArgumentError("output directory is absent: $parent"))
    end
    islink(parent) && throw(ArgumentError("output directory became a symlink"))
    return
end

function _preflighttarget(path::String)
    isdir(dirname(path)) || throw(ArgumentError(
        "atomic output parent is not a directory"))
    islink(dirname(path)) && throw(ArgumentError(
        "atomic output parent is a symlink"))
    islink(path) && throw(ArgumentError("refusing to replace symlink output $path"))
    ispath(path) && !isfile(path) && throw(ArgumentError(
        "refusing to replace non-file output $path"))
    return
end

function _fsyncdescriptor(descriptor, label::String)
    Sys.isunix() || throw(ArgumentError(
        "durable N6 output publication requires a POSIX host"))
    errorcode = ccall(:fsync, Cint, (Cint,), descriptor)
    iszero(errorcode) || throw(SystemError("fsync $label", Libc.errno()))
    return
end

function _fsyncdirectory(path::String)
    Sys.isunix() || throw(ArgumentError(
        "durable N6 output publication requires a POSIX host"))
    descriptor = ccall(:open, Cint, (Cstring, Cint), path, 0)
    descriptor >= 0 || throw(SystemError(
        "open output directory $(repr(path))", Libc.errno()))
    try
        _fsyncdescriptor(descriptor, "output directory $(repr(path))")
    finally
        ccall(:close, Cint, (Cint,), descriptor)
    end
    return
end

function _renamefile(source::String, target::String)
    errorcode = ccall(:jl_fs_rename, Int32, (Cstring, Cstring), source, target)
    iszero(errorcode) || throw(SystemError(
        "atomic rename $(repr(source)) to $(repr(target))", errorcode))
    return
end

function _stagebytes(path::String, bytes::Vector{UInt8})
    temporary, stream = mktemp(dirname(path); cleanup=false)
    staged = false
    try
        write(stream, bytes)
        flush(stream)
        chmod(temporary, 0o644)
        _fsyncdescriptor(fd(stream), "staged output $(repr(path))")
        close(stream)
        staged = true
        return temporary
    finally
        isopen(stream) && close(stream)
        !staged && ispath(temporary) && rm(temporary; force=true)
    end
end

function _reservedtemppath(parent::String)
    path, stream = mktemp(parent; cleanup=false)
    close(stream)
    return path
end

function _renameoverreserved(source::String, parent::String;
        renamefile=_renamefile)
    reserved = _reservedtemppath(parent)
    renamed = false
    try
        renamefile(source, reserved)
        renamed = true
        return reserved
    finally
        !renamed && ispath(reserved) && rm(reserved; force=true)
    end
end

function _rollbackbatch!(installed::Vector{String},
        backups::Dict{String,Union{Nothing,String}}, syncdirectory)
    for path in Iterators.reverse(installed)
        replacement = _renameoverreserved(path, dirname(path))
        backup = backups[path]
        backup === nothing || _renamefile(backup, path)
        backups[path] = nothing
        syncdirectory(dirname(path))
        rm(replacement; force=true)
        syncdirectory(dirname(path))
    end
    installedset = Set(installed)
    for (path, backup) in backups
        path in installedset && continue
        backup === nothing && continue
        ispath(path) && throw(ErrorException(
            "cannot restore atomic output backup over an existing path"))
        _renamefile(backup, path)
        backups[path] = nothing
        syncdirectory(dirname(path))
    end
    return
end

function _stagetargets(targets::Vector{Pair{String,Vector{UInt8}}})
    stages = Dict{String,String}()
    try
        for (path, bytes) in targets
            stages[path] = _stagebytes(path, bytes)
        end
    catch
        for stage in values(stages)
            ispath(stage) && rm(stage; force=true)
        end
        rethrow()
    end
    return stages
end

function _committarget!(path::String, stages::Dict{String,String},
        backups::Dict{String,Union{Nothing,String}}, installed::Vector{String},
        syncdirectory)
    backup = nothing
    if isfile(path)
        backup = _renameoverreserved(path, dirname(path))
        backups[path] = backup
        syncdirectory(dirname(path))
    end
    haskey(backups, path) || (backups[path] = nothing)
    try
        _renamefile(stages[path], path)
    catch
        if backup !== nothing && !ispath(path)
            _renamefile(backup, path)
            backups[path] = nothing
            syncdirectory(dirname(path))
        end
        rethrow()
    end
    delete!(stages, path)
    push!(installed, path)
    syncdirectory(dirname(path))
    return
end

function _cleanupbackups!(backups::Dict{String,Union{Nothing,String}},
        syncdirectory)
    for backup in values(backups)
        backup === nothing && continue
        try
            ispath(backup) && rm(backup; force=true)
            syncdirectory(dirname(backup))
        catch error
            @warn "could not remove committed output backup" backup exception=error
        end
    end
    return
end

function _atomicreplacebatch(targets::Vector{Pair{String,Vector{UInt8}}};
        syncdirectory=_fsyncdirectory)
    paths = String[first(target) for target in targets]
    length(unique(paths)) == length(paths) || throw(ArgumentError(
        "atomic output targets are not unique"))
    foreach(_preflighttarget, paths)
    stages = _stagetargets(targets)
    backups = Dict{String,Union{Nothing,String}}()
    installed = String[]
    try
        for path in paths
            _committarget!(path, stages, backups, installed, syncdirectory)
        end
    catch error
        try
            _rollbackbatch!(installed, backups, syncdirectory)
        catch rollback
            throw(ErrorException("atomic output rollback failed after " *
                "$(sprint(showerror, error)): $(sprint(showerror, rollback))"))
        finally
            for stage in values(stages)
                ispath(stage) && rm(stage; force=true)
            end
        end
        rethrow()
    end
    _cleanupbackups!(backups, syncdirectory)
    return
end

function _atomicreplacebytes(path::String, bytes::Vector{UInt8})
    _atomicreplacebatch(Pair{String,Vector{UInt8}}[path => bytes])
    return
end

function _atomicreplace(path::String, bytes::Vector{UInt8})
    _safedirectory(path, true)
    _atomicreplacebytes(path, bytes)
    return
end

function _checkbytes(path::String, expected::Vector{UInt8})
    _safedirectory(path, false)
    actual = _stablefilebytes(path, Int64(length(expected)),
        "checked output $path")
    actual == expected || throw(AssertionError("stale generated output: $path"))
    return
end

function _producersourcepaths()
    project = joinpath(REPO_ROOT, "Project.toml")
    isfile(project) && !islink(project) || throw(ArgumentError(
        "Parquet.jl Project.toml is not a regular file"))
    root = joinpath(REPO_ROOT, "src")
    isdir(root) && !islink(root) || throw(ArgumentError(
        "Parquet.jl source root is not a directory"))
    paths = String["Project.toml"]
    for (directory, directories, files) in walkdir(root; follow_symlinks=false)
        for name in directories
            path = joinpath(directory, name)
            islink(path) && throw(ArgumentError(
                "Parquet.jl source directory is a symbolic link"))
        end
        for name in files
            path = joinpath(directory, name)
            relative = replace(relpath(path, REPO_ROOT),
                Base.Filesystem.path_separator => '/')
            isfile(path) && !islink(path) || throw(ArgumentError(
                "Parquet.jl source entry is not a regular file: $relative"))
            push!(paths, relative)
        end
    end
    sort!(paths)
    return paths
end

function _producerfilemap()
    files = Dict{String,String}()
    for item in PRODUCER_DESCRIPTOR["file"]
        path = item["path"]
        haskey(files, path) && throw(ArgumentError(
            "duplicate Parquet.jl producer file: $path"))
        files[path] = item["sha256"]
    end
    return files
end

function _producersourcecomposite(paths::Vector{String}, payloads)
    context = SHA.SHA2_256_CTX()
    SHA.update!(context,
        codeunits(PRODUCER_DESCRIPTOR["source_composite_algorithm"] * "\0"))
    total = Int64(0)
    for path in paths
        bytes = payloads[path]
        total = Base.checked_add(total, Int64(length(bytes)))
        SHA.update!(context, codeunits("F\0"))
        SHA.update!(context, codeunits(path))
        SHA.update!(context, codeunits("\0"))
        SHA.update!(context, codeunits(string(length(bytes))))
        SHA.update!(context, codeunits("\0"))
        SHA.update!(context, SHA.sha256(bytes))
    end
    return bytes2hex(SHA.digest!(context)), total
end

function _checkproduceridentity()
    current = _stablefilebytes(PRODUCER_DESCRIPTOR_FILE,
        Int64(4 * 1024 * 1024), "Parquet.jl producer descriptor")
    current == PRODUCER_DESCRIPTOR_BYTES || throw(ArgumentError(
        "Parquet.jl producer descriptor changed after package loading"))
    files = _producerfilemap()
    sourcepaths = _producersourcepaths()
    support = Set(String[PRODUCER_DESCRIPTOR[field] for field in
        ("manifest_file", "harness_file", "runner_file", "bootstrap_file")])
    Set(keys(files)) == union(Set(sourcepaths), support) || throw(ArgumentError(
        "Parquet.jl producer file inventory differs"))
    payloads = Dict{String,Vector{UInt8}}()
    for (relative, digest) in files
        path = joinpath(REPO_ROOT, split(relative, '/')...)
        bytes = _stablefilebytes(path, Int64(64 * 1024 * 1024),
            "Parquet.jl producer file $relative")
        bytehash(bytes) == digest || throw(ArgumentError(
            "Parquet.jl producer file digest differs: $relative"))
        payloads[relative] = bytes
    end
    composite, total = _producersourcecomposite(sourcepaths, payloads)
    composite == CANONICAL_SOURCE_REVISION || throw(ArgumentError(
        "Parquet.jl source composite differs"))
    length(sourcepaths) == PRODUCER_DESCRIPTOR["source_file_count"] ||
        throw(ArgumentError("Parquet.jl source file count differs"))
    total == PRODUCER_DESCRIPTOR["source_total_bytes"] ||
        throw(ArgumentError("Parquet.jl source byte count differs"))
    return
end

function _checkwriteridentity()
    VERSION == CANONICAL_WRITER_VERSION || throw(ArgumentError(
        "N6 evidence writes require Julia $CANONICAL_WRITER_VERSION"))
    _checkproduceridentity()
    executable = String(Base.julia_cmd().exec[1])
    digest = filehash(executable; maximum=MAX_DECLARATION_BYTES,
        label="Julia writer executable")
    digest == CANONICAL_WRITER_EXECUTABLE_SHA256 || throw(ArgumentError(
        "Julia writer executable digest differs from the frozen toolchain"))
    return
end

function runharness(mode::Symbol)
    mode in (:write, :check) || throw(ArgumentError(
        "N6 harness mode must be :write or :check"))
    mode === :write && _checkwriteridentity()
    output = buildharness()
    targets = Pair{String,Vector{UInt8}}[
        _safeoutput(relative) => bytes for (relative, bytes) in output.files]
    push!(targets, EVIDENCE_FILE => output.evidence)
    if mode === :write
        for (path, _) in targets
            _safedirectory(path, true)
        end
        _atomicreplacebatch(targets)
    else
        for (path, bytes) in targets
            _checkbytes(path, bytes)
        end
    end
    return output
end

end
