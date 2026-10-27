"""
    Parquet.FormatError(message)

The input is not a valid Parquet file: corrupt or inconsistent metadata, a page that
does not decode, or a checksum mismatch. `message` says what is wrong.
"""
struct FormatError <: Exception
    message::String
end

function Base.showerror(io::IO, err::FormatError)
    print(io, "invalid Parquet file: ", err.message)
    return
end

"""
    Parquet.UnsupportedFeatureError(message)

The file uses something this package does not read, such as encryption or LZO
compression, or holds a value the requested Julia type cannot represent. `message`
names it and, where one exists, the way to read the file.
"""
struct UnsupportedFeatureError <: Exception
    message::String
end

function Base.showerror(io::IO, err::UnsupportedFeatureError)
    print(io, "unsupported Parquet feature: ", err.message)
    return
end

"""
    Parquet.LimitError(resource, requested, maximum)

A size or count exceeded a cap in [`Parquet.Limits`](@ref). `resource` names the
quantity (for example `:page_bytes` for `max_page_bytes`); `requested` is the size
the file asked for and `maximum` the cap. Raise the cap to read the file.
"""
struct LimitError <: Exception
    resource::Symbol
    requested::Int64
    maximum::Int64
end

function Base.showerror(io::IO, err::LimitError)
    print(io, "Parquet ", err.resource, " limit exceeded: requested ", err.requested,
        ", maximum ", err.maximum)
    return
end

"""
    Parquet.Limits(; kwargs...)

Resource bounds for reading and writing. Each field caps one quantity; exceeding it
throws a `LimitError`.

- `max_footer_bytes` (64 MiB), `max_page_header_bytes` (16 MiB), `max_page_bytes`
  (1 GiB), `max_page_index_bytes` (64 MiB): sizes of single metadata and page frames.
- `max_statistics_value_bytes` (4096): one written minimum or maximum.
- `max_string_bytes` (256 MiB), `max_decimal_bytes` (1 MiB): one value.
- `max_schema_name_bytes` (1 MiB): new column names one operation may intern.
- `max_metadata_depth` (128): schema and nested-value depth.
- `max_materialized_bytes` (unlimited): optional cap on the bytes one read allocates.
  Writes are not capped.
- `max_container_elements` (unlimited): optional cap on row and element counts.

Counts a file declares (values, rows, list and map lengths) are checked against the
bytes that back them before memory is reserved for them, and declared page sizes are
bounded by `max_page_bytes`, so corrupt metadata fails fast without the two caps.
"""
Base.@kwdef struct Limits
    max_footer_bytes::Int64 = 64 * 1024 * 1024
    max_page_header_bytes::Int64 = 16 * 1024 * 1024
    max_page_bytes::Int64 = 1024 * 1024 * 1024
    max_page_index_bytes::Int64 = 64 * 1024 * 1024
    max_statistics_value_bytes::Int64 = 4096
    max_materialized_bytes::Int64 = typemax(Int64)
    max_schema_name_bytes::Int64 = 1024 * 1024
    max_string_bytes::Int64 = 256 * 1024 * 1024
    max_decimal_bytes::Int64 = 1024 * 1024
    max_container_elements::Int64 = typemax(Int64)
    max_metadata_depth::Int = 128
end

# Bytes a read has decoded, against `max_materialized_bytes`: the file metadata, the
# pages it decompresses, the values it decodes and assembles, and the copy of an `IO`
# source. Charges accumulate and are never released; with no cap nothing is counted.
# Tasks of one read share the budget.
mutable struct _ByteBudget
    const maximum::Int64
    @atomic used::Int64
end

function _ByteBudget(limits::Limits)
    return _ByteBudget(limits.max_materialized_bytes, Int64(0))
end

# Charges to this budget return at once, so tasks can share it.
const _UNCAPPED = _ByteBudget(typemax(Int64), Int64(0))

function _budgetrequest(current::Int64, bytes::Integer, maximum::Int64,
    resource::Symbol)
    bytes >= 0 || throw(ArgumentError("cannot reserve a negative byte count"))
    requested = Int128(current) + Int128(bytes)
    requested <= maximum || throw(LimitError(resource,
        Int64(min(requested, typemax(Int64))), maximum))
    return Int64(requested)
end

function _charge!(budget::_ByteBudget, bytes::Integer)
    maximum = budget.maximum
    maximum == typemax(Int64) && return
    bytes >= 0 || throw(ArgumentError("cannot reserve a negative byte count"))
    # One charge never exceeds the cap, so the sum cannot overflow before it throws.
    bytes <= maximum || throw(LimitError(:materialized_bytes,
        Int64(min(bytes, typemax(Int64))), maximum))
    used = @atomic budget.used += Int64(bytes)
    used <= maximum || throw(LimitError(:materialized_bytes, used, maximum))
    return
end

function _chargearray!(budget::_ByteBudget, ::Type{T}, count::Integer) where {T}
    _charge!(budget, Int128(count) * Base.elsize(Vector{T}))
    return
end

struct _SchemaNameState
    names::Set{String}
    bytes::Int64
end

mutable struct _SchemaNameRegistry
    state::_SchemaNameState
    lock::ReentrantLock
end

const _SCHEMA_NAME_REGISTRY = _SchemaNameRegistry(
    _SchemaNameState(Set{String}(), Int64(0)), ReentrantLock())

function _schemanamecharge(name::String, maximum::Int64)
    return _budgetrequest(Int64(256), Int128(2) * ncodeunits(name), maximum,
        :schema_name_bytes)
end

function _validatetablenames(names::AbstractVector{String})
    seen = Set{String}()
    for name in names
        occursin('\0', name) && throw(UnsupportedFeatureError(
            "Parquet.Table cannot represent a top-level field name containing NUL; " *
            "use Parquet.File for low-level access"))
        name in seen && throw(UnsupportedFeatureError(
            "Parquet.Table requires unique top-level field names; " *
            "use Parquet.File for low-level access"))
        push!(seen, name)
    end
    return
end

# Interns validated names as Symbols. `max_schema_name_bytes` bounds the new name bytes
# ONE operation may intern, not the process-global total: interned Symbols are immortal,
# so charging every operation against a shared lifetime cap would let a single hostile
# file exhaust it and deny every later file that carries any not-yet-interned name. The
# registry still tracks the global total as an observability metric.
function _internschemanames(names::AbstractVector{String}, limits::Limits)
    _validatetablenames(names)
    registry = _SCHEMA_NAME_REGISTRY
    lock(registry.lock)
    try
        state = registry.state
        charge = Int64(0)
        for name in names
            name in state.names && continue
            charge = _budgetrequest(charge,
                _schemanamecharge(name, limits.max_schema_name_bytes),
                limits.max_schema_name_bytes, :schema_name_bytes)
        end
        if !iszero(charge)
            total = _budgetrequest(state.bytes, charge, typemax(Int64),
                :schema_name_bytes)
            registry.state = _SchemaNameState(union(state.names, names), total)
        end
    finally
        unlock(registry.lock)
    end
    return Symbol[Symbol(name) for name in names]
end

function _internedschemanamebytes()
    registry = _SCHEMA_NAME_REGISTRY
    lock(registry.lock)
    try
        return registry.state.bytes
    finally
        unlock(registry.lock)
    end
end

# Elements to reserve up front for a count claimed by file metadata. A claim larger than
# `bytes` of input could plausibly hold is not trusted: callers reserve this bound and
# grow only as decoded data backs the claim, so corrupt counts fail before they allocate.
function _claimcapacity(claim::Integer, bytes::Integer)
    return Int(clamp(Int128(claim), 0, 4096 + 64 * Int128(max(bytes, 0))))
end

function _checklimit(resource::Symbol, requested::Integer, maximum::Integer)
    requested <= maximum && return
    reported = requested > typemax(Int64) ? typemax(Int64) :
        requested < typemin(Int64) ? typemin(Int64) : Int64(requested)
    throw(LimitError(resource, reported, Int64(maximum)))
end
