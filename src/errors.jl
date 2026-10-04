struct FormatError <: Exception
    message::String
end

function Base.showerror(io::IO, err::FormatError)
    print(io, "invalid Parquet file: ", err.message)
    return
end

struct UnsupportedFeatureError <: Exception
    message::String
end

function Base.showerror(io::IO, err::UnsupportedFeatureError)
    print(io, "unsupported Parquet feature: ", err.message)
    return
end

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

Base.@kwdef struct Limits
    max_footer_bytes::Int64 = 64 * 1024 * 1024
    max_page_header_bytes::Int64 = 16 * 1024 * 1024
    max_page_bytes::Int64 = 1024 * 1024 * 1024
    max_page_index_bytes::Int64 = 64 * 1024 * 1024
    max_statistics_value_bytes::Int64 = 4096
    max_materialized_bytes::Int64 = 2 * 1024 * 1024 * 1024
    max_schema_name_bytes::Int64 = 1024 * 1024
    max_string_bytes::Int64 = 256 * 1024 * 1024
    max_decimal_bytes::Int64 = 1024 * 1024
    max_container_elements::Int64 = 100_000_000
    max_metadata_depth::Int = 128
end

mutable struct _LiveByteBudget
    maximum::Int64
    used::Int64
    lock::ReentrantLock
end

const _MATERIALIZED_ARRAY_HEADER_BYTES = Int64(64)
const _MATERIALIZED_OBJECT_BYTES = Int64(128)

function _LiveByteBudget(limits::Limits)
    return _LiveByteBudget(limits.max_materialized_bytes, Int64(0), ReentrantLock())
end

function _budgetrequest(current::Int64, bytes::Integer, maximum::Int64,
    resource::Symbol)
    bytes >= 0 || throw(ArgumentError("cannot reserve a negative byte count"))
    bytes <= typemax(Int64) || throw(LimitError(resource, typemax(Int64), maximum))
    requested = try
        Base.checked_add(current, Int64(bytes))
    catch err
        err isa OverflowError || rethrow()
        throw(LimitError(resource, typemax(Int64), maximum))
    end
    requested <= maximum || throw(LimitError(resource, requested, maximum))
    return requested
end

function _reserve!(budget::_LiveByteBudget, bytes::Integer;
    resource::Symbol=:materialized_bytes)
    lock(budget.lock)
    try
        budget.used = _budgetrequest(budget.used, bytes, budget.maximum, resource)
    finally
        unlock(budget.lock)
    end
    return
end

function _release!(budget::_LiveByteBudget, bytes::Integer)
    bytes >= 0 || throw(ArgumentError("cannot release a negative byte count"))
    bytes <= typemax(Int64) || throw(ArgumentError("released byte count exceeds Int64"))
    lock(budget.lock)
    try
        value = Int64(bytes)
        value <= budget.used || throw(ArgumentError(
            "cannot release $value bytes from a budget using $(budget.used) bytes"))
        budget.used -= value
    finally
        unlock(budget.lock)
    end
    return
end

function _budgetused(budget::_LiveByteBudget)
    lock(budget.lock)
    try
        return budget.used
    finally
        unlock(budget.lock)
    end
end

function _materializedproduct(count::Integer, width::Integer)
    count >= 0 || throw(ArgumentError("materialized element count must be nonnegative"))
    width >= 0 || throw(ArgumentError("materialized element width must be nonnegative"))
    count <= typemax(Int64) && width <= typemax(Int64) ||
        throw(LimitError(:materialized_bytes, typemax(Int64), typemax(Int64)))
    return try
        Base.checked_mul(Int64(count), Int64(width))
    catch err
        err isa OverflowError || rethrow()
        throw(LimitError(:materialized_bytes, typemax(Int64), typemax(Int64)))
    end
end

function _materializedsum(left::Integer, right::Integer)
    left >= 0 && right >= 0 ||
        throw(ArgumentError("materialized byte counts must be nonnegative"))
    left <= typemax(Int64) && right <= typemax(Int64) ||
        throw(LimitError(:materialized_bytes, typemax(Int64), typemax(Int64)))
    return try
        Base.checked_add(Int64(left), Int64(right))
    catch err
        err isa OverflowError || rethrow()
        throw(LimitError(:materialized_bytes, typemax(Int64), typemax(Int64)))
    end
end

function _materializedarraybytes(::Type{T}, count::Integer;
    header::Bool=true) where {T}
    width = Int64(Base.elsize(Vector{T}))
    bytes = _materializedproduct(count, width)
    (Base.isbitsunion(T) || Missing <: T) &&
        (bytes = _materializedsum(bytes, count))
    header && (bytes = _materializedsum(_MATERIALIZED_ARRAY_HEADER_BYTES, bytes))
    return bytes
end

function _materializedbitbytes(count::Integer; header::Bool=true)
    count >= 0 || throw(ArgumentError("materialized bit count must be nonnegative"))
    payload = cld(Int128(count), Int128(8))
    payload <= typemax(Int64) ||
        throw(LimitError(:materialized_bytes, typemax(Int64), typemax(Int64)))
    bytes = Int64(payload)
    header && (bytes = _materializedsum(_MATERIALIZED_ARRAY_HEADER_BYTES, bytes))
    return bytes
end

function _reservearray!(budget::_LiveByteBudget, ::Type{T}, count::Integer;
    header::Bool=true) where {T}
    bytes = _materializedarraybytes(T, count; header=header)
    _reserve!(budget, bytes)
    return bytes
end

function _reservebits!(budget::_LiveByteBudget, count::Integer; header::Bool=true)
    bytes = _materializedbitbytes(count; header=header)
    _reserve!(budget, bytes)
    return bytes
end

function _reserveobjects!(budget::_LiveByteBudget, count::Integer=1)
    bytes = _materializedproduct(count, _MATERIALIZED_OBJECT_BYTES)
    _reserve!(budget, bytes)
    return bytes
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
    bytes = Int64(ncodeunits(name))
    payload = try
        Base.checked_mul(bytes, Int64(2))
    catch err
        err isa OverflowError || rethrow()
        throw(LimitError(:schema_name_bytes, typemax(Int64), maximum))
    end
    return _budgetrequest(Int64(256), payload, maximum, :schema_name_bytes)
end

function _copyvalidatetablenames(names::AbstractVector{String},
    budget::_LiveByteBudget)
    temporary = Int64(0)
    try
        temporary = _materializedsum(temporary,
            _reservearray!(budget, String, length(names)))
        validated = Vector{String}(undef, length(names))
        temporary = _materializedsum(temporary,
            _reserveobjects!(budget))
        seen = Set{String}()
        position = 1
        for index in eachindex(names)
            name = names[index]
            validated[position] = name
            position += 1
            occursin('\0', name) && throw(UnsupportedFeatureError(
                "Parquet.Table cannot represent a top-level field name containing NUL; " *
                "use Parquet.File for low-level access"))
            name in seen && throw(UnsupportedFeatureError(
                "Parquet.Table requires unique top-level field names; " *
                "use Parquet.File for low-level access"))
            temporary = _materializedsum(temporary,
                _reserveobjects!(budget))
            push!(seen, name)
        end
        return validated, temporary
    catch
        iszero(temporary) || _release!(budget, temporary)
        rethrow()
    end
end

function _validatetablenames(names::AbstractVector{String},
    budget::_LiveByteBudget)
    _, temporary = _copyvalidatetablenames(names, budget)
    _release!(budget, temporary)
    return
end

function _validatetablenames(names::AbstractVector{String})
    return _validatetablenames(names, _LiveByteBudget(Limits()))
end

function _internschemanames(names::AbstractVector{String}, limits::Limits,
    budget::_LiveByteBudget)
    validated, temporary = _copyvalidatetablenames(names, budget)
    registry = _SCHEMA_NAME_REGISTRY
    outputcharge = Int64(0)
    output = nothing
    additions = 0
    try
        lock(registry.lock)
        try
            # max_schema_name_bytes bounds the new name bytes ONE operation may
            # intern, not the process-global total: interned Symbols are immortal,
            # so charging every operation against a shared lifetime cap would let a
            # single hostile file exhaust it and deny every later file that carries
            # any not-yet-interned name. The registry still tracks the global total
            # as an observability metric.
            state = registry.state
            charge = Int64(0)
            additions = 0
            for name in validated
                name in state.names && continue
                nextcharge = _schemanamecharge(name,
                    limits.max_schema_name_bytes)
                charge = _budgetrequest(charge, nextcharge,
                    limits.max_schema_name_bytes, :schema_name_bytes)
                additions += 1
            end
            requested = _budgetrequest(state.bytes, charge, typemax(Int64),
                :schema_name_bytes)
            requested >= state.bytes || throw(AssertionError(
                "schema-name registry byte accounting decreased"))
            outputcharge = _reservearray!(budget, Symbol,
                length(validated))
            output = Vector{Symbol}(undef, length(validated))
            symbols = something(output)
            if additions > 0
                replacementcount = try
                    Base.checked_add(length(state.names), additions)
                catch err
                    err isa OverflowError || rethrow()
                    throw(LimitError(:container_elements, typemax(Int64),
                        typemax(Int64)))
                end
                replacementcharge = _materializedsum(
                    _materializedproduct(replacementcount,
                        _MATERIALIZED_OBJECT_BYTES),
                    _MATERIALIZED_OBJECT_BYTES)
                _reserve!(budget, replacementcharge)
                temporary = _materializedsum(temporary,
                    replacementcharge)
                updated = Set{String}()
                sizehint!(updated, replacementcount)
                for name in state.names
                    push!(updated, name)
                end
                for name in validated
                    name in state.names || push!(updated, name)
                end
                for index in eachindex(validated)
                    symbols[index] = Symbol(validated[index])
                end
                newstate = _SchemaNameState(updated, requested)
                _release!(budget, temporary)
                temporary = Int64(0)
                registry.state = newstate
            end
        finally
            unlock(registry.lock)
        end
        symbols = something(output)
        if additions == 0
            for index in eachindex(validated)
                symbols[index] = Symbol(validated[index])
            end
            _release!(budget, temporary)
            temporary = Int64(0)
        end
        return symbols
    catch
        iszero(outputcharge) || _release!(budget, outputcharge)
        iszero(temporary) || _release!(budget, temporary)
        rethrow()
    end
end

function _internschemanames(names::AbstractVector{String}, limits::Limits)
    return _internschemanames(names, limits, _LiveByteBudget(limits))
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

function _checklimit(resource::Symbol, requested::Integer, maximum::Integer)
    requested <= maximum && return
    reported = requested > typemax(Int64) ? typemax(Int64) :
        requested < typemin(Int64) ? typemin(Int64) : Int64(requested)
    throw(LimitError(resource, reported, Int64(maximum)))
end
