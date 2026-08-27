abstract type AbstractSource end

function close!(::AbstractSource)
    return
end

function concurrentreads(::AbstractSource)
    return false
end

mutable struct OwnerRegion{B<:AbstractVector{UInt8},I}
    bytes::B
    io::I
    budget::Union{Nothing,_LiveByteBudget}
    materializedcharge::Int64
    closed::Bool
end

struct MemorySource{R<:OwnerRegion} <: AbstractSource
    region::R
end

struct BufferSlice{R<:OwnerRegion} <: AbstractVector{UInt8}
    region::R
    offset::Int64
    count::Int64
    function BufferSlice(region::R, offset::Int64, count::Int64) where {R<:OwnerRegion}
        offset >= 0 || throw(BoundsError(region.bytes, offset))
        count >= 0 || throw(ArgumentError("byte count must be nonnegative"))
        last = try
            Base.checked_add(offset, count)
        catch err
            err isa OverflowError || rethrow()
            throw(BoundsError(region.bytes, (offset, count)))
        end
        last <= length(region.bytes) || throw(BoundsError(region.bytes, (offset, count)))
        return new{R}(region, offset, count)
    end
end

function BufferSlice(region::OwnerRegion, offset::Integer, count::Integer)
    return BufferSlice(region, Int64(offset), Int64(count))
end

function Base.IndexStyle(::Type{<:BufferSlice})
    return IndexLinear()
end

function Base.size(bytes::BufferSlice)
    return (Int(bytes.count),)
end

function Base.length(bytes::BufferSlice)
    return Int(bytes.count)
end

function Base.getindex(bytes::BufferSlice, index::Int)
    bytes.region.closed && throw(ArgumentError("Parquet byte region is closed"))
    checkbounds(bytes, index)
    first = firstindex(bytes.region.bytes)
    return bytes.region.bytes[first + Int(bytes.offset) + index - 1]
end

function Base.copy(bytes::BufferSlice)
    bytes.region.closed && throw(ArgumentError("Parquet byte region is closed"))
    return collect(bytes)
end

function close!(region::OwnerRegion)
    region.closed && return
    region.closed = true
    charge = region.materializedcharge
    region.materializedcharge = Int64(0)
    try
        region.io === nothing || close(region.io)
    finally
        iszero(charge) || _release!(something(region.budget), charge)
    end
    return
end

function close!(source::MemorySource)
    close!(source.region)
    return
end

function Base.close(source::MemorySource)
    close!(source)
    return
end

function source(bytes::AbstractVector{UInt8};
    budget::_LiveByteBudget=_LiveByteBudget(Limits()))
    region = OwnerRegion(bytes, nothing, nothing, Int64(0), false)
    return MemorySource(region)
end

function _readsourcebytes(io::IO, budget::_LiveByteBudget)
    temporary = Int64(0)
    outputcharge = Int64(0)
    try
        temporary = _materializedsum(temporary,
            _reservearray!(budget, Vector{UInt8}, 0))
        chunks = Vector{UInt8}[]
        available = budget.maximum - _budgetused(budget)
        fixed = 3 * _MATERIALIZED_ARRAY_HEADER_BYTES + 2 * Int64(sizeof(Ptr{Cvoid}))
        blocksize = Int(min(Int64(64 * 1024), max(Int64(1),
            (available - fixed) ÷ 3)))
        temporary = _materializedsum(temporary,
            _reservearray!(budget, UInt8, blocksize))
        scratch = Vector{UInt8}(undef, blocksize)
        total = Int64(0)
        while !eof(io)
            count = readbytes!(io, scratch, blocksize)
            iszero(count) && continue
            total = try
                Base.checked_add(total, Int64(count))
            catch err
                err isa OverflowError || rethrow()
                throw(LimitError(:materialized_bytes, typemax(Int64),
                    budget.maximum))
            end
            charge = _reservearray!(budget, UInt8, count)
            temporary = _materializedsum(temporary, charge)
            chunk = Vector{UInt8}(undef, count)
            copyto!(chunk, 1, scratch, 1, count)
            pointercharge = _reservearray!(budget, Vector{UInt8}, 2;
                header=false)
            temporary = _materializedsum(temporary, pointercharge)
            push!(chunks, chunk)
        end
        total <= typemax(Int) || throw(LimitError(:materialized_bytes,
            typemax(Int64), budget.maximum))
        outputcharge = _reservearray!(budget, UInt8, total)
        bytes = Vector{UInt8}(undef, Int(total))
        position = 1
        for chunk in chunks
            copyto!(bytes, position, chunk, 1, length(chunk))
            position += length(chunk)
        end
        _release!(budget, temporary)
        return bytes, outputcharge
    catch
        iszero(outputcharge) || _release!(budget, outputcharge)
        iszero(temporary) || _release!(budget, temporary)
        rethrow()
    end
end

function source(io::IO; budget::_LiveByteBudget=_LiveByteBudget(Limits()))
    bytes, charge = _readsourcebytes(io, budget)
    try
        region = OwnerRegion(bytes, nothing, budget, charge, false)
        return MemorySource(region)
    catch
        _release!(budget, charge)
        rethrow()
    end
end

function source(path::AbstractString;
    budget::_LiveByteBudget=_LiveByteBudget(Limits()))
    io = open(path, "r")
    try
        count = filesize(io)
        bytes = iszero(count) ? UInt8[] : Mmap.mmap(io, Vector{UInt8}, count)
        region = OwnerRegion(bytes, io, nothing, Int64(0), false)
        return MemorySource(region)
    catch
        try
            close(io)
        catch
        end
        rethrow()
    end
end

function source(src::AbstractSource;
    budget::_LiveByteBudget=_LiveByteBudget(Limits()))
    return src
end

function sourcelength(src::MemorySource)
    src.region.closed && throw(ArgumentError("Parquet byte region is closed"))
    return Int64(length(src.region.bytes))
end

function concurrentreads(::MemorySource)
    return true
end

function readrange(src::MemorySource, offset::Integer, count::Integer)
    src.region.closed && throw(ArgumentError("Parquet byte region is closed"))
    return BufferSlice(src.region, offset, count)
end

function _checkedsourcelength(src::AbstractSource)
    total = sourcelength(src)
    total isa Integer && !(total isa Bool) || throw(ArgumentError(
        "source length callback must return an integer"))
    total >= 0 || throw(ArgumentError(
        "source length callback returned a negative length"))
    total <= typemax(Int64) || throw(ArgumentError(
        "source length callback returned a length that exceeds Int64"))
    return Int64(total)
end

function _readrangeexact(src::AbstractSource, total::Int64, offset::Int64,
        count::Int64)
    total >= 0 || throw(ArgumentError(
        "authoritative source length must be nonnegative"))
    offset >= 0 || throw(ArgumentError("source read offset must be nonnegative"))
    count >= 0 || throw(ArgumentError("source read count must be nonnegative"))
    stop = try
        Base.checked_add(offset, count)
    catch err
        err isa OverflowError || rethrow()
        throw(ArgumentError("source read range overflows Int64"))
    end
    stop <= total || throw(ArgumentError(
        "source read range extends past the authoritative source length"))
    count <= typemax(Int) || throw(ArgumentError(
        "source read count does not fit Int"))
    bytes = readrange(src, offset, count)
    bytes isa AbstractVector{UInt8} || throw(ArgumentError(
        "source read callback must return AbstractVector{UInt8}"))
    length(bytes) == count || throw(ArgumentError(
        "source read callback returned the wrong byte count"))
    byteaxes = axes(bytes)
    length(byteaxes) == 1 && byteaxes[1] isa Base.OneTo &&
        byteaxes[1] == Base.OneTo(Int(count)) || throw(ArgumentError(
        "source read callback must return a vector with Base.OneTo axes"))
    return bytes
end
