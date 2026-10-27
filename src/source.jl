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
    @atomic closed::Bool
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
    (@atomic bytes.region.closed) && throw(ArgumentError("Parquet byte region is closed"))
    checkbounds(bytes, index)
    first = firstindex(bytes.region.bytes)
    return bytes.region.bytes[first + Int(bytes.offset) + index - 1]
end

function Base.copy(bytes::BufferSlice)
    (@atomic bytes.region.closed) && throw(ArgumentError("Parquet byte region is closed"))
    return collect(bytes)
end

function close!(region::OwnerRegion)
    (@atomicswap region.closed = true) && return
    region.io === nothing || close(region.io)
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

function source(bytes::AbstractVector{UInt8}; budget::_ByteBudget=_UNCAPPED)
    return MemorySource(OwnerRegion(bytes, nothing, false))
end

# An `IO` is read into memory; its bytes are charged to `budget` as they are read.
function _readsourcebytes(io::IO, budget::_ByteBudget)
    bytes = UInt8[]
    block = Vector{UInt8}(undef, 64 * 1024)
    while !eof(io)
        count = readbytes!(io, block)
        _charge!(budget, count)
        append!(bytes, view(block, 1:count))
    end
    return bytes
end

function source(io::IO; budget::_ByteBudget=_UNCAPPED)
    return MemorySource(OwnerRegion(_readsourcebytes(io, budget), nothing, false))
end

function source(path::AbstractString; budget::_ByteBudget=_UNCAPPED)
    io = open(path, "r")
    try
        count = filesize(io)
        bytes = iszero(count) ? UInt8[] : Mmap.mmap(io, Vector{UInt8}, count)
        return MemorySource(OwnerRegion(bytes, io, false))
    catch
        try
            close(io)
        catch
        end
        rethrow()
    end
end

function source(src::AbstractSource; budget::_ByteBudget=_UNCAPPED)
    return src
end

function sourcelength(src::MemorySource)
    (@atomic src.region.closed) && throw(ArgumentError("Parquet byte region is closed"))
    return Int64(length(src.region.bytes))
end

function concurrentreads(::MemorySource)
    return true
end

function readrange(src::MemorySource, offset::Integer, count::Integer)
    (@atomic src.region.closed) && throw(ArgumentError("Parquet byte region is closed"))
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

# One column chunk fetched with a single read from a source that is not in memory;
# the page reads inside the chunk are served from these bytes.
struct _ChunkSource{B<:AbstractVector{UInt8}} <: AbstractSource
    bytes::B
    start::Int64
    total::Int64
end

function _ChunkSource(src::AbstractSource, start::Int64, stop::Int64)
    total = _checkedsourcelength(src)
    bytes = _contiguous(_readrangeexact(src, total, start, stop - start))
    return _ChunkSource(bytes, start, total)
end

function sourcelength(src::_ChunkSource)
    return src.total
end

function concurrentreads(::_ChunkSource)
    return true
end

function readrange(src::_ChunkSource, offset::Integer, count::Integer)
    first = Int(offset - src.start) + 1
    first >= 1 && first - 1 + Int(count) <= length(src.bytes) || throw(ArgumentError(
        "read outside the prefetched column chunk"))
    return view(src.bytes, first:(first - 1 + Int(count)))
end
