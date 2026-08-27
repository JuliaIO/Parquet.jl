function pagechecksum(bytes::AbstractVector{UInt8})
    return CRC32.crc32(bytes)
end

function _pagechecksumscratch(::CRC32.ByteArray)
    return Int64(0)
end

function _pagechecksumscratch(bytes::AbstractVector{UInt8})
    count = min(length(bytes), 24_576)
    return _materializedarraybytes(UInt8, count)
end

function verifypagechecksum(expected::Int32,
        bytes::AbstractVector{UInt8};
        budget::Union{Nothing,_LiveByteBudget}=nothing)
    scratch = budget === nothing ? Int64(0) : _pagechecksumscratch(bytes)
    iszero(scratch) || _reserve!(budget, scratch)
    try
        actual = pagechecksum(bytes)
        actual == reinterpret(UInt32, expected) && return
        throw(FormatError("page CRC32 mismatch"))
    finally
        iszero(scratch) || _release!(something(budget), scratch)
    end
end
