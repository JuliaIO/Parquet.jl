function pagechecksum(bytes::AbstractVector{UInt8})
    return CRC32.crc32(bytes)
end

function verifypagechecksum(expected::Int32, bytes::AbstractVector{UInt8})
    pagechecksum(bytes) == reinterpret(UInt32, expected) && return
    throw(FormatError("page CRC32 mismatch"))
end
