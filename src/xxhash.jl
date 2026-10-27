# XXH64 (https://github.com/Cyan4973/xxHash), the hash Parquet bloom filters use.

const _XXH_P1 = 0x9E3779B185EBCA87
const _XXH_P2 = 0xC2B2AE3D27D4EB4F
const _XXH_P3 = 0x165667B19E3779F9
const _XXH_P4 = 0x85EBCA77C2B2AE63
const _XXH_P5 = 0x27D4EB2F165667C5

@inline _xxhround(acc::UInt64, lane::UInt64) =
    bitrotate(acc + lane * _XXH_P2, 31) * _XXH_P1

@inline _xxhmerge(acc::UInt64, value::UInt64) =
    (acc ⊻ _xxhround(UInt64(0), value)) * _XXH_P1 + _XXH_P4

@inline _xxhload64(pointer::Ptr{UInt8}) = ltoh(unsafe_load(Ptr{UInt64}(pointer)))
@inline _xxhload32(pointer::Ptr{UInt8}) = UInt64(ltoh(unsafe_load(Ptr{UInt32}(pointer))))

"""
    _xxhash64(pointer, length, seed=0) -> UInt64

XXH64 of `length` bytes at `pointer`. The caller keeps the memory alive.
"""
function _xxhash64(pointer::Ptr{UInt8}, length::Int, seed::UInt64=UInt64(0))
    position = pointer
    stop = pointer + length
    if length >= 32
        limit = stop - 32
        v1 = seed + _XXH_P1 + _XXH_P2
        v2 = seed + _XXH_P2
        v3 = seed
        v4 = seed - _XXH_P1
        while position <= limit
            v1 = _xxhround(v1, _xxhload64(position))
            v2 = _xxhround(v2, _xxhload64(position + 8))
            v3 = _xxhround(v3, _xxhload64(position + 16))
            v4 = _xxhround(v4, _xxhload64(position + 24))
            position += 32
        end
        hash = bitrotate(v1, 1) + bitrotate(v2, 7) + bitrotate(v3, 12) + bitrotate(v4, 18)
        hash = _xxhmerge(hash, v1)
        hash = _xxhmerge(hash, v2)
        hash = _xxhmerge(hash, v3)
        hash = _xxhmerge(hash, v4)
    else
        hash = seed + _XXH_P5
    end
    hash += UInt64(length)
    while position + 8 <= stop
        hash ⊻= _xxhround(UInt64(0), _xxhload64(position))
        hash = bitrotate(hash, 27) * _XXH_P1 + _XXH_P4
        position += 8
    end
    if position + 4 <= stop
        hash ⊻= _xxhload32(position) * _XXH_P1
        hash = bitrotate(hash, 23) * _XXH_P2 + _XXH_P3
        position += 4
    end
    while position < stop
        hash ⊻= UInt64(unsafe_load(position)) * _XXH_P5
        hash = bitrotate(hash, 11) * _XXH_P1
        position += 1
    end
    return _xxhavalanche(hash)
end

@inline function _xxhavalanche(hash::UInt64)
    hash ⊻= hash >> 33
    hash *= _XXH_P2
    hash ⊻= hash >> 29
    hash *= _XXH_P3
    hash ⊻= hash >> 32
    return hash
end

# XXH64 (seed 0) of the 8 or 4 little-endian bytes of `value`.
@inline function _xxhash64(value::UInt64)
    hash = (_XXH_P5 + UInt64(8)) ⊻ _xxhround(UInt64(0), value)
    return _xxhavalanche(bitrotate(hash, 27) * _XXH_P1 + _XXH_P4)
end

@inline function _xxhash64(value::UInt32)
    hash = (_XXH_P5 + UInt64(4)) ⊻ (UInt64(value) * _XXH_P1)
    return _xxhavalanche(bitrotate(hash, 23) * _XXH_P2 + _XXH_P3)
end

function _xxhash64(bytes::Union{Vector{UInt8},Base.CodeUnits{UInt8,String}},
        seed::UInt64=UInt64(0))
    GC.@preserve bytes begin
        return _xxhash64(pointer(bytes), length(bytes), seed)
    end
end

_xxhash64(value::String, seed::UInt64=UInt64(0)) = _xxhash64(codeunits(value), seed)
