# Split-block Bloom filters, the Parquet format's bloom filters
# (https://github.com/apache/parquet-format/blob/master/BloomFilter.md). A value hashes
# with XXH64 over its PLAIN encoding, without the length prefix of a BYTE_ARRAY. The
# hash's upper 32 bits pick one 32-byte block of the bitset; its lower 32 bits, times
# eight salts, set one bit in each of the block's eight little-endian 32-bit words.

const _BLOOM_SALT = (0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d, 0x705495c7,
    0x2df1424b, 0x9efc4947, 0x5c6bfb31)

# The bitset sizes Arrow C++ and parquet-mr read and write.
const _BLOOM_MIN_BYTES = 32
const _BLOOM_MAX_BYTES = 128 * 1024 * 1024

"""
    _bloombytes(ndv, fpp) -> Int

The bitset size for `ndv` distinct values at false-positive probability `fpp`, as Arrow
C++ and parquet-mr size it: a power of two from 32 bytes to 128 MiB.
"""
function _bloombytes(ndv::Integer, fpp::Float64)
    bits = -8 * ndv / log1p(-fpp^(1 / 8))
    bits >= 8 * _BLOOM_MAX_BYTES && return _BLOOM_MAX_BYTES
    return nextpow(2, max(_BLOOM_MIN_BYTES, ceil(Int, bits / 8)))
end

# The block of a hash, as the offset of its first word in the bitset.
@inline _bloomblock(hash::UInt64, words::Int) =
    8 * Int(((hash >> 32) * UInt64(words >> 3)) >> 32)

@inline _bloommask(hash::UInt64, word::Int) =
    UInt32(1) << ((hash % UInt32 * _BLOOM_SALT[word]) >> 27)

"""
    _bloominsert!(words::Vector{UInt32}, hash::UInt64) -> words

Add the value whose hash is `hash` to the filter whose bitset is `words` (a multiple of
eight words).
"""
@inline function _bloominsert!(words::Vector{UInt32}, hash::UInt64)
    block = _bloomblock(hash, length(words))
    for word in 1:8
        @inbounds words[block + word] |= _bloommask(hash, word)
    end
    return words
end

"""
    _bloomcontains(words::Vector{UInt32}, hash::UInt64) -> Bool

Whether the filter may hold the value whose hash is `hash`. `false` is exact: the value
is not in the column chunk. `words` is a multiple of eight words, as `_bloomwords`
returns it.
"""
@inline function _bloomcontains(words::Vector{UInt32}, hash::UInt64)
    block = _bloomblock(hash, length(words))
    for word in 1:8
        @inbounds iszero(words[block + word] & _bloommask(hash, word)) && return false
    end
    return true
end

# The hash of one physical value: the integer or float bits, or the bytes of a
# BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, or INT96 value.
@inline _bloomhash(value::Union{Int32,Float32}) = _xxhash64(reinterpret(UInt32, value))
@inline _bloomhash(value::Union{Int64,Float64}) = _xxhash64(reinterpret(UInt64, value))
@inline _bloomhash(value::Union{String,Vector{UInt8},Base.CodeUnits{UInt8,String}}) =
    _xxhash64(value)

function _bloomhash(value::AbstractVector{UInt8})
    bytes = Vector{UInt8}(value)
    return _xxhash64(bytes)
end

"""
    _bloomfilterbytes(words::Vector{UInt32}) -> Vector{UInt8}

A filter as stored in a file: its Thrift `BloomFilterHeader`, then the bitset.
"""
function _bloomfilterbytes(words::Vector{UInt32})
    header = Metadata.BloomFilterHeader(numBytes=Int32(4 * length(words)),
        algorithm=Metadata.BloomFilterAlgorithm(BLOCK=Metadata.SplitBlockAlgorithm()),
        hash=Metadata.BloomFilterHash(XXHASH=Metadata.XxHash()),
        compression=Metadata.BloomFilterCompression(UNCOMPRESSED=Metadata.Uncompressed()))
    return append!(Thrift.encode(header), reinterpret(UInt8, words))
end

"""
    _bloomwords(bytes::AbstractVector{UInt8}) -> Vector{UInt32}

The bitset of a filter read from a file, starting at its header. Throws `FormatError`
for a header this package cannot use (another algorithm, hash, or compression, or a
size that is not a positive multiple of 32 bytes or overruns `bytes`).
"""
function _bloomwords(bytes::AbstractVector{UInt8})
    reader = Thrift.Reader(bytes)
    header = Thrift.decode(reader, Metadata.BloomFilterHeader)
    header.algorithm.BLOCK === nothing && throw(FormatError(
        "unsupported bloom filter algorithm"))
    header.hash.XXHASH === nothing && throw(FormatError("unsupported bloom filter hash"))
    header.compression.UNCOMPRESSED === nothing && throw(FormatError(
        "unsupported bloom filter compression"))
    count = Int(header.numBytes)
    count >= _BLOOM_MIN_BYTES && count % 32 == 0 || throw(FormatError(
        "bloom filter size $count is not a positive multiple of 32 bytes"))
    start = firstindex(bytes) + Thrift.consumed(reader)
    start + count - 1 <= lastindex(bytes) || throw(FormatError(
        "bloom filter bitset extends past its bytes"))
    words = Vector{UInt32}(undef, count >> 2)
    copyto!(reinterpret(UInt8, words), 1, bytes, start, count)
    return map!(ltoh, words, words)
end
