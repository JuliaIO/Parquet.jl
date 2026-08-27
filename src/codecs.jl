# Page compression codecs (Compression.md, Parquet 2.13.0). Every decompression targets the
# exact uncompressed size declared by the page header and never allocates beyond the limits.

import ChunkCodecCore
import ChunkCodecLibSnappy
import ChunkCodecLibZlib
import ChunkCodecLibZstd
import ChunkCodecLibLz4
import ChunkCodecLibBrotli

const SNAPPY_CODEC = ChunkCodecLibSnappy.SnappyCodec()
const GZIP_CODEC = ChunkCodecLibZlib.GzipCodec()
const ZSTD_CODEC = ChunkCodecLibZstd.ZstdCodec()
const LZ4_BLOCK_CODEC = ChunkCodecLibLz4.LZ4BlockCodec()
const BROTLI_CODEC = ChunkCodecLibBrotli.BrotliCodec()
const LZ4_BLOCK_DECODER = ChunkCodecLibLz4.LZ4BlockDecodeOptions()

function codecname(codec::Metadata.CompressionCodec.T)
    name = Thrift.name(codec)
    name === nothing && return "CompressionCodec.T($(codec.value))"
    return String(name)
end

function codecreadable(codec::Metadata.CompressionCodec.T)
    return codec == Metadata.CompressionCodec.UNCOMPRESSED || codec == Metadata.CompressionCodec.SNAPPY ||
        codec == Metadata.CompressionCodec.GZIP || codec == Metadata.CompressionCodec.BROTLI ||
        codec == Metadata.CompressionCodec.LZ4 || codec == Metadata.CompressionCodec.ZSTD ||
        codec == Metadata.CompressionCodec.LZ4_RAW
end

# The deprecated LZ4 codec is read-only: new files use LZ4_RAW (Compression.md).
function codecwritable(codec::Metadata.CompressionCodec.T)
    return codecreadable(codec) && codec != Metadata.CompressionCodec.LZ4
end

function _unreadablecodec(codec::Metadata.CompressionCodec.T)
    codec == Metadata.CompressionCodec.LZO &&
        throw(FormatError("LZO compression is not supported: no license-compatible LZO implementation is available"))
    throw(FormatError("unknown compression codec $(codecname(codec))"))
end

function _unwritablecodec(codec::Metadata.CompressionCodec.T)
    codec == Metadata.CompressionCodec.LZ4 &&
        throw(ArgumentError("the deprecated LZ4 codec is read-only; write LZ4_RAW instead"))
    codec == Metadata.CompressionCodec.LZO && throw(ArgumentError("LZO compression is not supported"))
    throw(ArgumentError("unknown compression codec $(codecname(codec))"))
end

function _contiguous(bytes::Vector{UInt8})
    return bytes
end

function _contiguous(bytes::SubArray{UInt8,1,Vector{UInt8},Tuple{UnitRange{Int}},true})
    return bytes
end

function _contiguous(bytes::BufferSlice)
    bytes.region.closed && throw(ArgumentError("Parquet byte region is closed"))
    first = firstindex(bytes.region.bytes) + Int(bytes.offset)
    return _contiguous(view(bytes.region.bytes, first:(first + Int(bytes.count) - 1)))
end

function _contiguous(bytes::AbstractVector{UInt8})
    return Vector{UInt8}(bytes)
end

function _contiguouscopycharge(::Vector{UInt8})
    return Int64(0)
end

function _contiguouscopycharge(
        ::SubArray{UInt8,1,Vector{UInt8},Tuple{UnitRange{Int}},true})
    return Int64(0)
end

function _contiguouscopycharge(bytes::BufferSlice)
    bytes.region.closed && throw(ArgumentError("Parquet byte region is closed"))
    first = firstindex(bytes.region.bytes) + Int(bytes.offset)
    viewbytes = view(bytes.region.bytes,
        first:(first + Int(bytes.count) - 1))
    return _contiguouscopycharge(viewbytes)
end

function _contiguouscopycharge(bytes::AbstractVector{UInt8})
    return _materializedarraybytes(UInt8, length(bytes))
end

function _codecfailure(err, codec::Metadata.CompressionCodec.T)
    err isa ChunkCodecCore.DecodedSizeError &&
        throw(FormatError("$(codecname(codec)) page did not decompress to the declared size"))
    err isa ChunkCodecCore.DecodingError &&
        throw(FormatError("$(codecname(codec)) page is corrupt: $(sprint(showerror, err))"))
    throw(err)
end

function _readbe32(bytes::AbstractVector{UInt8}, offset::Int)
    value = UInt32(0)
    @inbounds for index in 0:3
        value = (value << 8) | UInt32(bytes[offset + index])
    end
    return Int64(value)
end

function _lz4blocksize!(target::AbstractVector{UInt8}, block::AbstractVector{UInt8})
    size = try
        ChunkCodecCore.try_decode!(LZ4_BLOCK_DECODER, target, block)
    catch err
        err isa ChunkCodecCore.DecodingError || rethrow()
        return nothing
    end
    ChunkCodecCore.is_size(size) || return nothing
    decoded = Int(size)
    0 < decoded <= length(target) || return nothing
    return decoded
end

function _lz4emptyblock(block::AbstractVector{UInt8})
    size = try
        ChunkCodecCore.try_decode!(LZ4_BLOCK_DECODER, UInt8[], block)
    catch err
        err isa ChunkCodecCore.DecodingError || rethrow()
        return false
    end
    return ChunkCodecCore.is_size(size) && Int(size) == 0
end

# Hadoop BlockCompressorStream framing: each block starts with its total uncompressed size,
# followed by one or more compressed-size-prefixed LZ4 chunks. A zero block is an empty stream.
function _lz4hadoop!(output::Vector{UInt8}, source::AbstractVector{UInt8})
    isempty(source) && return false
    position = 1
    produced = 1
    while position <= length(source)
        length(source) - position + 1 >= 4 || return false
        original = _readbe32(source, position)
        position += 4
        if original == 0
            return position == length(source) + 1 && produced == length(output) + 1
        end
        original <= length(output) - produced + 1 || return false
        blockproduced = 0
        while blockproduced < original
            length(source) - position + 1 >= 4 || return false
            compressed = _readbe32(source, position)
            position += 4
            0 < compressed <= length(source) - position + 1 || return false
            block = view(source, position:(position + compressed - 1))
            target = view(output, (produced + blockproduced):(produced + original - 1))
            decoded = _lz4blocksize!(target, block)
            decoded === nothing && return false
            blockproduced += decoded
            position += compressed
        end
        produced += original
    end
    return produced == length(output) + 1
end

function _lz4arrowhadoop!(output::Vector{UInt8}, source::AbstractVector{UInt8})
    isempty(source) && return false
    position = 1
    produced = 1
    while position <= length(source)
        length(source) - position + 1 >= 8 || return false
        original = _readbe32(source, position)
        compressed = _readbe32(source, position + 4)
        position += 8
        compressed <= length(source) - position + 1 || return false
        original <= length(output) - produced + 1 || return false
        if original == 0
            if compressed > 0
                block = view(source, position:(position + compressed - 1))
                _lz4emptyblock(block) || return false
            end
        else
            compressed > 0 || return false
            block = view(source, position:(position + compressed - 1))
            target = view(output, produced:(produced + original - 1))
            decoded = _lz4blocksize!(target, block)
            decoded == original || return false
        end
        position += compressed
        produced += original
    end
    return produced == length(output) + 1
end

function _decompresslz4!(output::Vector{UInt8}, source::AbstractVector{UInt8})
    _lz4hadoop!(output, source) && return
    _lz4arrowhadoop!(output, source) && return
    ChunkCodecCore.decode!(LZ4_BLOCK_CODEC, output, source)
    return
end

function _decompress!(codec::Metadata.CompressionCodec.T, output::Vector{UInt8}, source::AbstractVector{UInt8})
    if codec == Metadata.CompressionCodec.SNAPPY
        ChunkCodecCore.decode!(SNAPPY_CODEC, output, source)
    elseif codec == Metadata.CompressionCodec.GZIP
        ChunkCodecCore.decode!(GZIP_CODEC, output, source)
    elseif codec == Metadata.CompressionCodec.ZSTD
        ChunkCodecCore.decode!(ZSTD_CODEC, output, source)
    elseif codec == Metadata.CompressionCodec.BROTLI
        ChunkCodecCore.decode!(BROTLI_CODEC, output, source)
    elseif codec == Metadata.CompressionCodec.LZ4_RAW
        ChunkCodecCore.decode!(LZ4_BLOCK_CODEC, output, source)
    else
        _decompresslz4!(output, source)
    end
    return
end

function _uncompressed(bytes::AbstractVector{UInt8}, expected::Int)
    length(bytes) == expected ||
        throw(FormatError("uncompressed page holds $(length(bytes)) bytes but declares $expected"))
    return bytes
end

"""
    decompress(codec, bytes, expected; limits) -> AbstractVector{UInt8}

Decompress one page payload to exactly `expected` bytes. Sizes are charged to
`limits.max_page_bytes` before allocation, UNCOMPRESSED pages are returned as-is, and any
codec failure or size disagreement is a `FormatError`.
"""
function decompress(codec::Metadata.CompressionCodec.T,
        bytes::AbstractVector{UInt8}, expected::Integer;
        limits::Limits=Limits(),
        budget::Union{Nothing,_LiveByteBudget}=nothing)
    expected >= 0 || throw(FormatError("negative uncompressed page size $expected"))
    _checklimit(:page_bytes, expected, limits.max_page_bytes)
    _checklimit(:page_bytes, length(bytes), limits.max_page_bytes)
    expected <= typemax(Int) || throw(FormatError("uncompressed page size $expected overflows"))
    codecreadable(codec) || _unreadablecodec(codec)
    codec == Metadata.CompressionCodec.UNCOMPRESSED && return _uncompressed(bytes, Int(expected))
    copycharge = budget === nothing ? Int64(0) : _contiguouscopycharge(bytes)
    iszero(copycharge) || _reserve!(budget, copycharge)
    try
        source = _contiguous(bytes)
        output = Vector{UInt8}(undef, Int(expected))
        try
            _decompress!(codec, output, source)
        catch err
            _codecfailure(err, codec)
        end
        return output
    finally
        iszero(copycharge) || _release!(something(budget), copycharge)
    end
end

function _checklevel(codec::Metadata.CompressionCodec.T, level::Integer, range::UnitRange{Int})
    level in range || throw(ArgumentError("$(codecname(codec)) compression level must be in $range, got $level"))
    return Int(level)
end

function _encoder(codec::Metadata.CompressionCodec.T, level::Union{Nothing,Integer})
    if codec == Metadata.CompressionCodec.SNAPPY
        level === nothing || throw(ArgumentError("SNAPPY has no compression level"))
        return ChunkCodecLibSnappy.SnappyEncodeOptions()
    elseif codec == Metadata.CompressionCodec.GZIP
        return ChunkCodecLibZlib.GzipEncodeOptions(; level=_checklevel(codec, something(level, 6), 0:9))
    elseif codec == Metadata.CompressionCodec.ZSTD
        return ChunkCodecLibZstd.ZstdEncodeOptions(; compressionLevel=_checklevel(codec, something(level, 3), -131072:22))
    elseif codec == Metadata.CompressionCodec.BROTLI
        return ChunkCodecLibBrotli.BrotliEncodeOptions(; quality=_checklevel(codec, something(level, 8), 0:11))
    end
    return ChunkCodecLibLz4.LZ4BlockEncodeOptions(; compressionLevel=_checklevel(codec, something(level, 0), 0:12))
end

"""
    compress(codec, bytes; level) -> Vector{UInt8}

Compress one page payload. `level` is codec specific (GZIP 0-9, ZSTD -131072..22, BROTLI
0-11, LZ4_RAW 0-12); SNAPPY takes none. The deprecated LZ4 codec and LZO are not writable.
"""
function compress(codec::Metadata.CompressionCodec.T, bytes::AbstractVector{UInt8};
    level::Union{Nothing,Integer}=nothing)
    length(bytes) <= typemax(Int32) || throw(ArgumentError("page payload exceeds 2^31 - 1 bytes"))
    codecwritable(codec) || _unwritablecodec(codec)
    if codec == Metadata.CompressionCodec.UNCOMPRESSED
        level === nothing || throw(ArgumentError("UNCOMPRESSED has no compression level"))
        return Vector{UInt8}(bytes)
    end
    return ChunkCodecCore.encode(_encoder(codec, level), _contiguous(bytes))
end
