const PARQUET_MAGIC = UInt8[0x50, 0x41, 0x52, 0x31]
const ENCRYPTED_MAGIC = UInt8[0x50, 0x41, 0x52, 0x45]

struct Footer{B<:AbstractVector{UInt8}}
    offset::Int64
    length::Int64
    encrypted::Bool
    bytes::B
end

# An open Parquet file: its source, footer, decoded metadata, and the read plan its
# keywords resolve to (`P` is `_ReadPlan`, defined with the readers). `owned` is whether
# Parquet opened the source and so closes it.
mutable struct File{S<:AbstractSource,F<:Footer,P}
    source::S
    footer::F
    metadata::Metadata.FileMetaData
    plan::P
    owned::Bool
    @atomic closed::Bool
end

function _readu32le(bytes::AbstractVector{UInt8}, offset::Int=1)
    checkbounds(bytes, offset:(offset + 3))
    value = UInt32(bytes[offset]) |
        UInt32(bytes[offset + 1]) << 8 |
        UInt32(bytes[offset + 2]) << 16 |
        UInt32(bytes[offset + 3]) << 24
    return value
end

function _magic(bytes::AbstractVector{UInt8}, offset::Int=1)
    checkbounds(bytes, offset:(offset + 3))
    first = bytes[offset]
    second = bytes[offset + 1]
    third = bytes[offset + 2]
    fourth = bytes[offset + 3]
    first == 0x50 && second == 0x41 && third == 0x52 && fourth == 0x31 &&
        return :plain
    first == 0x50 && second == 0x41 && third == 0x52 && fourth == 0x45 &&
        return :encrypted
    return :invalid
end

# Bytes read from the end of a source that is not in memory: the trailer and, for most
# files, the whole footer, in one request.
const _FOOTER_TAIL_BYTES = Int64(64 * 1024)

# The footer from the end of `src`. A source that is not in memory is read with one
# request for the last `_FOOTER_TAIL_BYTES` (and a second only for a larger footer); its
# leading magic is checked when that read covers the whole file.
function readfooter(src::AbstractSource, limits::Limits=Limits())
    total = _checkedsourcelength(src)
    total >= 12 || throw(FormatError("file is shorter than the minimum 12 bytes"))
    inmemory = src isa MemorySource
    tail = inmemory ? Int64(8) : min(total, _FOOTER_TAIL_BYTES)
    tailbytes = _readrangeexact(src, total, total - tail, tail)
    trailer = view(tailbytes, (tail - 7):tail)
    trailingmagic = _magic(trailer, 5)
    trailingmagic === :invalid && throw(FormatError("missing trailing PAR1 or PARE magic"))
    if inmemory || tail == total
        leading = inmemory ? _readrangeexact(src, total, Int64(0), Int64(4)) : tailbytes
        leadingmagic = _magic(leading)
        leadingmagic === :invalid && throw(FormatError("missing leading PAR1 or PARE magic"))
        leadingmagic === trailingmagic || throw(FormatError("leading and trailing magic differ"))
    end
    footerlength = Int64(_readu32le(trailer))
    footerlength <= total - 12 || throw(FormatError("footer length exceeds the file size"))
    _checklimit(:footer_bytes, footerlength, limits.max_footer_bytes)
    footeroffset = total - 8 - footerlength
    bytes = footerlength + 8 <= tail ?
        view(tailbytes, (tail - 7 - footerlength):(tail - 8)) :
        _readrangeexact(src, total, footeroffset, footerlength)
    return Footer(footeroffset, footerlength, trailingmagic === :encrypted, bytes)
end

# The file metadata a footer encodes. Encrypted files are refused: a PARE footer is
# itself encrypted, and a plaintext footer names its encryption algorithm.
function _decodefooter(footer::Footer, limits::Limits, budget::_ByteBudget)
    footer.encrypted && throw(UnsupportedFeatureError(
        "encrypted Parquet files are not supported"))
    reader = Thrift.Reader(footer.bytes; limits=limits, budget=budget)
    metadata = Thrift.decode(reader, Metadata.FileMetaData)
    metadata.encryption_algorithm === nothing || throw(UnsupportedFeatureError(
        "encrypted Parquet files are not supported (this one has a plaintext footer)"))
    Thrift.remaining(reader) == 0 || throw(FormatError("file footer has trailing bytes"))
    return metadata
end

function _readfilemetadata(file::File, limits::Limits,
    budget::_ByteBudget=_ByteBudget(limits))
    return _decodefooter(file.footer, limits, budget)
end

function Base.show(io::IO, file::File)
    metadata = file.metadata
    columns = isempty(metadata.schema) ? 0 : something(metadata.schema[1].num_children, 0)
    print(io, "Parquet.File(", metadata.num_rows, " rows, ", columns, " columns, ",
        length(metadata.row_groups), " row groups)")
    return
end

function close!(file::File)
    (@atomicswap file.closed = true) && return
    file.owned && close!(file.source)
    return
end

function Base.close(file::File)
    close!(file)
    return
end
