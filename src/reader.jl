
const PAR_MAGIC = "PAR1"
const SZ_PAR_MAGIC = length(PAR_MAGIC)
const SZ_FOOTER = 4
const SZ_VALID_PAR = 2*SZ_PAR_MAGIC + SZ_FOOTER

type ParFile
    path::AbstractString
    handle::IOStream
    meta_len::Int32
    meta::FileMetaData
end

function ParFile(path::AbstractString)
    f = open(path)
    try
        return ParFile(path, f)
    catch ex
        close(f)
        rethrow(ex)
    end
end

function ParFile(path::AbstractString, handle::IOStream)
    is_par_file(handle) || error("Not a parquet format file: $path")
    meta_len = metadata_length(handle)
    meta = metadata(handle, meta_len)
    ParFile(path, handle, meta_len, meta)
end

function show(io::IO, par::ParFile)
    println("Parquet file: $(par.path)")
    meta = par.meta
    println("    version: $(meta.version)")
    println("    nrows: $(meta.num_rows)")
    println("    created by: $(meta.created_by)")
end

# file format verification
function is_par_file(fname::AbstractString)
    open(fname) do io
        return is_par_file(io)
    end
end

function is_par_file(io)
    magic = Array(UInt8, 4)

    sz = filesize(io)
    (sz > SZ_VALID_PAR) || return false

    seekstart(io)
    read!(io, magic)
    (convert(ASCIIString, magic) == PAR_MAGIC) || return false

    seek(io, sz - SZ_PAR_MAGIC)
    read!(io, magic)
    (convert(ASCIIString, magic) == PAR_MAGIC) || return false

    true
end

# file metadata
function read_thrift(buff::Array{UInt8}, typ)
    t = TMemoryTransport(buff)
    p = TCompactProtocol(t)
    read(p, typ)
end

function metadata_length(io)
    sz = filesize(io)
    seek(io, sz - SZ_PAR_MAGIC - SZ_FOOTER)

    # read footer size as little endian signed Int32
    ProtoBuf.read_fixed(io, Int32)
end

function metadata(io, len::Integer=metadata_length(io))
    @logmsg("metadata len = $len")
    sz = filesize(io)
    seek(io, sz - SZ_PAR_MAGIC - SZ_FOOTER - len)
    meta_bytes = Array(UInt8, len)
    read!(io, meta_bytes)
    read_thrift(meta_bytes, FileMetaData)
end
