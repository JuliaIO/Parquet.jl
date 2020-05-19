using Thrift

function metadata(path)
    io = open(path)
    sz = filesize(io)
    seek(io, sz - SZ_PAR_MAGIC - SZ_FOOTER)

    # read footer size as little endian signed Int32
    meta_len = read(io, Int32)
    datasize = sz - meta_len - 2SZ_PAR_MAGIC - SZ_FOOTER
    seek(io, SZ_PAR_MAGIC + datasize)
    filemetadata = read_thrift(io, PAR2.FileMetaData)
end
