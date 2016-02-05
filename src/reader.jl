
const PAR_MAGIC = "PAR1"
const SZ_PAR_MAGIC = length(PAR_MAGIC)
const SZ_FOOTER = 4
const SZ_VALID_PAR = 2*SZ_PAR_MAGIC + SZ_FOOTER

type ParFile
    path::AbstractString
    handle::IOStream
    meta_len::Int32
    meta::FileMetaData
    page_meta_cache::Dict{ColumnChunk,Vector{PageHeader}}
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
    meta = metadata(handle, path, meta_len)
    ParFile(path, handle, meta_len, meta, Dict{ColumnChunk,Vector{PageHeader}}())
end

rowgroups(par::ParFile) = par.meta.row_groups
columns(par::ParFile, rowgroupidx::Integer) = columns(par, rowgroups(par)[rowgroupidx])
columns(par::ParFile, rowgroup::RowGroup) = rowgroup.columns
pages(par::ParFile, rowgroupidx::Integer, colidx::Integer) = pages(par, columns(par, rowgroupidx), colidx)
pages(par::ParFile, cols::Vector{ColumnChunk}, colidx::Integer) = pages(par, cols[colidx])
function pages(par::ParFile, col::ColumnChunk)
    (col in keys(par.page_meta_cache)) && (return par.page_meta_cache[col])
    # read pages from the file
    pos = page_offset(col)
    endpos = end_offset(col)
    io = par.handle
    pagevec = PageHeader[]
    while pos < endpos
        seek(io, pos)
        page = read_thrift(io, PageHeader)
        push!(pagevec, page)
        pos = (position(io) + page_size(page))
    end
    par.page_meta_cache[col] = pagevec
    pagevec
end

page_size(page::PageHeader) = Thrift.isfilled(page, :compressed_page_size) ? page.compressed_page_size : page.uncompressed_page_size

function page_offset(col::ColumnChunk)
    colmeta = col.meta_data
    offset = colmeta.data_page_offset
    Thrift.isfilled(colmeta, :index_page_offset) && (offset = min(offset, colmeta.index_page_offset))
    Thrift.isfilled(colmeta, :dictionary_page_offset) && (offset = min(offset, colmeta.dictionary_page_offset))
    offset
end
end_offset(col::ColumnChunk) = page_offset(col) + col.meta_data.total_compressed_size

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

# column metadata
function metadata(io, path::AbstractString, col::ColumnChunk)
    if isfilled(col, :file_path)
        @logmsg("opening file $(col.file_path) to read column metadata at $(col.file_offset)")
        closeio = true
        fio = open(col.file_path)
    else
        if io == nothing
            @logmsg("opening file $path to read column metadata at $(col.file_offset)")
            closeio = true
            fio = open(path)
        else
            @logmsg("reading column metadata at $(col.file_offset)")
            closeio = false
            fio = io
        end
    end
    seek(fio, col.file_offset)
    meta = read_thrift(fio, ColumnMetaData)
    closeio && close(fio)
    meta
end

# file metadata
read_thrift(buff::Array{UInt8}, typ) = read(TCompactProtocol(TMemoryTransport(buff)), typ)
read_thrift(io::IO, typ) = read(TCompactProtocol(TFileTransport(io)), typ)
read_thrift{T<:TTransport}(t::T, typ) = read(TCompactProtocol(t), typ)

function metadata_length(io)
    sz = filesize(io)
    seek(io, sz - SZ_PAR_MAGIC - SZ_FOOTER)

    # read footer size as little endian signed Int32
    ProtoBuf.read_fixed(io, Int32)
end

function metadata(io, path::AbstractString, len::Integer=metadata_length(io))
    @logmsg("metadata len = $len")
    sz = filesize(io)
    seek(io, sz - SZ_PAR_MAGIC - SZ_FOOTER - len)
    meta = read_thrift(io, FileMetaData)
    # go through all column chunks and read metadata from file offsets if required
    for grp in meta.row_groups
        for col in grp.columns
            if !isfilled(col, :meta_data)
                # need to read metadata from an offset
                col.meta_data = metadata(io, path, col)
            end
        end
    end
    meta
end

metadata(par::ParFile) = par.meta
