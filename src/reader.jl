
const PAR_MAGIC = "PAR1"
const SZ_PAR_MAGIC = length(PAR_MAGIC)
const SZ_FOOTER = 4
const SZ_VALID_PAR = 2*SZ_PAR_MAGIC + SZ_FOOTER

# unit of compression
type Page
    colchunk::ColumnChunk
    hdr::PageHeader
    pos::Int
    data::Vector{UInt8}
end

#type Column
#    rg::RowGroup
#    chunks::Vector{ColumnChunk}
#    pages::Vector{Page}
#end

type ParFile
    path::AbstractString
    handle::IOStream
    meta_len::Int32
    meta::FileMetaData
    page_meta_cache::LRU{ColumnChunk,Vector{Page}}
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

function ParFile(path::AbstractString, handle::IOStream; maxcache::Integer=10)
    is_par_file(handle) || error("Not a parquet format file: $path")
    meta_len = metadata_length(handle)
    meta = metadata(handle, path, meta_len)
    ParFile(path, handle, meta_len, meta, LRU{ColumnChunk,Vector{Page}}(maxcache))
end

colname(col::ColumnChunk) = colname(col.meta_data)
colname(col::ColumnMetaData) = join(col.path_in_schema, '.')
colnames(rowgroup::RowGroup) = [colname(col) for col in rowgroup.columns]

# return all rowgroups in the par file
rowgroups(par::ParFile) = par.meta.row_groups

# Return rowgroups that stores all the columns mentioned in `cnames`.
# Returned row groups can be further queried to get the range of rows.
rowgroups(par::ParFile, colname::AbstractString, rowrange::UnitRange=1:typemax(Int64)) = rowgroups(par, [colname], rowrange)
function rowgroups(par::ParFile, cnames, rowrange::UnitRange=1:typemax(Int64))
    R = RowGroup[]
    L = length(cnames)
    beginrow = 1
    for rowgrp in rowgroups(par)
        cnamesrg = colnames(rowgrp)
        found = length(intersect(cnames, cnamesrg))
        endrow = beginrow + rowgrp.num_rows - 1
        (found == L) && (length(intersect(beginrow:endrow)) > 0) && push!(R, rowgrp)
        beginrow = endrow + 1
    end
    R
end

columns(par::ParFile, rowgroupidx::Integer) = columns(par, rowgroups(par)[rowgroupidx])
columns(par::ParFile, rowgroup::RowGroup) = rowgroup.columns
columns(par::ParFile, rowgroup::RowGroup, colname::AbstractString) = columns(par, rowgroup, [colname])
function columns(par::ParFile, rowgroup::RowGroup, cnames)
    R = ColumnChunk[]
    for col in columns(par, rowgroup)
        (colname(col) in cnames) && push!(R, col)
    end
    R
end

pages(par::ParFile, rowgroupidx::Integer, colidx::Integer) = pages(par, columns(par, rowgroupidx), colidx)
pages(par::ParFile, cols::Vector{ColumnChunk}, colidx::Integer) = pages(par, cols[colidx])
function pages(par::ParFile, col::ColumnChunk)
    (col in keys(par.page_meta_cache)) && (return par.page_meta_cache[col])
    # read pages from the file
    pos = page_offset(col)
    endpos = end_offset(col)
    io = par.handle
    pagevec = Page[]
    while pos < endpos
        seek(io, pos)
        pagehdr = read_thrift(io, PageHeader)

        buff = Array(UInt8, page_size(pagehdr))
        page_data_pos = position(io)
        data = read!(io, buff)
        page = Page(col, pagehdr, page_data_pos, data)
        push!(pagevec, page)
        pos = position(io)
    end
    par.page_meta_cache[col] = pagevec
    pagevec
end

function bytes(page::Page, uncompressed::Bool=true)
    data = page.data
    codec = page.colchunk.meta_data.codec
    if uncompressed && (codec != CompressionCodec.UNCOMPRESSED)
        uncompressed_sz = page.hdr.uncompressed_page_size
        if codec == CompressionCodec.SNAPPY
            data = Snappy.uncompress(data)
        elseif codec == CompressionCodec.GZIP
            data = Zlib.decompress(data)
        else
            error("Unknown compression codec for column chunk: $codec")
        end
        (length(data) == uncompressed_sz) || error("failed to uncompress page. expected $(uncompressed_sz), got $(length(data)) bytes")
    end
    data
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
open(par::ParFile, col::ColumnChunk) = open(par.handle, par.path, col)
close(par::ParFile, col::ColumnChunk, io) = (par.handle == io) || close(io)
function open(io, path::AbstractString, col::ColumnChunk)
    if isfilled(col, :file_path)
        @logmsg("opening file $(col.file_path) to read column metadata at $(col.file_offset)")
        open(col.file_path)
    else
        if io == nothing
            @logmsg("opening file $path to read column metadata at $(col.file_offset)")
            open(path)
        else
            @logmsg("reading column metadata at $(col.file_offset)")
            io
        end
    end
end

function metadata(io, path::AbstractString, col::ColumnChunk)
    fio = open(io, path, col)
    seek(fio, col.file_offset)
    meta = read_thrift(fio, ColumnMetaData)
    (fio !== io) && close(fio)
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
