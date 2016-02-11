
const PAR_MAGIC = "PAR1"
const SZ_PAR_MAGIC = length(PAR_MAGIC)
const SZ_FOOTER = 4
const SZ_VALID_PAR = 2*SZ_PAR_MAGIC + SZ_FOOTER

# page is the unit of compression
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

# schema and helper methods
type Schema
    schema::Vector{SchemaElement}
    name_lookup::Dict{AbstractString,SchemaElement}

    function Schema(elems::Vector{SchemaElement})
        name_lookup = Dict{AbstractString,SchemaElement}()
        for sch in elems
            name_lookup[sch.name] = sch
        end
        new(elems, name_lookup)
    end
end

leafname(schname::AbstractString) = ('.' in schname) ? leafname(split(schname, '.')) : schname
leafname(schname::Vector) = schname[end]
elem(sch::Schema, schname) = sch.name_lookup[leafname(schname)]
isrequired(sch::Schema, schname) = (elem(sch, schname).repetition_type == FieldRepetitionType.REQUIRED)

max_repetition_level(sch::Schema, schname::AbstractString) = max_repetition_level(sch, split(schname, '.'))
max_repetition_level(sch::Schema, schname) = sum([isrequired(sch, namepart) for namepart in schname])

max_definition_level(sch::Schema, schname::AbstractString) = max_definition_level(sch, split(schname, '.'))
max_definition_level(sch::Schema, schname) = sum([!isrequired(sch, namepart) for namepart in schname])

# parquet file.
# Keeps a handle to the open file and the file metadata.
# Holds a LRU cache of raw bytes of the pages read.
type ParFile
    path::AbstractString
    handle::IOStream
    meta::FileMetaData
    schema::Schema
    page_cache::LRU{ColumnChunk,Vector{Page}}
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
    ParFile(path, handle, meta, Schema(meta.schema), LRU{ColumnChunk,Vector{Page}}(maxcache))
end

##
# layer 1 access
# can access raw (uncompressed) bytes from pages

colname(col::ColumnChunk) = colname(col.meta_data)
colname(col::ColumnMetaData) = join(col.path_in_schema, '.')
colnames(rowgroup::RowGroup) = [colname(col) for col in rowgroup.columns]

coltype(col::ColumnChunk) = coltype(col.meta_data)
coltype(col::ColumnMetaData) = col._type

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
    (col in keys(par.page_cache)) && (return par.page_cache[col])
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
    par.page_cache[col] = pagevec
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

##
# layer 2 access
# can access decoded values from pages
# TODO: map dictionary encoded values
function read_levels(io::IO, max_val::Integer, enc::Int32, num_values::Integer)
    bw = bitwidth(max_val)
    (bw == 0) && (return Int[])
    @logmsg("reading levels. enc:$enc ($(Thrift.enumstr(Encoding,enc))), max_val:$max_val, num_values:$num_values")

    if enc == Encoding.RLE
        read_hybrid(io, num_values, bw)
    elseif enc == Encoding.BIT_PACKED
        read_bitpacked_run_old(io, num_values, bw)
    elseif enc == Encoding.PLAIN
        # levels should never be of this type though
        read_plain(io, _Type.INT32, num_values)
    else
        error("unsupported encoding $enc ($(Thrift.enumstr(Encoding,enc))) for levels")
    end
end

function read_values(io::IO, enc::Int32, typ::Int32, num_values::Integer)
    @logmsg("reading values. enc:$enc ($(Thrift.enumstr(Encoding,enc))), num_values:$num_values")

    if enc == Encoding.PLAIN
        read_plain(io, typ, num_values)
    elseif enc == Encoding.PLAIN_DICTIONARY
        read_rle_dict(io, num_values)
    else
        #error("unsupported encoding $enc for pages")
        @logmsg("unsupported encoding $enc ($(Thrift.enumstr(Encoding,enc))) for pages")
        return Int[]
    end
end

function values(par::ParFile, page::Page)
    ctype = coltype(page.colchunk)
    rawbytes = bytes(page)
    io = IOBuffer(rawbytes)
    encs = page_encodings(page)
    num_values = page_num_values(page)
    typ = page.hdr._type

    if (typ == PageType.DATA_PAGE) || (typ == PageType.DATA_PAGE_V2)
        read_levels_and_values(io, encs, ctype, num_values, par, page)
    elseif typ == PageType.DICTIONARY_PAGE
        (read_plain_dict(io, num_values, ctype),)
    else
        ()
    end
end

function read_levels_and_values(io::IO, encs::Tuple, ctype::Int32, num_values::Integer, par::ParFile, page::Page)
    cname = colname(page.colchunk)
    enc, defn_enc, rep_enc = encs

    #@logmsg("before reading defn levels nb_available in page: $(nb_available(io))")
    # read definition levels. skipped if column is required
    defn_levels = isrequired(par.schema, cname) ? Int[] : read_levels(io, max_definition_level(par.schema, cname), defn_enc, num_values)

    #@logmsg("before reading repn levels nb_available in page: $(nb_available(io))")
    # read repetition levels. skipped if all columns are at 1st level
    repn_levels = ('.' in cname) ? read_levels(io, max_repetition_level(par.schema, cname), rep_enc, num_values) : Int[]

    #@logmsg("before reading values nb_available in page: $(nb_available(io))")
    # read values
    vals = read_values(io, enc, ctype, num_values)

    vals, defn_levels, repn_levels
end

# column and page metadata
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

function page_offset(col::ColumnChunk)
    colmeta = col.meta_data
    offset = colmeta.data_page_offset
    Thrift.isfilled(colmeta, :index_page_offset) && (offset = min(offset, colmeta.index_page_offset))
    Thrift.isfilled(colmeta, :dictionary_page_offset) && (offset = min(offset, colmeta.dictionary_page_offset))
    offset
end
end_offset(col::ColumnChunk) = page_offset(col) + col.meta_data.total_compressed_size

page_size(page::PageHeader) = Thrift.isfilled(page, :compressed_page_size) ? page.compressed_page_size : page.uncompressed_page_size

page_encodings(page::Page) = page_encodings(page.hdr)
function page_encodings(page::PageHeader)
    Thrift.isfilled(page, :data_page_header) ? page_encodings(page.data_page_header) :
    Thrift.isfilled(page, :data_page_header_v2) ? page_encodings(page.data_page_header_v2) :
    Thrift.isfilled(page, :dictionary_page_header) ? page_encodings(page.dictionary_page_header) : ()
end
page_encodings(page::DictionaryPageHeader) = (page.encoding,)
page_encodings(page::DataPageHeader) = (page.encoding, page.definition_level_encoding, page.repetition_level_encoding)
page_encodings(page::DataPageHeaderV2) = (page.encoding, Encoding.RLE, Encoding.RLE)

page_num_values(page::Page) = page_num_values(page.hdr)
function page_num_values(page::PageHeader)
    Thrift.isfilled(page, :data_page_header) ? page_num_values(page.data_page_header) :
    Thrift.isfilled(page, :data_page_header_v2) ? page_num_values(page.data_page_header_v2) :
    Thrift.isfilled(page, :dictionary_page_header) ? page_num_values(page.dictionary_page_header) : 0
end
page_num_values(page::Union{DataPageHeader,DataPageHeaderV2,DictionaryPageHeader}) = page.num_values

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
