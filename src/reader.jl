
const PAR_MAGIC = "PAR1"
const SZ_PAR_MAGIC = length(PAR_MAGIC)
const SZ_FOOTER = 4
const SZ_VALID_PAR = 2*SZ_PAR_MAGIC + SZ_FOOTER

# page is the unit of compression
mutable struct Page
    colchunk::ColumnChunk
    hdr::PageHeader
    pos::Int
    uncompressed_data::Vector{UInt8}
    nextpos::Int64
end

"""
Keeps a cache of pages read from a file.
Pages are kept as weak refs, so that they can be collected when there's memory pressure.
"""
struct PageLRU
    refs::Dict{Tuple{ColumnChunk,Int64},WeakRef}
    lck::ReentrantLock
    function PageLRU()
        new(Dict{Tuple{ColumnChunk,Int64},WeakRef}(), ReentrantLock())
    end
end

function cacheget(fetcher, lru::PageLRU, chunk::ColumnChunk, startpos::Int64)
    key = (chunk,startpos)
    lock(lru.lck) do
        filter!(kv->(kv[2].value !== nothing), lru.refs)
        page = haskey(lru.refs, key) ? lru.refs[key].value : nothing
        if page === nothing
            page = fetcher()::Page
            lru.refs[key] = WeakRef(page)
        end
        return page
    end
end

"""
    Parquet.File(path; map_logical_types) => Parquet.File

Represents a Parquet file at `path` open for reading. Options to map logical types can be provided via `map_logical_types`.

`map_logical_types` can be one of:

- `false`: no mapping is done (default)
- `true`: default mappings are attempted on all columns (bytearray => String, int96 => DateTime)
- A user supplied dict mapping column names to a tuple of type and a converter function

Returns a `Parquet.File` type that keeps a handle to the open file and the file metadata and also holds a LRU cache of raw bytes of the pages read.
"""
mutable struct File
    path::String
    handle::IOStream
    meta::FileMetaData
    schema::Schema
    page_cache::PageLRU
end

function File(path::AbstractString; map_logical_types::Dict=TLogicalTypeMap())
    f = open(path)
    try
        return File(path, f; map_logical_types=map_logical_types)
    catch ex
        close(f)
        rethrow()
    end
end

function File(path::AbstractString, handle::IOStream; map_logical_types::Dict=TLogicalTypeMap())
    is_par_file(handle) || error("Not a parquet format file: $path")
    meta_len = metadata_length(handle)
    meta = metadata(handle, path, meta_len)
    typemap = merge!(TLogicalTypeMap(), map_logical_types)
    file = File(String(path), handle, meta, Schema(meta.schema, typemap), PageLRU())
    finalizer(close, file)
    file
end

function close(par::Parquet.File)
    empty!(par.page_cache.refs)
    close(par.handle)
end

schema(par::Parquet.File) = par.schema

colname(par::Parquet.File, col::ColumnChunk) = colname(metadata(par,col))
colname(col::ColumnMetaData) = col.path_in_schema
function colnames(par::Parquet.File)
    names = Vector{Vector{String}}()
    cs = Int[]
    ns = String[]
    for x in par.schema.schema[2:end]
        if Parquet.num_children(x) > 0
            push!(cs, x.num_children)
            push!(ns, x.name)
        else
            if !isempty(cs)
                push!(names, [ns; x.name])
                cs[end] -= 1
                if cs[end] == 0
                    pop!(cs)
                    pop!(ns)
                end
            else
                push!(names, [x.name])
            end
        end
    end
    names
end

ncols(par::Parquet.File) = length(colnames(par))
nrows(par::Parquet.File) = par.meta.num_rows

coltype(par::Parquet.File, col::ColumnChunk) = coltype(metadata(par,col))
coltype(col::ColumnMetaData) = col._type

# return all rowgroups in the par file
rowgroups(par::Parquet.File) = par.meta.row_groups

function rowgroup_row_positions(par::Parquet.File)
    rgs = rowgroups(par)
    positions = Array{Int64}(undef, length(rgs)+1)
    idx = 1
    positions[idx] = Int64(1)
    for rg in rgs
        positions[idx+=1] = rg.num_rows
    end
    cumsum!(positions, positions)
end

columns(par::Parquet.File, rowgroupidx) = columns(par, rowgroups(par)[rowgroupidx])
columns(par::Parquet.File, rowgroup::RowGroup) = rowgroup.columns
columns(par::Parquet.File, rowgroup::RowGroup, colname::Vector{String}) = columns(par, rowgroup, [colname])
function columns(par::Parquet.File, rowgroup::RowGroup, cnames::Vector{Vector{String}})
    R = ColumnChunk[]
    for col in columns(par, rowgroup)
        (colname(par,col) in cnames) && push!(R, col)
    end
    R
end

##
# Iterator for pages in a column chunk
mutable struct ColumnChunkPages
    par::Parquet.File
    col::ColumnChunk
    startpos::Int64
    endpos::Int64

    function ColumnChunkPages(par::Parquet.File, col::ColumnChunk)
        startpos = page_offset(par, col)
        endpos = end_offset(par, col)
        new(par, col, startpos, endpos)
    end
end
eltype(::Type{ColumnChunkPages}) = Page
Base.iterate(ccp::ColumnChunkPages) = iterate(ccp, ccp.startpos)
function Base.iterate(ccp::ColumnChunkPages, startpos::Int64)
    if startpos >= ccp.endpos
        return nothing
    end

    page = cacheget(ccp.par.page_cache, ccp.col, startpos) do
        par = ccp.par
        io = par.handle
        seek(io, startpos)
        pagehdr = read_thrift(io, PageHeader)

        page_data_pos = position(io)
        pagesz = page_size(pagehdr)
        data = _use_mmap[] ? Mmap.mmap(io, Vector{UInt8}, (pagesz,), page_data_pos; grow=false, shared=false) : read!(io, Array{UInt8}(undef, pagesz))
        codec = metadata(ccp.par, ccp.col).codec

        if (codec != CompressionCodec.UNCOMPRESSED)
            uncompressed_sz = pagehdr.uncompressed_page_size
            #uncompressed_data = _use_mmap[] ? Mmap.mmap(Mmap.Anonymous(), Vector{UInt8}, (uncompressed_sz,), 0) : Array{UInt8}(undef, uncompressed_sz)
            uncompressed_data = Array{UInt8}(undef, uncompressed_sz)
            if codec == CompressionCodec.SNAPPY
                Snappy.snappy_uncompress(data, uncompressed_data)
            elseif codec == CompressionCodec.GZIP
                readbytes!(GzipDecompressorStream(IOBuffer(data)), uncompressed_data)
            elseif codec == CompressionCodec.ZSTD
                readbytes!(ZstdDecompressorStream(IOBuffer(data)), uncompressed_data)
            else
                error("Unknown compression codec for column chunk: $codec")
            end
            (length(uncompressed_data) == uncompressed_sz) || error("failed to uncompress page. expected $(uncompressed_sz), got $(length(uncompressed_data)) bytes")
        else
            uncompressed_data = data
        end

        nextpos = page_data_pos + pagesz
        page = Page(ccp.col, pagehdr, page_data_pos, uncompressed_data, nextpos)
    end

    page, page.nextpos
end

##
# Iterator for page values in a column chunk
mutable struct ColumnChunkPageValues{T}
    ccp::ColumnChunkPages
    max_repn::Int64
    max_defn::Int64
    has_repn_levels::Bool
    has_defn_levels::Bool
    repn_out::OutputState{Int32}
    defn_out::OutputState{Int32}
    valdict_out::OutputState{T}
    vals_out::OutputState{T}
    converter_fn::Function
end

function ColumnChunkPageValues(par::Parquet.File, col::ColumnChunk, ::Type{T}, converter_fn::Function=identity) where {T}
    cname = colname(par, col)

    max_repn = max_repetition_level(par.schema, cname)
    max_defn = max_definition_level(par.schema, cname)
    has_repn_levels = ((length(cname) > 1) && (max_repn > 0))
    has_defn_levels = !isrequired(par.schema, cname)

    repn_out = OutputState(Int32, 0)
    defn_out = OutputState(Int32, 0)
    valdict_out = OutputState(T, 0)
    vals_out = OutputState(T, 0)

    ccp = ColumnChunkPages(par, col)
    ColumnChunkPageValues{T}(ccp, max_repn, max_defn, has_repn_levels, has_defn_levels, repn_out, defn_out, valdict_out, vals_out, converter_fn)
end

function eltype(::Type{ColumnChunkPageValues{T}}) where {T}
   NamedTuple{(:value,:repn_level,:defn_level),Tuple{OutputState{T},OutputState{Int32},OutputState{Int32}}}
end

function Base.iterate(ccpv::ColumnChunkPageValues{T}) where {T}
    iterate(ccpv, ccpv.ccp.startpos)
end

function map_dict_vals(valdict::OutputState{T1}, vals::OutputState{T1}, map_vals::Vector{T2}) where {T1, T2}
    if !isempty(valdict.data) && (valdict.offset > 0)
        num_values = length(map_vals)
        @inbounds for idx in 1:num_values
            vals.data[idx] = valdict.data[map_vals[idx]+1]
        end
        vals.offset += num_values
    end
end

function Base.iterate(ccpv::ColumnChunkPageValues{T}, startpos::Int64) where {T}
    if startpos >= ccpv.ccp.endpos
        return nothing
    end

    read_data_page = false
    nextpos = startpos

    while !read_data_page
        page, nextpos = iterate(ccpv.ccp, nextpos)

        pagetype = page.hdr._type
        num_values = page_num_values(page)
        inp = InputState(page.uncompressed_data, 0)

        if (pagetype === PageType.DATA_PAGE) || (pagetype === PageType.DATA_PAGE_V2)
            read_data_page = true
            ccpv.has_repn_levels && reset_to_size(ccpv.repn_out, num_values)
            ccpv.has_defn_levels && reset_to_size(ccpv.defn_out, num_values)

            @debug("reading a data page for columnchunk")
            enc, defn_enc, repn_enc = page_encodings(page)
            nmissing = read_levels_and_nmissing(inp, ccpv.defn_out, ccpv.repn_out, defn_enc, repn_enc, Int(ccpv.max_defn), Int(ccpv.max_repn), num_values)
            nnonmissing = num_values - nmissing
            reset_to_size(ccpv.vals_out, nnonmissing)

            if enc === Encoding.PLAIN_DICTIONARY || enc === Encoding.RLE_DICTIONARY
                map_vals = read_data_dict(inp, nnonmissing)
                map_dict_vals(ccpv.valdict_out, ccpv.vals_out, map_vals)
            else
                if ccpv.converter_fn === identity
                    read_plain_values(inp, ccpv.vals_out, nnonmissing)
                else
                    read_plain_values(inp, ccpv.vals_out, nnonmissing, ccpv.converter_fn, ccpv.ccp.col.meta_data._type)
                end
            end
        elseif pagetype === PageType.DICTIONARY_PAGE
            ensure_additional_size(ccpv.valdict_out, num_values)
            if ccpv.converter_fn === identity
                read_plain_values(inp, ccpv.valdict_out, num_values)
            else
                read_plain_values(inp, ccpv.valdict_out, num_values, ccpv.converter_fn, ccpv.ccp.col.meta_data._type)
            end
        else
            error("unsupported page type $typ")
        end
    end
    iterator_result = NamedTuple{(:value,:repn_level,:defn_level),Tuple{OutputState{T},OutputState{Int32},OutputState{Int32}}}((ccpv.vals_out, ccpv.repn_out, ccpv.defn_out))
    iterator_result, nextpos
end

##
# layer 2 access
# can access decoded values from pages
function read_levels(inp::InputState, out::OutputState{Int32}, max_val::Int, enc::Int32, num_values::Int32) # levels are always 32 bits
    bit_width = UInt8(@bitwidth(max_val))
    @assert(bit_width !== 0)
    #@debug("reading levels. enc:$enc ($(Thrift.enumstr(Encoding,enc))), max_val:$max_val, num_values:$num_values")

    if enc === Encoding.RLE
        byte_width = @bit2bytewidth(bit_width)
        read_hybrid(inp, out, num_values, bit_width, byte_width)
    elseif enc === Encoding.BIT_PACKED
        read_bitpacked_run_old(inp, out, num_values, bit_width)
    else
        error("unsupported encoding $enc ($(Thrift.enumstr(Encoding,enc))) for levels")
    end
end

function read_levels_and_nmissing(inp::InputState, defn_out::OutputState{Int32}, repn_out::OutputState{Int32}, defn_enc::Int32, repn_enc::Int32, max_defn::Int, max_repn::Int, num_values::Int32)
    # read repetition levels. skipped if all columns are at 1st level
    if !isempty(repn_out.data)
        read_levels(inp, repn_out, max_repn, repn_enc, num_values)
    end

    # read definition levels. skipped if column is required
    nmissing = Int32(0)
    if !isempty(defn_out.data)
        defn_levels = defn_out.data
        defn_offset = defn_out.offset
        read_levels(inp, defn_out, max_defn, defn_enc, num_values)
        @inbounds for idx in 1:num_values
            (defn_levels[idx+defn_offset] === Int32(0)) && (nmissing += Int32(1))
        end
    end

    nmissing
end


# column and page metadata
open(par::Parquet.File, col::ColumnChunk) = open(par.handle, par.path, col)
close(par::Parquet.File, col::ColumnChunk, io) = (par.handle == io) || close(io)
function open(io, path::AbstractString, col::ColumnChunk)
    if hasproperty(col, :file_path)
        @debug("opening file to read column metadata", file=col.file_path, offset=col.file_offset)
        open(col.file_path)
    else
        if io === nothing
            @debug("opening file to read column metadata", file=path, offset=col.file_offset)
            open(path)
        else
            @debug("reading column metadata", offset=col.file_offset)
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

function page_offset(par::Parquet.File, col::ColumnChunk)
    colmeta = metadata(par, col)
    offset = colmeta.data_page_offset
    hasproperty(colmeta, :index_page_offset) && (offset = min(offset, colmeta.index_page_offset))
    hasproperty(colmeta, :dictionary_page_offset) && (offset = min(offset, colmeta.dictionary_page_offset))
    offset
end
end_offset(par::Parquet.File, col::ColumnChunk) = page_offset(par, col) + metadata(par,col).total_compressed_size

page_size(page::PageHeader) = hasproperty(page, :compressed_page_size) ? page.compressed_page_size : page.uncompressed_page_size

const INVALID_ENC = Int32(-1)
page_encodings(page::Page) = page_encodings(page.hdr)
function page_encodings(page::PageHeader)
    hasproperty(page, :data_page_header) ? page_encodings(page.data_page_header) :
    hasproperty(page, :data_page_header_v2) ? page_encodings(page.data_page_header_v2) :
    hasproperty(page, :dictionary_page_header) ? page_encodings(page.dictionary_page_header) :
    (INVALID_ENC,INVALID_ENC,INVALID_ENC)
end
page_encodings(page::DictionaryPageHeader) = (page.encoding,INVALID_ENC,INVALID_ENC)
page_encodings(page::DataPageHeader) = (page.encoding, page.definition_level_encoding, page.repetition_level_encoding)
page_encodings(page::DataPageHeaderV2) = (page.encoding, Encoding.RLE, Encoding.RLE)

page_num_values(page::Page) = page_num_values(page.hdr)
function page_num_values(page::PageHeader)
    hasproperty(page, :data_page_header) ? page_num_values(page.data_page_header) :
    hasproperty(page, :data_page_header_v2) ? page_num_values(page.data_page_header_v2) :
    hasproperty(page, :dictionary_page_header) ? page_num_values(page.dictionary_page_header) : Int32(0)
end
page_num_values(page::Union{DataPageHeader,DataPageHeaderV2,DictionaryPageHeader}) = page.num_values

# file metadata
read_thrift(buff::Array{UInt8}, ::Type{T}) where {T} = read(TCompactProtocol(TMemoryTransport(buff)), T)
read_thrift(io::IO, ::Type{T}) where {T} = read(TCompactProtocol(TFileTransport(io)), T)
read_thrift(t::TR, ::Type{T}) where {TR<:TTransport,T} = read(TCompactProtocol(t), T)

function metadata_length(io)
    sz = filesize(io)
    seek(io, sz - SZ_PAR_MAGIC - SZ_FOOTER)

    # read footer size as little endian signed Int32
    read_fixed(io, Int32)
end

function metadata(io, path::AbstractString, len::Integer=metadata_length(io))
    @debug("reading file metadata", len)
    sz = filesize(io)
    seek(io, sz - SZ_PAR_MAGIC - SZ_FOOTER - len)
    meta = read_thrift(io, FileMetaData)
    meta
end

metadata(par::Parquet.File) = par.meta

#=
function fill_column_metadata(par::Parquet.File)
    meta = par.meta
    # go through all column chunks and read metadata from file offsets if required
    for grp in meta.row_groups
        for col in grp.columns
            metadata(par, col)
        end
    end
end
=#

function metadata(par::Parquet.File, col::ColumnChunk)
    if !hasproperty(col, :meta_data)
        col.meta_data = metadata(par.handle, par.path, col)
    end
    col.meta_data
end

# file format verification
function is_par_file(fname::AbstractString)
    open(fname) do io
        return is_par_file(io)
    end
end

function is_par_file(io)
    sz = filesize(io)
    (sz > SZ_VALID_PAR) || return false

    seekstart(io)
    magic = Array{UInt8}(undef, 4)
    read!(io, magic)
    (String(magic) == PAR_MAGIC) || return false
    
    seek(io, sz - SZ_PAR_MAGIC)
    magic = Array{UInt8}(undef, 4)
    read!(io, magic)
    (String(magic) == PAR_MAGIC) || return false

    true
end
