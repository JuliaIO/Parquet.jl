using Tables
using DataAPI
using Thrift
using Snappy
using CodecZstd: ZstdCompressor
using CodecZlib: GzipCompressor
#using CodecLz4: LZ4HCCompressor # wating for CodecLz4.jl devs to fix a bug
using LittleEndianBase128
using Base.Iterators: partition
using CategoricalArrays: CategoricalArray, CategoricalValue

using Base: SkipMissing

if VERSION < v"1.3"
    using Missings: nonmissingtype
end

# a mapping of Julia types to _Type codes in Parquet format
const COL_TYPE_CODE = Dict{DataType, Int32}(
    Bool => PAR2._Type.BOOLEAN,
    Int32 => PAR2._Type.INT32,
    Int64 => PAR2._Type.INT64,
    #INT96 => 3,  // deprecated, only used by legacy implementations. # not supported by Parquet.jl
    Float32 => PAR2._Type.FLOAT,
    Float64 => PAR2._Type.DOUBLE,
    String => PAR2._Type.BYTE_ARRAY, # BYTE_ARRAY
    # FIXED_LEN_BYTE_ARRAY => 7, # current there is no Julia type that we support that maps to this type
    )

function write_thrift(fileio, thrift_obj)
    """write thrift definition to file"""
    pos_before_write = position(fileio)
    p = TCompactProtocol(TFileTransport(fileio))
    Thrift.write(p, thrift_obj)
    pos_after_write = position(fileio)

    size_of_written = pos_after_write - pos_before_write

    size_of_written
end

function compress_using_codec(colvals::AbstractArray, codec::Integer)::Vector{UInt8}
    """Compress `isbits` column types using codec"""
    uncompressed_byte_data = reinterpret(UInt8, colvals) |> collect

    if codec == PAR2.CompressionCodec.UNCOMPRESSED
        return uncompressed_byte_data
    elseif codec == PAR2.CompressionCodec.SNAPPY
        compressed_data = Snappy.compress(uncompressed_byte_data)
    elseif codec == PAR2.CompressionCodec.GZIP
        compressed_data = transcode(GzipCompressor, uncompressed_byte_data)
    elseif codec == PAR2.CompressionCodec.LZ4
        error("lz4 is not supported as data compressed with https://github.com/JuliaIO/CodecLz4.jl can't seem to be read by R or Python. If you know how to fix it please help out.")
        #compressed_data = transcode(LZ4HCCompressor, uncompressed_byte_data)
    elseif codec == PAR2.CompressionCodec.ZSTD
        compressed_data = transcode(ZstdCompressor, uncompressed_byte_data)
    else
        error("not yet implemented")
    end

    return compressed_data
end

function compress_using_codec(colvals::AbstractVector{String}, codec::Int)::Vector{UInt8}
    """Compress `String` column using codec"""
    # the output
    io = IOBuffer()

    # write the values
    for val in colvals
        # for string it needs to be stored as BYTE_ARRAY which needs the length
        # to be the first 4 bytes UInt32
        write(io, val |> sizeof |> UInt32 |> htol)
        # write each of the strings one after another
        write(io, val)
    end

    uncompressed_bytes = take!(io)
    return compress_using_codec(uncompressed_bytes, codec)
end

function write_defn_levels(data_to_compress_io, colvals::AbstractVector{Union{Missing, T}}) where T
    """ A function to write definition levels for `Union{Missing, T}`"""
    # if there is missing
    # use the bit packing algorithm to write the
    # definition_levels
    bytes_needed = ceil(Int, length(colvals) / 8sizeof(UInt8))
    tmp = UInt32((UInt32(bytes_needed) << 1) | 1)
    bitpacking_header = LittleEndianBase128.encode(tmp)

    tmpio = IOBuffer()
    not_missing_bits::BitArray = .!ismissing.(colvals)
    write(tmpio, not_missing_bits)
    seek(tmpio, 0)

    encoded_defn_data = read(tmpio, bytes_needed)

    encoded_defn_data_length = length(bitpacking_header) + bytes_needed
    # write the definition data
    write(data_to_compress_io, UInt32(encoded_defn_data_length) |> htol)
    write(data_to_compress_io, bitpacking_header)
    write(data_to_compress_io, encoded_defn_data)
end

function write_defn_levels(data_to_compress_io, colvals::AbstractVector)
    """ A function to write definition levels for NON-missing data
    """
    # if there is no missing can just use RLE of one
    # using rle
    rle_header = LittleEndianBase128.encode(UInt32(length(colvals)) << 1)
    repeated_value = UInt8(1)
    encoded_defn_data_length = sizeof(rle_header) + sizeof(repeated_value)

    # write the definition data
    write(data_to_compress_io, UInt32(encoded_defn_data_length) |> htol)
    write(data_to_compress_io, rle_header)
    write(data_to_compress_io, repeated_value)
end

# TODO turn this on when writing dictionary is necessary
# function write_col_dict(fileio, colvals::AbstractArray{T}, codec) where T
#     """ write the column dictionary page """
#     # note: `level`s does not return `missing` as a level
#     uvals = DataAPI.levels(colvals)
#
#     # do not support dictionary with more than 127 levels
#     # TODO relax this 127 restriction
#     if length(uvals) > 127
#         @warn "More than 127 levels in dictionary. Parquet.jl does not support this at this stage."
#         return (offset = missing, uncompressed_size = 0, compressed_size = 0)
#     end
#
#     if nonmissingtype(T) == String
#         # the raw bytes of made of on UInt32 to indicate string length
#         # and the content of the string
#         # so the formula for dict size is as below
#         uncompressed_dict_size = sizeof(UInt32)*length(uvals) + sum(sizeof, uvals)
#     else
#         uncompressed_dict_size = length(uvals)*sizeof(eltype(uvals))
#     end
#
#     compressed_uvals::Vector{UInt8} = compress_using_codec(uvals, codec)
#     compressed_dict_size = length(compressed_uvals)
#
#     # TODO do the CRC properly
#     crc = 0
#
#     # construct dictionary metadata
#     dict_page_header = PAR2.PageHeader()
#
#     dict_page_header._type = PAR2.PageType.DICTIONARY_PAGE
#     dict_page_header.uncompressed_page_size = uncompressed_dict_size
#     dict_page_header.compressed_page_size = compressed_dict_size
#     dict_page_header.crc = crc
#
#     dict_page_header.dictionary_page_header = PAR2.DictionaryPageHeader()
#     dict_page_header.dictionary_page_header.num_values = Int32(length(uvals))
#     dict_page_header.dictionary_page_header.encoding = PAR2.Encoding.PLAIN_DICTIONARY
#     dict_page_header.dictionary_page_header.is_sorted = false
#
#     before_write_page_header_pos = position(fileio)
#
#     dict_page_header_size = write_thrift(fileio, dict_page_header)
#
#     # write the dictionary data
#     write(fileio, compressed_uvals)
#
#     return (offset = before_write_page_header_pos, uncompressed_size = uncompressed_dict_size + dict_page_header_size, compressed_size = compressed_dict_size + dict_page_header_size)
# end


write_encoded_data(data_to_compress_io, colvals::AbstractVector{Union{Missing, T}}) where T =
    write_encoded_data(data_to_compress_io, skipmissing(colvals))

function write_encoded_data(data_to_compress_io, colvals::Union{AbstractVector{String}, SkipMissing{S}}) where S <: AbstractVector{Union{Missing, String}}
    """ Write encoded data for String type """
    # write the values
    for val in colvals
        # for string it needs to be stored as BYTE_ARRAY which needs the length
        # to be the first 4 bytes UInt32
        write(data_to_compress_io, val |> sizeof |> UInt32 |> htol)
        # write each of the strings one after another
        write(data_to_compress_io, val)
    end
end

function write_encoded_data(data_to_compress_io, colvals::Union{AbstractVector{Bool}, SkipMissing{S}}) where S <: AbstractVector{Union{Missing, Bool}}
    """ Write encoded data for Bool type """
    # write the bitacpked bits
    # write a bitarray seems to write 8 bytes at a time
    # so write to a tmpio first
    no_missing_bit_vec =  BitArray(colvals)
    bytes_needed = ceil(Int, length(no_missing_bit_vec) / 8sizeof(UInt8))
    tmpio = IOBuffer()
    write(tmpio, no_missing_bit_vec)
    seek(tmpio, 0)
    packed_bits = read(tmpio, bytes_needed)
    write(data_to_compress_io, packed_bits)
end

function write_encoded_data(data_to_compress_io, colvals::AbstractArray)
    """ Efficient write of encoded data for `isbits` types"""
    @assert isbitstype(eltype(colvals))
    write(data_to_compress_io, colvals |> htol)
end

function write_encoded_data(data_to_compress_io, colvals::SkipMissing)
    """ Write of encoded data for skipped missing types"""
    for val in colvals
        write(data_to_compress_io, val |> htol)
    end
end

function write_encoded_data(data_to_compress_io, colvals)
    """ Write of encoded data for the most general type.
    The only requirement is that colvals has to be iterable
    """
    for val in skipmissing(colvals)
        write(data_to_compress_io, val |> htol)
    end
end

# TODO set the encoding code into a dictionary
function write_col_page(fileio, colvals::AbstractArray, codec, ::Val{PAR2.Encoding.PLAIN})
    """
    Write a chunk of data into a data page using PLAIN encoding where the values
    are written back-to-back in memory and then compressed with the codec.
    For `String`s, the values are written with length (UInt32), followed by
    content; it is NOT null terminated.
    """

    # generate the data page header
    data_page_header = PAR2.PageHeader()

    # set up an IO buffer to write to
    data_to_compress_io = IOBuffer()

    # write repetition level data
    ## do nothing
    ## this seems to be related to nested columns
    ## and hence is not needed here as we only supported unnested column write

    # write definition levels
    write_defn_levels(data_to_compress_io, colvals)

    # write the encoded data
    write_encoded_data(data_to_compress_io, colvals)

    data_to_compress::Vector{UInt8} = take!(data_to_compress_io)

    compressed_data::Vector{UInt8} = compress_using_codec(data_to_compress, codec)

    uncompressed_page_size = length(data_to_compress)
    compressed_page_size = length(compressed_data)

    data_page_header._type = PAR2.PageType.DATA_PAGE
    data_page_header.uncompressed_page_size = uncompressed_page_size
    data_page_header.compressed_page_size = compressed_page_size

    # TODO proper CRC
    data_page_header.crc = 0

    data_page_header.data_page_header = PAR2.DataPageHeader()
    data_page_header.data_page_header.num_values = Int32(length(colvals))
    data_page_header.data_page_header.encoding = PAR2.Encoding.PLAIN
    data_page_header.data_page_header.definition_level_encoding = PAR2.Encoding.RLE
    data_page_header.data_page_header.repetition_level_encoding = PAR2.Encoding.RLE

    position_before_page_header_write = position(fileio)

    size_of_page_header_defn_repn = write_thrift(fileio, data_page_header)

    # write data
    write(fileio, compressed_data)

    return (
        offset = position_before_page_header_write,
        uncompressed_size = uncompressed_page_size + size_of_page_header_defn_repn,
        compressed_size = compressed_page_size + size_of_page_header_defn_repn,
    )
end

function write_col_page(fileio, colvals::AbstractArray, codec, ::Val{PAR2.Encoding.PLAIN_DICTIONARY})
    """write Dictionary encoding data page"""
    error("PLAIN_DICTIONARY encoding not implemented yet")

    # TODO finish the implementation
    rle_header = LittleEndianBase128.encode(UInt32(length(colvals)) << 1)
    repeated_value = UInt8(1)

    encoded_defn_data_length = sizeof(rle_header) + sizeof(repeated_value)

    ## write the encoded data length
    write(fileio, encoded_defn_data_length |> UInt32 |> htol)

    write(fileio, rle_header)
    write(fileio, repeated_value)

    position(fileio)

    # write the data

    ## firstly, bit pack it

    # the bitwidth to use
    bitwidth = ceil(UInt8, log(2, length(uvals)))
    # the max bitwidth is 32 according to documentation
    @assert bitwidth <= 32
    # to do that I have to figure out the Dictionary index of it
    # build a JuliaDict
    val_index_dict = Dict(zip(uvals, 1:length(uvals)))

    bitwidth_mask = UInt32(2^bitwidth-1)

    bytes_needed = ceil(Int, bitwidth*length(colvals) / 8)

    bit_packed_encoded_data = zeros(UInt8, bytes_needed)
    upto_byte = 1

    bits_written = 0
    bitsz = 8sizeof(UInt8)

    for val in colvals
        bit_packed_val = UInt32(val_index_dict[val]) & bitwidth_mask
        if bitwidth_mask <= bitsz - bits_written
            bit_packed_encoded_data[upto_byte] = (bit_packed_encoded_data[upto_byte] << bitwidth_mask) | bit_packed_val
        else
            # this must mean
            # bitwidth_mask > bitsz - bits_written
            # if the remaining bits is not enough to write a packed number
            42
        end
    end
end

function write_col_page(fileio, colvals::AbstractArray{T}, codec, encoding) where T
    error("Page encoding $encoding is yet not implemented.")
end

write_col(fileio, colvals::CategoricalArray, args...; kwars...) = begin
    throw("Currently CategoricalArrays are not supported.")
end

function write_col(fileio, colvals::AbstractArray{T}, colname, encoding, codec; nchunks = 1) where T
    """Write a column to a file"""
    # TODO turn writing dictionary on
    # Currently, writing the dictionary page is not turned on for any type.
    # Normally, for Boolean data, dictionary is not supported. However for other
    # data types, dictionary page CAN be supported. However, since Parquet.jl
    # only supports writing PLAIN encoding data, hence there is no need to write
    # a dictionary page until other dictionary-based encodings are supported
    dict_info = (offset = missing, uncompressed_size = 0, compressed_size = 0)

    num_vals_per_chunk = ceil(Int, length(colvals) / nchunks)

    chunk_info = [write_col_page(fileio, val_chunk, codec, Val(encoding)) for val_chunk in partition(colvals, num_vals_per_chunk)]

    sizes = reduce(chunk_info; init = dict_info) do x, y
        (
            uncompressed_size = x.uncompressed_size + y.uncompressed_size,
            compressed_size = x.compressed_size + y.compressed_size
        )
    end

    # write the column metadata
    # can probably write the metadata right after the data chunks
    col_meta = PAR2.ColumnMetaData()

    col_meta._type = COL_TYPE_CODE[eltype(colvals) |> nonmissingtype]
    # these are all the fields
    # TODO collect all the encodings used
    if eltype(colvals) == Bool
        col_meta.encodings = Int32[0, 3]
    else
        col_meta.encodings = Int32[2, 0, 3]
    end
    col_meta.path_in_schema = [colname]
    col_meta.codec = codec
    col_meta.num_values = length(colvals)

    col_meta.total_uncompressed_size = sizes.uncompressed_size
    col_meta.total_compressed_size = sizes.compressed_size

    col_meta.data_page_offset = chunk_info[1].offset
    if !ismissing(dict_info.offset)
        col_meta.dictionary_page_offset = dict_info.offset
    end

    # write the column meta data right after the data
    # keep track of the position so it can put into the column chunk
    # metadata
    col_meta_offset = position(fileio)
    write_thrift(fileio, col_meta)

    # Prep metadata for the filemetadata
    ## column chunk metadata
    col_chunk_meta = PAR2.ColumnChunk()

    col_chunk_meta.file_offset = col_meta_offset
    col_chunk_meta.meta_data = col_meta

    return (
        data_page_offset = chunk_info[1].offset,
        dictionary_page_offset =  dict_info.offset,
        col_chunk_meta = col_chunk_meta,
        col_meta_offset = col_meta_offset
    )
end

function create_schema_parent_node(ncols)
    """Create the parent node in the schema tree"""
    schmea_parent_node = PAR2.SchemaElement()
    schmea_parent_node.name = "schema"
    schmea_parent_node.num_children = ncols
    schmea_parent_node
end

function create_col_schema(type, colname)
    """Create a column node in the schema tree for non-strings"""
    schema_node = PAR2.SchemaElement()
    # look up type code
    schema_node._type = COL_TYPE_CODE[type |> nonmissingtype]
    schema_node.repetition_type = 1
    schema_node.name = colname
    schema_node.num_children = 0

    schema_node
end


function create_col_schema(type::Type{String}, colname)
    """create col schema for string"""
    schema_node = PAR2.SchemaElement()
    # look up type code
    schema_node._type = COL_TYPE_CODE[type]
    schema_node.repetition_type = 1
    schema_node.name = colname
    schema_node.num_children = 0

    # for string set converted type to UTF8
    schema_node.converted_type = PAR2.ConvertedType.UTF8

    logicalType = PAR2.LogicalType()
    logicalType.STRING = PAR2.StringType()

    schema_node.logicalType = logicalType

    schema_node
end


"""
    Write a parquet file from a Tables.jl compatible table e.g DataFrame

io                  -   A writable IO stream
tbl                 -   A Tables.jl columnaccessible table e.g. a DataFrame
compression_code    -   Default "SNAPPY". The compression codec. The supported
                        values are "UNCOMPRESSED", "SNAPPY", "ZSTD", "GZIP"
"""
function write_parquet(io::IO, x; compression_codec = "SNAPPY")
    tbl = Tables.Columns(x)

    # check that all types are supported
    sch = Tables.schema(tbl)
    err_msgs = String[]
    for type in sch.types
        if type <: CategoricalValue
            push!(err_msgs, "CategoricalArrays are not supported at this stage. \n")
        elseif !(nonmissingtype(type) <: Union{Int32, Int64, Float32, Float64, Bool, String})
            push!(err_msgs, "Column whose `eltype` is $type is not supported at this stage. \n")
        end
    end

    err_msgs = unique(err_msgs)
    if length(err_msgs) > 0
        throw(reduce(*, err_msgs))
    end

    # set the data page encoding
    # currently only PLAIN is supported
    # TODO add support for other encodings see
    # https://github.com/apache/parquet-format/blob/master/Encodings.md
    encoding = Encoding.PLAIN

    # convert a string or symbol compression codec into the numeric code
    codec = getproperty(PAR2.CompressionCodec, Symbol(uppercase(string(compression_codec))))

    # figure out the right number of chunks
    # TODO test that it works for all supported table
    nrows = Tables.rowcount(tbl)
    sample_size = min(100, nrows)
    rs = collect(Iterators.take(Tables.namedtupleiterator(tbl), sample_size))
    table_size_bytes = Base.summarysize(rs) / sample_size * nrows

    approx_raw_to_parquet_compression_ratio = 6
    approx_post_compression_size = (table_size_bytes / 2^30) / approx_raw_to_parquet_compression_ratio

    # if size is larger than 64mb and has more than 6 rows
    if (approx_post_compression_size > 0.064) & (nrows > 6)
        recommended_chunks = ceil(Int, approx_post_compression_size / 6) * 6
    else
        recommended_chunks = 1
    end

    colnames = String.(Tables.columnnames(tbl))
    _write_parquet(
        io,
        tbl,
        Tables.columnnames(tbl),
        recommended_chunks;
        encoding    =   Dict(col => encoding for col in colnames),
        codec       =   Dict(col => codec for col in colnames)
    )
end

"""
    Write a parquet file from a Tables.jl compatible table e.g DataFrame

path                -   The file path
tbl                 -   A Tables.jl columnaccessible table e.g. a DataFrame
compression_code    -   Default "SNAPPY". The compression codec. The supported
                        values are "UNCOMPRESSED", "SNAPPY", "ZSTD", "GZIP"
"""
function write_parquet(path, x; compression_codec = "SNAPPY")
    open(path, "w") do io
        write_parquet(io, x; compression_codec=compression_codec)
    end
end

function _write_parquet(io::IO, itr_vectors, colnames, nchunks; ncols = length(itr_vectors), encoding::Dict{String, Int32}, codec::Dict{String, Int32})

    """Internal method for writing parquet

    itr_vectors -   An iterable of `AbstractVector`s containing the values to be
                    written
    colnames    -   Column names for each of the vectors
    path        -   The output parquet file path
    nchunks     -   The number of chunks/pages to write for each column
    ncols       -   The number of columns. This is provided as an argument for
                    the case where the `length(itr_vectors)` is not defined,
                    e.g. lazy loading of remote resources.
    encoding    -   A dictionary mapping from column names to encoding
    codec       -   A dictionary mapping from column names to compression codec
    """
    write(io, "PAR1")

    # the + 1 comes from the fact that schema is a tree and there is an extra
    # parent node
    schemas = Vector{PAR2.SchemaElement}(undef, ncols + 1)
    schemas[1] = create_schema_parent_node(ncols)
    col_chunk_metas = Vector{PAR2.ColumnChunk}(undef, ncols)
    row_group_file_offset = missing

    # write the columns one by one
    # TODO parallelize this
    nrows = -1 # initialize it
    for (coli, (colname_sym, colvals)) in enumerate(zip(colnames, itr_vectors))
        colname = String(colname_sym)

        col_encoding = encoding[colname]
        col_codec = codec[colname]
        # write the data including metadata
        col_info = write_col(io, colvals, colname, col_encoding, col_codec; nchunks = nchunks)

        # the `row_group_file_offset` keeps track of where the data starts, so
        # keep it at the dictonary of the first data
        if coli == 1
            nrows = length(colvals)
            if ismissing(col_info.dictionary_page_offset)
                row_group_file_offset = col_info.data_page_offset
            else
                row_group_file_offset = col_info.dictionary_page_offset
            end
        end

        col_chunk_metas[coli] = col_info.col_chunk_meta

        # add the schema
        schemas[coli + 1] = create_col_schema(eltype(colvals) |> nonmissingtype, colname)
    end

    # now all the data is written we write the filemetadata
    # finalise it by writing the filemetadata
    filemetadata = PAR2.FileMetaData()
    filemetadata.version = 1
    filemetadata.schema = schemas
    filemetadata.num_rows = nrows
    filemetadata.created_by = "Parquet.jl $(Parquet.PARQUET_JL_VERSION)"

    # create row_groups
    # TODO do multiple row_groups
    row_group = PAR2.RowGroup()

    row_group.columns = col_chunk_metas
    row_group.total_byte_size = Int64(sum(x->x.meta_data.total_compressed_size, col_chunk_metas))
    row_group.num_rows = nrows
    if ismissing(row_group_file_offset)
        error("row_group_file_offset is not set")
    else
        row_group.file_offset = row_group_file_offset
    end
    row_group.total_compressed_size = Int64(sum(x->x.meta_data.total_compressed_size, col_chunk_metas))

    filemetadata.row_groups = [row_group]

    filemetadata_size = write_thrift(io, filemetadata)

    write(io, UInt32(filemetadata_size) |> htol)
    write(io, "PAR1")
end
