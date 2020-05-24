import Base: iterate, length, IteratorSize, IteratorEltype, eltype

const TYPES = (Bool, Int32, Int64, Int128, Float32, Float64, String, UInt8)

struct BitPackedIterator
    data::Vector{UInt8}
    bitwidth::Int32
end


iterate(bp::BitPackedIterator) = iterate(bp::BitPackedIterator, 1)

length(bp::BitPackedIterator) = div(8*length(bp.data), bp.bitwidth)

IteratorSize(::Type{BitPackedIterator}) = Base.HasLength()
IteratorEltype(::Type{BitPackedIterator}) = Base.HasEltype()
eltype(::Type{BitPackedIterator}) = UInt

function iterate(bp::BitPackedIterator, state)
    end_bit = state * bp.bitwidth
    end_byte = ceil(Int, end_bit / 8)

    if end_byte > length(bp.data)
        return nothing
    end

    start_bit = (state - 1) * bp.bitwidth + 1

    start_byte, bits_to_drop = divrem(start_bit-1, 8)

    start_byte += 1
    bits_to_drop = bits_to_drop

    # start bit shift the value
    value = UInt(0)

    @inbounds for byte in @view bp.data[end_byte:-1:start_byte]
        value = (value << 8) | byte
    end

    value >>= bits_to_drop

    (value & UInt(2^bp.bitwidth-1), state + 1)
end

function decompress_with_codec(compressed_data::Vector{UInt8}, codec)::Vector{UInt8}
    if codec == PAR2.CompressionCodec.SNAPPY
        uncompressed_data = Snappy.uncompress(compressed_data)
    else
        error("codedc $codec unsupported atm")
    end
end

zero_or_missing(::Type{String}) = missing
zero_or_missing(::Type{T}) where T = zero(T)

function read_column(path, col_num)
    filemetadata = Parquet.metadata(path)
    par = ParFile(path)
    fileio = open(path)

    T = TYPES[filemetadata.schema[col_num+1]._type+1]

    # TODO detect if missing is necessary
    res = Vector{Union{Missing, T}}(undef, nrows(par))
    res .= zero_or_missing(T)

    length(filemetadata.row_groups)

    from = 1
    last_from = from
    for row_group in filemetadata.row_groups
        colchunk_meta = row_group.columns[col_num].meta_data

        if isfilled(colchunk_meta, :dictionary_page_offset)
            seek(fileio, colchunk_meta.dictionary_page_offset)
            dict_page_header = read_thrift(fileio, PAR2.PageHeader)
            compressed_data = read(fileio, dict_page_header.compressed_page_size)
            uncompressed_data = decompress_with_codec(compressed_data, colchunk_meta.codec)
            @assert length(uncompressed_data) == dict_page_header.uncompressed_page_size

            if dict_page_header.dictionary_page_header.encoding == PAR2.Encoding.PLAIN_DICTIONARY
                # see https://github.com/apache/parquet-format/blob/master/Encodings.md#dictionary-encoding-plain_dictionary--2-and-rle_dictionary--8
                # which is in effect the plain encoding see https://github.com/apache/parquet-format/blob/master/Encodings.md#plain-plain--0
                dict = reinterpret(T, uncompressed_data)
            else
                error("Only Plain Dictionary encoding is supported")
            end
        else
            dict = nothing
        end

        # seek to the first data page
        seek(fileio, colchunk_meta.data_page_offset)

        # repeated read data page
        while from - last_from  < row_group.num_rows
            from = read_data_page_vals!(res, fileio, dict, colchunk_meta.codec, T, from) + 1
        end
        last_from = from
    end

    res
end

function read_data_page_vals!(res, fileio::IOStream, dict, codec, T, from::Integer = 1)
    """
    This function assumes
    """

    # the result length is used latter on to prevent writing too much data
    res_len = length(res)

    to = from # intialise to something

    data_page_header = read_thrift(fileio, PAR2.PageHeader)
    compressed_data = read(fileio, data_page_header.compressed_page_size)
    uncompressed_data = decompress_with_codec(compressed_data, codec)
    @assert length(uncompressed_data) == data_page_header.uncompressed_page_size

    # this is made up of these 3 things written back to back
    # * repetition levels - can be ignored for unnested data
    # * definition levels -
    # * values

    # definition levels
    # do_read_defn_lvls = isfilled(data_page_header.data_page_header, :statistics) &&
    #     isfilled(data_page_header.data_page_header.statistics, :null_count) &&
    #     data_page_header.data_page_header.statistics.null_count > 0
    uncompressed_data_io = IOBuffer(uncompressed_data, read=true, write=false, append=false)

    if data_page_header.data_page_header.definition_level_encoding == PAR2.Encoding.RLE
        # for unnested columns the highest possible value for definiton is 1
        # which can represented with just one bit so the bit width is always 1
        bitwidth = 1
        encoded_data_len = read(uncompressed_data_io, UInt32)
        pos_before_encoded_data = position(uncompressed_data_io)
        encoded_data_header = Parquet._read_varint(uncompressed_data_io, UInt32)

        if iseven(encoded_data_header)
            # RLE encoded
            rle_len = Int(encoded_data_header >> 1)
            rle_val = read(uncompressed_data_io, 1)
            pos_after_reading_encoded_data = position(uncompressed_data_io)
        else
            # bitpacked encoded
            bit_pack_len = Int(encoded_data_header >> 1)
        end
    else
        error("encoding not supported")
    end

    @assert pos_after_reading_encoded_data - pos_before_encoded_data == encoded_data_len

    # this is how many values should have been read
    num_values_check = data_page_header.data_page_header.num_values

    # valuess
    if data_page_header.data_page_header.encoding == PAR2.Encoding.PLAIN
        # just return the data as is
        # TODO would it better if take! is done?

        if T == Bool
            # for boolean every bit is a value so the length is 8 times
            digits(UInt8, read(uncompressed_data_io), base=2)
            len_raw_data = 8length(raw_data)
        else
            pos_for_pointer = position(uncompressed_data_io) + 1
            src_ptr = Ptr{T}(pointer(uncompressed_data, pos_for_pointer))
            dest_ptr = Ptr{T}(pointer(res, from))
            # copy content over
            GC.@preserve src_ptr dest_ptr unsafe_copyto!(dest_ptr, src_ptr, num_values_check)
            to = min(from + num_values_check - 1, res_len)


            # raw_data = reinterpret(T, read(uncompressed_data_io))
            # len_raw_data = length(raw_data)
            # to = min(from + len_raw_data - 1, res_len)
            #res[from:to] .= raw_data
        end
    elseif data_page_header.data_page_header.encoding == PAR2.Encoding.PLAIN_DICTIONARY
        # this means the data is encoded in integers format which form the indices to the data
        bitwidth = Int(read(uncompressed_data_io, UInt8))

        # the documented max bitwidth is
        @assert bitwidth <= 32

        while !eof(uncompressed_data_io)
            # println(position(uncompressed_data_io))
            encoded_data_header = Parquet._read_varint(uncompressed_data_io, UInt32)

            if iseven(encoded_data_header)
                # RLE encoded
                rle_len = Int(encoded_data_header >> 1)
                rle_val_vec::Vector{UInt8} = read(uncompressed_data_io, ceil(Int, bitwidth/8))
                rle_val = UInt(0)

                for tmp in @view rle_val_vec[end:-1:1]
                    rle_val = rle_val << 8
                    rle_val = rle_val | tmp
                end

                to = min(from + rle_len - 1, res_len)
                res[from:to] .= dict[rle_val+1]

                from = from + rle_len
            else
                # bitpacked encoded
                bit_pack_len = Int(encoded_data_header >> 1)
                @assert (bit_pack_len >= 1) && (bit_pack_len <= 2^31 - 1)
                bytes_to_read = bitwidth*bit_pack_len
                data = read(uncompressed_data_io, bytes_to_read)
                bp = BitPackedIterator(data, bitwidth)
                # now need a decoding algorithm to break it up
                # reading `bitwidth` bits at a time
                l = length(bp)
                to = min(from + l - 1, res_len)

                for (v, i) in zip(bp, from:to)
                    res[i] = dict[v+1]
                end
                from = from + l
            end
        end
    else
        erorr("encoding not supported")
    end

    to
end
