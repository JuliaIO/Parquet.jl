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

function read_column(path, col_num)
    filemetadata = Parquet.metadata(path)
    par = ParFile(path)
    fileio = open(path)

    T = TYPES[filemetadata.schema[col_num+1]._type+1]

    # TODO detect if missing is necessary
    if T == String
        # the memory structure of String is different to other supported types
        # so it's better to initialise it with missing
        res = Vector{Union{Missing, String}}(missing, nrows(par))
    else
        res = Vector{Union{Missing, T}}(undef, nrows(par))
    end

    from = 1
    last_from = from

    j = 1
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
                if T == String
                    dict = Vector{String}(undef, dict_page_header.dictionary_page_header.num_values)
                    uncompressed_data_io = IOBuffer(uncompressed_data)
                    j = 1
                    while !eof(uncompressed_data_io)
                        str_len = read(uncompressed_data_io, UInt32)
                        dict[j] = String(read(uncompressed_data_io, str_len))
                        j += 1
                    end
                else
                    dict = reinterpret(T, uncompressed_data)
                end
            else
                error("Only Plain Dictionary encoding is supported")
            end
        else
            dict = nothing
        end

        # seek to the first data page
        seek(fileio, colchunk_meta.data_page_offset)

        # repeated read data page

        while (from - last_from  < row_group.num_rows) & (from <= length(res))
            from = read_data_page_vals!(res, fileio, dict, colchunk_meta.codec, T, from) + 1
        end
        last_from = from

        # (j == 1) && return res
        j += 1

    end

    res
end

function read_data_page_vals!(res, fileio::IOStream, dict, codec, T, from::Integer = 1)
    """
    Read one data page
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

    uncompressed_data_io = IOBuffer(uncompressed_data, read=true, write=false, append=false)

    # this will be set in future
    has_missing = false

    # the number of values stored in this page
    num_values = data_page_header.data_page_header.num_values

    # definition levels
    # do_read_defn_lvls = isfilled(data_page_header.data_page_header, :statistics) &&
    #     isfilled(data_page_header.data_page_header.statistics, :null_count) &&
    #     data_page_header.data_page_header.statistics.null_count > 0
    if data_page_header.data_page_header.definition_level_encoding == PAR2.Encoding.RLE
        # for unnested columns the highest possible value for definiton is 1
        # which can represented with just one bit so the bit width is always 1
        bitwidth = 1
        encoded_data_len = read(uncompressed_data_io, UInt32)
        pos_before_encoded_data = position(uncompressed_data_io)
        encoded_data_header = Parquet._read_varint(uncompressed_data_io, UInt32)

        # TODO it's possible to be mixing RLE and bitpacked in one algorithm
        if iseven(encoded_data_header)
            # RLE encoded
            rle_len = Int(encoded_data_header >> 1)
            rle_val = read(uncompressed_data_io, UInt8)

            pos_after_reading_encoded_data = position(uncompressed_data_io)

            if T == String
                # strings memoery are stored differently so can't benefit from this
            else
                # fill the memory location with all missing
                GC.@preserve res begin
                    dest_ptr = Ptr{UInt8}(pointer(res, res_len+1)) + from - 1
                    tmparray = unsafe_wrap(Vector{UInt8}, dest_ptr, num_values)
                    fill!(tmparray, rle_val)
                end
            end
        else
            # the only reaosn to use bitpacking is because there are missings
            has_missing = true

            # bitpacked encoded
            bit_pack_len = Int(encoded_data_header >> 1)

            bytes_to_read = bitwidth*bit_pack_len
            data = read(uncompressed_data_io, bytes_to_read)

            pos_after_reading_encoded_data = position(uncompressed_data_io)

            # the structure of Vector{Union{T, Missing}} is
            # * the `values::T` first
            # * the missing are stored with UInt8(0) for missing
            # * and UInt8(1) otherwise
            # see https://docs.julialang.org/en/v1/devdocs/isbitsunionarrays/

            # TODO I suspect this is not the fastest way to unpack bitwidth = 1
            # data
            @assert bitwidth == 1
            bp = BitPackedIterator(data, bitwidth)

            missing_bytes::Vector{UInt8} = BitPackedIterator(data, bitwidth) |> collect

            if T == String
                # do nothing
            else
                GC.@preserve missing_bytes res begin
                    src_ptr = Ptr{UInt8}(pointer(missing_bytes))
                    dest_ptr = Ptr{UInt8}(pointer(res, res_len+1)) + from - 1
                    # copy content over
                    unsafe_copyto!(dest_ptr, src_ptr, res_len)
                end
            end
        end
    else
        error("no definition encoding not supported")
    end

    # this line ensures that we have read all the encoded definition data
    @assert pos_after_reading_encoded_data - pos_before_encoded_data == encoded_data_len

    # read values
    if data_page_header.data_page_header.encoding == PAR2.Encoding.PLAIN
        # just return the data as is
        if T == Bool
            to = min(from + num_values - 1, res_len)

            if has_missing
                upto = 1
                raw_data = Vector{Bool}(undef, 8)
                for (i, missing_byte) in zip(from:to, missing_bytes)
                    if missing_byte == 1
                        if upto == 1
                            digits!(raw_data, read(uncompressed_data_io, UInt8), base=2)
                        end
                        res[i] = raw_data[upto]
                        upto += 1
                        if upto == 9
                            upto = 1
                        end
                    end
                end
            else
                # for boolean every bit is a value so the length is 8 times
                i = from
                while !eof(uncompressed_data_io)
                    udi = read(uncompressed_data_io, UInt8)
                    raw_data = Base.unsafe_wrap(Vector{Bool}, pointer(res, i) |>  Ptr{Bool}, (8,))
                    digits!(raw_data, udi, base=2)

                    if i + 8 - 1 <= res_len
                        digits!(raw_data, udi, base=2)
                        i += 8
                    else
                        for rd in digits(Bool, udi, base=2, pad = 8)
                            if i <= res_len
                                res[i] = rd
                            end
                            i += 1
                        end
                    end
                end
            end
        elseif T == String
            to = min(from + num_values - 1, res_len)
            if has_missing
                for (i, missing_byte) in zip(from:to, missing_bytes)
                    if missing_byte == 1
                        # 1 means not missing
                        str_len = read(uncompressed_data_io, UInt32)
                        res[i] = String(read(uncompressed_data_io, str_len))
                    end
                end
            else
                i = from
                while !eof(uncompressed_data_io)
                    str_len = read(uncompressed_data_io, UInt32)
                    res[i] = String(read(uncompressed_data_io, str_len))
                    i = i + 1
                end
            end

        else
            if has_missing
                raw_data = reinterpret(T, read(uncompressed_data_io))
                to = min(from + num_values - 1, res_len)

                j = 1
                for (i, missing_byte) in zip(from:to, missing_bytes)
                    if missing_byte == 1
                        # 1 means not missing
                        res[i] = raw_data[j]
                        j += 1
                    end
                end
            else
                # if there is no missing, can just copy the data into the
                # right memory location
                # the copying approach is alot faster than the commented out
                # assignment approach
                pos_for_pointer = position(uncompressed_data_io) + 1
                src_ptr = Ptr{T}(pointer(uncompressed_data, pos_for_pointer))
                dest_ptr = Ptr{T}(pointer(res, from))
                # copy content over
                GC.@preserve src_ptr dest_ptr unsafe_copyto!(dest_ptr, src_ptr, num_values)
                to = min(from + num_values - 1, res_len)
            end
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
