import Base: iterate, length, IteratorSize, IteratorEltype, eltype, @_gc_preserve_begin, @_gc_preserve_end

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

function decompress_with_codec!(uncompressed_data::Vector{UInt8}, compressed_data::Vector{UInt8}, codec)
    if codec == PAR2.CompressionCodec.SNAPPY
        Snappy.snappy_uncompress(compressed_data, uncompressed_data)
    else
        error("codedc $codec unsupported atm")
    end
end

read_column(path, col_num) = begin
    filemetadata = Parquet.metadata(path)
    read_column(path, filemetadata, col_num)
end

function read_column(path, filemetadata, col_num)
    T = TYPES[filemetadata.schema[col_num+1]._type+1]

    par = ParFile(path)
    # TODO detect if missing is necessary
    if T == String
        # the memory structure of String is different to other supported types
        # so it's better to initialise it with missing
        res = Vector{Union{Missing, String}}(missing, nrows(par))
    else
        res = Vector{Union{Missing, T}}(undef, nrows(par))
    end
    close(par)

    fileio = open(path)

    # I thnk there is a bug with Julia's multithreaded reads
    # which can be fixed by doing the below
    # DO NOT remove the code below or multithreading will fail
    println("$(position(fileio))")
    # if true
    # not_used = open(tempname()*string(col_num), "w")
    # write(not_used, position(fileio))
    # close(not_used)
    # end

    # to reduce allocations we make a compressed_data array to store compressed data
    compressed_data_buffer = Vector{UInt8}(undef, 100)
    compressed_data = UInt8[] # initialise it

    from = 1
    last_from = from

    j = 1
    for row_group in filemetadata.row_groups
        colchunk_meta = row_group.columns[col_num].meta_data

        if isfilled(colchunk_meta, :dictionary_page_offset)
            seek(fileio, colchunk_meta.dictionary_page_offset)
            dict_page_header = read_thrift(fileio, PAR2.PageHeader)

            # use the
            readbytes!(fileio, compressed_data_buffer, dict_page_header.compressed_page_size)
            GC.@preserve compressed_data_buffer begin
                compressed_data = unsafe_wrap(Vector{UInt8}, pointer(compressed_data_buffer), dict_page_header.compressed_page_size)
            end
            # compressed_data = read(fileio, dict_page_header.compressed_page_size)

            uncompressed_data = Vector{UInt8}(undef, dict_page_header.uncompressed_page_size)

            decompress_with_codec!(uncompressed_data, compressed_data, colchunk_meta.codec)
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
                    # nvals = dict_page_header.dictionary_page_header.num_values
                    # GC.@preserve uncompressed_data begin
                    #     dict = unsafe_wrap(Vector{T}, Ptr{T}(pointer(uncompressed_data)), nvals)
                    # end
                end
            else
                error("Only Plain Dictionary encoding is supported")
            end
        else
            dict = nothing
        end

        # seek to the first data page
        seek(fileio, colchunk_meta.data_page_offset)

        # the buffer is resizable and is used to reduce the amount of allocations
        uncompressed_data_buffer = Vector{UInt8}(undef, 1048584)

        # repeated read data page
        while (from - last_from  < row_group.num_rows) & (from <= length(res))
            from = read_data_page_vals!(res, uncompressed_data_buffer, fileio, dict, colchunk_meta.codec, T, from)

            if from isa Tuple
                return from
            else
                from += 1
            end
        end
        last_from = from

        # (j == 2) && return res
        j += 1

    end

    res
end

function read_data_page_vals!(res, uncompressed_data_buffer::Vector{UInt8}, fileio::IOStream, dict, codec, T, from::Integer = 1)
    """
    Read one data page
    """

    # the result length is used latter on to prevent writing too much data
    res_len = length(res)

    data_page_header = read_thrift(fileio, PAR2.PageHeader)

    # the number of values stored in this page
    num_values = data_page_header.data_page_header.num_values
    # read values
    to = from + num_values - 1
    @assert to <= res_len

    #compressed_data = read(fileio, data_page_header.compressed_page_size)
    compressed_data_buffer = Vector{UInt8}(undef, ceil(Int, data_page_header.compressed_page_size))

    readbytes!(fileio, compressed_data_buffer, data_page_header.compressed_page_size)

    # resize the buffer if it's too small
    if data_page_header.uncompressed_page_size > length(uncompressed_data_buffer)
        uncompressed_data_buffer = Vector{UInt8}(undef, ceil(Int, data_page_header.uncompressed_page_size*1.1))
    end

    t1 = @_gc_preserve_begin uncompressed_data_buffer

    GC.@preserve compressed_data_buffer uncompressed_data_buffer begin
        compressed_data = unsafe_wrap(Vector{UInt8}, pointer(compressed_data_buffer), data_page_header.compressed_page_size)
        uncompressed_data = unsafe_wrap(Vector{UInt8}, pointer(uncompressed_data_buffer), data_page_header.uncompressed_page_size)
        # uncompressed_data = Vector{UInt8}(undef, data_page_header.uncompressed_page_size)
        # decompression seems to be quite slow and uses lots of RAM!
        decompress_with_codec!(uncompressed_data, compressed_data, codec)
    end

    @assert length(uncompressed_data) == data_page_header.uncompressed_page_size

    uncompressed_data_io = IOBuffer(uncompressed_data, read=true, write=false, append=false)

    # this is made up of these 3 things written back to back
    # * repetition levels - can be ignored for unnested data
    # * definition levels -
    # * values

    # this will be set in future
    has_missing = false

    # initialise it to something
    missing_bytes = Vector{UInt8}(undef, num_values)
    missing_bytes_io = IOBuffer(missing_bytes, write=true)

    # definition levels
    if data_page_header.data_page_header.definition_level_encoding == PAR2.Encoding.RLE
        # for unnested columns the highest possible value for definiton is 1
        # which can represented with just one bit so the bit width is always 1
        bitwidth = 1
        encoded_data_len = read(uncompressed_data_io, UInt32)
        pos_before_encoded_data = position(uncompressed_data_io)

        from_defn = from

        pos_after_reading_encoded_data = pos_before_encoded_data

        while (pos_after_reading_encoded_data - pos_before_encoded_data) < encoded_data_len
            encoded_data_header = Parquet._read_varint(uncompressed_data_io, UInt32)

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
                        # TODO there is a better way to locate the missing bytes
                        # find the location of missing
                        dest_ptr = Ptr{UInt8}(pointer(res, res_len+1)) + from_defn - 1
                        tmparray = unsafe_wrap(Vector{UInt8}, dest_ptr, rle_len)
                        fill!(tmparray, rle_val)
                    end
                end

                write(missing_bytes_io, fill(rle_val, rle_len))

                from_defn += rle_len
                @assert from_defn - from == position(missing_bytes_io)
                @assert position(missing_bytes_io) <= num_values
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

                tmp_missing_bytes::Vector{UInt8} = BitPackedIterator(data, bitwidth) |> collect

                len_of_tmp_missing_bytes = length(tmp_missing_bytes)
                @assert mod(len_of_tmp_missing_bytes, 8) == 0

                # the tmp_missing_bytes is always in a multiple of 8 so need to
                # be careful not to write too much
                # compute the new from_defn
                new_from_defn = min(from_defn + len_of_tmp_missing_bytes, from + num_values)

                len_to_write = new_from_defn - from_defn

                if len_to_write == len_of_tmp_missing_bytes
                    write(missing_bytes_io, tmp_missing_bytes)
                elseif len_to_write < len_of_tmp_missing_bytes
                    tmp_missing_bytes_smaller = unsafe_wrap(Vector{UInt8}, pointer(tmp_missing_bytes), len_to_write)
                    write(missing_bytes_io, tmp_missing_bytes_smaller)
                else
                    error("something is wrong")
                end

                if T == String
                    # do nothing
                else
                    if len_to_write == len_of_tmp_missing_bytes
                        GC.@preserve tmp_missing_bytes res begin
                            # if not too long then can straight copy
                            src_ptr = Ptr{UInt8}(pointer(tmp_missing_bytes))
                            dest_ptr = Ptr{UInt8}(pointer(res, res_len+1)) + from_defn - 1
                            # copy content over
                            unsafe_copyto!(dest_ptr, src_ptr, length(tmp_missing_bytes))
                        end
                    elseif len_to_write < len_of_tmp_missing_bytes
                        GC.@preserve tmp_missing_bytes_smaller res begin
                            src_ptr = Ptr{UInt8}(pointer(tmp_missing_bytes_smaller))
                            dest_ptr = Ptr{UInt8}(pointer(res, res_len+1)) + from_defn - 1
                            # copy content over
                            unsafe_copyto!(dest_ptr, src_ptr, len_to_write)
                        end
                    else
                        error("something is wrong")
                    end

                end
                from_defn = new_from_defn
            end
        end
    else
        error("no definition encoding not supported")
    end

    # this line ensures that we have read all the encoded definition data
    @assert pos_after_reading_encoded_data - pos_before_encoded_data == encoded_data_len

    if has_missing
        @assert position(missing_bytes_io) == num_values
    end



    if data_page_header.data_page_header.encoding == PAR2.Encoding.PLAIN
        # just return the data as is
        if T == Bool
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
                    GC.@preserve res begin
                        raw_data = Base.unsafe_wrap(Vector{Bool}, pointer(res, i) |>  Ptr{Bool}, (8,))
                    end
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
                # raw_data = reinterpret(T, read(uncompressed_data_io))
                arr_pos = position(uncompressed_data_io) + 1
                # seek till the end
                seek(uncompressed_data_io, uncompressed_data_io.size + 1)
                # TODO remove this allocation too
                ok = uncompressed_data[arr_pos:end]
                raw_data =  reinterpret(T, ok)
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
                GC.@preserve uncompressed_data res begin
                    src_ptr = Ptr{T}(pointer(uncompressed_data, pos_for_pointer))
                    dest_ptr = Ptr{T}(pointer(res, from))
                    # copy content over
                    unsafe_copyto!(dest_ptr, src_ptr, num_values)
                end
            end
        end
    elseif data_page_header.data_page_header.encoding == PAR2.Encoding.PLAIN_DICTIONARY
        # this means the data is encoded in integers format which form the indices to the data
        bitwidth = Int(read(uncompressed_data_io, UInt8))

        # the documented max bitwidth is
        @assert bitwidth <= 32

        rle_cnt = 0
        bp_cnt = 0
        rle_size = 0
        bp_size = 0
        while !eof(uncompressed_data_io)
            encoded_data_header = Parquet._read_varint(uncompressed_data_io, UInt32)

            if iseven(encoded_data_header)
                rle_cnt += 1
                # RLE encoded
                rle_len = Int(encoded_data_header >> 1)
                rle_val_vec::Vector{UInt8} = read(uncompressed_data_io, ceil(Int, bitwidth/8))
                rle_val = UInt(0)

                for tmp in @view rle_val_vec[end:-1:1]
                    rle_val = rle_val << 8
                    rle_val = rle_val | tmp
                end

                if has_missing
                    index = from:min(to, from + rle_len - 1)
                    for (i, missing_byte) in zip(index, missing_bytes)
                        if missing_byte == 1
                            res[i] = dict[rle_val+1]
                        end
                    end
                else
                    res[from:min(to, from + rle_len - 1)] .= dict[rle_val+1]
                end

                rle_size += rle_len
                from = from + rle_len
            else
                bp_cnt += 1
                # bitpacked encoded
                bit_pack_len = Int(encoded_data_header >> 1)
                @assert (bit_pack_len >= 1) && (bit_pack_len <= 2^31 - 1)
                bytes_to_read = bitwidth*bit_pack_len
                data = read(uncompressed_data_io, bytes_to_read)
                # TODO remove the collect here
                bp = BitPackedIterator(data, bitwidth) |> collect
                # now need a decoding algorithm to break it up
                # reading `bitwidth` bits at a time
                l = length(bp)

                index = from:min(from + l - 1, to)

                if has_missing
                    j = 1
                    for (i, missing_byte) in zip(index, missing_bytes)
                        if missing_byte == 1
                            res[i] = dict[bp[j]+1]
                            j += 1
                        end
                    end
                else
                    for (i, v) in zip(index, bp)
                        res[i] = dict[v+1]
                    end
                end

                bp_size += l
                from = from + l
            end
        end
        # println("rle_cnt $rle_cnt bp_cnt $bp_cnt rle_size $rle_size bp_size $bp_size")
    else
        erorr("encoding not supported")
    end

     @_gc_preserve_end t2

    return to
end
