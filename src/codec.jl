# ref: https://github.com/apache/parquet-format/blob/master/Encodings.md

macro bitwidth(i)
    quote
        ceil(Int, log(2, $(esc(i))+1))
    end
end
macro bit2bytewidth(i)
    quote
        ceil(Int, $(esc(i))/8)
    end
end
macro byt2itype(i)
    quote
        ($(esc(i)) <= 4) ? Int32 : ($(esc(i)) <= 8) ? Int64 : Int128
    end
end
macro byt2uitype(i)
    quote
        ($(esc(i)) <= 4) ? UInt32 : ($(esc(i)) <= 8) ? UInt64 : UInt128
    end
end
macro byt2uitype_small(i)
    quote
        ($(esc(i)) <= 1) ? UInt8 : ($(esc(i)) <= 2) ? UInt16 : ($(esc(i)) <= 4) ? UInt32 : ($(esc(i)) <= 8) ? UInt64 : UInt128
    end
end

const MSB = 0x80
const MASK7 = 0x7f
const MASK8 = 0xff
const MASK3 = 0x07
function MASKN(nbits::UInt8)
    byte_width = @bit2bytewidth(nbits)
    type_small = @byt2uitype_small(byte_width)
    MASKN(nbits, type_small)
end
function MASKN(nbits::UInt8, ::Type{T}) where {T}
    O = convert(T, 0x1)
    (O << nbits) - O
end

#read_fixed(io::IO, typ::Type{UInt32}) = _read_fixed(io, convert(UInt32,0), 4)
#read_fixed(io::IO, typ::Type{UInt64}) = _read_fixed(io, convert(UInt64,0), 8)
read_fixed(io::IO, typ::Type{Int32}) = reinterpret(Int32, _read_fixed(io, convert(UInt32,0), 4))
#read_fixed(io::IO, typ::Type{Int64}) = reinterpret(Int64, _read_fixed(io, convert(UInt64,0), 8))
#read_fixed(io::IO, typ::Type{Int128}) = reinterpret(Int128, _read_fixed(io, convert(UInt128, 0), 12))   # INT96: 12 bytes little endian
#read_fixed(io::IO, typ::Type{Float32}) = reinterpret(Float32, _read_fixed(io, convert(UInt32,0), 4))
#read_fixed(io::IO, typ::Type{Float64}) = reinterpret(Float64, _read_fixed(io, convert(UInt64,0), 8))
function _read_fixed(io::IO, ret::T, N::Int) where {T <: Unsigned}
    for n in 0:(N-1)
        byte = convert(T, read(io, UInt8))
        ret |= (byte << *(8,n))
    end
    ret
end

function _read_fixed_bigendian(io::IO, ret::T, N::Int) where {T <: Unsigned}
    for n in (N-1):0
        byte = convert(T, read(io, UInt8))
        ret |= (byte << *(8,n))
    end
end

mutable struct InputState
    data::Vector{UInt8}
    offset::Int
end

mutable struct OutputState{T}
    data::Vector{T}
    offset::Int
end

function OutputState(::Type{T}, size) where {T}
    arr = Array{T}(undef, size)
    OutputState{T}(arr, 0)
end

function ensure_additional_size(iostate, additional_size)
    needed_size = iostate.offset + additional_size
    if length(iostate.data) < needed_size
        resize!(iostate.data, needed_size)
    end
    nothing
end

function reset_to_size(iostate, size)
    iostate.offset = 0
    (length(iostate.data) == size) || resize!(iostate.data, size)
    nothing
end

read_fixed(inp::InputState, typ::Type{UInt32}) = _read_fixed(inp, convert(UInt32,0), 4)
read_fixed(inp::InputState, typ::Type{UInt64}) = _read_fixed(inp, convert(UInt64,0), 8)
read_fixed(inp::InputState, typ::Type{Int32}) = reinterpret(Int32, _read_fixed(inp, convert(UInt32,0), 4))
read_fixed(inp::InputState, typ::Type{Int64}) = reinterpret(Int64, _read_fixed(inp, convert(UInt64,0), 8))
read_fixed(inp::InputState, typ::Type{Int128}) = reinterpret(Int128, _read_fixed(inp, convert(UInt128, 0), 12))
read_fixed(inp::InputState, typ::Type{Float32}) = reinterpret(Float32, _read_fixed(inp, convert(UInt32,0), 4))
read_fixed(inp::InputState, typ::Type{Float64}) = reinterpret(Float64, _read_fixed(inp, convert(UInt64,0), 8))
function _read_fixed(inp::InputState, ret::T, N::Int) where {T <: Unsigned}
    data = inp.data
    offset = inp.offset
    for n in 0:(N-1)
        byte = convert(T, data[1+offset])
        offset += 1
        ret |= (byte << *(8,n))
    end
    inp.offset = offset
    ret
end

function _read_varint(inp::InputState, ::Type{T}) where {T <: Integer}
    data = inp.data
    offset = inp.offset
    res = zero(T)
    n = 0
    byte = UInt8(MSB)
    while (byte & MSB) != 0
        byte = data[1+offset]
        offset += 1
        res |= (convert(T, byte & MASK7) << (7*n))
        n += 1
    end
    inp.offset = offset

    # in case of overflow, consider it as missing field and return default value
    if (n-1) > sizeof(T)
        #@debug("overflow reading $T. returning 0")
        return zero(T)
    end
    res
end

# parquet types:   BOOLEAN, INT32, INT64,  INT96,   FLOAT,  DOUBLE, BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY
# enum values:           0,     1,     2,      3,       4,       5,          6,                    7
const PLAIN_JTYPES = (Bool, Int32, Int64, Int128, Float32, Float64,      UInt8,               UInt8)

# read plain encoding (PLAIN = 0)
read_plain_byte_array(inp::InputState) = read_plain_byte_array(inp::InputState, read_fixed(inp, Int32))
function read_plain_byte_array(inp::InputState, count::Int32)
    arr = inp.data[(1+inp.offset):(inp.offset+count)]  # TODO: Return subarr?
    inp.offset += count
    arr
end

# read plain values _Type.BOOLEAN
function read_plain_values(inp::InputState, out::OutputState{Bool}, count::Int32)
    #@debug("reading plain values", type=Bool, count=count)
    read_bitpacked_booleans(inp, out, count)
end
# read plain values _Type.BYTE_ARRAY
function read_plain_values(inp::InputState, out::OutputState{Vector{UInt8}}, count::Int32)
    # _Type.FIXED_LEN_BYTE_ARRAY is most likely same as byte array
    #@debug("reading plain values", type=Vector{UInt8}, count=count)
    arr = out.data
    offset = out.offset
    @assert (offset + count) <= length(arr)
    @inbounds for i in 1:count
        arr[i+offset] = read_plain_byte_array(inp)
    end
    out.offset += count
    nothing
end
function read_plain_values(inp::InputState, out::OutputState{T}, count::Int32, converter_fn::Function, storage_type::Int32) where T <: Union{Decimal,Float64,Int16,Int32,Int64,Int128}
    arr = out.data
    offset = out.offset
    @assert (offset + count) <= length(arr)
    if storage_type === _Type.FIXED_LEN_BYTE_ARRAY
        elem_bytes_len = Int32((length(inp.data) - inp.offset) / length(arr))
        #@debug("reading decimal plain values", offset, count, length(arr), length(inp.data), inp.offset, elem_bytes_len)
        @inbounds for i in 1:count
            arr[i+offset] = converter_fn(read_plain_byte_array(inp, elem_bytes_len))
        end
    elseif storage_type === _Type.INT64
        @inbounds for i in 1:count
            arr[i+offset] = converter_fn(read_fixed(inp, Int64))
        end
    elseif storage_type === _Type.INT32
        @inbounds for i in 1:count
            arr[i+offset] = converter_fn(read_fixed(inp, Int32))
        end
    else
        error("unsupported storage type $(storage_type) for $T")
    end
    out.offset += count
    nothing
end
function read_plain_values(inp::InputState, out::OutputState{String}, count::Int32, converter_fn::Function, storage_type::Int32)
    #@debug("reading plain values", type=Vector{UInt8}, count=count)
    arr = out.data
    offset = out.offset
    @assert (offset + count) <= length(arr)
    if storage_type === _Type.BYTE_ARRAY
        @inbounds for i in 1:count
            arr[i+offset] = converter_fn(read_plain_byte_array(inp))
        end
    elseif storage_type === _Type.FIXED_LEN_BYTE_ARRAY
        elem_bytes_len = Int32((length(inp.data) - inp.offset) / length(arr))
        @inbounds for i in 1:count
            arr[i+offset] = converter_fn(read_plain_byte_array(inp, elem_bytes_len))
        end
    else
        error("unsupported storage type $(storage_type) for String")
    end
    out.offset += count
    nothing
end
# read_plain_values of type T
function read_plain_values(inp::InputState, out::OutputState{T}, count::Int32) where {T}
    #@debug("reading plain values", type=T, count=count)
    arr = out.data
    offset = out.offset
    @assert (offset + count) <= length(arr)
    @inbounds for i in 1:count
        arr[i+offset] = read_fixed(inp, T)
    end
    #@debug("read $(length(arr)) plain values")
    out.offset += count
    nothing
end
function read_plain_values(inp::InputState, out::OutputState{DateTime}, count::Int32, converter_fn::Function, storage_type::Int32)
    #@debug("reading plain values", type=T, count=count)
    arr = out.data
    offset = out.offset
    @assert (offset + count) <= length(arr)
    if storage_type === _Type.INT96
        @inbounds for i in 1:count
            arr[i+offset] = converter_fn(read_fixed(inp, Int128))
        end
    else
        error("unsupported storage type $(storage_type) for DateTime")
    end
    #@debug("read $(length(arr)) plain values")
    out.offset += count
    nothing
end

function read_bitpacked_booleans(inp::InputState, out::OutputState{Bool}, count::Int32)
    #@debug("reading bitpacked booleans", count)
    arrpos = 1
    bits = UInt8(0)
    bitpos = 9

    data = inp.data
    data_offset = inp.offset

    arr = out.data
    offset = out.offset
    @assert (offset+count) <= length(arr)

    @inbounds while arrpos <= count
        if bitpos > 8
            bits = data[1+data_offset]
            data_offset += 1
            #@debug("bits", bits, bitstring(bits))
            bitpos = 1
        end
        arr[arrpos+offset] = Bool(bits & 0x1)
        arrpos += 1
        bits >>= 1
        bitpos += 1
    end
    out.offset += count
    inp.offset = data_offset
    nothing
end

# read data dictionary (RLE_DICTIONARY = 8, or PLAIN_DICTIONARY = 2 in a data page)
function read_data_dict(inp::InputState, count::Int32)
    bits = inp.data[inp.offset+=1]
    #@info("bits", bits)
    byte_width = @bit2bytewidth(bits)
    typ = @byt2itype(byte_width)
    out = OutputState(typ, count)

    #@info("reading read_hybrid", count, read_len)
    read_hybrid(inp, out, count, bits, byte_width; read_len=false)
    out.data
end

# read RLE or bit backed format (RLE = 3)
function read_hybrid(inp::InputState, out::OutputState{T}, count::Int32, bits::UInt8, byt::Int; read_len::Bool=true) where {T}
    len = Int32(0)
    if read_len
        len = read_fixed(inp, Int32)
    end
    #@debug("reading hybrid data", len, count, bits)
    mask = MASKN(bits)
    arr = out.data
    arrpos = 1
    offset = out.offset - 1    # to counter arrpos starting at 1
    while arrpos <= count
        runhdr = _read_varint(inp, Int)
        isbitpack = ((runhdr & 0x1) == 0x1)
        runhdr >>= 1
        nrunhdrbits = runhdr * 8
        nitems = min(isbitpack ? nrunhdrbits : runhdr, count - arrpos + 1)

        if isbitpack
            runcount = min(nrunhdrbits, length(arr)-offset-arrpos)
            read_bitpacked_run(inp, out, runcount, bits, byt, mask)
            if nrunhdrbits > runcount
                nbytes_to_skip = div(nrunhdrbits - runcount, 8)
                @debug("skipping trailing bytes in bitpacked run", nbytes_to_skip)
                inp.offset += nbytes_to_skip
            end
            #out.offset += runcount
        else # rle
            read_type = @byt2uitype(byt)
            read_rle_run(inp, out, nitems, bits, byt, read_type)
            #out.offset += nitems
        end
        arrpos += nitems
    end
    nothing
end

function read_rle_run(inp::InputState, out::OutputState{T}, count::Int, bits::UInt8, byt::Int, read_type::Type{V}) where {T,V}
    #@debug("read_rle_run", count, T, bits, byt)
    rawval = _read_fixed(inp, zero(V), byt)
    val = reinterpret(T, rawval)
    arr = out.data
    offset = out.offset
    @assert length(arr) >= (count+offset)
    @inbounds for idx in 1:count
        arr[idx+offset] = val
    end
    out.offset += count
    nothing
end

function read_bitpacked_run(inp::InputState, out::OutputState{T}, count::Int, bits::UInt8, byt::Int, mask::V=MASKN(bits)) where {T,V}
    bitbuff = zero(V)
    nbitsbuff = UInt8(0)
    shift = UInt8(0)

    data = inp.data
    arr = out.data
    offset = out.offset
    arridx = 1
    dataidx = 1 + inp.offset
    while arridx <= count
        #@debug("arridx:$arridx nbitsbuff:$nbitsbuff shift:$shift bits:$bits")
        if nbitsbuff > 0
            # we have leftover bits, which must be appended
            if nbitsbuff < bits
                # but only append if we need to read more in this cycle
                @inbounds arr[arridx+offset] = bitbuff & MASKN(nbitsbuff, V)
                shift = nbitsbuff
                nbitsbuff = UInt8(0)
                bitbuff = zero(V)
            end
        end

        # fill buffer
        while (nbitsbuff + shift) < bits
             # shift 8 bits and read directly into bitbuff
            bitbuff |= (V(data[dataidx]) << nbitsbuff)
            dataidx += 1
            nbitsbuff += UInt8(8)
        end

        # set values
        while ((nbitsbuff + shift) >= bits) && (arridx <= count)
            if shift > 0
                remshift = bits - shift
                #@debug("setting part from bitbuff nbitsbuff:$nbitsbuff, shift:$shift, remshift:$remshift")
                arr[arridx+offset] |= convert(T, (bitbuff << shift) & mask)
                bitbuff >>= remshift
                nbitsbuff -= remshift
                shift = UInt8(0)
            else
                #@debug("setting all from bitbuff nbitsbuff:$nbitsbuff")
                arr[arridx+offset] = convert(T, bitbuff & mask)
                bitbuff >>= bits
                nbitsbuff -= bits
            end
            arridx += 1
        end
    end
    inp.offset = dataidx - 1
    out.offset += count
    nothing
end

# read bit packed in deprecated format (BIT_PACKED = 4)
function read_bitpacked_run_old(inp::InputState, out::OutputState{T}, count::Int32, bits::UInt8, mask::V=MASKN(bits)) where {T <: Integer, V <: Integer}
    # the mask is of the smallest bounding type for bits
    # T is one of the types that map on to the appropriate Julia type in Parquet (which may be larger than the mask type)
    bitbuff = zero(V)
    nbitsbuff = 0

    data = inp.data
    arr = out.data
    offset = out.offset
    arridx = Int32(1)
    dataidx = Int32(1 + inp.offset)
    while arridx <= count
        diffnbits = bits - nbitsbuff
        while diffnbits > 8
            # shift 8 bits and read directly into bitbuff
            bitbuff <<= 8
            bitbuff |= data[dataidx]
            dataidx += Int32(1)
            nbitsbuff += 8
            diffnbits -= 8
        end

        if diffnbits > 0
            # read next byte from input
            nxtdata = data[dataidx]
            dataidx += Int32(1)
            # shift bitbuff by diffnbits, add diffnbits and set result
            nbitsbuff = 8 - diffnbits
            arr[arridx+offset] = convert(T, ((bitbuff << diffnbits) | (nxtdata >> nbitsbuff)) & mask)
            arridx += Int32(1)
            # keep remaining bits in bitbuff
            bitbuff <<= 8
            bitbuff |= nxtdata
        else
            # set result
            arr[arridx+offset] = convert(T, (bitbuff >> abs(diffnbits)) & mask)
            arridx += Int32(1)
            nbitsbuff -= bits
        end
    end
    inp.offset = dataidx - 1
    out.offset += count
    nothing
end

function logical_timestamp(barr; offset::Dates.Period=Dates.Second(0))
    nanos = read(IOBuffer(barr[1:8]), Int64)
    julian_days = read(IOBuffer(barr[9:12]), Int32)
    Dates.julian2datetime(julian_days) + Dates.Nanosecond(nanos) + offset
end

function logical_timestamp(i128::Int128; offset::Dates.Period=Dates.Second(0))
    iob = IOBuffer()
    write(iob, i128)
    logical_timestamp(take!(iob); offset=offset)
end

logical_string(bytes::Vector{UInt8}) = String(bytes)

function logical_decimal(bytes::Union{Int64,Int32,Vector{UInt8}}, precision::Integer, scale::Integer; use_float::Bool=false)
    T = logical_decimal_unscaled_type(Int32(precision))
    if scale == 0
        logical_decimal_integer(bytes, T)
    elseif use_float
        logical_decimal_float64(bytes, T, Int32(scale))
    else
        logical_decimal_scaled(bytes, T, Int32(scale))
    end
end

function logical_decimal_integer(intval::Union{Int64,Int32}, ::Type{T}) where T <: Union{UInt16,UInt32,UInt64,UInt128}
    signed(T)(intval)
end

function logical_decimal_integer(bytes::Vector{UInt8}, ::Type{T}) where T <: Union{UInt16,UInt32,UInt64,UInt128}
    N = length(bytes)
    uintval = T(0)
    for idx in 1:N
        uintval |= (T(bytes[idx]) << *(8,N-idx))
    end
    reinterpret(signed(T), uintval)
end

function logical_decimal_float64(bytes::Union{Int64,Int32,Vector{UInt8}}, ::Type{T}, scale::Int32) where T <: Union{UInt16,UInt32,UInt64,UInt128}
    if scale > 0
        logical_decimal_integer(bytes, T) / 10^scale
    else
        logical_decimal_integer(bytes, T) * 10^abs(scale)
    end
end

function logical_decimal_scaled(bytes::Union{Int64,Int32,Vector{UInt8}}, ::Type{T}, scale::Int32) where T <: Union{UInt16,UInt32,UInt64,UInt128}
    intval = logical_decimal_integer(bytes, T)
    sign = (intval < 0) ? 1 : 0
    intval = abs(intval)
    Decimal(sign, intval, -scale)
end