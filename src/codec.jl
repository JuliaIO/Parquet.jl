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

read_fixed(io::IO, typ::Type{UInt32}) = _read_fixed(io, convert(UInt32,0), 4)
read_fixed(io::IO, typ::Type{UInt64}) = _read_fixed(io, convert(UInt64,0), 8)
read_fixed(io::IO, typ::Type{Int32}) = reinterpret(Int32, _read_fixed(io, convert(UInt32,0), 4))
read_fixed(io::IO, typ::Type{Int64}) = reinterpret(Int64, _read_fixed(io, convert(UInt64,0), 8))
read_fixed(io::IO, typ::Type{Int128}) = reinterpret(Int128, _read_fixed(io, convert(UInt128, 0), 12))   # INT96: 12 bytes little endian
read_fixed(io::IO, typ::Type{Float32}) = reinterpret(Float32, _read_fixed(io, convert(UInt32,0), 4))
read_fixed(io::IO, typ::Type{Float64}) = reinterpret(Float64, _read_fixed(io, convert(UInt64,0), 8))
function _read_fixed(io::IO, ret::T, N::Int) where {T <: Unsigned}
    for n in 0:(N-1)
        byte = convert(T, read(io, UInt8))
        ret |= (byte << *(8,n))
    end
    ret
end

function _read_varint(io::IO, ::Type{T}) where {T <: Integer}
    res = zero(T)
    n = 0
    byte = UInt8(MSB)
    while (byte & MSB) != 0
        byte = read(io, UInt8)
        res |= (convert(T, byte & MASK7) << (7*n))
        n += 1
    end
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
function read_plain(io::IO, typ::Int32, jtype::Type{T}=PLAIN_JTYPES[typ+1]) where {T}
    if typ === _Type.BYTE_ARRAY
        count = read_fixed(io, Int32)
        #@debug("reading bytearray length:$count")
        read!(io, Array{UInt8}(undef, count))
    elseif typ === _Type.BOOLEAN
        error("not implemented") # reading single boolean values is not possible, vectors are read via read_bitpacked_booleans
    elseif typ === _Type.FIXED_LEN_BYTE_ARRAY
        #@debug("reading fixedlenbytearray length:$count")
        #read!(io, Array{UInt8}(count))
        error("not implemented") # this is likely same as BYTE_ARRAY for decoding purpose
    else
        #@debug("reading type:$jtype, typenum:$typ")
        read_fixed(io, jtype)
    end
end

# read plain values or dictionary (PLAIN_DICTIONARY = 2)
function read_plain_values(io::IO, count::Int32, typ::Int32)
    #@debug("reading plain values", type=typ, count=count)
    if typ == _Type.BOOLEAN
        arr = read_bitpacked_booleans(io, count)
    else
        arr = [read_plain(io, typ) for i in 1:count]
    end
    #@debug("read $(length(arr)) plain values")
    arr
end

function read_bitpacked_booleans(io::IO, count::Int32)::Vector{Bool}
    #@debug("reading bitpacked booleans", count)
    arr = fill!(Array{Bool}(undef, count), false)
    arrpos = 1
    bits = UInt8(0)
    bitpos = 9
    while arrpos <= count
        if bitpos > 8
            bits = read(io, UInt8)
            #@debug("bits", bits, bitstring(bits))
            bitpos = 1
        end
        arr[arrpos] = Bool(bits & 0x1)
        arrpos += 1
        bits >>= 1
        bitpos += 1
    end
    arr
end

# read rle dictionary (RLE_DICTIONARY = 8, or PLAIN_DICTIONARY = 2 in a data page)
function read_rle_dict(io::IO, count::Int32)
    bits = read(io, UInt8)
    #@debug("reading rle dictionary bits:$bits")
    arr = read_hybrid(io, count, Val{bits}(); read_len=false)
    #@debug("read $(length(arr)) dictionary values")
    arr
end

# read RLE or bit backed format (RLE = 3)
function read_hybrid(io::IO, count::Int32, ::Val{W}; read_len::Bool=true) where {W}
    byte_width = @bit2bytewidth(W)
    typ = @byt2itype(byte_width)
    arr = Array{typ}(undef, count)

    read_hybrid(io, count, W, byte_width, typ, arr; read_len=read_len)
end
function read_hybrid(io::IO, count::Int32, bits::UInt8, byt::Int, typ::Type{T}, arr::Vector{T}; read_len::Bool=true) where {T <: Integer}
    len = read_len ? read_fixed(io, Int32) : Int32(0)
    @debug("reading hybrid data", len, count, bits)
    mask = MASKN(bits)
    arrpos = 1
    while arrpos <= count
        runhdr = _read_varint(io, Int)
        isbitpack = ((runhdr & 0x1) == 0x1)
        runhdr >>= 1
        nitems = isbitpack ? min(runhdr*8, count - arrpos + 1) : runhdr
        #@debug("nitems=$nitems, isbitpack:$isbitpack, runhdr:$runhdr, remaininglen: $(count - arrpos + 1)")
        #@debug("creating sub array for $nitems items at $arrpos, total length:$(length(arr))")
        subarr = unsafe_wrap(Array, pointer(arr, arrpos), nitems, own=false)

        if isbitpack
            read_bitpacked_run(io, runhdr, bits, byt, typ, subarr, mask)
        else # rle
            read_type = @byt2uitype(byt)
            read_rle_run(io, runhdr, bits, byt, typ, subarr, read_type)
        end
        arrpos += nitems
    end
    arr
end

function read_rle_run(io::IO, count::Int, bits::UInt8, byt::Int, reinterpreted_typ::Type{T}, arr::Vector{T}, read_type::Type{V}) where {T <: Integer, V <: Integer}
    #@debug("read_rle_run", count, T, bits, byt)
    val = reinterpret(T, _read_fixed(io, zero(V), byt))
    @assert length(arr) >= count
    @inbounds for idx in 1:count
        arr[idx] = val
    end
    arr
end

function read_bitpacked_run(io::IO, grp_count::Int, ::Val{W}) where {W}
    byte_width = @bit2bytewidth(W)
    typ = @byt2itype(byte_width)
    arr = Array{typ}(undef, grp_count)
    read_bitpacked_run(io, grp_count, W, byte_width, typ, arr)
end
function read_bitpacked_run(io::IO, grp_count::Int, bits::UInt8, byt::Int, typ::Type{T}, arr::Vector{T}, mask::V=MASKN(bits)) where {T <: Integer, V <: Integer}
    count = min(grp_count * 8, length(arr))
    # multiple of 8 values at a time are bit packed together
    nbytes = bits * grp_count # same as: round(Int, (bits * grp_count * 8) / 8)
    #@debug("read_bitpacked_run. grp_count:$grp_count, count:$count, nbytes:$nbytes, nbits:$bits, available:$(bytesavailable(io))")
    data = Array{UInt8}(undef, min(nbytes, bytesavailable(io)))
    read!(io, data)

    bitbuff = zero(V)
    nbitsbuff = UInt8(0)
    shift = UInt8(0)

    arridx = 1
    dataidx = 1
    while arridx <= count
        #@debug("arridx:$arridx nbitsbuff:$nbitsbuff shift:$shift bits:$bits")
        if nbitsbuff > 0
            # we have leftover bits, which must be appended
            if nbitsbuff < bits
                # but only append if we need to read more in this cycle
                @inbounds arr[arridx] = bitbuff & MASKN(nbitsbuff, V)
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
                arr[arridx] |= convert(T, (bitbuff << shift) & mask)
                bitbuff >>= remshift
                nbitsbuff -= remshift
                shift = UInt8(0)
            else
                #@debug("setting all from bitbuff nbitsbuff:$nbitsbuff")
                arr[arridx] = convert(T, bitbuff & mask)
                bitbuff >>= bits
                nbitsbuff -= bits
            end
            arridx += 1
        end
    end
    arr
end

# read bit packed in deprecated format (BIT_PACKED = 4)
function read_bitpacked_run_old(io::IO, count::Int32, ::Val{W}) where {W}
    byte_width = @bit2bytewidth(W)
    typ = @byt2itype(byte_width)
    arr = Array{typ}(undef, count)
    read_bitpacked_run_old(io, count, W, byte_width, typ, arr)
end
function read_bitpacked_run_old(io::IO, count::Int32, bits::UInt8, byt::Int, typ::Type{T}, arr::Vector{T}, mask::V=MASKN(bits)) where {T <: Integer, V <: Integer}
    # multiple of 8 values at a time are bit packed together
    nbytes = round(Int, (bits * count) / 8)
    #@debug("read_bitpacked_run. count:$count, nbytes:$nbytes, nbits:$bits")
    data = Array{UInt8}(undef, nbytes)
    read!(io, data)

    # the mask is of the smallest bounding type for bits
    # T is one of the types that map on to the appropriate Julia type in Parquet (which may be larger than the mask type)
    bitbuff = zero(V)
    nbitsbuff = 0

    arridx = Int32(1)
    dataidx = Int32(1)
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
            arr[arridx] = convert(T, ((bitbuff << diffnbits) | (nxtdata >> nbitsbuff)) & mask)
            arridx += Int32(1)
            # keep remaining bits in bitbuff
            bitbuff <<= 8
            bitbuff |= nxtdata
        else
            # set result
            arr[arridx] = convert(T, (bitbuff >> abs(diffnbits)) & mask)
            arridx += Int32(1)
            nbitsbuff -= bits
        end
    end
    arr
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

logical_string(bytes::Vector{UInt8}) = String(copy(bytes))
