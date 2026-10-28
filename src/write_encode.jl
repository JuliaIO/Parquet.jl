# Column chunk encoding for the writer: data pages (levels, dictionary or PLAIN values),
# the dictionary page, statistics, and the page index entries of one column chunk.

# pyarrow's defaults: a data page holds at most this many rows ...
const _PAGE_ROW_LIMIT = 20_000
# ... and a chunk stops dictionary encoding once its dictionary exceeds this many bytes.
const _DICTIONARY_LIMIT = 1024 * 1024
# A chunk's dictionary probe stops early when the first _PROBE_PREFIX values of its first
# page hold more than _PROBE_DISTINCT (95%) distinct values.
const _PROBE_PREFIX = 4096
const _PROBE_DISTINCT = 3891
# Byte-array ColumnIndex bounds longer than this are truncated (parquet-mr's default).
const _INDEX_BOUND_BYTES = 64

ENDIAN_BOM == 0x04030201 || error("Parquet.jl writes PLAIN values on little-endian hosts only")

## RLE/bit-packed hybrid streams

# Length of the run of values equal to `values[index]`, counting at most `cap`.
@inline function _hybridrun(values::AbstractVector, index::Int, count::Int, cap::Int)
    value = @inbounds values[index]
    stop = count - index < cap ? count : index + cap - 1
    next = index + 1
    @inbounds while next <= stop && values[next] == value
        next += 1
    end
    return next - index
end

"""
    _encodehybrid!(output, values, bitwidth) -> output

Append `values` (each below `2^bitwidth`) as an RLE/bit-packed hybrid stream: a run of at
least eight equal values becomes an RLE run, other values bit-packed groups of eight.
"""
function _encodehybrid!(output::Vector{UInt8}, values::AbstractVector{<:Unsigned},
        bitwidth::Int)
    count = length(values)
    bytewidth = cld(bitwidth, 8)
    index = 1
    while index <= count
        run = _hybridrun(values, index, count, typemax(Int))
        if run >= 8
            value = UInt64(@inbounds values[index])
            _writehybridvarint!(output, UInt64(run) << 1)
            for shift in 0:(bytewidth - 1)
                push!(output, (value >> (8 * shift)) % UInt8)
            end
            index += run
            continue
        end
        start = index
        groups = 1
        index += 8
        while index <= count && _hybridrun(values, index, count, 8) < 8
            groups += 1
            index += 8
        end
        _writehybridvarint!(output, (UInt64(groups) << 1) | 0x01)
        _bitpack!(output, values, start, min(index - 1, count), 8 * groups, bitwidth)
    end
    return output
end

# Packs values[start:stop], padded with zeros to `total` values (a multiple of eight),
# least significant bit first. Values are below 2^bitwidth with bitwidth <= 32, so the
# accumulator flushes 32 bits at a time.
function _bitpack!(output::Vector{UInt8}, values::AbstractVector{<:Unsigned}, start::Int,
        stop::Int, total::Int, bitwidth::Int)
    position = length(output)
    finish = position + (total * bitwidth) >> 3
    resize!(output, finish)
    accumulator = UInt64(0)
    bits = 0
    @inbounds for index in start:stop
        accumulator |= UInt64(values[index]) << bits
        bits += bitwidth
        if bits >= 32
            _storeuint32!(output, position + 1, accumulator % UInt32)
            position += 4
            accumulator >>= 32
            bits -= 32
        end
    end
    # The last partial word, then the zero padding.
    @inbounds while position < finish
        position += 1
        output[position] = accumulator % UInt8
        accumulator >>= 8
    end
    return output
end

# V1 data pages prefix each level stream with its 4-byte length; V2 pages do not.
function _encodelevels!(output::Vector{UInt8}, levels::AbstractVector{UInt16},
        bitwidth::Int, prefixed::Bool)
    prefixed || return _encodehybrid!(output, levels, bitwidth)
    start = length(output)
    resize!(output, start + 4)
    _encodehybrid!(output, levels, bitwidth)
    _storeuint32!(output, start + 1, UInt32(length(output) - start - 4))
    return output
end

# A level stream of `count` (at least eight) equal levels: one RLE run, as
# _encodelevels! writes it.
function _encodelevelrun!(output::Vector{UInt8}, count::Int, level::UInt16,
        bitwidth::Int, prefixed::Bool)
    start = length(output)
    prefixed && resize!(output, start + 4)
    _writehybridvarint!(output, UInt64(count) << 1)
    for shift in 0:(cld(bitwidth, 8) - 1)
        push!(output, (level >> (8 * shift)) % UInt8)
    end
    prefixed && _storeuint32!(output, start + 1, UInt32(length(output) - start - 4))
    return output
end

@inline function _storeuint32!(output::Vector{UInt8}, index::Int, value::UInt32)
    @inbounds for shift in 0:3
        output[index + shift] = (value >> (8 * shift)) % UInt8
    end
    return
end

## Page value buffers

# Present values of one page, laid out as PLAIN: BYTE_ARRAY values carry a 4-byte length.
# FIXED_LEN_BYTE_ARRAY buffers check every value against `width`; BYTE_ARRAY buffers
# (width 0) against `maxlength`.
mutable struct _BytesBuffer
    data::Vector{UInt8}
    starts::Vector{Int}
    lengths::Vector{Int32}
    prefixed::Bool
    width::Int
    maxlength::Int
end

_BytesBuffer(prefixed::Bool, width::Int, maxlength::Int) =
    _BytesBuffer(UInt8[], Int[], Int32[], prefixed, width, maxlength)

@noinline function _bytelengtherror(buffer::_BytesBuffer, length::Int)
    buffer.prefixed || throw(ArgumentError("a FIXED_LEN_BYTE_ARRAY($(buffer.width)) " *
        "value has $length bytes"))
    length > typemax(Int32) && throw(ArgumentError(
        "a Parquet byte array value exceeds 2^31 - 1 bytes"))
    throw(LimitError(:string_bytes, Int64(length), Int64(buffer.maxlength)))
end

function _resetbuffer!(buffer::_BytesBuffer)
    empty!(buffer.data)
    empty!(buffer.starts)
    empty!(buffer.lengths)
    return buffer
end

_resetbuffer!(buffer::Vector) = empty!(buffer)
_buffercount(buffer::_BytesBuffer) = length(buffer.starts)
_buffercount(buffer::Vector) = length(buffer)
_bufferbytes(buffer::_BytesBuffer) = length(buffer.data)
_bufferbytes(buffer::Vector{Bool}) = length(buffer) >> 3
_bufferbytes(buffer::Vector{P}) where {P} = sizeof(P) * length(buffer)

@inline function _appendbytes!(buffer::_BytesBuffer, source::Ptr{UInt8}, length::Int)
    (buffer.prefixed ? length <= buffer.maxlength : length == buffer.width) ||
        _bytelengtherror(buffer, length)
    data = buffer.data
    base = Base.length(data)
    start = buffer.prefixed ? base + 5 : base + 1
    resize!(data, start + length - 1)
    buffer.prefixed && _storeuint32!(data, base + 1, UInt32(length))
    GC.@preserve data unsafe_copyto!(pointer(data, start), source, length)
    push!(buffer.starts, start)
    push!(buffer.lengths, Int32(length))
    return
end

@inline function _appendvalue!(buffer::_BytesBuffer, value::Union{String,SubString{String}})
    GC.@preserve value _appendbytes!(buffer, pointer(value), ncodeunits(value))
    return
end

@inline function _appendvalue!(buffer::_BytesBuffer, value::DataStrings.DataString)
    _appendpayload!(buffer, value.p, value.data)
    return
end

@inline function _appendvalue!(buffer::_BytesBuffer, value::DataStrings.DataBytes)
    _appendpayload!(buffer, value.p, value.data)
    return
end

# A DataStrings payload: up to 12 bytes inline in its two words, otherwise a view.
@inline function _appendpayload!(buffer::_BytesBuffer, payload::DataStrings.StringPayload,
        data::Vector{UInt8})
    length = Int(DataStrings.payloadlength(payload))
    if length <= DataStrings.INLINE_MAX
        words = Ref((htol((payload.a >> 32) | (payload.b << 32)), htol(payload.b >> 32)))
        GC.@preserve words _appendbytes!(buffer,
            Ptr{UInt8}(Base.unsafe_convert(Ptr{Tuple{UInt64,UInt64}}, words)), length)
    else
        GC.@preserve data _appendbytes!(buffer,
            pointer(data, DataStrings.payloadpos(payload)), length)
    end
    return
end

@inline function _appendvalue!(buffer::_BytesBuffer, value::Vector{UInt8})
    GC.@preserve value _appendbytes!(buffer, pointer(value), length(value))
    return
end

@inline function _appendvalue!(buffer::_BytesBuffer, value::Base.CodeUnits{UInt8,String})
    source = value.s
    GC.@preserve source _appendbytes!(buffer, pointer(source), length(value))
    return
end

@inline function _appendvalue!(buffer::_BytesBuffer, value::NTuple{N,UInt8}) where {N}
    words = Ref(value)
    GC.@preserve words _appendbytes!(buffer,
        Ptr{UInt8}(Base.unsafe_convert(Ptr{NTuple{N,UInt8}}, words)), N)
    return
end

function _appendvalue!(buffer::_BytesBuffer, value::AbstractString)
    _appendvalue!(buffer, codeunits(value))
    return
end

function _appendvalue!(buffer::_BytesBuffer, value::AbstractVector{UInt8})
    (buffer.prefixed ? length(value) <= buffer.maxlength : length(value) == buffer.width) ||
        _bytelengtherror(buffer, length(value))
    data = buffer.data
    base = length(data)
    start = buffer.prefixed ? base + 5 : base + 1
    resize!(data, start + length(value) - 1)
    buffer.prefixed && _storeuint32!(data, base + 1, UInt32(length(value)))
    copyto!(data, start, value, firstindex(value), length(value))
    push!(buffer.starts, start)
    push!(buffer.lengths, Int32(length(value)))
    return
end

function _appendvalue!(buffer::_BytesBuffer, value::Base.CodeUnits)
    _appendvalue!(buffer, collect(UInt8, value))
    return
end

@inline _appendvalue!(buffer::Vector{P}, value) where {P} = push!(buffer, value)

@inline _bytesview(buffer::_BytesBuffer, index::Int) =
    (pointer(buffer.data, buffer.starts[index]), Int(buffer.lengths[index]))

## Dictionaries

# Open-addressing tables map a value to its dictionary index. Slots hold the index plus
# one; zero is empty. Fixed-width values compare by bit pattern, so -0.0, 0.0, and every
# NaN payload stay distinct.
mutable struct _NumberDictionary{P,U}
    values::Vector{P}
    slots::Vector{Int32}
    shift::Int
end

function _NumberDictionary{P}() where {P}
    U = P === Bool ? UInt8 : sizeof(P) == 4 ? UInt32 : UInt64
    return _NumberDictionary{P,U}(P[], zeros(Int32, 1024), 64 - 10)
end

@inline _hashslot(bits::Unsigned, shift::Int) =
    Int((UInt64(bits) * 0x9E3779B97F4A7C15) >> shift) + 1

# Adds `value` at the empty `slot` its probe ended on; returns its index.
function _dictionaryinsert!(dictionary::_NumberDictionary{P}, value::P,
        slot::Int) where {P}
    push!(dictionary.values, value)
    index = length(dictionary.values)
    @inbounds dictionary.slots[slot] = Int32(index)
    2 * index > length(dictionary.slots) && _rehash!(dictionary)
    return UInt32(index - 1)
end

function _rehash!(dictionary::_NumberDictionary{P,U}) where {P,U}
    slots = zeros(Int32, 2 * length(dictionary.slots))
    shift = dictionary.shift - 1
    mask = length(slots) - 1
    @inbounds for (index, value) in enumerate(dictionary.values)
        slot = _hashslot(reinterpret(U, value), shift)
        while slots[slot] != 0
            slot = (slot & mask) + 1
        end
        slots[slot] = Int32(index)
    end
    dictionary.slots = slots
    dictionary.shift = shift
    return dictionary
end

_dictionarycount(dictionary::_NumberDictionary) = length(dictionary.values)
_dictionarybytes(dictionary::_NumberDictionary{P}) where {P} =
    sizeof(P) * length(dictionary.values)

# Byte-array entries live in `entries`, already laid out as the dictionary page's PLAIN
# payload, and hash with `_byteshash`.
mutable struct _BytesDictionary
    entries::_BytesBuffer
    hashes::Vector{UInt64}
    slots::Vector{Int32}
    shift::Int
    # Which entries the page being flushed uses, for its statistics.
    marks::Vector{Bool}
end

_BytesDictionary(buffer::_BytesBuffer) = _BytesDictionary(
    _BytesBuffer(buffer.prefixed, buffer.width, buffer.maxlength), UInt64[],
    zeros(Int32, 1024), 64 - 10, Bool[])

function _dictionaryindex!(dictionary::_BytesDictionary, source::Ptr{UInt8}, length::Int)
    hash = _byteshash(source, length)
    slots = dictionary.slots
    mask = Base.length(slots) - 1
    slot = _hashslot(hash, dictionary.shift)
    entries = dictionary.entries
    @inbounds while true
        entry = slots[slot]
        if entry == 0
            _appendbytes!(entries, source, length)
            push!(dictionary.hashes, hash)
            index = Base.length(dictionary.hashes)
            slots[slot] = Int32(index)
            2 * index > Base.length(slots) && _rehash!(dictionary)
            return UInt32(index - 1)
        end
        if dictionary.hashes[entry] == hash && entries.lengths[entry] == length
            data = entries.data
            equal = GC.@preserve data iszero(Base.memcmp(
                pointer(data, entries.starts[entry]), source, length))
            equal && return UInt32(entry - 1)
        end
        slot = (slot & mask) + 1
    end
end

function _rehash!(dictionary::_BytesDictionary)
    slots = zeros(Int32, 2 * length(dictionary.slots))
    shift = dictionary.shift - 1
    mask = length(slots) - 1
    @inbounds for (index, hash) in enumerate(dictionary.hashes)
        slot = _hashslot(hash, shift)
        while slots[slot] != 0
            slot = (slot & mask) + 1
        end
        slots[slot] = Int32(index)
    end
    dictionary.slots = slots
    dictionary.shift = shift
    return dictionary
end

_dictionarycount(dictionary::_BytesDictionary) = length(dictionary.entries.starts)
_dictionarybytes(dictionary::_BytesDictionary) = length(dictionary.entries.data)

# The distinct XXH64 hashes of a column chunk's values: they size its bloom filter and
# are inserted into it. Slots hold hashes; zero marks an empty slot, so a zero hash is
# kept in `zero`.
mutable struct _HashSet
    slots::Vector{UInt64}
    count::Int
    shift::Int
    zero::Bool
end

_HashSet() = _HashSet(zeros(UInt64, 1024), 0, 64 - 10, false)

_hashcount(set::_HashSet) = set.count + set.zero

function _resethashes!(set::_HashSet)
    set.count = 0
    set.zero = false
    return _resetslots!(set)
end

function _addhash!(set::_HashSet, hash::UInt64)
    iszero(hash) && (set.zero = true; return)
    slots = set.slots
    mask = length(slots) - 1
    slot = Int(hash >> set.shift) + 1
    @inbounds while true
        entry = slots[slot]
        entry == hash && return
        if iszero(entry)
            slots[slot] = hash
            set.count += 1
            2 * set.count > length(slots) && _rehash!(set)
            return
        end
        slot = (slot & mask) + 1
    end
end

function _rehash!(set::_HashSet)
    slots = zeros(UInt64, 2 * length(set.slots))
    shift = set.shift - 1
    mask = length(slots) - 1
    @inbounds for hash in set.slots
        iszero(hash) && continue
        slot = Int(hash >> shift) + 1
        while !iszero(slots[slot])
            slot = (slot & mask) + 1
        end
        slots[slot] = hash
    end
    set.slots = slots
    set.shift = shift
    return set
end

function _addhashes!(set::_HashSet, values::Vector{P}) where {P}
    for value in values
        _addhash!(set, _bloomhash(value))
    end
    return set
end

function _addhashes!(set::_HashSet, values::_BytesBuffer)
    data = values.data
    GC.@preserve data for index in eachindex(values.starts)
        pointer, length = _bytesview(values, index)
        _addhash!(set, _xxhash64(pointer, length))
    end
    return set
end

_newdictionary(::Vector{P}) where {P} = _NumberDictionary{P}()
_newdictionary(buffer::_BytesBuffer) = _BytesDictionary(buffer)

# Dictionary indices of values[first:last], in indices[first:last]. The table lives in
# locals between insertions, so a lookup is a hash, a probe, and one compare.
function _dictionaryencode!(indices::Vector{UInt32}, dictionary::_NumberDictionary{P,U},
        values::Vector{P}, first::Int, last::Int) where {P,U}
    slots = dictionary.slots
    entries = dictionary.values
    shift = dictionary.shift
    @inbounds for index in first:last
        value = values[index]
        bits = reinterpret(U, value)
        slot = _hashslot(bits, shift)
        while true
            entry = slots[slot]
            if iszero(entry)
                indices[index] = _dictionaryinsert!(dictionary, value, slot)
                slots = dictionary.slots
                shift = dictionary.shift
                break
            elseif reinterpret(U, entries[entry]) == bits
                indices[index] = UInt32(entry - 1)
                break
            end
            slot = (slot & (length(slots) - 1)) + 1
        end
    end
    return indices
end

function _dictionaryencode!(indices::Vector{UInt32}, dictionary::_BytesDictionary,
        values::_BytesBuffer, first::Int, last::Int)
    data = values.data
    GC.@preserve data begin
        @inbounds for index in first:last
            indices[index] = _dictionaryindex!(dictionary,
                pointer(data, values.starts[index]), Int(values.lengths[index]))
        end
    end
    return indices
end

# Bit width of the indices into a dictionary of `count` entries (at least one, as
# Arrow C++ writes).
_indexbitwidth(count::Int) = max(1, 64 - leading_zeros(UInt64(max(count, 1) - 1)))

# PLAIN payload of a dictionary page.
_dictionarypayload!(output::Vector{UInt8}, dictionary::_BytesDictionary) =
    append!(output, dictionary.entries.data)

function _dictionarypayload!(output::Vector{UInt8}, dictionary::_NumberDictionary)
    return _plainpayload!(output, dictionary.values)
end

## PLAIN and explicit value encodings

function _plainpayload!(output::Vector{UInt8}, values::Vector{P}) where {P<:Union{Int32,Int64,Float32,Float64}}
    base = length(output)
    bytes = sizeof(P) * length(values)
    resize!(output, base + bytes)
    GC.@preserve output values unsafe_copyto!(pointer(output, base + 1),
        Ptr{UInt8}(pointer(values)), bytes)
    return output
end

function _plainpayload!(output::Vector{UInt8}, values::Vector{Bool})
    base = length(output)
    count = cld(length(values), 8)
    resize!(output, base + count)
    fill!(view(output, (base + 1):(base + count)), 0x00)
    @inbounds for (index, value) in enumerate(values)
        value || continue
        zero = index - 1
        output[base + (zero >> 3) + 1] |= 0x01 << (zero & 7)
    end
    return output
end

_plainpayload!(output::Vector{UInt8}, values::_BytesBuffer) = append!(output, values.data)

function _bytesviews(values::_BytesBuffer)
    return [view(values.data, start:(start + Int(length) - 1))
        for (start, length) in zip(values.starts, values.lengths)]
end

function _bytesmatrix(values::_BytesBuffer, width::Int)
    return reshape(copy(values.data), width, length(values.starts))
end

function _explicitpayload!(output::Vector{UInt8}, encoding::Metadata.Encoding.T,
        values, element::Metadata.SchemaElement)
    if encoding == Metadata.Encoding.PLAIN
        _plainpayload!(output, values)
    elseif encoding == Metadata.Encoding.DELTA_BINARY_PACKED
        append!(output, encode_delta_binary_packed(values))
    elseif encoding == Metadata.Encoding.BYTE_STREAM_SPLIT
        append!(output, values isa _BytesBuffer ?
            encode_byte_stream_split_fixed(_bytesmatrix(values, Int(element.type_length))) :
            encode_byte_stream_split(values))
    elseif encoding == Metadata.Encoding.DELTA_LENGTH_BYTE_ARRAY
        append!(output, encode_delta_length_byte_array(_bytesviews(values)))
    elseif encoding == Metadata.Encoding.DELTA_BYTE_ARRAY
        append!(output, values.prefixed ? encode_delta_byte_array(_bytesviews(values)) :
            encode_delta_byte_array_fixed(_bytesmatrix(values, Int(element.type_length))))
    else
        # RLE-encoded BOOLEAN values carry a 4-byte length like V1 levels.
        start = length(output)
        resize!(output, start + 4)
        _encodehybrid!(output, UInt8.(values), 1)
        _storeuint32!(output, start + 1, UInt32(length(output) - start - 4))
    end
    return output
end

## Statistics

# The sort order of a leaf's statistics, from its annotation.
@enum _Order::UInt8 _ORDER_SIGNED _ORDER_UNSIGNED _ORDER_FLOAT _ORDER_BOOLEAN _ORDER_BYTES _ORDER_DECIMAL _ORDER_FLOAT16 _ORDER_NONE

function _statisticorder(element::Metadata.SchemaElement)
    semantics = _leafstatisticsemantics(element)
    if semantics.floating
        return element.type_ == Metadata.Type.FIXED_LEN_BYTE_ARRAY ? _ORDER_FLOAT16 :
            _ORDER_FLOAT
    end
    comparison = semantics.comparison
    comparison === :signed && return _ORDER_SIGNED
    comparison === :unsigned && return _ORDER_UNSIGNED
    comparison === :boolean && return _ORDER_BOOLEAN
    comparison === :unsigned_bytes && return _ORDER_BYTES
    if comparison === :decimal
        return element.type_ in (Metadata.Type.INT32, Metadata.Type.INT64) ?
            _ORDER_SIGNED : _ORDER_DECIMAL
    end
    return _ORDER_NONE
end

# IEEE 754 total order as unsigned integers: negative values reverse, positive values
# move above them.
# Branch-free, so the bounds loops vectorize: the arithmetic shift spreads the sign bit.
@inline function _totalkey(bits::U) where {U<:Unsigned}
    sign = one(U) << (8 * sizeof(U) - 1)
    return bits ⊻ (reinterpret(U, reinterpret(signed(U), bits) >> (8 * sizeof(U) - 1)) | sign)
end

# A NaN's magnitude bits exceed infinity's.
@inline _isnanbits(bits::UInt16) = bits & 0x7fff > 0x7c00
@inline _isnanbits(bits::UInt32) = bits & 0x7fffffff > 0x7f800000
@inline _isnanbits(bits::UInt64) = bits & 0x7fffffffffffffff > 0x7ff0000000000000

# The comparable key of a fixed-width value: signed integers as themselves, unsigned
# annotations by their bits, floats by IEEE 754 total order.
struct _SignedKey end
struct _UnsignedKey end
struct _FloatKey end
@inline (::_SignedKey)(value) = value
@inline (::_UnsignedKey)(value::Union{Int32,Int64}) = unsigned(value)
@inline (::_FloatKey)(value::Float32) = _totalkey(reinterpret(UInt32, value))
@inline (::_FloatKey)(value::Float64) = _totalkey(reinterpret(UInt64, value))

_orderkey(::Type{<:AbstractFloat}, ::_Order) = _FloatKey()
_orderkey(::Type{P}, order::_Order) where {P} =
    order == _ORDER_UNSIGNED ? _UnsignedKey() : _SignedKey()

# Minimum and maximum of a page's values: (found, min, max, nans). Floats skip NaN
# unless every value is NaN.
function _numberbounds(values::Vector{P}, order::_Order) where {P}
    isempty(values) && return false, zero(P), zero(P), 0
    key = _orderkey(P, order)
    return key isa _UnsignedKey ? _numberbounds(values, _UnsignedKey()) :
        _numberbounds(values, key)
end

@inline _fromtotalkey(key::U) where {U<:Unsigned} =
    iszero(key & (one(U) << (8 * sizeof(U) - 1))) ? ~key : key ⊻ (one(U) << (8 * sizeof(U) - 1))

function _numberbounds(values::Vector{P}, ::_SignedKey) where {P<:Union{Int32,Int64}}
    lo = hi = first(values)
    @inbounds @simd for index in eachindex(values)
        value = values[index]
        lo = min(lo, value)
        hi = max(hi, value)
    end
    return true, lo, hi, 0
end

function _numberbounds(values::Vector{P}, ::_UnsignedKey) where {P<:Union{Int32,Int64}}
    lo = hi = unsigned(first(values))
    @inbounds @simd for index in eachindex(values)
        value = unsigned(values[index])
        lo = min(lo, value)
        hi = max(hi, value)
    end
    return true, reinterpret(P, lo), reinterpret(P, hi), 0
end

function _numberbounds(values::Vector{Bool}, ::_SignedKey)
    return true, all(values), any(values), 0
end

# Float bounds follow the type-defined order's rules: NaN is left out (a page of only NaN
# has no bounds), and a zero minimum is -0.0 and a zero maximum +0.0.
function _numberbounds(values::Vector{P}, ::_FloatKey) where {P<:Union{Float32,Float64}}
    U = P === Float32 ? UInt32 : UInt64
    lo = typemax(U)
    hi = zero(U)
    nans = 0
    @inbounds @simd for index in eachindex(values)
        bits = reinterpret(U, values[index])
        key = _totalkey(bits)
        nan = _isnanbits(bits)
        nans += nan
        lo = min(lo, ifelse(nan, typemax(U), key))
        hi = max(hi, ifelse(nan, zero(U), key))
    end
    nans == length(values) && return false, zero(P), zero(P), nans
    lower, upper = reinterpret(P, _fromtotalkey(lo)), reinterpret(P, _fromtotalkey(hi))
    return true, iszero(lower) ? -zero(P) : lower, iszero(upper) ? zero(P) : upper, nans
end

# Three-way comparison of two byte strings under a byte order.
function _comparebytes(left::Ptr{UInt8}, leftlength::Int, right::Ptr{UInt8},
        rightlength::Int, order::_Order)
    if order == _ORDER_DECIMAL
        # Equal widths compare as signed big-endian integers: the first byte is signed.
        leftsign = leftlength > 0 && unsafe_load(left) >= 0x80
        rightsign = rightlength > 0 && unsafe_load(right) >= 0x80
        leftsign != rightsign && return leftsign ? -1 : 1
    elseif order == _ORDER_FLOAT16
        leftkey = _totalkey(UInt16(unsafe_load(left)) | (UInt16(unsafe_load(left + 1)) << 8))
        rightkey = _totalkey(UInt16(unsafe_load(right)) | (UInt16(unsafe_load(right + 1)) << 8))
        return cmp(leftkey, rightkey)
    end
    common = min(leftlength, rightlength)
    result = Base.memcmp(left, right, common)
    result != 0 && return result < 0 ? -1 : 1
    return cmp(leftlength, rightlength)
end

@inline _float16bits(pointer::Ptr{UInt8}) =
    UInt16(unsafe_load(pointer)) | (UInt16(unsafe_load(pointer + 1)) << 8)

# Indexes of the minimum and maximum of a page's byte values, and their NaN count.
function _bytesbounds(values::_BytesBuffer, order::_Order)
    count = length(values.starts)
    count == 0 && return false, 0, 0, 0
    data = values.data
    lo = hi = 0
    nans = 0
    GC.@preserve data begin
        for index in 1:count
            pointer, length = _bytesview(values, index)
            if order == _ORDER_FLOAT16 && _isnanbits(_float16bits(pointer))
                nans += 1
                continue
            end
            if lo == 0
                lo = hi = index
                continue
            end
            lopointer, lolength = _bytesview(values, lo)
            hipointer, hilength = _bytesview(values, hi)
            _comparebytes(pointer, length, lopointer, lolength, order) < 0 && (lo = index)
            _comparebytes(pointer, length, hipointer, hilength, order) > 0 && (hi = index)
        end
    end
    # A page of only FLOAT16 NaN has no bounds.
    return lo != 0, lo, hi, nans
end

# Bounds of the entries `indices` use, marking them in `marks` first.
function _usedbounds(entries::_BytesBuffer, indices::Vector{UInt32}, marks::Vector{Bool},
        order::_Order)
    count = length(entries.starts)
    fill!(resize!(marks, count), false)
    @inbounds for index in indices
        marks[Int(index) + 1] = true
    end
    lo = hi = 0
    data = entries.data
    GC.@preserve data for entry in 1:count
        @inbounds marks[entry] || continue
        if lo == 0
            lo = hi = entry
            continue
        end
        pointer, length = _bytesview(entries, entry)
        lopointer, lolength = _bytesview(entries, lo)
        hipointer, hilength = _bytesview(entries, hi)
        _comparebytes(pointer, length, lopointer, lolength, order) < 0 && (lo = entry)
        _comparebytes(pointer, length, hipointer, hilength, order) > 0 && (hi = entry)
    end
    return lo != 0, lo, hi
end

_boundbytes(value::P) where {P<:Union{Int32,Int64,Float32,Float64}} =
    collect(reinterpret(UInt8, [value]))
_boundbytes(value::Bool) = UInt8[value ? 0x01 : 0x00]

function _boundbytes(values::_BytesBuffer, index::Int)
    start = values.starts[index]
    return values.data[start:(start + Int(values.lengths[index]) - 1)]
end

function _comparebounds(left::Vector{UInt8}, right::Vector{UInt8}, order::_Order,
        ::Type{P}) where {P}
    if P <: Union{Int32,Int64,Float32,Float64,Bool}
        key = _orderkey(P, order)
        return cmp(key(reinterpret(P, left)[1]), key(reinterpret(P, right)[1]))
    end
    return GC.@preserve left right _comparebytes(pointer(left), length(left),
        pointer(right), length(right), order)
end

## Column chunks

# File-wide encoding settings.
struct _ChunkOptions
    codec::Metadata.CompressionCodec.T
    level::Union{Nothing,Int}
    encoder::Any
    v2::Bool
    pagesize::Int
    pagerows::Int
    checksum::Bool
    statistics::Bool
    pageindex::Bool
    statlimit::Int64
    limits::Limits
end

# One leaf column of the schema and how its chunks are encoded. `utf8` marks STRING and
# ENUM leaves, whose values must be UTF-8.
struct _LeafSpec
    element::Metadata.SchemaElement
    path::Vector{String}
    maxrep::Int
    maxdef::Int
    order::_Order
    encoding::Union{Nothing,Metadata.Encoding.T}
    dictionary::Bool
    bloom::Bool
    utf8::Bool
end

function _isutf8leaf(element::Metadata.SchemaElement)
    element.type_ == Metadata.Type.BYTE_ARRAY || return false
    logical = element.logicalType
    logical !== nothing && (logical.STRING !== nothing || logical.ENUM !== nothing) &&
        return true
    return element.converted_type in (Metadata.ConvertedType.UTF8,
        Metadata.ConvertedType.ENUM)
end

# Row `row` (1-based, counted from the start of its column chunk) holds a STRING or ENUM
# value that is not UTF-8; the writer turns it into an error naming the column.
struct _InvalidUTF8 <: Exception
    row::Int
    leaf::Vector{String}
end

# A dictionary is probed on the chunk's first page with values: it is kept only when
# it makes that page smaller, and stops growing once it passes _DICTIONARY_LIMIT.
@enum _DictionaryState::UInt8 _DICT_OFF _DICT_PROBING _DICT_ACTIVE _DICT_FULL _DICT_POOLED

struct _PageRecord
    offset::Int64
    size::Int32
    firstrow::Int64
    nulls::Int64
    found::Bool
    min::Vector{UInt8}
    max::Vector{UInt8}
end

mutable struct _ChunkWriter{B,D}
    leaf::_LeafSpec
    options::_ChunkOptions
    values::B
    definitions::Vector{UInt16}
    repetitions::Vector{UInt16}
    indices::Vector{UInt32}
    body::Vector{UInt8}
    page::Vector{UInt8}
    compressed::Vector{UInt8}
    data::Vector{UInt8}
    dictionary::D
    state::_DictionaryState
    pages::Vector{_PageRecord}
    dictionarypages::Int32
    plainpages::Int32
    entries::Int64
    nulls::Int64
    nans::Int64
    # A page had values but only NaN, so it has no bounds (see _nanpage!).
    nanpage::Bool
    uncompressed::Int64
    found::Bool
    min::Vector{UInt8}
    max::Vector{UInt8}
    # The distinct value hashes of a chunk that gets a bloom filter.
    hashes::Union{Nothing,_HashSet}
end

function _pagebuffer(element::Metadata.SchemaElement, limits::Limits)
    physical = element.type_
    physical == Metadata.Type.BOOLEAN && return Bool[]
    physical == Metadata.Type.INT32 && return Int32[]
    physical == Metadata.Type.INT64 && return Int64[]
    physical == Metadata.Type.FLOAT && return Float32[]
    physical == Metadata.Type.DOUBLE && return Float64[]
    maxlength = Int(min(limits.max_string_bytes, typemax(Int32)))
    physical == Metadata.Type.BYTE_ARRAY && return _BytesBuffer(true, 0, maxlength)
    physical == Metadata.Type.FIXED_LEN_BYTE_ARRAY &&
        return _BytesBuffer(false, Int(element.type_length), maxlength)
    throw(UnsupportedFeatureError("writing $physical columns is not supported"))
end

# Buffers one task reuses across the column chunks it encodes, so a chunk allocates
# little more than its own output.
struct _EncodeScratch
    buffers::Dict{Any,Any}
    dictionaries::Dict{Any,Any}
    hashes::_HashSet
    definitions::Vector{UInt16}
    repetitions::Vector{UInt16}
    indices::Vector{UInt32}
    body::Vector{UInt8}
    page::Vector{UInt8}
    compressed::Vector{UInt8}
end

_newscratch() = _EncodeScratch(Dict{Any,Any}(), Dict{Any,Any}(), _HashSet(), UInt16[],
    UInt16[], UInt32[], UInt8[], UInt8[], UInt8[])

_encodescratch() =
    get!(_newscratch, task_local_storage(), :ParquetEncodeScratch)::_EncodeScratch

function _scratchbuffer(scratch::_EncodeScratch, element::Metadata.SchemaElement,
        limits::Limits)
    fresh = _pagebuffer(element, limits)
    key = fresh isa _BytesBuffer ? (_BytesBuffer, fresh.prefixed) : typeof(fresh)
    buffer = get!(scratch.buffers, key, fresh)
    if buffer isa _BytesBuffer
        buffer.width = fresh.width
        buffer.maxlength = fresh.maxlength
    end
    return _resetbuffer!(buffer)
end

function _scratchdictionary(scratch::_EncodeScratch, values)
    values isa Vector{Bool} && return nothing
    key = values isa _BytesBuffer ? (_BytesDictionary, values.prefixed) : typeof(values)
    dictionary = get!(() -> _newdictionary(values), scratch.dictionaries, key)
    if dictionary isa _BytesDictionary
        dictionary.entries.width = values.width
        dictionary.entries.maxlength = values.maxlength
    end
    return _resetdictionary!(dictionary)
end

function _resetdictionary!(dictionary::_NumberDictionary)
    empty!(dictionary.values)
    _resetslots!(dictionary)
    return dictionary
end

function _resetdictionary!(dictionary::_BytesDictionary)
    _resetbuffer!(dictionary.entries)
    empty!(dictionary.hashes)
    _resetslots!(dictionary)
    return dictionary
end

# Large tables shrink back so a reset never clears megabytes for a small chunk.
function _resetslots!(table)
    if length(table.slots) > 1 << 16
        table.slots = zeros(eltype(table.slots), 1024)
        table.shift = 64 - 10
    else
        fill!(table.slots, zero(eltype(table.slots)))
    end
    return table
end

function _ChunkWriter(leaf::_LeafSpec, options::_ChunkOptions, scratch::_EncodeScratch)
    values = _scratchbuffer(scratch, leaf.element, options.limits)
    dictionary = _scratchdictionary(scratch, values)
    usedictionary = leaf.encoding === nothing && leaf.dictionary && dictionary !== nothing
    empty!(scratch.definitions)
    empty!(scratch.repetitions)
    return _ChunkWriter{typeof(values),typeof(dictionary)}(leaf, options, values,
        scratch.definitions, scratch.repetitions, scratch.indices, scratch.body,
        scratch.page, scratch.compressed, UInt8[], dictionary,
        usedictionary ? _DICT_PROBING : _DICT_OFF, _PageRecord[], Int32(0), Int32(0),
        Int64(0), Int64(0), Int64(0), false, Int64(0), false, UInt8[], UInt8[],
        leaf.bloom ? _resethashes!(scratch.hashes) : nothing)
end

# A pooled chunk buffers dictionary indices instead of values.
_pagevalues(chunk::_ChunkWriter) = chunk.state == _DICT_POOLED ? length(chunk.indices) :
    _buffercount(chunk.values)

_pageentries(chunk::_ChunkWriter) = chunk.leaf.maxrep > 0 ? length(chunk.repetitions) :
    chunk.leaf.maxdef > 0 ? length(chunk.definitions) : _pagevalues(chunk)

# Encodes the buffered page values into `chunk.body` and returns the page's encoding.
function _encodepagevalues!(chunk::_ChunkWriter)
    body = chunk.body
    values = chunk.values
    leaf = chunk.leaf
    leaf.encoding === nothing ||
        return _explicitpayload!(body, leaf.encoding, values, leaf.element),
            leaf.encoding
    state = chunk.state
    if state == _DICT_POOLED
        bitwidth = _indexbitwidth(_dictionarycount(chunk.dictionary))
        push!(body, UInt8(bitwidth))
        _encodehybrid!(body, chunk.indices, bitwidth)
        return body, Metadata.Encoding.RLE_DICTIONARY
    end
    # A page without values never commits a probing chunk to its dictionary.
    if state == _DICT_OFF || state == _DICT_FULL ||
            (state == _DICT_PROBING && _buffercount(values) == 0)
        _plainpayload!(body, values)
        return body, Metadata.Encoding.PLAIN
    end
    dictionary = chunk.dictionary
    count = _buffercount(values)
    indices = resize!(chunk.indices, count)
    first = 1
    if state == _DICT_PROBING && count > _PROBE_PREFIX
        # A probe page whose first _PROBE_PREFIX values are nearly all distinct would not
        # be smaller with a dictionary, so the rest of the page is not hashed. Repeated
        # values, sorted runs, and short cycles all show up within the prefix.
        _dictionaryencode!(indices, dictionary, values, 1, _PROBE_PREFIX)
        if _dictionarycount(dictionary) > _PROBE_DISTINCT
            chunk.state = _DICT_OFF
            _resetdictionary!(dictionary)
            _plainpayload!(body, values)
            return body, Metadata.Encoding.PLAIN
        end
        first = _PROBE_PREFIX + 1
    end
    _dictionaryencode!(indices, dictionary, values, first, count)
    bitwidth = _indexbitwidth(_dictionarycount(dictionary))
    push!(body, UInt8(bitwidth))
    _encodehybrid!(body, chunk.indices, bitwidth)
    if chunk.state == _DICT_PROBING
        if length(body) + _dictionarybytes(dictionary) >= _bufferbytes(values)
            # The dictionary does not pay for itself on this page; write the chunk PLAIN.
            chunk.state = _DICT_OFF
            _resetdictionary!(dictionary)
            empty!(body)
            _plainpayload!(body, values)
            return body, Metadata.Encoding.PLAIN
        end
        chunk.state = _DICT_ACTIVE
    end
    _dictionarybytes(dictionary) > _DICTIONARY_LIMIT && (chunk.state = _DICT_FULL)
    return body, Metadata.Encoding.RLE_DICTIONARY
end

# Updates the page and chunk statistics; returns the page's (found, min, max) bounds.
function _pagebounds!(chunk::_ChunkWriter{B}, encoding::Metadata.Encoding.T) where {B}
    order = chunk.leaf.order
    order == _ORDER_NONE && return false, UInt8[], UInt8[]
    values = chunk.values
    if chunk.state == _DICT_POOLED
        # Pooled dictionaries are sorted, so the page's extreme indices are its bounds.
        isempty(chunk.indices) && return false, UInt8[], UInt8[]
        lo, hi = extrema(chunk.indices)
        minimum, maximum = _dictionarybound(chunk.dictionary, Int(lo) + 1),
            _dictionarybound(chunk.dictionary, Int(hi) + 1)
        P = values isa _BytesBuffer ? _BytesBuffer : eltype(values)
        nans = 0
    elseif values isa _BytesBuffer && encoding == Metadata.Encoding.RLE_DICTIONARY &&
            order != _ORDER_FLOAT16
        # A dictionary page's bounds are among the entries it uses, compared once each.
        entries = chunk.dictionary.entries
        found, lo, hi = _usedbounds(entries, chunk.indices, chunk.dictionary.marks, order)
        found || return false, UInt8[], UInt8[]
        minimum, maximum = _boundbytes(entries, lo), _boundbytes(entries, hi)
        P = _BytesBuffer
        nans = 0
    elseif values isa _BytesBuffer
        found, lo, hi, nans = _bytesbounds(values, order)
        chunk.nans += nans
        found || return _nanpage!(chunk, nans)
        minimum, maximum = _boundbytes(values, lo), _boundbytes(values, hi)
        if order == _ORDER_FLOAT16
            # FLOAT16 zero bounds as for other floats: -0.0 minimum, +0.0 maximum.
            _float16bits(minimum) & 0x7fff == 0 && (minimum = UInt8[0x00, 0x80])
            _float16bits(maximum) & 0x7fff == 0 && (maximum = UInt8[0x00, 0x00])
        end
        P = _BytesBuffer
        nans = 0
    else
        found, lo, hi, nans = _numberbounds(values, order)
        chunk.nans += nans
        found || return _nanpage!(chunk, nans)
        minimum, maximum = _boundbytes(lo), _boundbytes(hi)
        P = eltype(values)
        nans = 0
    end
    chunk.nans += nans
    if !chunk.found
        chunk.found = true
        chunk.min = minimum
        chunk.max = maximum
    else
        _comparebounds(minimum, chunk.min, order, P) < 0 && (chunk.min = minimum)
        _comparebounds(maximum, chunk.max, order, P) > 0 && (chunk.max = maximum)
    end
    return true, minimum, maximum
end

# A page whose values are all NaN has no bounds, and a column index, whose pages must each
# have bounds unless they hold only nulls, is not written for its chunk.
function _nanpage!(chunk::_ChunkWriter, nans::Int)
    nans > 0 && (chunk.nanpage = true)
    return false, UInt8[], UInt8[]
end

_float16bits(bound::Vector{UInt8}) = UInt16(bound[1]) | (UInt16(bound[2]) << 8)

_levelwidth(level::Int) = 64 - leading_zeros(UInt64(level))

# Compresses `source` into the reusable `buffer` and returns a view of the result.
function _compressinto!(buffer::Vector{UInt8}, options::_ChunkOptions,
        source::Vector{UInt8})
    encoder = options.encoder
    bound = ChunkCodecCore.encode_bound(encoder, length(source))
    length(buffer) < bound && resize!(buffer, bound)
    written = ChunkCodecCore.try_encode!(encoder, buffer, source)
    ChunkCodecCore.is_size(written) || throw(ArgumentError(
        "$(codecname(options.codec)) output exceeded its own size bound"))
    return view(buffer, 1:Int64(written))
end

# The index of the first value in `buffer` that is not UTF-8, or 0. A buffer whose bytes
# are all ASCII (length prefixes included) passes in one scan.
function _invalidutf8value(buffer::_BytesBuffer)
    data = buffer.data
    _isasciibytes(view(data, 1:length(data))) && return 0
    for index in eachindex(buffer.starts)
        start = buffer.starts[index]
        value = view(data, start:(start + Int(buffer.lengths[index]) - 1))
        _isasciibytes(value) || isvalid(String, value) || return index
    end
    return 0
end

# Every value of a STRING or ENUM page must be UTF-8.
function _checkutf8(chunk::_ChunkWriter{_BytesBuffer}, firstrow::Int)
    index = _invalidutf8value(chunk.values)
    index == 0 || throw(_InvalidUTF8(firstrow + _valuerow(chunk, index), chunk.leaf.path))
    return
end

_checkutf8(::_ChunkWriter, ::Int) = nothing

# The row, counted from 1 within its page, of the page's `value`-th present value.
function _valuerow(chunk::_ChunkWriter, value::Int)
    leaf = chunk.leaf
    leaf.maxdef == 0 && return value
    maxdef = UInt16(leaf.maxdef)
    row = 0
    seen = 0
    for (entry, level) in enumerate(chunk.definitions)
        (leaf.maxrep == 0 || chunk.repetitions[entry] == 0x0000) && (row += 1)
        level == maxdef && (seen += 1) == value && return row
    end
    return row
end

# Frames the buffered page into `chunk.data` and resets the page buffers.
function _flushpage!(chunk::_ChunkWriter, rows::Int, firstrow::Int)
    leaf = chunk.leaf
    options = chunk.options
    # A pooled chunk's values are its dictionary entries, checked when it is built.
    leaf.utf8 && chunk.state != _DICT_POOLED && _checkutf8(chunk, firstrow)
    entries = _pageentries(chunk)
    count = _pagevalues(chunk)
    nulls = entries - count
    empty!(chunk.body)
    body, encoding = _encodepagevalues!(chunk)
    found, minimum, maximum = options.statistics ? _pagebounds!(chunk, encoding) :
        (false, UInt8[], UInt8[])
    page = chunk.page
    empty!(page)
    leaf.maxrep > 0 && _encodelevels!(page, chunk.repetitions, _levelwidth(leaf.maxrep),
        !options.v2)
    replength = length(page)
    if leaf.maxdef > 0
        width = _levelwidth(leaf.maxdef)
        # A flat page without nulls has one run of the maximum level.
        leaf.maxrep == 0 && nulls == 0 && entries >= 8 ?
            _encodelevelrun!(page, entries, UInt16(leaf.maxdef), width, !options.v2) :
            _encodelevels!(page, chunk.definitions, width, !options.v2)
    end
    levellength = length(page)
    limits = options.limits
    if options.v2
        compressed = options.codec == Metadata.CompressionCodec.UNCOMPRESSED ? body :
            _compressinto!(chunk.compressed, options, body)
        iscompressed = compressed !== body && !isempty(body) && length(compressed) < length(body)
        stored = iscompressed ? compressed : body
        uncompressedsize = levellength + length(body)
        compressedsize = levellength + length(stored)
        _checklimit(:page_bytes, uncompressedsize, limits.max_page_bytes)
        _checklimit(:page_bytes, compressedsize, limits.max_page_bytes)
        uncompressedsize <= typemax(Int32) || throw(LimitError(:page_bytes,
            Int64(uncompressedsize), Int64(typemax(Int32))))
        append!(page, stored)
        header = Metadata.PageHeader(type_=Metadata.PageType.DATA_PAGE_V2,
            uncompressed_page_size=Int32(uncompressedsize),
            compressed_page_size=Int32(compressedsize),
            crc=options.checksum ? reinterpret(Int32, pagechecksum(page)) : nothing,
            data_page_header_v2=Metadata.DataPageHeaderV2(num_values=Int32(entries),
                num_nulls=Int32(nulls), num_rows=Int32(rows), encoding=encoding,
                definition_levels_byte_length=Int32(levellength - replength),
                repetition_levels_byte_length=Int32(replength),
                is_compressed=iscompressed))
        frame = page
        uncompressed = uncompressedsize
    else
        append!(page, body)
        _checklimit(:page_bytes, length(page), limits.max_page_bytes)
        length(page) <= typemax(Int32) || throw(LimitError(:page_bytes,
            Int64(length(page)), Int64(typemax(Int32))))
        frame = options.codec == Metadata.CompressionCodec.UNCOMPRESSED ? page :
            _compressinto!(chunk.compressed, options, page)
        _checklimit(:page_bytes, length(frame), limits.max_page_bytes)
        header = Metadata.PageHeader(type_=Metadata.PageType.DATA_PAGE,
            uncompressed_page_size=Int32(length(page)),
            compressed_page_size=Int32(length(frame)),
            crc=options.checksum ? reinterpret(Int32, pagechecksum(frame)) : nothing,
            data_page_header=Metadata.DataPageHeader(num_values=Int32(entries),
                encoding=encoding, definition_level_encoding=Metadata.Encoding.RLE,
                repetition_level_encoding=Metadata.Encoding.RLE))
        uncompressed = length(page)
    end
    headerbytes = Thrift.encode(header)
    _checklimit(:page_header_bytes, length(headerbytes), limits.max_page_header_bytes)
    offset = Int64(length(chunk.data))
    append!(chunk.data, headerbytes)
    append!(chunk.data, frame)
    framesize = length(chunk.data) - offset
    framesize <= typemax(Int32) || throw(LimitError(:page_bytes, Int64(framesize),
        Int64(typemax(Int32))))
    push!(chunk.pages, _PageRecord(offset, Int32(framesize), Int64(firstrow),
        Int64(nulls), found, minimum, maximum))
    encoding == Metadata.Encoding.RLE_DICTIONARY ?
        (chunk.dictionarypages += Int32(1)) : (chunk.plainpages += Int32(1))
    chunk.entries += entries
    chunk.nulls += nulls
    chunk.uncompressed += length(headerbytes) + uncompressed
    chunk.hashes === nothing || _addhashes!(chunk.hashes, chunk.values)
    _resetbuffer!(chunk.values)
    empty!(chunk.definitions)
    empty!(chunk.repetitions)
    return
end

"""
    _EncodedChunk

An encoded column chunk: its dictionary page (empty when unused) and data pages, ready to
write in that order, with the metadata that records them. Page offsets are relative to
the start of `data`.
"""
struct _EncodedChunk
    dictionary::Vector{UInt8}
    data::Vector{UInt8}
    encodings::Vector{Metadata.Encoding.T}
    encodingstats::Vector{Metadata.PageEncodingStats}
    values::Int64
    uncompressed::Int64
    statistics::Union{Nothing,Metadata.Statistics}
    pages::Vector{_PageRecord}
    columnindex::Union{Nothing,Metadata.ColumnIndex}
    # The bloom filter as stored (header and bitset); empty when the chunk has none.
    bloom::Vector{UInt8}
end

function _dictionaryframe(chunk::_ChunkWriter)
    options = chunk.options
    payload = _dictionarypayload!(UInt8[], chunk.dictionary)
    _checklimit(:page_bytes, length(payload), options.limits.max_page_bytes)
    length(payload) <= typemax(Int32) || throw(LimitError(:page_bytes,
        Int64(length(payload)), Int64(typemax(Int32))))
    compressed = options.codec == Metadata.CompressionCodec.UNCOMPRESSED ? payload :
        _compressinto!(chunk.compressed, options, payload)
    _checklimit(:page_bytes, length(compressed), options.limits.max_page_bytes)
    header = Metadata.PageHeader(type_=Metadata.PageType.DICTIONARY_PAGE,
        uncompressed_page_size=Int32(length(payload)),
        compressed_page_size=Int32(length(compressed)),
        crc=options.checksum ? reinterpret(Int32, pagechecksum(compressed)) : nothing,
        dictionary_page_header=Metadata.DictionaryPageHeader(
            num_values=Int32(_dictionarycount(chunk.dictionary)),
            encoding=Metadata.Encoding.PLAIN,
            is_sorted=chunk.state == _DICT_POOLED && chunk.leaf.order != _ORDER_NONE))
    headerbytes = Thrift.encode(header)
    frame = append!(headerbytes, compressed)
    return frame, Int64(length(frame) - length(compressed) + length(payload))
end

# A BYTE_ARRAY page bound longer than _INDEX_BOUND_BYTES is truncated: the minimum to a
# prefix, the maximum to a prefix with its last byte incremented, which still bounds it.
# STRING and ENUM bounds are cut between characters and increment a character, so they
# stay UTF-8. Other bounds (FIXED_LEN_BYTE_ARRAY ones must keep their width) are whole.
function _indexbound(bound::Vector{UInt8}, leaf::_LeafSpec, upper::Bool)
    leaf.order == _ORDER_BYTES && leaf.element.type_ == Metadata.Type.BYTE_ARRAY &&
        length(bound) > _INDEX_BOUND_BYTES || return bound
    leaf.utf8 && return _utf8indexbound(bound, upper)
    prefix = bound[1:_INDEX_BOUND_BYTES]
    upper || return prefix
    for index in _INDEX_BOUND_BYTES:-1:1
        if prefix[index] != 0xff
            prefix[index] += 0x01
            return prefix[1:index]
        end
    end
    return bound
end

function _utf8indexbound(bound::Vector{UInt8}, upper::Bool)
    characters = Char[]
    bytes = 0
    for character in String(copy(bound))
        (bytes += ncodeunits(character)) > _INDEX_BOUND_BYTES && break
        push!(characters, character)
    end
    upper || return Vector{UInt8}(String(characters))
    for index in length(characters):-1:1
        # The next code point, skipping surrogates, which UTF-8 cannot hold.
        character = characters[index]
        character == '\U10ffff' && continue
        characters[index] = character == '\ud7ff' ? '\ue000' : character + 1
        return Vector{UInt8}(String(characters[1:index]))
    end
    return bound
end

function _boundaryorder(chunk::_ChunkWriter{B}) where {B}
    P = B <: _BytesBuffer ? _BytesBuffer : eltype(B)
    order = chunk.leaf.order
    ascending = descending = true
    previous = nothing
    for page in chunk.pages
        page.found || continue
        if previous !== nothing
            low = _comparebounds(page.min, previous.min, order, P)
            high = _comparebounds(page.max, previous.max, order, P)
            (low < 0 || high < 0) && (ascending = false)
            (low > 0 || high > 0) && (descending = false)
        end
        previous = page
    end
    ascending && return Metadata.BoundaryOrder.ASCENDING
    descending && return Metadata.BoundaryOrder.DESCENDING
    return Metadata.BoundaryOrder.UNORDERED
end

function _finishchunk(chunk::_ChunkWriter)
    leaf = chunk.leaf
    options = chunk.options
    usesdictionary = chunk.dictionarypages > 0
    dictionary, dictionarybytes = usesdictionary ? _dictionaryframe(chunk) :
        (UInt8[], Int64(0))
    plainencoding = something(leaf.encoding, Metadata.Encoding.PLAIN)
    encodings = Metadata.Encoding.T[]
    (leaf.maxrep > 0 || leaf.maxdef > 0) && push!(encodings, Metadata.Encoding.RLE)
    (usesdictionary || chunk.plainpages > 0 && plainencoding == Metadata.Encoding.PLAIN) &&
        push!(encodings, Metadata.Encoding.PLAIN)
    chunk.plainpages > 0 && plainencoding != Metadata.Encoding.PLAIN &&
        !(plainencoding in encodings) && push!(encodings, plainencoding)
    usesdictionary && push!(encodings, Metadata.Encoding.RLE_DICTIONARY)
    pagetype = options.v2 ? Metadata.PageType.DATA_PAGE_V2 : Metadata.PageType.DATA_PAGE
    stats = Metadata.PageEncodingStats[]
    usesdictionary && push!(stats, Metadata.PageEncodingStats(
        page_type=Metadata.PageType.DICTIONARY_PAGE, encoding=Metadata.Encoding.PLAIN,
        count=Int32(1)))
    usesdictionary && push!(stats, Metadata.PageEncodingStats(page_type=pagetype,
        encoding=Metadata.Encoding.RLE_DICTIONARY, count=chunk.dictionarypages))
    chunk.plainpages > 0 && push!(stats, Metadata.PageEncodingStats(page_type=pagetype,
        encoding=plainencoding, count=chunk.plainpages))
    statistics = options.statistics ? _chunkstatistics(chunk) : nothing
    columnindex = options.pageindex && options.statistics && leaf.order != _ORDER_NONE &&
        !chunk.nanpage ? _columnindex(chunk) : nothing
    return _EncodedChunk(dictionary, chunk.data, encodings, stats, chunk.entries,
        chunk.uncompressed + dictionarybytes, statistics, chunk.pages, columnindex,
        _chunkbloom(chunk))
end

# The false-positive probability of the bloom filters the writer sizes (parquet-mr's and
# DuckDB's default).
const _BLOOM_FPP = 0.01

function _chunkbloom(chunk::_ChunkWriter)
    hashes = chunk.hashes
    hashes === nothing && return UInt8[]
    # A pooled chunk buffers no values; its dictionary holds the values it uses.
    chunk.state == _DICT_POOLED && _addhashes!(hashes, _dictionaryvalues(chunk.dictionary))
    count = _hashcount(hashes)
    iszero(count) && return UInt8[]
    words = zeros(UInt32, _bloombytes(count, _BLOOM_FPP) >> 2)
    hashes.zero && _bloominsert!(words, UInt64(0))
    for hash in hashes.slots
        iszero(hash) || _bloominsert!(words, hash)
    end
    return _bloomfilterbytes(words)
end

_dictionaryvalues(dictionary::_NumberDictionary) = dictionary.values
_dictionaryvalues(dictionary::_BytesDictionary) = dictionary.entries

function _chunkstatistics(chunk::_ChunkWriter)
    floating = chunk.leaf.order in (_ORDER_FLOAT, _ORDER_FLOAT16)
    nans = floating ? chunk.nans : nothing
    limit = chunk.options.statlimit
    if !chunk.found || length(chunk.min) > limit || length(chunk.max) > limit
        return Metadata.Statistics(null_count=chunk.nulls, nan_count=nans)
    end
    return Metadata.Statistics(null_count=chunk.nulls, min_value=chunk.min,
        max_value=chunk.max, is_min_value_exact=true, is_max_value_exact=true,
        nan_count=nans)
end

function _columnindex(chunk::_ChunkWriter)
    order = chunk.leaf.order
    pages = chunk.pages
    return Metadata.ColumnIndex(
        null_pages=Bool[!page.found for page in pages],
        min_values=Vector{UInt8}[page.found ? _indexbound(page.min, chunk.leaf, false) :
            UInt8[] for page in pages],
        max_values=Vector{UInt8}[page.found ? _indexbound(page.max, chunk.leaf, true) :
            UInt8[] for page in pages],
        boundary_order=_boundaryorder(chunk),
        null_counts=Int64[page.nulls for page in pages])
end

## Row sources

# The rows of a flat leaf: one level entry and at most one value per row.
# `base` is the chunk row of `column[first]` when a chunk is written in parts.
struct _FlatRows{V<:AbstractVector,F}
    column::V
    first::Int
    last::Int
    convert::F
    optional::Bool
    name::String
    base::Int
end

_FlatRows(column, first, last, convert, optional, name) =
    _FlatRows(column, first, last, convert, optional, name, 0)

function _writerows!(chunk::_ChunkWriter, rows::_FlatRows)
    row = rows.first
    while row <= rows.last
        start = row
        stop = min(rows.last, row + min(chunk.options.pagerows, rows.last) - 1)
        row = _gatherflat!(chunk, rows, row, stop)
        _flushpage!(chunk, row - start, start - rows.first + rows.base)
        start == rows.first && rows.base == 0 &&
            _hintoutput!(chunk, rows.last - rows.first + 1, row - start)
    end
    return chunk
end

# Sizes the chunk's output from its first page, so it grows at most a few times.
function _hintoutput!(chunk::_ChunkWriter, rows::Int, pagerows::Int)
    sizehint!(chunk.data, cld(rows, max(pagerows, 1)) * length(chunk.data) + 256)
    return
end

function _gatherflat!(chunk::_ChunkWriter, rows::_FlatRows, row::Int, stop::Int)
    values = chunk.values
    definitions = chunk.definitions
    limit = chunk.options.pagesize
    column = rows.column
    convert = rows.convert
    optional = rows.optional
    @inbounds while row <= stop
        value = column[row]
        if ismissing(value)
            optional || _requiredmissing(rows.name)
            push!(definitions, 0x0000)
        else
            optional && push!(definitions, 0x0001)
            _appendvalue!(values, convert(value))
        end
        row += 1
        _bufferbytes(values) >= limit && break
    end
    return row
end

# Fixed-width pages fill preallocated buffers: the page's byte limit caps its values up
# front, so the loop has no per-value growth checks.
function _gatherflat!(chunk::_ChunkWriter{Vector{P}}, rows::_FlatRows, row::Int,
        stop::Int) where {P}
    values = chunk.values
    definitions = chunk.definitions
    capacity = _pagecapacity(values, chunk.options.pagesize)
    column = rows.column
    convert = rows.convert
    optional = rows.optional
    resize!(values, min(stop - row + 1, capacity))
    optional && resize!(definitions, stop - row + 1)
    count = 0
    entry = 0
    @inbounds while row <= stop
        value = column[row]
        entry += 1
        row += 1
        if ismissing(value)
            optional || _requiredmissing(rows.name)
            definitions[entry] = 0x0000
        else
            optional && (definitions[entry] = 0x0001)
            count += 1
            values[count] = convert(value)
            count == capacity && break
        end
    end
    resize!(values, count)
    optional && resize!(definitions, entry)
    return row
end

# The values a page holds before reaching `limit` bytes, as the byte check in the
# generic gather counts them.
_pagecapacity(::Vector{Bool}, limit::Int) = limit > typemax(Int) >> 3 ? typemax(Int) : 8 * limit
_pagecapacity(::Vector{P}, limit::Int) where {P} = cld(limit, sizeof(P))

@noinline function _requiredmissing(name::String)
    throw(ArgumentError("column $(repr(name)) has missing values but its Parquet " *
        "field is required; write it with nullable=true or without missing values"))
end

# A shredded leaf: level streams and dense values for whole rows.
struct _LeveledRows{V<:AbstractVector}
    repetitions::Vector{UInt16}
    definitions::Vector{UInt16}
    values::V
    rows::Int
end

function _writerows!(chunk::_ChunkWriter, source::_LeveledRows)
    leaf = chunk.leaf
    maxrep = leaf.maxrep
    maxdef = UInt16(leaf.maxdef)
    repetitions = source.repetitions
    definitions = source.definitions
    values = source.values
    entries = maxrep > 0 ? length(repetitions) : maxdef > 0 ? length(definitions) :
        length(values)
    limit = chunk.options.pagesize
    entry = 1
    value = 1
    row = 0
    @inbounds while row < source.rows
        first = row
        while row < source.rows && row - first < chunk.options.pagerows
            next = entry + 1
            if maxrep > 0
                while next <= entries && repetitions[next] != 0x0000
                    next += 1
                end
            end
            for index in entry:(next - 1)
                maxrep > 0 && push!(chunk.repetitions, repetitions[index])
                if maxdef > 0
                    level = definitions[index]
                    push!(chunk.definitions, level)
                    level == maxdef || continue
                end
                _appendvalue!(chunk.values, values[value])
                value += 1
            end
            entry = next
            row += 1
            _bufferbytes(chunk.values) + 2 * (length(chunk.definitions) +
                length(chunk.repetitions)) >= limit && break
        end
        _flushpage!(chunk, row - first, first)
        first == 0 && _hintoutput!(chunk, source.rows, row)
    end
    return chunk
end

_dictionarybound(dictionary::_NumberDictionary, index::Int) =
    _boundbytes(dictionary.values[index])
_dictionarybound(dictionary::_BytesDictionary, index::Int) =
    _boundbytes(dictionary.entries, index)

# The rows of a pooled flat leaf. `codes[ref + offset]` is a ref's dictionary index plus
# one, or zero for missing.
struct _PooledRows{R<:AbstractVector}
    refs::R
    first::Int
    last::Int
    codes::Vector{Int32}
    offset::Int
    optional::Bool
    name::String
end

function _writerows!(chunk::_ChunkWriter, rows::_PooledRows)
    row = rows.first
    codes = rows.codes
    refs = rows.refs
    while row <= rows.last
        start = row
        stop = min(rows.last, row + min(chunk.options.pagerows, rows.last) - 1)
        indices = chunk.indices
        definitions = chunk.definitions
        empty!(indices)
        @inbounds for index in row:stop
            code = codes[Int(refs[index]) + rows.offset]
            if code == 0
                rows.optional || _requiredmissing(rows.name)
                push!(definitions, 0x0000)
            else
                rows.optional && push!(definitions, 0x0001)
                push!(indices, UInt32(code - 1))
            end
        end
        row = stop + 1
        _flushpage!(chunk, row - start, start - rows.first)
        start == rows.first && _hintoutput!(chunk, rows.last - rows.first + 1, row - start)
    end
    return chunk
end

"""
    _encodepooledchunk(leaf, column, first, last, convert, optional, name, options)

Encode rows `first:last` of a pooled column with a dictionary of the pool entries those
rows use, sorted by value. Returns `nothing` when the leaf does not take a dictionary,
the entries pass _DICTIONARY_LIMIT, or every row is missing; the caller then encodes the
values directly.
"""
function _encodepooledchunk(leaf::_LeafSpec, column, first::Int, last::Int, convert,
        optional::Bool, name::String, options::_ChunkOptions)
    leaf.encoding === nothing && leaf.dictionary || return nothing
    leaf.order in (_ORDER_FLOAT, _ORDER_FLOAT16) && return nothing
    chunk = _ChunkWriter(leaf, options, _encodescratch())
    chunk.dictionary === nothing && return nothing
    # The chunk's buffers take the leaf's physical type, known at run time.
    return _barrier(_encodepooled!, chunk, _pagebuffer(leaf.element, options.limits),
        column, first, last, convert, optional, name)::Union{Nothing,_EncodedChunk}
end

function _encodepooled!(chunk::_ChunkWriter, entries, column, first::Int, last::Int,
        convert, optional::Bool, name::String)
    leaf = chunk.leaf
    dictionary = chunk.dictionary
    pool = column.pool
    refs = column.refs
    low = firstindex(pool)
    used = falses(lastindex(pool) - low + 1)
    @inbounds for index in first:last
        used[Int(refs[index]) - low + 1] = true
    end
    poolrefs = Int[]
    for ref in low:lastindex(pool)
        used[ref - low + 1] || continue
        value = DataAPI.unwrap(pool[ref])
        value === missing && continue
        _appendvalue!(entries, convert(value))
        push!(poolrefs, ref)
    end
    (isempty(poolrefs) || _bufferbytes(entries) > _DICTIONARY_LIMIT) && return nothing
    if leaf.utf8 && entries isa _BytesBuffer
        # The error names the first row that uses an invalid entry.
        position = _invalidutf8value(entries)
        if position != 0
            bad = poolrefs[position]
            row = findfirst(index -> Int(refs[index]) == bad, first:last)::Int
            throw(_InvalidUTF8(row, leaf.path))
        end
    end
    order = _poolorder(entries, leaf.order)
    codes = zeros(Int32, length(used))
    for (rank, position) in enumerate(order)
        codes[poolrefs[position] - low + 1] = Int32(rank)
        _adddictionaryentry!(dictionary, entries, position)
    end
    chunk.state = _DICT_POOLED
    _writerows!(chunk, _PooledRows(refs, first, last, codes, 1 - low, optional, name))
    return _finishchunk(chunk)
end

function _poolorder(entries::Vector{P}, order::_Order) where {P}
    order == _ORDER_NONE && return collect(eachindex(entries))
    return sortperm(entries; by=_orderkey(P, order))
end

function _poolorder(entries::_BytesBuffer, order::_Order)
    count = length(entries.starts)
    order == _ORDER_NONE && return collect(1:count)
    data = entries.data
    return GC.@preserve data sort!(collect(1:count); lt=(left, right) -> begin
        leftpointer, leftlength = _bytesview(entries, left)
        rightpointer, rightlength = _bytesview(entries, right)
        _comparebytes(leftpointer, leftlength, rightpointer, rightlength, order) < 0
    end)
end

_adddictionaryentry!(dictionary::_NumberDictionary, entries::Vector, position::Int) =
    push!(dictionary.values, entries[position])

function _adddictionaryentry!(dictionary::_BytesDictionary, entries::_BytesBuffer,
        position::Int)
    data = entries.data
    GC.@preserve data begin
        pointer, length = _bytesview(entries, position)
        _appendbytes!(dictionary.entries, pointer, length)
    end
    return
end

"""
    _encodechunk(leaf, rows, options) -> _EncodedChunk

Encode one column chunk of `leaf` from a row source.
"""
function _encodechunk(leaf::_LeafSpec, rows, options::_ChunkOptions)
    chunk = _ChunkWriter(leaf, options, _encodescratch())
    # The chunk's buffers take the leaf's physical type, known at run time.
    return _barrier(_encoderows!, chunk, rows)::_EncodedChunk
end

function _encoderows!(chunk::_ChunkWriter, rows)
    _writerows!(chunk, rows)
    return _finishchunk(chunk)
end
