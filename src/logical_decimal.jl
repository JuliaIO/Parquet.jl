"""
    Decimal(unscaled, scale)

An exact Parquet decimal value equal to `unscaled * 10^(-scale)`.
"""
struct Decimal
    unscaled::BigInt
    scale::Int32
    function Decimal(unscaled::Integer, scale::Integer)
        0 <= scale <= typemax(Int32) ||
            throw(ArgumentError("decimal scale must be between 0 and $(typemax(Int32))"))
        return new(BigInt(unscaled), Int32(scale))
    end
end

function Base.:(==)(left::Decimal, right::Decimal)
    return left.scale == right.scale && left.unscaled == right.unscaled
end

function Base.isequal(left::Decimal, right::Decimal)
    return isequal(left.scale, right.scale) && isequal(left.unscaled, right.unscaled)
end

function Base.hash(value::Decimal, seed::UInt)
    return hash(value.unscaled, hash(value.scale, hash(:Decimal, seed)))
end

function Base.copy(value::Decimal)
    return Decimal(copy(value.unscaled), value.scale)
end

function Base.show(io::IO, value::Decimal)
    print(io, "Parquet.Decimal(", value.unscaled, ", ", value.scale, ")")
    return
end

function _decimalparameters(element::Metadata.SchemaElement)
    logical = element.logicalType
    if logical !== nothing
        decimal = logical.DECIMAL
        decimal === nothing && return nothing
        return decimal.precision, decimal.scale
    end
    element.converted_type == Metadata.ConvertedType.DECIMAL || return nothing
    precision = element.precision
    precision === nothing && throw(FormatError(
        "legacy DECIMAL column $(repr(element.name)) has no precision"))
    return precision, something(element.scale, Int32(0))
end

function _fixeddecimalprecision(width::Integer)
    width > 0 || return 0
    bits = Base.checked_sub(Base.checked_mul(Int64(width), Int64(8)), Int64(1))
    return setprecision(BigFloat, 256) do
        return floor(Int64, BigFloat(bits) * log10(BigFloat(2)))
    end
end

function _validatedecimalmetadata(element::Metadata.SchemaElement,
    precision::Int32, scale::Int32)
    precision > 0 || throw(FormatError(
        "DECIMAL column $(repr(element.name)) has nonpositive precision $precision"))
    0 <= scale <= precision || throw(FormatError(
        "DECIMAL column $(repr(element.name)) has invalid scale $scale for precision $precision"))
    physical = element.type_
    if physical == Metadata.Type.INT32
        precision <= 9 || throw(FormatError(
            "INT32 DECIMAL column $(repr(element.name)) has precision $precision above 9"))
    elseif physical == Metadata.Type.INT64
        precision <= 18 || throw(FormatError(
            "INT64 DECIMAL column $(repr(element.name)) has precision $precision above 18"))
    elseif physical == Metadata.Type.FIXED_LEN_BYTE_ARRAY
        width = element.type_length
        width !== nothing && width > 0 || throw(FormatError(
            "fixed DECIMAL column $(repr(element.name)) has no positive width"))
        capacity = _fixeddecimalprecision(width)
        precision <= capacity || throw(FormatError(
            "fixed DECIMAL column $(repr(element.name)) has precision $precision " *
            "above its $width-byte capacity $capacity"))
    elseif physical != Metadata.Type.BYTE_ARRAY
        throw(FormatError("DECIMAL annotation on $(repr(element.name)) requires " *
            "INT32, INT64, BYTE_ARRAY, or FIXED_LEN_BYTE_ARRAY physical storage"))
    end
    return
end

function _decimallogicalkind(element::Metadata.SchemaElement)
    parameters = _decimalparameters(element)
    parameters === nothing && return nothing
    _validatedecimalmetadata(element, parameters...)
    return :decimal
end

function _decimaldigits(value::BigInt)
    iszero(value) && return 1
    return ndigits(abs(value); base=10)
end

function _checkdecimalvalue(value::BigInt, precision::Int32, name::String,
    error::Type{<:Exception})
    digits = _decimaldigits(value)
    digits <= precision && return
    message = "DECIMAL column $(repr(name)) contains a $digits-digit value " *
        "for precision $precision"
    throw(error(message))
end

function _contiguousdecimalbytes(bytes::AbstractVector{UInt8})
    bytes isa StridedVector{UInt8} && stride(bytes, 1) == 1 && return bytes
    return collect(bytes)
end

function _importdecimalmagnitude(bytes::AbstractVector{UInt8})
    input = _contiguousdecimalbytes(bytes)
    value = BigInt(0)
    # Import one-byte words in most-significant-first order.
    GC.@preserve input value begin
        ccall((:__gmpz_import, Base.GMP.libgmp), Cvoid,
            (Ref{BigInt}, Csize_t, Cint, Csize_t, Cint, Csize_t, Ptr{Cvoid}),
            value, length(input), 1, 1, 1, 0, pointer(input))
    end
    return value
end

function _fromtwoscomplement(bytes::AbstractVector{UInt8})
    isempty(bytes) && throw(FormatError("DECIMAL byte array is empty"))
    value = _importdecimalmagnitude(bytes)
    iszero(first(bytes) & 0x80) && return value
    return value - (BigInt(1) << (8 * length(bytes)))
end

function _twoscomplementwidth(value::BigInt)
    magnitude = Base.GMP.MPZ.sizeinbase(value, 2)
    bits = if value >= 0
        Base.checked_add(magnitude, 1)
    else
        poweroftwo = Base.GMP.MPZ.scan1(value, 0) == magnitude - 1
        poweroftwo ? magnitude : Base.checked_add(magnitude, 1)
    end
    return max(1, cld(bits, 8))
end

function _exportdecimalmagnitude!(output::Vector{UInt8}, value::BigInt)
    iszero(value) && return output
    bytecount = cld(Base.GMP.MPZ.sizeinbase(value, 2), 8)
    start = length(output) - bytecount + 1
    target = @view output[start:end]
    # Export one-byte words in most-significant-first order.
    _, written = Base.GMP.MPZ.export!(target, value; order=1, endian=1, nails=0)
    written == bytecount || error("GMP exported $written bytes, expected $bytecount")
    return output
end

function _negatetwoscomplement!(output::Vector{UInt8})
    for index in eachindex(output)
        @inbounds output[index] = ~output[index]
    end
    for index in lastindex(output):-1:firstindex(output)
        @inbounds output[index] += UInt8(1)
        @inbounds iszero(output[index]) || break
    end
    return output
end

function _totwoscomplement(value::BigInt, width::Integer)
    width > 0 || throw(ArgumentError("DECIMAL byte width must be positive"))
    required = _twoscomplementwidth(value)
    required <= width || throw(ArgumentError(
        "DECIMAL value does not fit in $width signed bytes"))
    output = zeros(UInt8, Int(width))
    _exportdecimalmagnitude!(output, value)
    value >= 0 && return output
    return _negatetwoscomplement!(output)
end

function _preflightdecimalconversion(element::Metadata.SchemaElement, limits::Limits)
    element.type_ == Metadata.Type.FIXED_LEN_BYTE_ARRAY || return
    width = element.type_length
    width === nothing && throw(FormatError(
        "fixed DECIMAL column $(repr(element.name)) has no width"))
    _checklimit(:decimal_bytes, width, limits.max_decimal_bytes)
    _checklimit(:string_bytes, width, limits.max_string_bytes)
    return
end

function _fromparquetdecimal(element::Metadata.SchemaElement, value, limits::Limits)
    precision, scale = something(_decimalparameters(element))
    physical = element.type_
    unscaled = if physical == Metadata.Type.INT32
        value isa Int32 || throw(FormatError(
            "INT32 DECIMAL column $(repr(element.name)) contains a non-INT32 value"))
        BigInt(value)
    elseif physical == Metadata.Type.INT64
        value isa Int64 || throw(FormatError(
            "INT64 DECIMAL column $(repr(element.name)) contains a non-INT64 value"))
        BigInt(value)
    else
        value isa AbstractVector{UInt8} || throw(FormatError(
            "binary DECIMAL column $(repr(element.name)) contains a non-byte-array value"))
        _checklimit(:decimal_bytes, length(value), limits.max_decimal_bytes)
        _checklimit(:string_bytes, length(value), limits.max_string_bytes)
        physical == Metadata.Type.FIXED_LEN_BYTE_ARRAY &&
            length(value) != element.type_length && throw(FormatError(
                "fixed DECIMAL column $(repr(element.name)) has a value with the wrong width"))
        _fromtwoscomplement(value)
    end
    _checkdecimalvalue(unscaled, precision, element.name, FormatError)
    D = _decimalhost(element)
    D === Decimal && return Decimal(unscaled, scale)
    return reinterpret(D, unscaled)
end

function _toparquetdecimal(element::Metadata.SchemaElement, value, limits::Limits)
    if value isa DataDecimals.Decimal
        fast = _toparquetdecimal(element, value)
        fast === nothing || return fast
    end
    value isa DataDecimals.AbstractDecimal && (value = Decimal(BigInt(DataDecimals.unscaled(value)), DataDecimals.scale(value)))
    value isa Decimal || throw(ArgumentError(
        "DECIMAL column $(repr(element.name)) contains a non-Decimal value"))
    precision, scale = something(_decimalparameters(element))
    value.scale == scale || throw(ArgumentError(
        "DECIMAL column $(repr(element.name)) requires scale $scale, got $(value.scale)"))
    physical = element.type_
    if physical == Metadata.Type.INT32
        _checkdecimalvalue(value.unscaled, precision, element.name, ArgumentError)
        typemin(Int32) <= value.unscaled <= typemax(Int32) || throw(ArgumentError(
            "DECIMAL value does not fit in INT32"))
        return Int32(value.unscaled)
    elseif physical == Metadata.Type.INT64
        _checkdecimalvalue(value.unscaled, precision, element.name, ArgumentError)
        typemin(Int64) <= value.unscaled <= typemax(Int64) || throw(ArgumentError(
            "DECIMAL value does not fit in INT64"))
        return Int64(value.unscaled)
    end
    width = physical == Metadata.Type.FIXED_LEN_BYTE_ARRAY ?
        Int(element.type_length) : _twoscomplementwidth(value.unscaled)
    _checklimit(:decimal_bytes, width, limits.max_decimal_bytes)
    _checklimit(:string_bytes, width, limits.max_string_bytes)
    _checkdecimalvalue(value.unscaled, precision, element.name, ArgumentError)
    return _totwoscomplement(value.unscaled, width)
end

function _decimallogicalvalue(kind::Symbol, element::Metadata.SchemaElement, value,
    limits::Limits)
    kind === :decimal || return nothing
    ismissing(value) && return missing
    return _fromparquetdecimal(element, value, limits)
end

function _decimalphysicalvalue(kind::Symbol, element::Metadata.SchemaElement, value,
    limits::Limits)
    kind === :decimal || return nothing
    ismissing(value) && return missing
    return _toparquetdecimal(element, value, limits)
end

function _decimalhost(element::Metadata.SchemaElement)
    p, s = something(_decimalparameters(element))
    p <= 76 || return Decimal
    T = p <= 9 ? Int32 : p <= 18 ? Int64 : p <= 38 ? Int128 : DataDecimals.Int256
    return DataDecimals.Decimal{Int(p),Int(s),T}
end

# A DataDecimals.Decimal of the element's scale converts from its storage integer
# without a BigInt; other values take the general path above.
function _toparquetdecimal(element::Metadata.SchemaElement,
        value::DataDecimals.Decimal{P,S,T}) where {P,S,T}
    precision, scale = something(_decimalparameters(element))
    S == scale || return nothing
    unscaled = DataDecimals.unscaled(value)
    bound = precision >= P ? _decimalbound(T, Val(P)) : T(10)^precision
    -bound < unscaled < bound || throw(ArgumentError("DECIMAL column " *
        "$(repr(element.name)) contains a value with more than $(min(P, precision)) digits"))
    physical = element.type_
    physical == Metadata.Type.INT32 && return Int32(unscaled)
    physical == Metadata.Type.INT64 && return Int64(unscaled)
    physical == Metadata.Type.FIXED_LEN_BYTE_ARRAY || return nothing
    width = Int(element.type_length)
    width <= sizeof(T) || return nothing
    bytes = Vector{UInt8}(undef, width)
    for index in 1:width
        bytes[index] = (unscaled >> (8 * (width - index))) % UInt8
    end
    return bytes
end

# DECIMAL values of precision up to 76 decode straight into the storage integer `T` of
# `D = DataDecimals.Decimal{P,S,T}`: INT32 and INT64 values convert, byte arrays are
# big-endian two's complement. `bound` is 10^P; a value at or past it has more digits
# than the column's precision. `maxbytes` is the limit on one byte-array value.
struct _DecimalValue{D,T}
    bound::T
    element::Metadata.SchemaElement
    maxbytes::Int64
end

function _DecimalValue(::Type{D}, element::Metadata.SchemaElement,
        limits::Limits) where {P,S,T,D<:DataDecimals.Decimal{P,S,T}}
    return _DecimalValue{D,T}(_decimalbound(T, Val(P)), element, limits.max_decimal_bytes)
end

@noinline function _decimalrangeerror(convert::_DecimalValue{D}) where {D}
    precision, _ = something(_decimalparameters(convert.element))
    throw(FormatError("DECIMAL column $(repr(convert.element.name)) contains a value " *
        "with more than $precision digits"))
end

@inline function _checkeddecimal(convert::_DecimalValue{D,T}, value::T) where {D,T}
    bound = convert.bound
    (value < bound) & (value > -bound) || _decimalrangeerror(convert)
    return reinterpret(D, value)
end

function (convert::_DecimalValue{D,T})(value::Union{Int32,Int64}) where {D,T}
    W = promote_type(T, typeof(value))
    wide = W(value)
    bound = W(convert.bound)
    (wide < bound) & (wide > -bound) || _decimalrangeerror(convert)
    return reinterpret(D, wide % T)
end

function (convert::_DecimalValue{D,T})(bytes::AbstractVector{UInt8}) where {D,T}
    count = length(bytes)
    count > 0 || throw(FormatError(
        "DECIMAL column $(repr(convert.element.name)) contains an empty byte array"))
    _checklimit(:decimal_bytes, count, convert.maxbytes)
    negative = bytes[1] >= 0x80
    # Bytes past the storage width can only repeat the sign.
    extra = max(count - sizeof(T), 0)
    for index in 1:extra
        bytes[index] == (negative ? 0xff : 0x00) || _decimalrangeerror(convert)
    end
    bits = negative ? typemax(unsigned(T)) : zero(unsigned(T))
    for index in (extra + 1):count
        bits = (bits << 8) | unsigned(T)(bytes[index])
    end
    value = signed(bits)
    extra > 0 && signbit(value) != negative && _decimalrangeerror(convert)
    return _checkeddecimal(convert, value)
end

# PLAIN INT32/INT64 values whose storage type matches copy in bulk; `_aftercopy!` then
# checks them against the precision bound.
function _aftercopy!(convert::_DecimalValue{D,T}, dest::Vector, offset::Int,
        count::Int) where {D,T}
    bound = convert.bound
    valid = true
    GC.@preserve dest begin
        values = Ptr{T}(pointer(dest, offset + 1))
        @inbounds for index in 1:count
            value = unsafe_load(values, index)
            valid &= (value < bound) & (value > -bound)
        end
    end
    valid || _decimalrangeerror(convert)
    return
end
