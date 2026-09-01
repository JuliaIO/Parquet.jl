# DECIMAL logical values are `Decimals.Decimal{P,S,T}`: an isbits fixed-scale decimal
# equal to `unscaled * 10^(-S)`. `T` is the narrowest signed integer tier that always
# holds `10^P - 1`, so a decoded column is a dense `Vector{Decimal{P,S,T}}` with no
# per-value heap object (Parquet 2.13.0 README "DECIMAL").

const _DECIMAL_MAX_PRECISION = 76

function _decimalcapacity(::Type{Int32})
    return 9
end

function _decimalcapacity(::Type{Int64})
    return 18
end

function _decimalcapacity(::Type{Int128})
    return 38
end

function _decimalcapacity(::Type{Decimals.Int256})
    return _DECIMAL_MAX_PRECISION
end

function _decimalstorage(precision::Integer)
    precision <= 9 && return Int32
    precision <= 18 && return Int64
    precision <= 38 && return Int128
    return Decimals.Int256
end

function _decimaltype(precision::Integer, scale::Integer)
    return Decimal{Int(precision),Int(scale),_decimalstorage(precision)}
end

function _decimalpowertable(::Type{T}) where {T}
    capacity = _decimalcapacity(T)
    powers = Vector{T}(undef, capacity + 1)
    for exponent in 0:capacity
        powers[exponent + 1] = T(big(10)^exponent)
    end
    return powers
end

const _DECIMAL_POWERS_INT32 = _decimalpowertable(Int32)
const _DECIMAL_POWERS_INT64 = _decimalpowertable(Int64)
const _DECIMAL_POWERS_INT128 = _decimalpowertable(Int128)
const _DECIMAL_POWERS_INT256 = _decimalpowertable(Decimals.Int256)

function _decimalpowers(::Type{Int32})
    return _DECIMAL_POWERS_INT32
end

function _decimalpowers(::Type{Int64})
    return _DECIMAL_POWERS_INT64
end

function _decimalpowers(::Type{Int128})
    return _DECIMAL_POWERS_INT128
end

function _decimalpowers(::Type{Decimals.Int256})
    return _DECIMAL_POWERS_INT256
end

# Largest magnitude a value of precision `precision` may carry, in `T`.
function _decimalmagnitudelimit(::Type{T}, precision::Integer) where {T}
    return _decimalpowers(T)[Int(precision) + 1] - one(T)
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
    precision <= _DECIMAL_MAX_PRECISION || throw(UnsupportedFeatureError(
        "DECIMAL column $(repr(element.name)) has precision $precision above the " *
        "supported maximum $_DECIMAL_MAX_PRECISION"))
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

function _decimallogicaleltype(kind::Symbol, element::Metadata.SchemaElement)
    kind === :decimal || return nothing
    precision, scale = something(_decimalparameters(element))
    return _decimaltype(precision, scale)
end

# Digit counts only reach the error paths, so the BigInt widening stays off the
# per-value routes.
function _decimaldigits(value::Integer)
    iszero(value) && return 1
    return ndigits(big(value); base=10)
end

function _checkdecimalvalue(unscaled::T, precision::Integer, name::String,
    error::Type{<:Exception}) where {T<:Integer}
    precision > _decimalcapacity(T) && return
    limit = _decimalmagnitudelimit(T, precision)
    -limit <= unscaled <= limit && return
    digits = _decimaldigits(unscaled)
    message = "DECIMAL column $(repr(name)) contains a $digits-digit value " *
        "for precision $precision"
    throw(error(message))
end

# Minimal signed two's-complement byte width, matching the Parquet BYTE_ARRAY rule.
function _twoscomplementwidth(value::T) where {T<:Integer}
    magnitude = value < 0 ? ~value : value
    bits = 8 * sizeof(T) - leading_zeros(magnitude)
    return max(1, cld(bits + 1, 8))
end

function _checkdecimalpadding(bytes::AbstractVector{UInt8}, retained::Int,
    negative::Bool, name::String)
    start = firstindex(bytes)
    kept = start + length(bytes) - retained
    pad = negative ? 0xff : 0x00
    @inbounds for index in start:(kept - 1)
        bytes[index] == pad || throw(FormatError(
            "binary DECIMAL column $(repr(name)) has a value that does not fit " *
            "its declared precision"))
    end
    (!iszero(@inbounds(bytes[kept]) & 0x80)) == negative || throw(FormatError(
        "binary DECIMAL column $(repr(name)) has a value that does not fit " *
        "its declared precision"))
    return
end

# Big-endian two's complement to a little-endian machine integer. Leading sign bytes
# beyond `sizeof(T)` are verified and dropped; shorter values are sign extended.
function _decimalfrombytes(::Type{T}, bytes::AbstractVector{UInt8},
    name::String) where {T<:Integer}
    count = length(bytes)
    count > 0 || throw(FormatError("DECIMAL byte array is empty"))
    start = firstindex(bytes)
    negative = !iszero(@inbounds(bytes[start]) & 0x80)
    retained = count
    if count > sizeof(T)
        retained = sizeof(T)
        _checkdecimalpadding(bytes, retained, negative, name)
    end
    value = negative ? -one(T) : zero(T)
    @inbounds for index in (start + count - retained):(start + count - 1)
        value = (value << 8) | T(bytes[index])
    end
    return value
end

function _decimalbytes!(output::AbstractVector{UInt8}, unscaled::Integer)
    value = unscaled
    @inbounds for index in lastindex(output):-1:firstindex(output)
        output[index] = value % UInt8
        value >>= 8
    end
    return output
end

function _decimalbytes(unscaled::Integer, width::Integer)
    width > 0 || throw(ArgumentError("DECIMAL byte width must be positive"))
    _twoscomplementwidth(unscaled) <= width || throw(ArgumentError(
        "DECIMAL value does not fit in $width signed bytes"))
    return _decimalbytes!(Vector{UInt8}(undef, Int(width)), unscaled)
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

function _decimalreadunscaled(::Type{Decimal{P,S,T}}, element::Metadata.SchemaElement,
    value, limits::Limits) where {P,S,T}
    physical = element.type_
    if physical == Metadata.Type.INT32
        value isa Int32 || throw(FormatError(
            "INT32 DECIMAL column $(repr(element.name)) contains a non-INT32 value"))
        _checkdecimalvalue(value, P, element.name, FormatError)
        return value % T
    elseif physical == Metadata.Type.INT64
        value isa Int64 || throw(FormatError(
            "INT64 DECIMAL column $(repr(element.name)) contains a non-INT64 value"))
        _checkdecimalvalue(value, P, element.name, FormatError)
        return value % T
    end
    value isa AbstractVector{UInt8} || throw(FormatError(
        "binary DECIMAL column $(repr(element.name)) contains a non-byte-array value"))
    _checklimit(:decimal_bytes, length(value), limits.max_decimal_bytes)
    _checklimit(:string_bytes, length(value), limits.max_string_bytes)
    physical == Metadata.Type.FIXED_LEN_BYTE_ARRAY &&
        length(value) != element.type_length && throw(FormatError(
            "fixed DECIMAL column $(repr(element.name)) has a value with the wrong width"))
    unscaled = _decimalfrombytes(T, value, element.name)
    _checkdecimalvalue(unscaled, P, element.name, FormatError)
    return unscaled
end

function _decimalreadvalue(::Type{D}, element::Metadata.SchemaElement, value,
    limits::Limits) where {D<:Decimal}
    return reinterpret(D, _decimalreadunscaled(D, element, value, limits))
end

function _fromparquetdecimal(element::Metadata.SchemaElement, value, limits::Limits)
    precision, scale = something(_decimalparameters(element))
    return _decimalreadvalue(_decimaltype(precision, scale), element, value, limits)
end

function _toparquetdecimal(element::Metadata.SchemaElement, value, limits::Limits)
    value isa Decimal || throw(ArgumentError(
        "DECIMAL column $(repr(element.name)) contains a non-Decimal value"))
    precision, scale = something(_decimalparameters(element))
    Decimals.scale(value) == scale || throw(ArgumentError(
        "DECIMAL column $(repr(element.name)) requires scale $scale, got " *
        "$(Decimals.scale(value))"))
    unscaled = Decimals.unscaled(value)
    _checkdecimalvalue(unscaled, precision, element.name, ArgumentError)
    physical = element.type_
    if physical == Metadata.Type.INT32
        typemin(Int32) <= unscaled <= typemax(Int32) || throw(ArgumentError(
            "DECIMAL value does not fit in INT32"))
        return unscaled % Int32
    elseif physical == Metadata.Type.INT64
        typemin(Int64) <= unscaled <= typemax(Int64) || throw(ArgumentError(
            "DECIMAL value does not fit in INT64"))
        return unscaled % Int64
    end
    width = physical == Metadata.Type.FIXED_LEN_BYTE_ARRAY ?
        Int(element.type_length) : _twoscomplementwidth(unscaled)
    _checklimit(:decimal_bytes, width, limits.max_decimal_bytes)
    _checklimit(:string_bytes, width, limits.max_string_bytes)
    return _decimalbytes(unscaled, width)
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

# Bulk conversion behind a type barrier so the element loop is type stable.
function _decimalreadcolumn(::Type{D}, element::Metadata.SchemaElement,
    values::AbstractVector, limits::Limits) where {D<:Decimal}
    output = _convertedvector(D, values)
    for (index, value) in enumerate(values)
        output[index] = ismissing(value) ? missing :
            _decimalreadvalue(D, element, value, limits)
    end
    return output
end

function _decimallogicalvalues(element::Metadata.SchemaElement,
    values::AbstractVector, limits::Limits)
    precision, scale = something(_decimalparameters(element))
    return _decimalreadcolumn(_decimaltype(precision, scale), element, values, limits)
end

function _decimalphysicalvalues(element::Metadata.SchemaElement,
    values::AbstractVector, limits::Limits)
    output = _convertedvector(_physicaleltype(element.type_), values)
    for (index, value) in enumerate(values)
        output[index] = ismissing(value) ? missing :
            _toparquetdecimal(element, value, limits)
    end
    return output
end
