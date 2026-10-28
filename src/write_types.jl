# How each Julia element type writes to a Parquet leaf.

import DataAPI

"""
    _WriteMap{F,P}

How values of one Julia type write to a Parquet leaf: the leaf's physical type and
annotations, and `convert`, a callable that maps a value to its physical value of type
`P` (`Bool`, `Int32`, `Int64`, `Float32`, `Float64`, a string or byte vector for
BYTE_ARRAY, or `NTuple{N,UInt8}` for FIXED_LEN_BYTE_ARRAY).
"""
struct _WriteMap{F,P}
    physical::Metadata.Type.T
    type_length::Union{Nothing,Int32}
    logical::Union{Nothing,Metadata.LogicalType}
    converted::Union{Nothing,Metadata.ConvertedType.T}
    scale::Union{Nothing,Int32}
    precision::Union{Nothing,Int32}
    convert::F
end

function _WriteMap(::Type{P}, physical::Metadata.Type.T, convert::F; type_length=nothing,
        logical=nothing, converted=nothing, scale=nothing, precision=nothing) where {P,F}
    return _WriteMap{F,P}(physical, type_length, logical, converted, scale, precision,
        convert)
end

_writephysicaltype(::_WriteMap{F,P}) where {F,P} = P

function _writeelement(map::_WriteMap, name::AbstractString, optional::Bool)
    return Metadata.SchemaElement(
        type_=map.physical,
        type_length=map.type_length,
        repetition_type=optional ? Metadata.FieldRepetitionType.OPTIONAL :
            Metadata.FieldRepetitionType.REQUIRED,
        name=String(name),
        converted_type=map.converted,
        scale=map.scale,
        precision=map.precision,
        logicalType=map.logical,
    )
end

# Value converters. Each is a singleton so encoding loops specialize on it.
struct _AsInt32 end
(::_AsInt32)(x::Union{Int8,Int16,UInt8,UInt16}) = Int32(x)

struct _Bits{T} end
(::_Bits{T})(x) where {T} = reinterpret(T, x)

struct _DateDays end
(::_DateDays)(x::Dates.Date) = _toparquetdate(x)

struct _TimeNanos end
(::_TimeNanos)(x::Dates.Time) = Dates.value(x)

struct _DateTimeMillis end
(::_DateTimeMillis)(x::Dates.DateTime) = _toparquetdatetime(x)

# Dates.value of a timestamp or period, times an exact unit factor.
struct _Ticks{S} end
(::_Ticks{1})(x) = Int64(Dates.value(x))
function (::_Ticks{S})(x) where {S}
    ticks = Dates.value(x)
    product, overflow = Base.mul_with_overflow(Int64(ticks), Int64(S))
    overflow && throw(ArgumentError("$x does not fit an Int64 count of " *
        "$(S == 1000 ? "milliseconds" : "seconds")"))
    return product
end

struct _UUIDBytes end
(::_UUIDBytes)(x::UUIDs.UUID) = _bigendianbytes(x.value, Val(16))

struct _Float16Bytes end
(::_Float16Bytes)(x::Float16) = _littleendianbytes(reinterpret(UInt16, x), Val(2))

struct _IntervalBytes end
function (::_IntervalBytes)(x::Durations.Duration)
    months, days, nanoseconds = x.months, x.days, x.nanoseconds
    milliseconds, remainder = divrem(nanoseconds, Int64(1_000_000))
    months >= 0 && days >= 0 && milliseconds >= 0 && iszero(remainder) &&
        milliseconds <= typemax(UInt32) || throw(ArgumentError(
        "Parquet INTERVAL holds nonnegative months, days, and whole milliseconds below " *
        "2^32; $x does not fit"))
    return _intervaltuple(UInt32(months), UInt32(days), UInt32(milliseconds))
end
function (::_IntervalBytes)(x::Union{Dates.Month,Dates.Year})
    months = Dates.value(convert(Dates.Month, x))
    0 <= months <= typemax(UInt32) || throw(ArgumentError(
        "Parquet INTERVAL holds nonnegative month counts below 2^32; $x does not fit"))
    return _intervaltuple(UInt32(months), UInt32(0), UInt32(0))
end

function _intervaltuple(months::UInt32, days::UInt32, milliseconds::UInt32)
    m = _littleendianbytes(months, Val(4))
    d = _littleendianbytes(days, Val(4))
    s = _littleendianbytes(milliseconds, Val(4))
    return (m..., d..., s...)
end

# A decimal's unscaled storage, narrowed to the leaf's INT32 or INT64. `reinterpret`
# can build a Decimal{P} whose coefficient has more than P digits, so each value is
# checked against 10^P before it is stored.
struct _DecimalInt{T,P} end
function (::_DecimalInt{T,P})(x::DataDecimals.Decimal) where {T,P}
    _checkdecimaldigits(x, Val(P))
    return DataDecimals.unscaled(x) % T
end

# A decimal's unscaled storage as W big-endian two's complement bytes.
struct _DecimalBytes{W,P} end
function (::_DecimalBytes{W,P})(x::DataDecimals.Decimal) where {W,P}
    _checkdecimaldigits(x, Val(P))
    return _bigendianbytes(DataDecimals.unscaled(x), Val(W))
end

@inline function _checkdecimaldigits(x::DataDecimals.Decimal, ::Val{P}) where {P}
    unscaled = DataDecimals.unscaled(x)
    bound = _decimalbound(typeof(unscaled), Val(P))
    -bound < unscaled < bound || throw(ArgumentError(
        "DECIMAL($P) value with coefficient $unscaled has more than $P digits"))
    return
end

@generated _decimalbound(::Type{T}, ::Val{P}) where {T,P} = T(10)^P

struct _TaggedBytes end
(::_TaggedBytes)(x::Union{JSONValue,BSONValue}) = x.bytes

struct _AsString end
(::_AsString)(x::Union{Symbol,AbstractChar,Enum}) = string(x)

@inline function _bigendianbytes(value::Integer, ::Val{W}) where {W}
    return ntuple(i -> (value >> (8 * (W - i))) % UInt8, Val(W))
end

@inline function _littleendianbytes(value::Unsigned, ::Val{W}) where {W}
    return ntuple(i -> (value >> (8 * (i - 1))) % UInt8, Val(W))
end

function _intlogical(bits::Int, signed::Bool)
    return Metadata.LogicalType(INTEGER=Metadata.IntType(bitWidth=Int8(bits),
        isSigned=signed))
end

function _timestamplogical(unit::UInt8, adjusted::Bool)
    return Metadata.LogicalType(TIMESTAMP=Metadata.TimestampType(
        isAdjustedToUTC=adjusted, unit=_canonicaltimeunit(unit)))
end

const _INTERVAL_MAP = _WriteMap(NTuple{12,UInt8}, Metadata.Type.FIXED_LEN_BYTE_ARRAY,
    _IntervalBytes(); type_length=Int32(12), converted=Metadata.ConvertedType.INTERVAL)

"""
    _writemap(T) -> Union{_WriteMap,Nothing}

The leaf mapping for values of concrete type `T`, or `nothing` when `T` has no
type-determined mapping.
"""
_writemap(::Type) = nothing
_writemap(::Type{Bool}) = _WriteMap(Bool, Metadata.Type.BOOLEAN, identity)
_writemap(::Type{Int32}) = _WriteMap(Int32, Metadata.Type.INT32, identity)
_writemap(::Type{Int64}) = _WriteMap(Int64, Metadata.Type.INT64, identity)
_writemap(::Type{Float32}) = _WriteMap(Float32, Metadata.Type.FLOAT, identity)
_writemap(::Type{Float64}) = _WriteMap(Float64, Metadata.Type.DOUBLE, identity)
for (T, bits, signed, converted) in ((Int8, 8, true, :INT_8), (Int16, 16, true, :INT_16),
        (UInt8, 8, false, :UINT_8), (UInt16, 16, false, :UINT_16))
    @eval _writemap(::Type{$T}) = _WriteMap(Int32, Metadata.Type.INT32, _AsInt32();
        logical=_intlogical($bits, $signed), converted=Metadata.ConvertedType.$converted)
end
_writemap(::Type{UInt32}) = _WriteMap(Int32, Metadata.Type.INT32, _Bits{Int32}();
    logical=_intlogical(32, false), converted=Metadata.ConvertedType.UINT_32)
_writemap(::Type{UInt64}) = _WriteMap(Int64, Metadata.Type.INT64, _Bits{Int64}();
    logical=_intlogical(64, false), converted=Metadata.ConvertedType.UINT_64)
_writemap(::Type{Float16}) = _WriteMap(NTuple{2,UInt8},
    Metadata.Type.FIXED_LEN_BYTE_ARRAY, _Float16Bytes(); type_length=Int32(2),
    logical=Metadata.LogicalType(FLOAT16=Metadata.Float16Type()))

function _writemap(::Type{T}) where {T<:AbstractString}
    isconcretetype(T) || return nothing
    return _WriteMap(T, Metadata.Type.BYTE_ARRAY, identity;
        logical=Metadata.LogicalType(STRING=Metadata.StringType()),
        converted=Metadata.ConvertedType.UTF8)
end

function _writemap(::Type{T}) where {T<:Union{Symbol,Char}}
    return _WriteMap(String, Metadata.Type.BYTE_ARRAY, _AsString();
        logical=Metadata.LogicalType(STRING=Metadata.StringType()),
        converted=Metadata.ConvertedType.UTF8)
end

# An @enum value writes its name.
function _writemap(::Type{T}) where {T<:Enum}
    return _WriteMap(String, Metadata.Type.BYTE_ARRAY, _AsString();
        logical=Metadata.LogicalType(ENUM=Metadata.EnumType()),
        converted=Metadata.ConvertedType.ENUM)
end

function _writemap(::Type{T}) where {T<:AbstractVector{UInt8}}
    isconcretetype(T) || return nothing
    return _WriteMap(T, Metadata.Type.BYTE_ARRAY, identity)
end

function _writemap(::Type{NTuple{N,UInt8}}) where {N}
    N > 0 || return nothing
    return _WriteMap(NTuple{N,UInt8}, Metadata.Type.FIXED_LEN_BYTE_ARRAY, identity;
        type_length=Int32(N))
end

_writemap(::Type{UUIDs.UUID}) = _WriteMap(NTuple{16,UInt8},
    Metadata.Type.FIXED_LEN_BYTE_ARRAY, _UUIDBytes(); type_length=Int32(16),
    logical=Metadata.LogicalType(UUID=Metadata.UUIDType()))

_writemap(::Type{Dates.Date}) = _WriteMap(Int32, Metadata.Type.INT32, _DateDays();
    logical=Metadata.LogicalType(DATE=Metadata.DateType()),
    converted=Metadata.ConvertedType.DATE)

_writemap(::Type{Dates.Time}) = _WriteMap(Int64, Metadata.Type.INT64, _TimeNanos();
    logical=Metadata.LogicalType(TIME=Metadata.TimeType(isAdjustedToUTC=false,
        unit=_canonicaltimeunit(_TEMPORAL_NANOS))))

_writemap(::Type{Dates.DateTime}) = _WriteMap(Int64, Metadata.Type.INT64,
    _DateTimeMillis(); logical=_timestamplogical(_TEMPORAL_MILLIS, false),
    converted=Metadata.ConvertedType.TIMESTAMP_MILLIS)

# TIMESTAMP has no seconds unit, so second-resolution values write as milliseconds.
function _timestampmap(::Type{P}, adjusted::Bool) where {P}
    unit, factor = P === Dates.Millisecond ? (_TEMPORAL_MILLIS, 1) :
        P === Dates.Microsecond ? (_TEMPORAL_MICROS, 1) :
        P === Dates.Nanosecond ? (_TEMPORAL_NANOS, 1) :
        P === Dates.Second ? (_TEMPORAL_MILLIS, 1000) : (nothing, 0)
    unit === nothing && return nothing
    converted = unit == _TEMPORAL_MILLIS ? Metadata.ConvertedType.TIMESTAMP_MILLIS :
        unit == _TEMPORAL_MICROS ? Metadata.ConvertedType.TIMESTAMP_MICROS : nothing
    return _WriteMap(Int64, Metadata.Type.INT64, _Ticks{factor}();
        logical=_timestamplogical(unit, adjusted), converted=converted)
end

_writemap(::Type{Durations.Timestamp{P}}) where {P} = _timestampmap(P, false)
_writemap(::Type{Durations.ZonedTimestamp{P,Z}}) where {P,Z} = _timestampmap(P, true)

# Elapsed-time periods write as INT64 counts of their own unit; larger fixed units as
# seconds. Month and Year are calendar periods and write as INTERVAL.
_writemap(::Type{P}) where {P<:Union{Dates.Nanosecond,Dates.Microsecond,Dates.Millisecond,
    Dates.Second}} = _WriteMap(Int64, Metadata.Type.INT64, _Ticks{1}())
_writemap(::Type{Dates.Minute}) = _WriteMap(Int64, Metadata.Type.INT64, _Ticks{60}())
_writemap(::Type{Dates.Hour}) = _WriteMap(Int64, Metadata.Type.INT64, _Ticks{3600}())
_writemap(::Type{Dates.Day}) = _WriteMap(Int64, Metadata.Type.INT64, _Ticks{86400}())
_writemap(::Type{Dates.Week}) = _WriteMap(Int64, Metadata.Type.INT64, _Ticks{604800}())
_writemap(::Type{<:Union{Dates.Month,Dates.Year}}) = _INTERVAL_MAP
_writemap(::Type{Durations.Duration}) = _INTERVAL_MAP

function _writemap(::Type{DataDecimals.Decimal{P,S,T}}) where {P,S,T}
    logical = Metadata.LogicalType(DECIMAL=Metadata.DecimalType(scale=Int32(S),
        precision=Int32(P)))
    keywords = (; logical=logical, converted=Metadata.ConvertedType.DECIMAL,
        scale=Int32(S), precision=Int32(P))
    P <= 9 && return _WriteMap(Int32, Metadata.Type.INT32, _DecimalInt{Int32,P}();
        keywords...)
    P <= 18 && return _WriteMap(Int64, Metadata.Type.INT64, _DecimalInt{Int64,P}();
        keywords...)
    width = _decimalwritewidth(Int32(P), Limits())
    return _WriteMap(NTuple{Int(width),UInt8}, Metadata.Type.FIXED_LEN_BYTE_ARRAY,
        _DecimalBytes{Int(width),P}(); type_length=width, keywords...)
end

_writemap(::Type{JSONValue}) = _WriteMap(fieldtype(JSONValue, :bytes),
    Metadata.Type.BYTE_ARRAY, _TaggedBytes();
    logical=Metadata.LogicalType(JSON=Metadata.JsonType()),
    converted=Metadata.ConvertedType.JSON)
_writemap(::Type{BSONValue}) = _WriteMap(fieldtype(BSONValue, :bytes),
    Metadata.Type.BYTE_ARRAY, _TaggedBytes();
    logical=Metadata.LogicalType(BSON=Metadata.BsonType()),
    converted=Metadata.ConvertedType.BSON)

# Types whose leaf annotation depends on the values (precision and scale).
_writevaluescanned(T::Type) = T <: Union{Decimal,DataDecimals.DecimalValue}

"""
    _writeunsupported(T, path)

An `ArgumentError` for a field of a type Parquet cannot store, with advice where a
lossless alternative exists.
"""
function _writeunsupported(T::Type, path::AbstractString)
    advice = if T <: Union{Int128,UInt128}
        "; Parquet has no 128-bit integers, so convert it to a DataDecimals.Decimal " *
        "(Decimal{$(T === Int128 ? 38 : 39),0} holds every $T)"
    elseif T <: BigInt
        "; convert it to a DataDecimals.Decimal{P,0} with enough digits"
    elseif T <: Durations.Timestamp || T <: Durations.ZonedTimestamp
        "; Parquet timestamps have second, millisecond, microsecond, or nanosecond " *
        "resolution"
    elseif T <: Dates.Period || T <: Dates.CompoundPeriod
        "; convert it to Durations.Duration or a fixed Dates period"
    elseif T === Nothing
        "; use missing for null values"
    else
        ""
    end
    return ArgumentError("Parquet cannot write field $(repr(String(path))) of type $T" *
        advice)
end
