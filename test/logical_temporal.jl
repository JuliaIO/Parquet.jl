using Dates

if !@isdefined(TH)
    const TH = Parquet.Thrift
end
if !@isdefined(MD)
    const MD = Parquet.Metadata
end

function temporaltestunit(unit::Symbol)
    unit === :millis && return MD.TimeUnit(MILLIS=MD.MilliSeconds())
    unit === :micros && return MD.TimeUnit(MICROS=MD.MicroSeconds())
    unit === :nanos && return MD.TimeUnit(NANOS=MD.NanoSeconds())
    throw(ArgumentError("unsupported test unit $unit"))
end

function temporaltestelement(name, physical; logical=nothing, converted=nothing)
    return MD.SchemaElement(name=name, type_=physical,
        repetition_type=MD.FieldRepetitionType.OPTIONAL,
        logicalType=logical, converted_type=converted)
end

function temporaltesttime(unit::Symbol, adjusted::Bool=true)
    physical = unit === :millis ? MD.Type.INT32 : MD.Type.INT64
    annotation = MD.TimeType(isAdjustedToUTC=adjusted, unit=temporaltestunit(unit))
    return temporaltestelement("time", physical;
        logical=MD.LogicalType(TIME=annotation))
end

function temporaltesttimestamp(unit::Symbol, adjusted::Bool=true)
    annotation = MD.TimestampType(isAdjustedToUTC=adjusted, unit=temporaltestunit(unit))
    return temporaltestelement("timestamp", MD.Type.INT64;
        logical=MD.LogicalType(TIMESTAMP=annotation))
end

function temporaltestinteger(width::Int, signed::Bool)
    physical = width == 64 ? MD.Type.INT64 : MD.Type.INT32
    annotation = MD.IntType(bitWidth=Int8(width), isSigned=signed)
    return temporaltestelement("integer", physical;
        logical=MD.LogicalType(INTEGER=annotation))
end

@testset "temporal and integer logical recognition" begin
    for unit in (:millis, :micros, :nanos), adjusted in (false, true)
        time = temporaltesttime(unit, adjusted)
        timestamp = temporaltesttimestamp(unit, adjusted)
        timekind = Parquet._temporallogicalkind(time)
        timestampkind = Parquet._temporallogicalkind(timestamp)
        @test timekind isa Parquet._TimeLogicalKind
        @test timestampkind isa Parquet._TimestampLogicalKind
        @test timekind.is_adjusted_to_utc == adjusted
        @test timestampkind.is_adjusted_to_utc == adjusted
        @test Parquet._temporallogicaleltype(time, Int64) === Time
        expected = unit === :millis ? DateTime :
            unit === :micros ? Parquet.Timestamp{:micros} : Parquet.Timestamp{:nanos}
        @test Parquet._temporallogicaleltype(timestamp, Int64) === expected
    end

    for width in (8, 16, 32, 64), signed in (false, true)
        element = temporaltestinteger(width, signed)
        kind = Parquet._temporallogicalkind(element)
        signedtypes = (Int8, Int16, Int32, Int64)
        unsignedtypes = (UInt8, UInt16, UInt32, UInt64)
        expected = (signed ? signedtypes : unsignedtypes)[trailing_zeros(width) - 2]
        @test kind isa Parquet._IntegerLogicalKind
        @test kind.bitwidth == width
        @test kind.signed == signed
        @test Parquet._temporallogicaleltype(element, Int64) === expected
    end

    node = Parquet.SchemaNode(temporaltesttime(:nanos), ["time"], Int16(1),
        Int16(0), Int32(1), Parquet.SchemaNode[])
    @test Parquet._temporallogicalkind(node) isa Parquet._TimeLogicalKind
    @test Parquet._temporallogicaleltype(node, Int64) === Time

    legacy = (
        (MD.ConvertedType.TIME_MILLIS, MD.Type.INT32, Time),
        (MD.ConvertedType.TIME_MICROS, MD.Type.INT64, Time),
        (MD.ConvertedType.TIMESTAMP_MILLIS, MD.Type.INT64, DateTime),
        (MD.ConvertedType.TIMESTAMP_MICROS, MD.Type.INT64,
            Parquet.Timestamp{:micros}),
        (MD.ConvertedType.INT_8, MD.Type.INT32, Int8),
        (MD.ConvertedType.INT_16, MD.Type.INT32, Int16),
        (MD.ConvertedType.INT_32, MD.Type.INT32, Int32),
        (MD.ConvertedType.INT_64, MD.Type.INT64, Int64),
        (MD.ConvertedType.UINT_8, MD.Type.INT32, UInt8),
        (MD.ConvertedType.UINT_16, MD.Type.INT32, UInt16),
        (MD.ConvertedType.UINT_32, MD.Type.INT32, UInt32),
        (MD.ConvertedType.UINT_64, MD.Type.INT64, UInt64),
    )
    for (converted, physical, expected) in legacy
        element = temporaltestelement("legacy", physical; converted=converted)
        @test Parquet._temporallogicalkind(element) !== nothing
        @test Parquet._temporallogicaleltype(element, Int64) === expected
    end
end

@testset "modern annotation precedence and unsupported units" begin
    modern = temporaltestelement("value", MD.Type.INT64;
        logical=MD.LogicalType(TIMESTAMP=MD.TimestampType(
            isAdjustedToUTC=false, unit=temporaltestunit(:nanos))),
        converted=MD.ConvertedType.TIME_MICROS)
    kind = Parquet._temporallogicalkind(modern)
    @test kind isa Parquet._TimestampLogicalKind
    @test kind.unit == Parquet._TEMPORAL_NANOS
    @test !kind.is_adjusted_to_utc

    stringlogical = MD.LogicalType(STRING=MD.StringType())
    modernstring = temporaltestelement("value", MD.Type.BYTE_ARRAY;
        logical=stringlogical, converted=MD.ConvertedType.TIMESTAMP_MICROS)
    @test Parquet._temporallogicalkind(modernstring) === nothing

    unknownunit = MD.TimeUnit(
        unknown_fields=(TH.RawField(42, TH.STRUCT, UInt8[0x00]),))
    unknowntime = temporaltestelement("value", MD.Type.INT32;
        logical=MD.LogicalType(TIME=MD.TimeType(
            isAdjustedToUTC=true, unit=unknownunit)),
        converted=MD.ConvertedType.TIME_MILLIS)
    unknowntimestamp = temporaltestelement("value", MD.Type.INT64;
        logical=MD.LogicalType(TIMESTAMP=MD.TimestampType(
            isAdjustedToUTC=true, unit=MD.TimeUnit())),
        converted=MD.ConvertedType.TIMESTAMP_MICROS)
    physical64 = Int64[1, 2]
    @test_throws Parquet.UnsupportedFeatureError Parquet._temporallogicalkind(
        unknowntime)
    @test_throws Parquet.UnsupportedFeatureError Parquet._temporallogicalvalues(
        unknowntime, Int32[1, 2])
    @test_throws Parquet.FormatError Parquet._temporallogicalkind(unknowntimestamp)
    @test_throws Parquet.FormatError Parquet._temporallogicalvalues(
        unknowntimestamp, physical64)

    unknownlogical = MD.LogicalType(
        unknown_fields=(TH.RawField(2555, TH.STRUCT, UInt8[0x00]),))
    unknowntype = temporaltestelement("value", MD.Type.INT64;
        logical=unknownlogical, converted=MD.ConvertedType.TIMESTAMP_MICROS)
    @test Parquet._temporallogicalkind(unknowntype) === nothing
    @test Parquet._temporalphysicalvalues(unknowntype, physical64) === physical64
end

@testset "temporal and integer schema validation" begin
    for unit in (:millis, :micros, :nanos)
        wrongtime = unit === :millis ? MD.Type.INT64 : MD.Type.INT32
        time = MD.TimeType(isAdjustedToUTC=true, unit=temporaltestunit(unit))
        @test_throws Parquet.FormatError Parquet._temporallogicalkind(
            temporaltestelement("bad", wrongtime;
                logical=MD.LogicalType(TIME=time)))
        timestamp = MD.TimestampType(isAdjustedToUTC=true,
            unit=temporaltestunit(unit))
        @test_throws Parquet.FormatError Parquet._temporallogicalkind(
            temporaltestelement("bad", MD.Type.INT32;
                logical=MD.LogicalType(TIMESTAMP=timestamp)))
    end

    for width in (8, 16, 32, 64)
        wrong = width == 64 ? MD.Type.INT32 : MD.Type.INT64
        integer = MD.IntType(bitWidth=Int8(width), isSigned=true)
        @test_throws Parquet.FormatError Parquet._temporallogicalkind(
            temporaltestelement("bad", wrong;
                logical=MD.LogicalType(INTEGER=integer)))
    end
    for width in (-128, 0, 7, 24, 63, 65)
        integer = MD.IntType(bitWidth=Int8(width), isSigned=true)
        @test_throws Parquet.FormatError Parquet._temporallogicalkind(
            temporaltestelement("bad", MD.Type.INT32;
                logical=MD.LogicalType(INTEGER=integer)))
    end

    legacybad = (
        (MD.ConvertedType.TIME_MILLIS, MD.Type.INT64),
        (MD.ConvertedType.TIME_MICROS, MD.Type.INT32),
        (MD.ConvertedType.TIMESTAMP_MILLIS, MD.Type.INT32),
        (MD.ConvertedType.TIMESTAMP_MICROS, MD.Type.INT32),
        (MD.ConvertedType.INT_8, MD.Type.INT64),
        (MD.ConvertedType.INT_64, MD.Type.INT32),
        (MD.ConvertedType.UINT_32, MD.Type.INT64),
        (MD.ConvertedType.UINT_64, MD.Type.INT32),
    )
    for (converted, physical) in legacybad
        @test_throws Parquet.FormatError Parquet._temporallogicalkind(
            temporaltestelement("bad", physical; converted=converted))
    end
end

@testset "TIME conversions" begin
    cases = (
        (:millis, Int32(45_296_789), Time(12, 34, 56, 789)),
        (:micros, Int64(45_296_789_123), Time(12, 34, 56, 789, 123)),
        (:nanos, Int64(45_296_789_123_456), Time(12, 34, 56, 789, 123, 456)),
    )
    for (unit, physical, expected) in cases, adjusted in (false, true)
        element = temporaltesttime(unit, adjusted)
        logical = Parquet._temporallogicalvalue(element, physical)
        @test logical == expected
        @test Parquet._temporalphysicalvalue(element, logical) == physical
    end

    boundaries = (
        (:millis, Int32(0), Int32(86_399_999)),
        (:micros, Int64(0), Int64(86_399_999_999)),
        (:nanos, Int64(0), Int64(86_399_999_999_999)),
    )
    for (unit, firstvalue, lastvalue) in boundaries
        element = temporaltesttime(unit)
        expectedlast = unit === :millis ? Time(23, 59, 59, 999) :
            unit === :micros ? Time(23, 59, 59, 999, 999) :
            Time(23, 59, 59, 999, 999, 999)
        @test Parquet._temporallogicalvalue(element, firstvalue) == Time(0)
        @test Parquet._temporallogicalvalue(element, lastvalue) == expectedlast
        @test_throws Parquet.FormatError Parquet._temporallogicalvalue(
            element, typeof(firstvalue)(-1))
        @test_throws Parquet.FormatError Parquet._temporallogicalvalue(
            element, typeof(firstvalue)(lastvalue + 1))
    end

    millis = temporaltesttime(:millis)
    micros = temporaltesttime(:micros)
    nanos = temporaltesttime(:nanos)
    @test_throws Parquet.FormatError Parquet._temporallogicalvalue(millis, Int64(0))
    @test_throws Parquet.FormatError Parquet._temporallogicalvalue(micros, Int32(0))
    @test_throws ArgumentError Parquet._temporalphysicalvalue(millis, Int32(0))
    @test Parquet._temporalphysicalvalue(millis,
        Time(Dates.Nanosecond(1_000_000))) == Int32(1)
    @test Parquet._temporalphysicalvalue(micros,
        Time(Dates.Nanosecond(1_000))) == Int64(1)
    @test Parquet._temporalphysicalvalue(nanos,
        Time(Dates.Nanosecond(1))) == Int64(1)
    @test_throws ArgumentError Parquet._temporalphysicalvalue(
        millis, Time(Dates.Nanosecond(1)))
    @test_throws ArgumentError Parquet._temporalphysicalvalue(
        micros, Time(Dates.Nanosecond(1)))

    optionalphysical = Union{Missing,Int64}[missing, 0, 86_399_999_999_999]
    optional = Parquet._temporallogicalvalues(nanos, optionalphysical)
    @test optional isa Vector{Union{Missing,Time}}
    @test isequal(Parquet._temporalphysicalvalues(nanos, optional), optionalphysical)
end

@testset "TIMESTAMP conversions" begin
    millis = temporaltesttimestamp(:millis, false)
    values = (
        (Int64(-1), DateTime(1969, 12, 31, 23, 59, 59, 999)),
        (Int64(0), DateTime(1970, 1, 1)),
        (Int64(951_827_696_789), DateTime(2000, 2, 29, 12, 34, 56, 789)),
    )
    for (physical, expected) in values
        @test Parquet._temporallogicalvalue(millis, physical) == expected
        @test Parquet._temporalphysicalvalue(millis, expected) == physical
    end

    epoch = Dates.value(DateTime(1970, 1, 1))
    upper = typemax(Int64) - epoch
    for physical in (typemin(Int64), upper)
        logical = Parquet._temporallogicalvalue(millis, physical)
        @test Parquet._temporalphysicalvalue(millis, logical) == physical
    end
    @test_throws Parquet.FormatError Parquet._temporallogicalvalue(millis, upper + 1)
    @test_throws ArgumentError Parquet._temporalphysicalvalue(
        millis, DateTime(Dates.UTM(typemin(Int64))))
    @test_throws Parquet.FormatError Parquet._temporallogicalvalue(millis, Int32(0))
    @test_throws ArgumentError Parquet._temporalphysicalvalue(millis, Int64(0))

    for unit in (:micros, :nanos), adjusted in (false, true),
        ticks in (typemin(Int64), Int64(-1), Int64(0), typemax(Int64))
        element = temporaltesttimestamp(unit, adjusted)
        value = Parquet._temporallogicalvalue(element, ticks)
        expectedtype = unit === :micros ?
            Parquet.Timestamp{:micros} : Parquet.Timestamp{:nanos}
        @test value isa expectedtype
        @test value.ticks == ticks
        @test value.is_adjusted_to_utc == adjusted
        @test Parquet._timestampunit(value) === unit
        @test Parquet._temporalphysicalvalue(element, value) == ticks
    end

    @test isbitstype(Parquet.Timestamp{:micros})
    @test isbitstype(Parquet.Timestamp{:nanos})
    first = Parquet.Timestamp(Int64(1), :micros, true)
    second = Parquet.Timestamp(Int64(1), :micros, true)
    @test first == second
    @test isequal(first, second)
    @test hash(first) == hash(second)
    @test sprint(show, first) == "Timestamp(1, :micros, true)"
    @test_throws ArgumentError Parquet.Timestamp(Int64(1), :millis, true)
    @test_throws ArgumentError Parquet._temporalphysicalvalue(
        temporaltesttimestamp(:micros, true), Parquet.Timestamp(1, :nanos, true))
    @test_throws ArgumentError Parquet._temporalphysicalvalue(
        temporaltesttimestamp(:micros, true), Parquet.Timestamp(1, :micros, false))

    optionalphysical = Union{Missing,Int64}[missing, typemin(Int64), typemax(Int64)]
    nanos = temporaltesttimestamp(:nanos, true)
    optional = Parquet._temporallogicalvalues(nanos, optionalphysical)
    @test optional isa Vector{Union{Missing,Parquet.Timestamp{:nanos}}}
    @test isequal(Parquet._temporalphysicalvalues(nanos, optional), optionalphysical)
end

@testset "INTEGER conversions" begin
    cases = (
        (8, true, Int32[-128, 0, 127], Int8[-128, 0, 127]),
        (16, true, Int32[-32768, 0, 32767], Int16[-32768, 0, 32767]),
        (32, true, Int32[typemin(Int32), 0, typemax(Int32)],
            Int32[typemin(Int32), 0, typemax(Int32)]),
        (64, true, Int64[typemin(Int64), 0, typemax(Int64)],
            Int64[typemin(Int64), 0, typemax(Int64)]),
        (8, false, Int32[0, 127, 255], UInt8[0, 127, 255]),
        (16, false, Int32[0, 32767, 65535], UInt16[0, 32767, 65535]),
        (32, false, Int32[0, typemax(Int32), typemin(Int32), -1],
            UInt32[0, 0x7fffffff, 0x80000000, 0xffffffff]),
        (64, false, Int64[0, typemax(Int64), typemin(Int64), -1],
            UInt64[0, 0x7fffffffffffffff, 0x8000000000000000, 0xffffffffffffffff]),
    )
    for (width, signed, physical, logical) in cases
        element = temporaltestinteger(width, signed)
        decoded = Parquet._temporallogicalvalues(element, physical)
        @test decoded == logical
        @test eltype(decoded) === eltype(logical)
        @test Parquet._temporalphysicalvalues(element, logical) == physical
        optionalphysical = Union{Missing,eltype(physical)}[missing, first(physical), last(physical)]
        optional = Parquet._temporallogicalvalues(element, optionalphysical)
        @test eltype(optional) === Union{Missing,eltype(logical)}
        @test isequal(Parquet._temporalphysicalvalues(element, optional), optionalphysical)
    end

    invalid = (
        (temporaltestinteger(8, true), Int32(-129)),
        (temporaltestinteger(8, true), Int32(128)),
        (temporaltestinteger(16, true), Int32(-32769)),
        (temporaltestinteger(16, true), Int32(32768)),
        (temporaltestinteger(8, false), Int32(-1)),
        (temporaltestinteger(8, false), Int32(256)),
        (temporaltestinteger(16, false), Int32(-1)),
        (temporaltestinteger(16, false), Int32(65536)),
    )
    for (element, physical) in invalid
        @test_throws Parquet.FormatError Parquet._temporallogicalvalue(element, physical)
    end
    @test_throws Parquet.FormatError Parquet._temporallogicalvalue(
        temporaltestinteger(8, true), Int64(1))
    @test_throws ArgumentError Parquet._temporalphysicalvalue(
        temporaltestinteger(8, true), Int16(1))
end

@testset "temporal conversion limits" begin
    limits = Parquet.Limits(max_container_elements=1)
    @test_throws Parquet.LimitError Parquet._temporallogicalvalues(
        temporaltesttime(:nanos), Int64[0, 1]; limits=limits)
    @test_throws Parquet.LimitError Parquet._temporalphysicalvalues(
        temporaltesttimestamp(:micros),
        [Parquet.Timestamp(0, :micros, true),
            Parquet.Timestamp(1, :micros, true)]; limits=limits)
    @test_throws Parquet.LimitError Parquet._temporallogicalvalues(
        temporaltestinteger(16, false), Int32[0, 1]; limits=limits)
end
