using Dates
using Test

const LCMD = Parquet.Metadata

function logicalcolumnmetadata(bytes::Vector{UInt8})
    file = Parquet.File(bytes)
    return try
        Parquet.Thrift.decode(file.footer.bytes, LCMD.FileMetaData)
    finally
        close(file)
    end
end

function logicalcolumnvalues(bytes::Vector{UInt8}, name::Symbol=:value)
    table = Parquet.Table(bytes)
    return try
        copy(getproperty(table.columns, name))
    finally
        close(table)
    end
end

function logicalcolumnunit(unit::LCMD.TimeUnit)
    unit.MILLIS !== nothing && return :millis
    unit.MICROS !== nothing && return :micros
    unit.NANOS !== nothing && return :nanos
    return nothing
end

function logicalcolumnconverted(kind::Symbol, unit::Symbol, ::Bool)
    if kind === :time
        unit === :millis && return LCMD.ConvertedType.TIME_MILLIS
        unit === :micros && return LCMD.ConvertedType.TIME_MICROS
    else
        unit === :millis && return LCMD.ConvertedType.TIMESTAMP_MILLIS
        unit === :micros && return LCMD.ConvertedType.TIMESTAMP_MICROS
    end
    return nothing
end

function logicalcolumntime(unit::Symbol)
    unit === :millis && return Time(12, 34, 56, 789)
    unit === :micros && return Time(12, 34, 56, 789, 123)
    return Time(12, 34, 56, 789, 123, 456)
end

function logicalcolumntimestamp(unit::Symbol, adjusted::Bool)
    unit === :millis && !adjusted && return DateTime(2000, 2, 29, 12, 34, 56, 789)
    ticks = unit === :millis ? Int64(951_827_696_789) :
        unit === :micros ? Int64(951_827_696_789_123) :
        Int64(951_827_696_789_123_456)
    return Parquet.Timestamp(ticks, unit, adjusted)
end

function logicalcolumntimestamptype(unit::Symbol, adjusted::Bool)
    unit === :millis && return adjusted ? Parquet.Timestamp{:millis} : DateTime
    unit === :micros && return Parquet.Timestamp{:micros}
    return Parquet.Timestamp{:nanos}
end

@testset "LogicalColumn vector contract" begin
    values = ["alpha", "beta"]
    column = Parquet.LogicalColumn(values, :enum)
    @test column isa AbstractVector{String}
    @test eltype(column) === String
    @test size(column) == (2,)
    @test axes(column) == (Base.OneTo(2),)
    @test length(column) == 2
    @test parent(column) === values
    @test collect(column) == values
    column[2] = "gamma"
    @test values == ["alpha", "gamma"]
    @test column == values

    copied = copy(column)
    @test copied isa Parquet.LogicalColumn
    @test typeof(copied.spec) === typeof(column.spec)
    @test copied == column
    @test parent(copied) !== parent(column)
    copied[1] = "changed"
    @test column[1] == "alpha"

    nulls = Parquet.LogicalColumn(Missing[missing], :enum)
    @test eltype(nulls) === Union{Missing,String}
    @test isequal(collect(nulls), Union{Missing,String}[missing])

    time1 = Parquet.LogicalColumn(Time[], :time; unit=:millis, adjusted=false)
    time2 = Parquet.LogicalColumn(Time[], :time; unit=:nanos, adjusted=true)
    decimal1 = Parquet.LogicalColumn(Parquet.Decimal[], :decimal;
        precision=9, scale=0)
    decimal2 = Parquet.LogicalColumn(Parquet.Decimal[], :decimal;
        precision=20, scale=4)
    @test typeof(time1) === typeof(time2)
    @test typeof(decimal1) === typeof(decimal2)
    typedtime = Parquet.LogicalColumn(Time[Time(0)], :time; unit=:nanos,
        adjusted=false)
    @test (@inferred typedtime[1]) isa Time
    @test (@inferred Parquet._logicalcolumnwriteelement(:value, decimal1.spec,
        false, Parquet.Limits())) isa LCMD.SchemaElement
end

@testset "LogicalColumn ENUM metadata and round trips" begin
    expected = Union{Missing,String}["alpha", missing, "omega"]
    for pageversion in (:v1, :v2)
        column = Parquet.LogicalColumn(copy(expected), :enum)
        bytes = Parquet._encodefile((value=column,); pageversion=pageversion,
            encoding=:plain)
        @test isequal(logicalcolumnvalues(bytes), expected)
        element = logicalcolumnmetadata(bytes).schema[2]
        @test element.type_ == LCMD.Type.BYTE_ARRAY
        @test element.type_length === nothing
        @test element.repetition_type == LCMD.FieldRepetitionType.OPTIONAL
        @test element.logicalType.ENUM !== nothing
        @test element.converted_type == LCMD.ConvertedType.ENUM
    end
end

@testset "LogicalColumn TIME metadata and round trips" begin
    for pageversion in (:v1, :v2), unit in (:millis, :micros, :nanos),
        adjusted in (false, true)
        expected = Time[logicalcolumntime(unit)]
        column = Parquet.LogicalColumn(copy(expected), :time; unit=unit,
            adjusted=adjusted)
        bytes = Parquet._encodefile((value=column,); pageversion=pageversion,
            encoding=:plain)
        @test logicalcolumnvalues(bytes) == expected
        element = logicalcolumnmetadata(bytes).schema[2]
        physical = unit === :millis ? LCMD.Type.INT32 : LCMD.Type.INT64
        @test element.type_ == physical
        @test element.repetition_type == LCMD.FieldRepetitionType.REQUIRED
        @test element.logicalType.TIME.isAdjustedToUTC == adjusted
        @test logicalcolumnunit(element.logicalType.TIME.unit) === unit
        @test element.converted_type == logicalcolumnconverted(:time, unit, adjusted)
    end
end

@testset "LogicalColumn TIMESTAMP metadata and round trips" begin
    for pageversion in (:v1, :v2), unit in (:millis, :micros, :nanos),
        adjusted in (false, true)
        expected = [logicalcolumntimestamp(unit, adjusted)]
        column = Parquet.LogicalColumn(copy(expected), :timestamp; unit=unit,
            adjusted=adjusted)
        bytes = Parquet._encodefile((value=column,); pageversion=pageversion,
            encoding=:plain)
        @test logicalcolumnvalues(bytes) == expected
        element = logicalcolumnmetadata(bytes).schema[2]
        @test element.type_ == LCMD.Type.INT64
        @test element.repetition_type == LCMD.FieldRepetitionType.REQUIRED
        @test element.logicalType.TIMESTAMP.isAdjustedToUTC == adjusted
        @test logicalcolumnunit(element.logicalType.TIMESTAMP.unit) === unit
        @test element.converted_type == logicalcolumnconverted(:timestamp, unit, adjusted)
    end
end

@testset "LogicalColumn empty and all-null parameterized columns" begin
    for pageversion in (:v1, :v2), unit in (:millis, :micros, :nanos),
        adjusted in (false, true)
        T = logicalcolumntimestamptype(unit, adjusted)
        emptycolumn = Parquet.LogicalColumn(T[], :timestamp; unit=unit,
            adjusted=adjusted)
        emptybytes = Parquet._encodefile((value=emptycolumn,);
            pageversion=pageversion, encoding=:plain)
        emptyvalues = logicalcolumnvalues(emptybytes)
        @test isempty(emptyvalues)
        @test eltype(emptyvalues) === T
        emptyelement = logicalcolumnmetadata(emptybytes).schema[2]
        @test emptyelement.repetition_type == LCMD.FieldRepetitionType.REQUIRED
        @test emptyelement.logicalType.TIMESTAMP.isAdjustedToUTC == adjusted
        @test logicalcolumnunit(emptyelement.logicalType.TIMESTAMP.unit) === unit

        nullcolumn = Parquet.LogicalColumn(Missing[missing, missing], :timestamp;
            unit=unit, adjusted=adjusted)
        nullbytes = Parquet._encodefile((value=nullcolumn,);
            pageversion=pageversion, encoding=:plain)
        nullvalues = logicalcolumnvalues(nullbytes)
        @test isequal(nullvalues, Union{Missing,T}[missing, missing])
        @test eltype(nullvalues) === Union{Missing,T}
        nullelement = logicalcolumnmetadata(nullbytes).schema[2]
        @test nullelement.repetition_type == LCMD.FieldRepetitionType.OPTIONAL
        @test nullelement.logicalType.TIMESTAMP.isAdjustedToUTC == adjusted
        @test logicalcolumnunit(nullelement.logicalType.TIMESTAMP.unit) === unit
    end

    for pageversion in (:v1, :v2)
        emptycolumn = Parquet.LogicalColumn(Parquet.Decimal[], :decimal;
            precision=20, scale=4)
        emptybytes = Parquet._encodefile((value=emptycolumn,);
            pageversion=pageversion, encoding=:plain)
        emptyvalues = logicalcolumnvalues(emptybytes)
        @test isempty(emptyvalues)
        @test eltype(emptyvalues) === Parquet.Decimal
        emptyelement = logicalcolumnmetadata(emptybytes).schema[2]
        @test emptyelement.repetition_type == LCMD.FieldRepetitionType.REQUIRED
        @test emptyelement.precision == 20
        @test emptyelement.scale == 4

        nullcolumn = Parquet.LogicalColumn(Missing[missing, missing], :decimal;
            precision=20, scale=4)
        nullbytes = Parquet._encodefile((value=nullcolumn,);
            pageversion=pageversion, encoding=:plain)
        nullvalues = logicalcolumnvalues(nullbytes)
        @test isequal(nullvalues,
            Union{Missing,Parquet.Decimal}[missing, missing])
        @test eltype(nullvalues) === Union{Missing,Parquet.Decimal}
        nullelement = logicalcolumnmetadata(nullbytes).schema[2]
        @test nullelement.repetition_type == LCMD.FieldRepetitionType.OPTIONAL
        @test nullelement.logicalType.DECIMAL.precision == 20
        @test nullelement.logicalType.DECIMAL.scale == 4
        @test nullelement.precision == 20
        @test nullelement.scale == 4
        @test nullelement.converted_type == LCMD.ConvertedType.DECIMAL
    end
end

@testset "LogicalColumn DECIMAL storage boundaries" begin
    input = (
        p9=Parquet.LogicalColumn(
            Parquet.Decimal[Parquet.Decimal(999_999_999, 2)], :decimal;
            precision=9, scale=2),
        p10=Parquet.LogicalColumn(
            Parquet.Decimal[Parquet.Decimal(9_999_999_999, 2)], :decimal;
            precision=10, scale=2),
        p18=Parquet.LogicalColumn(
            Parquet.Decimal[Parquet.Decimal(big"999999999999999999", 2)], :decimal;
            precision=18, scale=2),
        p19=Parquet.LogicalColumn(
            Parquet.Decimal[Parquet.Decimal(big"9999999999999999999", 2)], :decimal;
            precision=19, scale=2),
    )
    for pageversion in (:v1, :v2)
        bytes = Parquet._encodefile(input; pageversion=pageversion, encoding=:plain)
        for name in keys(input)
            @test logicalcolumnvalues(bytes, name) == collect(getproperty(input, name))
        end
        elements = Dict(element.name => element for element in
            logicalcolumnmetadata(bytes).schema[2:end])
        @test elements["p9"].type_ == LCMD.Type.INT32
        @test elements["p10"].type_ == LCMD.Type.INT64
        @test elements["p18"].type_ == LCMD.Type.INT64
        @test elements["p19"].type_ == LCMD.Type.FIXED_LEN_BYTE_ARRAY
        @test elements["p19"].type_length == 9
        for (name, precision) in zip(("p9", "p10", "p18", "p19"),
            Int32[9, 10, 18, 19])
            element = elements[name]
            @test element.logicalType.DECIMAL.precision == precision
            @test element.logicalType.DECIMAL.scale == 2
            @test element.precision == precision
            @test element.scale == 2
            @test element.converted_type == LCMD.ConvertedType.DECIMAL
        end
    end
end

@testset "LogicalColumn encoding policies" begin
    input = (
        enum=Parquet.LogicalColumn(fill("alpha", 64), :enum),
        time=Parquet.LogicalColumn(fill(Time(1), 64), :time;
            unit=:micros, adjusted=false),
        timestamp=Parquet.LogicalColumn([
            Parquet.Timestamp(index, :micros, false) for index in Int64(1):Int64(64)
        ], :timestamp; unit=:micros, adjusted=false),
        decimal=Parquet.LogicalColumn(fill(
            Parquet.Decimal(big"1234567890123456789", 2), 64), :decimal;
            precision=19, scale=2),
    )
    policy = (enum=:dictionary, time=:delta_binary_packed,
        timestamp=:delta_binary_packed, decimal=:delta_byte_array)
    for pageversion in (:v1, :v2)
        bytes = Parquet._encodefile(input; pageversion=pageversion,
            encoding=policy)
        for name in keys(input)
            @test logicalcolumnvalues(bytes, name) == collect(getproperty(input, name))
        end
        chunks = logicalcolumnmetadata(bytes).row_groups[1].columns
        @test LCMD.Encoding.RLE_DICTIONARY in chunks[1].meta_data.encodings
        @test LCMD.Encoding.DELTA_BINARY_PACKED in chunks[2].meta_data.encodings
        @test LCMD.Encoding.DELTA_BINARY_PACKED in chunks[3].meta_data.encodings
        @test LCMD.Encoding.DELTA_BYTE_ARRAY in chunks[4].meta_data.encodings
    end
end

@testset "LogicalColumn constructor validation" begin
    LC = Parquet.LogicalColumn
    @test_throws ArgumentError LC(("not", "a", "vector"), :enum)
    @test_throws ArgumentError LC(String[], "enum")
    @test_throws ArgumentError LC(String[], :unknown)
    @test_throws ArgumentError LC([], :enum)
    @test_throws ArgumentError LC(Int[], :enum)
    @test_throws ArgumentError LC(String[], :enum; unit=:millis)
    @test_throws ArgumentError LC(String[], :enum; adjusted=false)
    @test_throws ArgumentError LC(String[], :enum; precision=1)
    @test_throws ArgumentError LC(String[], :enum; scale=0)

    @test_throws ArgumentError LC(Time[], :time; adjusted=false)
    @test_throws ArgumentError LC(Time[], :time; unit=:seconds, adjusted=false)
    @test_throws ArgumentError LC(Time[], :time; unit="millis", adjusted=false)
    @test_throws ArgumentError LC(Time[], :time; unit=:millis)
    @test_throws ArgumentError LC(Time[], :time; unit=:millis, adjusted=1)
    @test_throws ArgumentError LC(DateTime[], :time; unit=:millis, adjusted=false)
    @test_throws ArgumentError LC(Time[], :time; unit=:millis, adjusted=false,
        precision=9)

    @test_throws ArgumentError LC(DateTime[], :timestamp; adjusted=false)
    @test_throws ArgumentError LC(DateTime[], :timestamp; unit=:micros,
        adjusted=false)
    @test_throws ArgumentError LC(Parquet.Timestamp{:nanos}[], :timestamp;
        unit=:micros, adjusted=false)
    @test_throws ArgumentError LC(DateTime[], :timestamp; unit=:millis,
        adjusted=false, scale=0)

    @test_throws ArgumentError LC(Parquet.Decimal[], :decimal)
    @test_throws ArgumentError LC(Parquet.Decimal[], :decimal; precision=1)
    @test_throws ArgumentError LC(Parquet.Decimal[], :decimal; precision=true, scale=0)
    @test_throws ArgumentError LC(Parquet.Decimal[], :decimal; precision=1, scale=false)
    @test_throws ArgumentError LC(Parquet.Decimal[], :decimal; precision=0, scale=0)
    @test_throws ArgumentError LC(Parquet.Decimal[], :decimal; precision=1, scale=-1)
    @test_throws ArgumentError LC(Parquet.Decimal[], :decimal; precision=1, scale=2)
    @test_throws ArgumentError LC(Parquet.Decimal[], :decimal;
        precision=Int64(typemax(Int32)) + 1, scale=0)
    @test_throws ArgumentError LC(Int[], :decimal; precision=9, scale=0)
    @test_throws ArgumentError LC(Parquet.Decimal[], :decimal; precision=9,
        scale=0, adjusted=false)
end

@testset "LogicalColumn write-time value validation" begin
    millis = Parquet.LogicalColumn(
        Time[Time(Dates.Nanosecond(1))], :time; unit=:millis, adjusted=false)
    micros = Parquet.LogicalColumn(
        Time[Time(Dates.Nanosecond(1))], :time; unit=:micros, adjusted=false)
    @test_throws ArgumentError Parquet._encodefile((value=millis,))
    @test_throws ArgumentError Parquet._encodefile((value=micros,))

    timestampvalues = Parquet.Timestamp{:micros}[
        Parquet.Timestamp(0, :micros, true)]
    timestamp = Parquet.LogicalColumn(timestampvalues, :timestamp; unit=:micros,
        adjusted=true)
    timestampvalues[1] = Parquet.Timestamp(0, :micros, false)
    @test_throws ArgumentError Parquet._encodefile((value=timestamp,))

    wrongscale = Parquet.LogicalColumn(
        Parquet.Decimal[Parquet.Decimal(1, 3)], :decimal; precision=9, scale=2)
    excessdigits = Parquet.LogicalColumn(
        Parquet.Decimal[Parquet.Decimal(1_000_000_000, 2)], :decimal;
        precision=9, scale=2)
    @test_throws ArgumentError Parquet._encodefile((value=wrongscale,))
    @test_throws ArgumentError Parquet._encodefile((value=excessdigits,))

    invalid = String(UInt8[0xff])
    @test !isvalid(invalid)
    badenum = Parquet.LogicalColumn(String[invalid], :enum)
    @test_throws ArgumentError Parquet._encodefile((value=badenum,))

    wideempty = Parquet.LogicalColumn(Parquet.Decimal[], :decimal;
        precision=19, scale=2)
    limits = Parquet.Limits(max_decimal_bytes=8)
    @test_throws Parquet.LimitError Parquet._encodefile((value=wideempty,);
        limits=limits)
end
