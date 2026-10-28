using Test
using Dates
import Tables
using CSV, CategoricalArrays, PooledArrays, SentinelArrays
import TimeZones

const WTMD = Parquet.Metadata
const WTD = Parquet.Durations
const WTDec = Parquet.DataDecimals

function wtwrite(table; kw...)
    return take!(Parquet.write(IOBuffer(), table; kw...))
end

function wtleaf(bytes::Vector{UInt8})
    file = Parquet.File(bytes)
    try
        metadata = Parquet.Thrift.decode(copy(file.footer.bytes), WTMD.FileMetaData)
        return metadata.schema[end]
    finally
        close(file)
    end
end

function wtvalues(bytes::Vector{UInt8}, name::Symbol)
    table = Parquet.Table(bytes)
    try
        return collect(Tables.getcolumn(Tables.columns(table), name))
    finally
        close(table)
    end
end

# The stored INT64 count of a timestamp value, whatever type the reader returns.
wtticks(x::Missing) = missing
wtticks(x::Union{WTD.Timestamp,WTD.ZonedTimestamp}) = Dates.value(x)
wtticks(x::DateTime) = Dates.value(x) - Dates.value(DateTime(1970))

wtinterval(x::WTD.Duration) = (Int(x.months), Int(x.days), Int(div(x.nanoseconds, 1_000_000)))

function wterror(f)
    try
        f()
    catch err
        return err
    end
    return nothing
end

@testset "write Durations timestamps" begin
    scale(P) = P === Second ? 1 : P === Millisecond ? 1_000 : P === Microsecond ? 10^6 : 10^9
    stamps(P) = [WTD.Timestamp{P}(Dates.UTInstant(P(1_700_000_000 * scale(P) + i)))
        for i in 1:4]
    for (P, unit, factor) in ((Second, :MILLIS, 1000), (Millisecond, :MILLIS, 1),
            (Microsecond, :MICROS, 1), (Nanosecond, :NANOS, 1))
        values = stamps(P)
        bytes = wtwrite((value=values,))
        leaf = wtleaf(bytes)
        timestamp = leaf.logicalType.TIMESTAMP
        @test leaf.type_ == WTMD.Type.INT64
        @test !timestamp.isAdjustedToUTC
        @test getproperty(timestamp.unit, unit) !== nothing
        @test wtticks.(wtvalues(bytes, :value)) == Dates.value.(values) .* factor
        zoned = [WTD.ZonedTimestamp{P,Symbol("America/New_York")}(x, Dates.UTC)
            for x in values]
        bytes = wtwrite((value=zoned,))
        @test wtleaf(bytes).logicalType.TIMESTAMP.isAdjustedToUTC
        @test wtticks.(wtvalues(bytes, :value)) == Dates.value.(values) .* factor
    end
    nullable = Union{Missing,WTD.Timestamp{Microsecond}}[missing; stamps(Microsecond)[2:end]]
    bytes = wtwrite((value=nullable,))
    @test wtleaf(bytes).repetition_type == WTMD.FieldRepetitionType.OPTIONAL
    @test isequal(wtticks.(wtvalues(bytes, :value)), wtticks.(nullable))
    huge = [WTD.Timestamp{Second}(Dates.UTInstant(Second(typemax(Int64) ÷ 10)))]
    err = wterror(() -> wtwrite((late=huge,)))
    @test err isa ArgumentError && occursin("\"late\"", err.msg)
    # Explicit units through LogicalColumn convert exactly or refuse.
    nanos = stamps(Nanosecond)
    exact = [WTD.Timestamp{Nanosecond}(Dates.UTInstant(Nanosecond(1_000 * i))) for i in 1:3]
    bytes = wtwrite((value=Parquet.LogicalColumn(exact, :timestamp; unit=:micros,
        adjusted=false),))
    @test wtticks.(wtvalues(bytes, :value)) == [1, 2, 3]
    @test_throws "loses precision" wtwrite((value=Parquet.LogicalColumn(nanos,
        :timestamp; unit=:micros, adjusted=false),))
    @test_throws ArgumentError Parquet.LogicalColumn(nanos, :timestamp; unit=:micros,
        adjusted=true)
end

@testset "write Dates periods and Durations.Duration" begin
    bytes = wtwrite((ns=Nanosecond.(1:3), us=Microsecond.(1:3), ms=Millisecond.(1:3),
        s=Second.(1:3), min=Minute.(1:3), h=Hour.(1:3), d=Day.(1:3), w=Week.(1:3)))
    table = Parquet.Table(bytes)
    try
        # The ARROW:schema gives each INT64 column its duration unit back; Minute
        # through Week are stored as seconds.
        columns = Tables.columns(table)
        @test Tables.getcolumn(columns, :ns) == Nanosecond.(1:3)
        @test Tables.getcolumn(columns, :us) == Microsecond.(1:3)
        @test Tables.getcolumn(columns, :ms) == Millisecond.(1:3)
        @test Tables.getcolumn(columns, :s) == Second.(1:3)
        @test Tables.getcolumn(columns, :min) == Second.(60 .* (1:3))
        @test Tables.getcolumn(columns, :h) == Second.(3600 .* (1:3))
        @test Tables.getcolumn(columns, :d) == Second.(86400 .* (1:3))
        @test Tables.getcolumn(columns, :w) == Second.(604800 .* (1:3))
    finally
        close(table)
    end
    durations = [WTD.Duration(i, 2i, 3_000_000i) for i in 1:3]
    for (values, expected) in ((durations, [(i, 2i, 3i) for i in 1:3]),
            (Month.(1:3), [(i, 0, 0) for i in 1:3]),
            (Year.(1:3), [(12i, 0, 0) for i in 1:3]))
        bytes = wtwrite((value=values,))
        leaf = wtleaf(bytes)
        @test leaf.type_ == WTMD.Type.FIXED_LEN_BYTE_ARRAY && leaf.type_length == 12
        @test leaf.converted_type == WTMD.ConvertedType.INTERVAL
        @test wtinterval.(wtvalues(bytes, :value)) == expected
    end
    for bad in ([WTD.Duration(-1, 0, 0)], [WTD.Duration(0, 0, 5)], [Month(-1)])
        err = wterror(() -> wtwrite((span=bad,)))
        @test err isa ArgumentError && occursin("\"span\"", err.msg)
    end
end

@testset "write Symbol, Char, pooled, and categorical columns as strings" begin
    bytes = wtwrite((sym=[:a, :bb], char=['x', 'é']))
    @test wtvalues(bytes, :sym) == ["a", "bb"]
    @test wtvalues(bytes, :char) == ["x", "é"]
    @test wtleaf(bytes).logicalType.STRING !== nothing
    cat = categorical(["red", missing, "blue", "red"])
    pooled = PooledArray(["x", "y", "x", "x"])
    bytes = wtwrite((cat=cat, pooled=pooled); dictionary=true)
    @test isequal(wtvalues(bytes, :cat), ["red", missing, "blue", "red"])
    @test wtvalues(bytes, :pooled) == ["x", "y", "x", "x"]
end

@testset "write narrows abstract and Union columns from their values" begin
    bytes = wtwrite((value=Any[1, 2, 3],))
    @test wtleaf(bytes).type_ == WTMD.Type.INT64
    @test wtvalues(bytes, :value) == [1, 2, 3]
    bytes = wtwrite((value=Any[1, 2.5, missing],))
    @test wtleaf(bytes).type_ == WTMD.Type.DOUBLE
    @test isequal(wtvalues(bytes, :value), [1.0, 2.5, missing])
    strings = Union{Missing,String,SubString{String}}["a", SubString("xbc", 2), missing]
    @test isequal(wtvalues(wtwrite((value=strings,)), :value), ["a", "bc", missing])
    @test Tables.getcolumn(Tables.columns(Parquet.Table(wtwrite((value=Any[missing, missing],)))),
        :value) isa AbstractVector{Missing}
    structs = Any[(a=1, b="x"), (a=missing, b="y")]
    @test length(wtvalues(wtwrite((value=structs,)), :value)) == 2
    @test_throws "mixes values of types Int64 and String" wtwrite((value=Any[1, "a"],))
    @test_throws "use missing for null values" wtwrite(
        (value=Union{Nothing,Int}[1, nothing],))
    @test_throws "does not convert to Float64 exactly" wtwrite(
        (value=Any[2^53 + 1, 0.5],))
    # Int8 and UInt64 promote to UInt64, which cannot hold -1.
    @test_throws "column \"value\" value -1 does not convert to UInt64 exactly" wtwrite(
        (value=Any[Int8(-1), UInt64(2)^63],))
end

@testset "write errors for types Parquet cannot store" begin
    err = wterror(() -> wtwrite((id=Int32[1], big=BigInt[1])))
    @test err isa ArgumentError
    @test occursin("\"big\"", err.msg) && occursin("Decimal{P,0}", err.msg)
    err = wterror(() -> wtwrite((nested=[[BigInt(1)]],)))
    @test occursin("\"nested.element\"", err.msg)
end

@testset "write byte, fixed-width, and CSV.jl container columns" begin
    bytes = wtwrite((bytes=[Parquet.DataStrings.DataBytes(UInt8[i, i]) for i in 1:3],
        fixed=[ntuple(k -> UInt8(i + k), 3) for i in 1:3]))
    @test collect.(wtvalues(bytes, :bytes)) == [UInt8[i, i] for i in 1:3]
    @test collect.(wtvalues(bytes, :fixed)) == [UInt8[i + 1, i + 2, i + 3] for i in 1:3]
    @test wtleaf(bytes).type_length == 3
    chained = ChainedVector([[1, 2], [3]])
    sentinel = SentinelVector{Int}(undef, 3)
    sentinel[1] = 7
    sentinel[2] = missing
    sentinel[3] = 9
    bytes = wtwrite((chained=chained, sentinel=sentinel))
    @test wtvalues(bytes, :chained) == 1:3
    @test isequal(wtvalues(bytes, :sentinel), [7, missing, 9])

    # Chunks are read part by part across row group and page boundaries; page row
    # indexes stay right for filtered reads.
    parts = [collect(1:5), Int[], collect(6:11), [12]]
    sentinels = map(parts) do part
        vector = SentinelVector{Int}(undef, length(part))
        for (i, x) in enumerate(part)
            vector[i] = iszero(x % 4) ? missing : x
        end
        vector
    end
    strings = ChainedVector([string.(part) for part in parts])
    table = (a=ChainedVector(parts), b=ChainedVector(sentinels), c=strings)
    for rowgroupsize in (4, 7, 12)
        bytes = take!(Parquet.write(IOBuffer(), table; rowgroupsize, pagesize=16))
        @test wtvalues(bytes, :a) == 1:12
        @test isequal(wtvalues(bytes, :b), [iszero(x % 4) ? missing : x for x in 1:12])
        @test wtvalues(bytes, :c) == string.(1:12)
        filtered = Parquet.Table(bytes; filter=Parquet.col(:a) >= 9)
        @test collect(filtered.a) == 9:12 && collect(filtered.c) == string.(9:12)
    end
end

@testset "write DataDecimals by storage" begin
    small = reinterpret(WTDec.Decimal{5,2,Int64}, Int64[1, -2, 99999])
    wide = reinterpret(WTDec.Decimal{38,4,Int128}, Int128[1, -2, 10^20])
    bytes = wtwrite((small=small, wide=wide))
    leaf = wtleaf(bytes)
    @test leaf.type_ == WTMD.Type.FIXED_LEN_BYTE_ARRAY && leaf.type_length == 16
    @test leaf.logicalType.DECIMAL.precision == 38 && leaf.logicalType.DECIMAL.scale == 4
    @test WTDec.unscaled.(wtvalues(bytes, :small)) == [1, -2, 99999]
    @test WTDec.unscaled.(wtvalues(bytes, :wide)) == Int128[1, -2, 10^20]
    toolarge = reinterpret(WTDec.Decimal{3,0,Int32}, Int32[5000])
    err = wterror(() -> wtwrite((amount=toolarge,)))
    @test err isa ArgumentError && occursin("\"amount\"", err.msg) &&
        occursin("more than 3 digits", err.msg)
end

@testset "write ecosystem types inside nested columns" begin
    stamps = [WTD.Timestamp{Microsecond}(Dates.UTInstant(Microsecond(i))) for i in 1:3]
    bytes = wtwrite((lists=[stamps[1:i] for i in 1:3],
        structs=[(d=WTD.Duration(i, 0, 0), s=:x) for i in 1:3]))
    table = Parquet.Table(bytes)
    try
        lists = Tables.getcolumn(Tables.columns(table), :lists)
        @test [wtticks.(collect(row)) for row in lists] == [collect(1:i) for i in 1:3]
    finally
        close(table)
    end
end

@testset "CSV.File with datetime columns writes through Parquet.write" begin
    rows = 20_000
    text = "id,when,day,name\n" * join(("$i,2024-01-0$(1 + i % 9)T12:34:56.$(lpad(i % 1000, 3, '0')),2024-02-0$(1 + i % 9),n$(i % 3)"
        for i in 1:rows), "\n")
    file = CSV.File(IOBuffer(text); ntasks=4)
    when = Tables.getcolumn(file, :when)
    @test eltype(when) <: WTD.Timestamp
    path = tempname() * ".parquet"
    try
        @test (file |> Parquet.write(path)) == path
        # Read a copy: a memory-mapped file stays mapped until garbage collection,
        # and Windows refuses to delete a mapped file.
        table = Parquet.Table(read(path))
        try
            columns = Tables.columns(table)
            @test Tables.getcolumn(columns, :id) == 1:rows
            @test wtticks.(collect(Tables.getcolumn(columns, :when))) == Dates.value.(when)
            @test collect(Tables.getcolumn(columns, :name)) == Tables.getcolumn(file, :name)
        finally
            close(table)
        end
    finally
        rm(path; force=true)
    end
end

@testset "ZonedDateTime columns write as UTC timestamps with their zone" begin
    zone = TimeZones.TimeZone("America/New_York")
    times = [TimeZones.ZonedDateTime(2024, 1, 1, 9, zone),
        TimeZones.ZonedDateTime(2024, 7, 1, 9, 30, zone)]
    table = Parquet.Table(wtwrite((t=times, o=[missing; times[2:2]])))
    try
        Z = Symbol("America/New_York")
        @test eltype(table.t) == WTD.ZonedTimestamp{Millisecond,Z}
        @test eltype(table.o) == Union{Missing,WTD.ZonedTimestamp{Millisecond,Z}}
        @test [value.utc for value in table.t] ==
            [WTD.ZonedTimestamp{Millisecond,Z}(time).utc for time in times]
        @test isequal(table.o[1], missing)
        element = Parquet._tablemetadata(table).schema[2]
        @test element.logicalType.TIMESTAMP.isAdjustedToUTC
    finally
        close(table)
    end
    mixed = [times[1], TimeZones.ZonedDateTime(2024, 1, 1, 9, TimeZones.tz"UTC")]
    @test_throws "column \"t\" holds ZonedDateTime values in time zones" wtwrite((t=mixed,))
    # A column of only missing stays a null column.
    @test Tables.schema(Parquet.Table(wtwrite((n=[missing, missing],)))).types ==
        (Missing,)
end
