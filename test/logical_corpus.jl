using Test
using UUIDs
import Dates, Tables

@testset "Apache logical-type corpus" begin
    corpus = get(ENV, "PARQUET_TESTING_DIR", joinpath(@__DIR__, "parquet-testing"))
    data = joinpath(corpus, "data")
    fixture = joinpath(data, "int32_decimal.parquet")
    if !isfile(fixture)
        @info "parquet-testing corpus not found; skipping logical fixtures" corpus
    else
        expecteddecimal = Parquet.DataDecimals.Decimal64{2}[
            Parquet.DataDecimals.Decimal64{2}(index) for index in 1:24
        ]
        for name in (
                "int32_decimal.parquet",
                "int64_decimal.parquet",
                "byte_array_decimal.parquet",
                "fixed_length_decimal.parquet",
                "fixed_length_decimal_legacy.parquet")
            table = Parquet.Table(joinpath(data, name))
            @test Parquet.Tables.columns(table).value == expecteddecimal
            close(table)
        end

        nonzero = Parquet.Table(joinpath(data, "float16_nonzeros_and_nans.parquet"))
        expectedbits = Union{Missing,UInt16}[
            missing, 0x3c00, 0xc000, 0x7e00, 0x0000, 0xbc00, 0x8000, 0x4000]
        actualbits = Union{Missing,UInt16}[
            ismissing(value) ? missing : reinterpret(UInt16, value)
            for value in Parquet.Tables.columns(nonzero).x
        ]
        @test isequal(actualbits, expectedbits)
        close(nonzero)

        zeros = Parquet.Table(joinpath(data, "float16_zeros_and_nans.parquet"))
        expectedbits = Union{Missing,UInt16}[missing, 0x0000, 0x7e00]
        actualbits = Union{Missing,UInt16}[
            ismissing(value) ? missing : reinterpret(UInt16, value)
            for value in Parquet.Tables.columns(zeros).x
        ]
        @test isequal(actualbits, expectedbits)
        close(zeros)

        json = Parquet.Table(joinpath(data, "json.parquet"))
        expectedjson = Union{Missing,Parquet.JSONValue}[
            Parquet.JSONValue(codeunits("{\"a\":1}")),
            Parquet.JSONValue(codeunits("{\"a\":1,\"b\":null}")),
            Parquet.JSONValue(codeunits("[1,null,3]")),
            missing,
        ]
        @test isequal(Parquet.Tables.columns(json).json_field, expectedjson)
        close(json)

        bson = Parquet.Table(joinpath(data, "bson.parquet"))
        expectedbson = Union{Missing,Parquet.BSONValue}[
            Parquet.BSONValue(hex2bytes("0c0000001061000100000000")),
            Parquet.BSONValue(hex2bytes("0f000000106100010000000a620000")),
            missing,
        ]
        @test isequal(Parquet.Tables.columns(bson).bson_field, expectedbson)
        close(bson)

        unknown = Parquet.Table(joinpath(data, "unknown-logical-type.parquet"))
        @test Parquet.Tables.columns(unknown)[2] == [
            collect(codeunits("unknown string $index")) for index in 1:3
        ]
        close(unknown)
    end
end

@testset "INT96 timestamps" begin
    # 8 little-endian bytes of nanoseconds within the day, then 4 of Julian day number.
    int96(day, nanoseconds) = vcat(reinterpret(UInt8, [htol(Int64(nanoseconds))]),
        reinterpret(UInt8, [htol(UInt32(day))]))
    element = Parquet.Metadata.SchemaElement(name="ts", type_=Parquet.Metadata.Type.INT96)
    epoch = 2_440_588
    for (day, nanoseconds, ticks) in ((epoch, 0, 0), (epoch, 123, 123),
            (epoch - 1, 86_400_000_000_000 - 1, -1),
            (epoch + 19_000, 1_000, 19_000 * 86_400_000_000_000 + 1_000))
        value = Parquet._logicalvalue(element, int96(day, nanoseconds))
        @test value isa Parquet.Durations.Timestamp{Dates.Nanosecond}
        @test Dates.value(value) == ticks
    end
    # Spark writes 9999-12-31 placeholders, outside the nanosecond range; the error
    # names the keyword that reads them.
    error = try
        Parquet._logicalvalue(element, int96(5_373_484, 0))
    catch err
        err
    end
    @test error isa Parquet.UnsupportedFeatureError
    @test occursin("9999-12-31", error.message) && occursin("int96=Dates.Microsecond", error.message)
    @test Parquet._fromint96(int96(5_373_484, 1_000), element, Dates.Microsecond) ==
        Parquet.Durations.Timestamp{Dates.Microsecond}(Dates.UTInstant(
            Dates.Microsecond((5_373_484 - epoch) * 86_400_000_000 + 1)))
    @test_throws Parquet.UnsupportedFeatureError Parquet._fromint96(int96(epoch, 1),
        element, Dates.Microsecond)
    @test_throws Parquet.UnsupportedFeatureError Parquet._fromint96(int96(epoch, 1_000),
        element, Dates.Millisecond)
    @test_throws Parquet.FormatError Parquet._logicalvalue(element, UInt8[1, 2, 3])

    corpus = get(ENV, "PARQUET_TESTING_DIR", joinpath(@__DIR__, "parquet-testing"))
    data = joinpath(corpus, "data")
    if isfile(joinpath(data, "int96_timestamp_order.parquet"))
        # Expected ticks are pyarrow 25's nanosecond timestamps for the same files.
        for (name, column, expected) in (
                ("int96_timestamp_order.parquet", :ts,
                    [86399999999999, 86400000000000, -50803199999999877, 1000]),
                ("alltypes_plain.parquet", :timestamp_col,
                    [1235865600000000000, 1235865660000000000, 1238544000000000000,
                        1238544060000000000, 1233446400000000000, 1233446460000000000,
                        1230768000000000000, 1230768060000000000]),
                ("alltypes_dictionary.parquet", :timestamp_col,
                    [1230768000000000000, 1230768060000000000]))
            table = Parquet.Table(joinpath(data, name))
            try
                values = getproperty(Parquet.Tables.columns(table), column)
                @test all(value -> value isa Parquet.Durations.Timestamp{Dates.Nanosecond},
                    values)
                @test [Dates.value(value) for value in values] == expected
                # A rewrite of the table stores INT96 as an INT64 nanosecond TIMESTAMP.
                rewrite = Parquet.Table(take!(Parquet.write(IOBuffer(), table)))
                try
                    @test isequal(getproperty(Parquet.Tables.columns(rewrite), column),
                        values)
                    element = only(element for element in
                        Parquet._tablemetadata(rewrite).schema if element.name == String(column))
                    @test element.type_ == Parquet.Metadata.Type.INT64
                    @test element.logicalType.TIMESTAMP.unit.NANOS !== nothing
                finally
                    close(rewrite)
                end
                io = IOBuffer()
                Parquet.write(io, Tables.columntable(table))
                rewritten = Parquet.Table(take!(io))
                try
                    @test isequal(getproperty(Parquet.Tables.columns(rewritten), column), values)
                finally
                    close(rewritten)
                end
            finally
                close(table)
            end
        end
        # Spark wrote years 9999 and 290000, beyond the nanosecond range; at Spark's
        # microsecond precision they read as Spark reads them.
        spark = joinpath(data, "int96_from_spark.parquet")
        @test_throws Parquet.UnsupportedFeatureError Parquet.Table(spark)
        table = Parquet.Table(spark; int96=Dates.Microsecond)
        @test eltype(table.a) == Union{Missing,Parquet.Durations.Timestamp{Dates.Microsecond}}
        @test isequal([ismissing(value) ? missing : Dates.value(value) for value in table.a],
            [1704141296123456, 1704070800000000, 253402225200000000, 1735599600000000,
                missing, 9089380393200000000])
        # Read at microseconds, the rewrite stores microseconds, which hold year 290000.
        rewrite = Parquet.Table(take!(Parquet.write(IOBuffer(), table)))
        @test isequal(rewrite.a, table.a)
        @test Parquet._tablemetadata(rewrite).schema[2].logicalType.TIMESTAMP.unit.MICROS !==
            nothing
        close(rewrite)
        close(table)
        @test_throws ArgumentError Parquet.Table(spark; int96=Dates.Second)
    else
        @info "parquet-testing corpus not found; skipping INT96 fixtures" corpus
    end
end
