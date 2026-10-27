using Test
using UUIDs
import Tables

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
        @test value isa Parquet.Timestamp{:nanos}
        @test value.ticks == ticks && !value.is_adjusted_to_utc
    end
    @test_throws Parquet.FormatError Parquet._logicalvalue(element,
        int96(5_373_484, 0))
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
                @test all(value -> value isa Parquet.Timestamp{:nanos} &&
                    !value.is_adjusted_to_utc, values)
                @test [value.ticks for value in values] == expected
                @test_throws Parquet.UnsupportedFeatureError Parquet.write(
                    IOBuffer(), table)
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
        # Spark wrote years 9999 and 290000, beyond the nanosecond range.
        @test_throws Parquet.FormatError Parquet.Table(
            joinpath(data, "int96_from_spark.parquet"))
    else
        @info "parquet-testing corpus not found; skipping INT96 fixtures" corpus
    end
end
