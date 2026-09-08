using Test
using UUIDs

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
            @test table.columns.value == expecteddecimal
            close(table)
        end

        nonzero = Parquet.Table(joinpath(data, "float16_nonzeros_and_nans.parquet"))
        expectedbits = Union{Missing,UInt16}[
            missing, 0x3c00, 0xc000, 0x7e00, 0x0000, 0xbc00, 0x8000, 0x4000]
        actualbits = Union{Missing,UInt16}[
            ismissing(value) ? missing : reinterpret(UInt16, value)
            for value in nonzero.columns.x
        ]
        @test isequal(actualbits, expectedbits)
        close(nonzero)

        zeros = Parquet.Table(joinpath(data, "float16_zeros_and_nans.parquet"))
        expectedbits = Union{Missing,UInt16}[missing, 0x0000, 0x7e00]
        actualbits = Union{Missing,UInt16}[
            ismissing(value) ? missing : reinterpret(UInt16, value)
            for value in zeros.columns.x
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
        @test isequal(json.columns.json_field, expectedjson)
        close(json)

        bson = Parquet.Table(joinpath(data, "bson.parquet"))
        expectedbson = Union{Missing,Parquet.BSONValue}[
            Parquet.BSONValue(hex2bytes("0c0000001061000100000000")),
            Parquet.BSONValue(hex2bytes("0f000000106100010000000a620000")),
            missing,
        ]
        @test isequal(bson.columns.bson_field, expectedbson)
        close(bson)

        unknown = Parquet.Table(joinpath(data, "unknown-logical-type.parquet"))
        @test unknown.columns[2] == [
            collect(codeunits("unknown string $index")) for index in 1:3
        ]
        close(unknown)
    end
end
