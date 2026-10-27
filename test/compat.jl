using Tables

@testset "PARQUET-816: old parquet-mr dictionary chunk sizes" begin
    # parquet-mr before 1.2.9 left the dictionary page header out of the chunk size.
    path = joinpath(get(ENV, "PARQUET_TESTING_DIR", joinpath(@__DIR__, "parquet-testing")),
        "data", "nation.dict-malformed.parquet")
    if isfile(path)
        table = Parquet.Table(path)
        @test Tables.rowcount(table) == 25
        @test table.nation_key[1:5] == 0:4
        @test table.region_key[1:5] == [0, 1, 1, 1, 4]
        @test String(collect(table.name[2])) == "ARGENTINA"
        @test startswith(String(collect(table.comment_col[1])),
            " haggle. carefully final deposits detect")
    else
        @warn "parquet-testing corpus not found; skipping PARQUET-816"
    end
end

@testset "TIME of 24:00:00" begin
    bytes = take!(Parquet.write(IOBuffer(), (t=Int32[0, 86_400_000], x=[1, 2])))
    n = length(bytes)
    start = n - 8 - Int(Parquet._readu32le(bytes, n - 7))
    metadata = Parquet.Thrift.decode(view(bytes, (start + 1):(n - 8)),
        Parquet.Metadata.FileMetaData)
    elements = copy(metadata.schema)
    elements[2] = Parquet._withfields(elements[2];
        converted_type=Parquet.Metadata.ConvertedType.TIME_MILLIS,
        logicalType=Parquet.Metadata.LogicalType(TIME=Parquet.Metadata.TimeType(
            isAdjustedToUTC=false, unit=Parquet.Metadata.TimeUnit(
            MILLIS=Parquet.Metadata.MilliSeconds()))))
    footer = Parquet.Thrift.encode(Parquet._withfields(metadata; schema=elements,
        key_value_metadata=nothing))
    timed = vcat(bytes[1:start], footer, reinterpret(UInt8, [htol(UInt32(length(footer)))]),
        b"PAR1")
    err = try
        Parquet.Table(timed)
    catch e
        e
    end
    @test err isa Parquet.UnsupportedFeatureError
    @test occursin("24:00:00", err.message) && occursin("drop=[:t]", err.message)
    @test Parquet.Table(timed; drop=[:t]).x == [1, 2]
end
