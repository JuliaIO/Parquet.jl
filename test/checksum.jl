@testset "page CRC32" begin
    bytes = collect(codeunits("123456789"))
    expected = UInt32(0xcbf43926)
    @test Parquet.pagechecksum(bytes) == expected
    Parquet.verifypagechecksum(reinterpret(Int32, expected), bytes)
    @test_throws Parquet.FormatError Parquet.verifypagechecksum(Int32(0), bytes)

    src = Parquet.source(bytes)
    slice = Parquet.readrange(src, 0, length(bytes))
    scratch = Parquet._pagechecksumscratch(slice)
    constrained = Parquet._LiveByteBudget(Parquet.Limits(
        max_materialized_bytes=scratch - 1))
    @test_throws Parquet.LimitError Parquet.verifypagechecksum(
        reinterpret(Int32, expected), slice; budget=constrained)
    @test Parquet._budgetused(constrained) == 0
    sufficient = Parquet._LiveByteBudget(Parquet.Limits(
        max_materialized_bytes=scratch))
    Parquet.verifypagechecksum(reinterpret(Int32, expected), slice;
        budget=sufficient)
    @test Parquet._budgetused(sufficient) == 0
    @test_throws Parquet.FormatError Parquet.verifypagechecksum(Int32(0),
        slice; budget=sufficient)
    @test Parquet._budgetused(sufficient) == 0
    Parquet.close!(src)
end
