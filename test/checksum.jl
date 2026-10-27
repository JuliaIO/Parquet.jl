@testset "page CRC32" begin
    bytes = collect(codeunits("123456789"))
    expected = UInt32(0xcbf43926)
    @test Parquet.pagechecksum(bytes) == expected
    Parquet.verifypagechecksum(reinterpret(Int32, expected), bytes)
    @test_throws Parquet.FormatError Parquet.verifypagechecksum(Int32(0), bytes)

    src = Parquet.source(bytes)
    slice = Parquet.readrange(src, 0, length(bytes))
    Parquet.verifypagechecksum(reinterpret(Int32, expected), slice)
    @test_throws Parquet.FormatError Parquet.verifypagechecksum(Int32(0), slice)
    Parquet.close!(src)
end
