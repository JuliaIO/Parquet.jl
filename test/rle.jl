@testset "RLE and bit-packed hybrid" begin
    values = UInt64[0, 1, 2, 3, 4, 5, 6, 7]
    encoded = Parquet.encode_hybrid(values, 3)
    @test encoded == UInt8[0x03, 0x88, 0xc6, 0xfa]
    decoded, position = Parquet.decode_hybrid(encoded, 8, 3)
    @test decoded == values
    @test position == 5

    rle = UInt8[0x10, 0x03]
    decoded, position = Parquet.decode_hybrid(rle, 8, 3)
    @test decoded == fill(UInt64(3), 8)
    @test position == 3

    prefixed = Parquet.encode_hybrid(UInt64[1, 0, 1], 1; length_prefix=true)
    decoded, position = Parquet.decode_hybrid(prefixed, 3, 1; length_prefix=true)
    @test decoded == UInt64[1, 0, 1]
    @test position == length(prefixed) + 1

    zerosencoded = Parquet.encode_hybrid(zeros(UInt64, 11), 0)
    decoded, _ = Parquet.decode_hybrid(zerosencoded, 11, 0)
    @test decoded == zeros(UInt64, 11)

    @test_throws Parquet.FormatError Parquet.decode_hybrid(UInt8[0x00], 1, 1)
    @test_throws Parquet.FormatError Parquet.decode_hybrid(encoded[1:(end - 1)], 8, 3)
    @test_throws Parquet.FormatError Parquet.decode_hybrid(fill(UInt8(0x80), 10), 1, 1)
    hugeheader = vcat(fill(UInt8(0xff), 9), UInt8[0x01])
    @test_throws Parquet.FormatError Parquet.decode_hybrid(hugeheader, 1, 64)
    hugerle = UInt8[]
    Parquet._writehybridvarint!(hugerle, UInt64(typemax(Int32) + Int64(1)) << 1)
    push!(hugerle, 0x00)
    @test_throws Parquet.FormatError Parquet.decode_hybrid(hugerle, 1, 1)
    hugegroups = UInt64(typemax(Int32) ÷ 8 + 1)
    hugepacked = UInt8[]
    Parquet._writehybridvarint!(hugepacked, (hugegroups << 1) | 0x01)
    @test_throws Parquet.FormatError Parquet.decode_hybrid(hugepacked, 1, 1)
    @test_throws ArgumentError Parquet.encode_hybrid(UInt64[8], 3)
    @test_throws ArgumentError Parquet.decode_hybrid(encoded, 8, 65)
end

@testset "deprecated BIT_PACKED levels" begin
    bytes = UInt8[0x05, 0x39, 0x77]
    values, position = Parquet.decode_bit_packed(bytes, 8, 3)
    @test values == UInt64.(0:7)
    @test position == 4
    padded = UInt8[0xaa, 0x05, 0x39, 0x70, 0xbb]
    values, position = Parquet.decode_bit_packed(padded, 7, 3; offset=2)
    @test values == UInt64.(0:6)
    @test position == 5
    @test Parquet.decode_bit_packed(UInt8[], 4, 0) == (zeros(UInt64, 4), 1)
    @test_throws Parquet.FormatError Parquet.decode_bit_packed(bytes[1:2], 8, 3)
    @test_throws Parquet.FormatError Parquet.decode_bit_packed(UInt8[0x05, 0x39, 0x71], 7, 3)
    @test_throws Parquet.LimitError Parquet.decode_bit_packed(bytes, 8, 3;
        limits=Parquet.Limits(max_container_elements=7))
    @test_throws ArgumentError Parquet.decode_bit_packed(bytes, -1, 3)
    @test_throws ArgumentError Parquet.decode_bit_packed(bytes, 8, 65)
end
