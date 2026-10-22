@testset "PLAIN primitives" begin
    for values in (
        Int32[typemin(Int32), -1, 0, 1, typemax(Int32)],
        Int64[typemin(Int64), -1, 0, 1, typemax(Int64)],
        Float32[-Inf, -0.0, 0.0, 1.5, Inf, NaN],
        Float64[-Inf, -0.0, 0.0, 1.5, Inf, NaN],
    )
        encoded = Parquet.encode_plain(values)
        decoded, position = Parquet.decode_plain(eltype(values), encoded, length(values))
        @test isequal(decoded, values)
        @test position == length(encoded) + 1
        @test_throws Parquet.FormatError Parquet.decode_plain(eltype(values), encoded[1:(end - 1)], length(values))
    end

    booleans = Bool[true, false, true, true, false, false, false, true, true]
    encoded = Parquet.encode_plain(booleans)
    @test encoded == UInt8[0x8d, 0x01]
    decoded, position = Parquet.decode_plain(Bool, encoded, length(booleans))
    @test decoded == booleans
    @test position == 3
    @test_throws Parquet.LimitError Parquet.decode_plain(Bool, encoded, 3;
        limits=Parquet.Limits(max_container_elements=2))
    @test_throws Parquet.LimitError Parquet.decode_plain(Int32, zeros(UInt8, 12), 3;
        limits=Parquet.Limits(max_container_elements=2))
end

@testset "PLAIN byte arrays" begin
    values = [UInt8[], UInt8[0x00, 0xff], collect(codeunits("Parquet"))]
    encoded = Parquet.encode_plain_byte_array(values)
    decoded, position = Parquet.decode_plain_byte_array(encoded, length(values))
    @test decoded == values
    @test position == length(encoded) + 1
    @test Parquet.encode_plain_byte_array(["abc"]) == UInt8[0x03, 0x00, 0x00, 0x00, 0x61, 0x62, 0x63]

    negative = UInt8[0xff, 0xff, 0xff, 0xff]
    @test_throws Parquet.FormatError Parquet.decode_plain_byte_array(negative, 1)
    oversized = UInt8[0x04, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03, 0x04]
    @test_throws Parquet.LimitError Parquet.decode_plain_byte_array(oversized, 1;
        limits=Parquet.Limits(max_string_bytes=3))
    @test_throws Parquet.FormatError Parquet.decode_plain_byte_array(UInt8[0x00], 1000)
end

@testset "PLAIN fixed byte arrays" begin
    values = reshape(UInt8[0x01, 0x02, 0x03, 0x04, 0x05, 0x06], 2, 3)
    encoded = Parquet.encode_plain_fixed(values)
    decoded, position = Parquet.decode_plain_fixed(encoded, 3, 2)
    @test decoded == values
    @test position == 7
    @test_throws Parquet.FormatError Parquet.decode_plain_fixed(encoded, 4, 2)
end
