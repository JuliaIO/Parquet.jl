using Test

const DMD = Parquet.Metadata
const DINT256 = Decimals.Int256

struct DecimalConversionProbe{T} <: AbstractVector{T}
    length::Int
end

function Base.IndexStyle(::Type{<:DecimalConversionProbe})
    return IndexLinear()
end

function Base.size(values::DecimalConversionProbe)
    return (values.length,)
end

function Base.getindex(::DecimalConversionProbe, ::Int)
    throw(ErrorException("DECIMAL conversion probe was indexed"))
end

function decimalreadallocation(element, values, limits)
    caught = Ref{Any}(nothing)
    allocated = @allocated caught[] = try
        Parquet._logicalvalues(element, values; limits=limits)
        nothing
    catch err
        err
    end
    return caught[], allocated
end

function decimalwriteallocation(element, values, limits)
    caught = Ref{Any}(nothing)
    allocated = @allocated caught[] = try
        Parquet._physicalvalues(element, values; limits=limits)
        nothing
    catch err
        err
    end
    return caught[], allocated
end

function decimaltestelement(name, physical, precision, scale; width=nothing,
    modern=true)
    logical = modern ? DMD.LogicalType(
        DECIMAL=DMD.DecimalType(precision=Int32(precision), scale=Int32(scale))) : nothing
    converted = modern ? DMD.ConvertedType.DECIMAL : DMD.ConvertedType.DECIMAL
    return DMD.SchemaElement(
        name=String(name),
        type_=physical,
        type_length=width === nothing ? nothing : Int32(width),
        repetition_type=DMD.FieldRepetitionType.OPTIONAL,
        converted_type=converted,
        precision=modern ? nothing : Int32(precision),
        scale=modern ? nothing : Int32(scale),
        logicalType=logical,
    )
end

# Big-endian two's complement of `value` in exactly `width` bytes.
function decimalreference(value::Integer, width::Integer)
    magnitude = value < 0 ? big(value) + (big(1) << (8 * width)) : big(value)
    bytes = Vector{UInt8}(undef, Int(width))
    for index in Int(width):-1:1
        bytes[index] = UInt8(magnitude & 0xff)
        magnitude >>= 8
    end
    return bytes
end

@testset "DECIMAL metadata validation" begin
    for (physical, precision, width) in (
            (DMD.Type.INT32, 9, nothing),
            (DMD.Type.INT64, 18, nothing),
            (DMD.Type.BYTE_ARRAY, 40, nothing),
            (DMD.Type.FIXED_LEN_BYTE_ARRAY, 9, 4))
        element = decimaltestelement("value", physical, precision, 2; width=width)
        @test Parquet._decimallogicalkind(element) === :decimal
        @test Parquet._decimallogicaleltype(:decimal, element) ===
            Decimal{precision,2,Parquet._decimalstorage(precision)}
    end
    legacy = decimaltestelement("legacy", DMD.Type.INT64, 18, 4; modern=false)
    @test Parquet._decimallogicalkind(legacy) === :decimal
    @test Parquet._decimallogicaleltype(:decimal, legacy) === Decimal64{4}
    @test_throws Parquet.FormatError Parquet._decimallogicalkind(
        decimaltestelement("bad", DMD.Type.FLOAT, 5, 2))
    @test_throws Parquet.FormatError Parquet._decimallogicalkind(
        decimaltestelement("bad", DMD.Type.INT32, 10, 2))
    @test_throws Parquet.FormatError Parquet._decimallogicalkind(
        decimaltestelement("bad", DMD.Type.INT64, 19, 2))
    @test_throws Parquet.FormatError Parquet._decimallogicalkind(
        decimaltestelement("bad", DMD.Type.FIXED_LEN_BYTE_ARRAY, 3, 0; width=1))
    @test_throws Parquet.FormatError Parquet._decimallogicalkind(
        decimaltestelement("bad", DMD.Type.BYTE_ARRAY, 0, 0))
    @test_throws Parquet.FormatError Parquet._decimallogicalkind(
        decimaltestelement("bad", DMD.Type.BYTE_ARRAY, 4, 5))
    @test_throws Parquet.UnsupportedFeatureError Parquet._decimallogicalkind(
        decimaltestelement("wide", DMD.Type.BYTE_ARRAY, 77, 0))
end

@testset "DECIMAL storage tiers" begin
    for (precision, storage) in ((1, Int32), (9, Int32), (10, Int64), (18, Int64),
            (19, Int128), (38, Int128), (39, DINT256), (76, DINT256))
        @test Parquet._decimalstorage(precision) === storage
        @test Parquet._decimaltype(precision, 0) === Decimal{precision,0,storage}
        @test Parquet._decimalcapacity(storage) >= precision
    end
    @test Parquet._decimaltype(9, 9) === Decimal{9,9,Int32}
    @test Parquet._decimaltype(38, 0) === Decimal128{0}
end

@testset "DECIMAL big-endian conversion" begin
    cases = (
        (UInt8[0x00], 0),
        (UInt8[0x01], 1),
        (UInt8[0x7f], 127),
        (UInt8[0x80], -128),
        (UInt8[0xff], -1),
        (UInt8[0x00, 0x80], 128),
        (UInt8[0xff, 0x7f], -129),
        (UInt8[0x80, 0x00], -32768),
        (UInt8[0x7f, 0xff], 32767),
    )
    for T in (Int32, Int64, Int128, DINT256)
        for (bytes, expected) in cases
            @test Parquet._decimalfrombytes(T, bytes, "value") == T(expected)
            # A leading run of sign bytes is dropped, not misread.
            padded = vcat(fill(expected < 0 ? 0xff : 0x00, 3), bytes)
            @test Parquet._decimalfrombytes(T, padded, "value") == T(expected)
        end
    end

    for T in (Int32, Int64, Int128, DINT256)
        width = sizeof(T)
        for value in (typemin(T), typemin(T) + one(T), T(-1), T(0), T(1),
                typemax(T) - one(T), typemax(T))
            bytes = decimalreference(value, width)
            @test Parquet._decimalfrombytes(T, bytes, "value") == value
            @test Parquet._decimalbytes(value, width) == bytes
        end
    end

    # Sign extension when the wire value is narrower than the storage tier.
    @test Parquet._decimalfrombytes(Int64, UInt8[0xff], "value") == Int64(-1)
    @test Parquet._decimalfrombytes(Int128, UInt8[0x80], "value") == Int128(-128)
    @test Parquet._decimalfrombytes(DINT256, UInt8[0xff, 0x00], "value") ==
        DINT256(-256)
    @test Parquet._decimalfrombytes(Int64, UInt8[0x00, 0xff], "value") == Int64(255)

    # Sign extension when the declared byte width is wider than the value.
    @test Parquet._decimalbytes(Int32(-1), 8) == fill(0xff, 8)
    @test Parquet._decimalbytes(Int32(128), 8) ==
        UInt8[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80]
    @test Parquet._decimalbytes(Int64(-129), 4) == UInt8[0xff, 0xff, 0xff, 0x7f]

    # A value that genuinely needs more than the storage tier is rejected.
    @test_throws Parquet.FormatError Parquet._decimalfrombytes(
        Int32, UInt8[0x01, 0x00, 0x00, 0x00, 0x00], "value")
    @test_throws Parquet.FormatError Parquet._decimalfrombytes(
        Int32, UInt8[0xfe, 0xff, 0xff, 0xff, 0xff], "value")
    # Trimming may not flip the sign of the retained head byte.
    @test_throws Parquet.FormatError Parquet._decimalfrombytes(
        Int32, UInt8[0x00, 0x80, 0x00, 0x00, 0x00], "value")
    @test_throws Parquet.FormatError Parquet._decimalfrombytes(
        Int32, UInt8[0xff, 0x7f, 0xff, 0xff, 0xff], "value")
    @test_throws Parquet.FormatError Parquet._decimalfrombytes(
        Int32, UInt8[], "value")

    noncontiguous = @view UInt8[0x00, 0xaa, 0x80, 0xbb][1:2:4]
    @test Parquet._decimalfrombytes(Int32, noncontiguous, "value") == Int32(128)
end

@testset "DECIMAL two's-complement widths" begin
    for (value, width) in ((-129, 2), (-128, 1), (-1, 1), (0, 1), (127, 1), (128, 2),
            (32767, 2), (-32768, 2), (32768, 3), (-32769, 3))
        for T in (Int32, Int64, Int128, DINT256)
            @test Parquet._twoscomplementwidth(T(value)) == width
        end
    end
    for T in (Int32, Int64, Int128, DINT256)
        @test Parquet._twoscomplementwidth(typemax(T)) == sizeof(T)
        @test Parquet._twoscomplementwidth(typemin(T)) == sizeof(T)
    end
    @test_throws ArgumentError Parquet._decimalbytes(Int32(128), 1)
    @test_throws ArgumentError Parquet._decimalbytes(Int32(-129), 1)
    @test_throws ArgumentError Parquet._decimalbytes(Int32(1), 0)
end

@testset "DECIMAL exact values" begin
    element = decimaltestelement("value", DMD.Type.BYTE_ARRAY, 20, 4)
    D = Decimal{20,4,Int128}
    cases = (
        (UInt8[0x00], 0),
        (UInt8[0x7f], 127),
        (UInt8[0x00, 0x80], 128),
        (UInt8[0x80], -128),
        (UInt8[0xff, 0x7f], -129),
        (UInt8[0xff], -1),
    )
    for (bytes, unscaled) in cases
        value = Parquet._fromparquetdecimal(element, bytes, Parquet.Limits())
        @test value === reinterpret(D, Int128(unscaled))
        @test Parquet._toparquetdecimal(element, value, Parquet.Limits()) == bytes
    end
end

@testset "DECIMAL physical round trips" begin
    int32 = decimaltestelement("int32", DMD.Type.INT32, 9, 2)
    int64 = decimaltestelement("int64", DMD.Type.INT64, 18, 6)
    fixed = decimaltestelement("fixed", DMD.Type.FIXED_LEN_BYTE_ARRAY, 9, 2; width=4)
    variable = decimaltestelement("variable", DMD.Type.BYTE_ARRAY, 9, 2)
    for (element, raw, expected) in (
            (int32, Int32(-12345), reinterpret(Decimal{9,2,Int32}, Int32(-12345))),
            (int64, Int64(123456789012345678),
                reinterpret(Decimal{18,6,Int64}, Int64(123456789012345678))),
            (fixed, UInt8[0xff, 0xff, 0xcf, 0xc7],
                reinterpret(Decimal{9,2,Int32}, Int32(-12345))),
            (variable, UInt8[0xcf, 0xc7],
                reinterpret(Decimal{9,2,Int32}, Int32(-12345))))
        decoded = Parquet._fromparquetdecimal(element, raw, Parquet.Limits())
        @test decoded === expected
        @test Parquet._toparquetdecimal(element, decoded, Parquet.Limits()) == raw
    end
    @test Parquet._toparquetdecimal(fixed,
        reinterpret(Decimal{9,2,Int32}, Int32(128)), Parquet.Limits()) ==
        UInt8[0x00, 0x00, 0x00, 0x80]
    # A narrower physical integer than the precision tier still decodes exactly.
    narrow = decimaltestelement("narrow", DMD.Type.INT64, 5, 2)
    @test Parquet._fromparquetdecimal(narrow, Int64(-99999), Parquet.Limits()) ===
        reinterpret(Decimal{5,2,Int32}, Int32(-99999))
    @test_throws Parquet.FormatError Parquet._fromparquetdecimal(
        narrow, Int64(100000), Parquet.Limits())
    @test_throws ArgumentError Parquet._toparquetdecimal(
        int32, reinterpret(Decimal{9,3,Int32}, Int32(1)), Parquet.Limits())
    @test_throws ArgumentError Parquet._toparquetdecimal(
        int32, reinterpret(Decimal{18,2,Int64}, Int64(1_000_000_000)),
        Parquet.Limits())
    @test_throws ArgumentError Parquet._toparquetdecimal(int32, 1, Parquet.Limits())
    @test_throws Parquet.FormatError Parquet._fromparquetdecimal(
        fixed, UInt8[0x01], Parquet.Limits())
    @test_throws Parquet.FormatError Parquet._fromparquetdecimal(
        int32, Int64(1), Parquet.Limits())
    @test_throws Parquet.FormatError Parquet._fromparquetdecimal(
        int32, Int32(1_000_000_000), Parquet.Limits())
end

@testset "DECIMAL scale extremes" begin
    zeroscale = decimaltestelement("zero", DMD.Type.INT32, 9, 0)
    fullscale = decimaltestelement("full", DMD.Type.INT32, 9, 9)
    @test Parquet._fromparquetdecimal(zeroscale, Int32(123), Parquet.Limits()) ===
        reinterpret(Decimal{9,0,Int32}, Int32(123))
    @test Parquet._fromparquetdecimal(fullscale, Int32(-1), Parquet.Limits()) ===
        reinterpret(Decimal{9,9,Int32}, Int32(-1))
    wide = decimaltestelement("wide", DMD.Type.FIXED_LEN_BYTE_ARRAY, 76, 76;
        width=32)
    value = Parquet._fromparquetdecimal(wide, fill(0xff, 32), Parquet.Limits())
    @test value === reinterpret(Decimal{76,76,DINT256}, DINT256(-1))
    @test Parquet._toparquetdecimal(wide, value, Parquet.Limits()) == fill(0xff, 32)
end

@testset "DECIMAL precision bounds" begin
    for (precision, storage) in ((9, Int32), (18, Int64), (38, Int128), (76, DINT256))
        limit = storage(big(10)^precision - 1)
        @test Parquet._decimalmagnitudelimit(storage, precision) == limit
        Parquet._checkdecimalvalue(limit, precision, "value", ArgumentError)
        Parquet._checkdecimalvalue(-limit, precision, "value", ArgumentError)
        @test_throws ArgumentError Parquet._checkdecimalvalue(
            limit + one(storage), precision, "value", ArgumentError)
        @test_throws ArgumentError Parquet._checkdecimalvalue(
            -limit - one(storage), precision, "value", ArgumentError)
    end
    # A declared precision wider than the storage tier can never be exceeded.
    Parquet._checkdecimalvalue(typemax(Int32), 20, "value", ArgumentError)
    Parquet._checkdecimalvalue(typemin(Int64), 25, "value", ArgumentError)
    @test Parquet._decimaldigits(0) == 1
    @test Parquet._decimaldigits(typemin(Int128)) == 39
end

@testset "DECIMAL bulk column conversion" begin
    element = decimaltestelement("value", DMD.Type.FIXED_LEN_BYTE_ARRAY, 20, 3;
        width=9)
    D = Decimal{20,3,Int128}
    unscaled = Int128[-1, 0, 1, big(10)^19, -big(10)^19]
    raw = Vector{UInt8}[decimalreference(value, 9) for value in unscaled]
    logical = Parquet._logicalvalues(element, raw)
    @test logical isa Vector{D}
    @test logical == [reinterpret(D, value) for value in unscaled]
    @test Parquet._physicalvalues(element, logical) == raw

    optional = Vector{Union{Missing,Vector{UInt8}}}(raw)
    optional[2] = missing
    decoded = Parquet._logicalvalues(element, optional)
    @test decoded isa Vector{Union{Missing,D}}
    @test ismissing(decoded[2])
    @test decoded[1] === reinterpret(D, Int128(-1))
    roundtrip = Parquet._physicalvalues(element, decoded)
    @test ismissing(roundtrip[2])
    @test roundtrip[1] == raw[1]

    @test isempty(Parquet._logicalvalues(element, Vector{UInt8}[]))
    @test Parquet._logicalvalues(element, Vector{UInt8}[]) isa Vector{D}
end

@testset "DECIMAL resource bounds" begin
    element = decimaltestelement("value", DMD.Type.BYTE_ARRAY, 20, 2)
    D = Decimal{20,2,Int128}
    limits = Parquet.Limits(max_string_bytes=1)
    @test_throws Parquet.LimitError Parquet._fromparquetdecimal(
        element, UInt8[0x00, 0x01], limits)
    @test_throws Parquet.LimitError Parquet._toparquetdecimal(
        element, reinterpret(D, Int128(128)), limits)

    @test Parquet.Limits().max_decimal_bytes == 1024 * 1024
    decimal_limits = Parquet.Limits(max_decimal_bytes=4)
    decode_error = try
        Parquet._fromparquetdecimal(element, UInt8[0x00, 0x00, 0x00, 0x00, 0x01],
            decimal_limits)
        nothing
    catch err
        err
    end
    @test decode_error isa Parquet.LimitError
    @test decode_error.resource == :decimal_bytes
    @test decode_error.requested == 5
    @test decode_error.maximum == 4

    large = reinterpret(D, Int128(1) << 31)
    encode_error = try
        Parquet._toparquetdecimal(element, large, decimal_limits)
        nothing
    catch err
        err
    end
    @test encode_error isa Parquet.LimitError
    @test encode_error.resource == :decimal_bytes
    @test encode_error.requested == 5
    @test encode_error.maximum == 4

    fixed = decimaltestelement("fixed", DMD.Type.FIXED_LEN_BYTE_ARRAY, 11, 2;
        width=5)
    @test_throws Parquet.LimitError Parquet._toparquetdecimal(
        fixed, reinterpret(Decimal{11,2,Int64}, Int64(1)), decimal_limits)
    exact = Parquet.Limits(max_decimal_bytes=2, max_string_bytes=2)
    @test Parquet._fromparquetdecimal(element, UInt8[0x00, 0x80], exact) ===
        reinterpret(D, Int128(128))
    @test Parquet._toparquetdecimal(element, reinterpret(D, Int128(128)), exact) ==
        UInt8[0x00, 0x80]

    oversized = fill(UInt8(0xff), 64 * 1024)
    rejection_time = @elapsed try
        Parquet._fromparquetdecimal(element, oversized,
            Parquet.Limits(max_decimal_bytes=16))
    catch err
        err isa Parquet.LimitError || rethrow()
    end
    @test rejection_time < 1.0
end

@testset "DECIMAL fixed-width conversion preflight" begin
    element = decimaltestelement("fixed", DMD.Type.FIXED_LEN_BYTE_ARRAY, 19, 2;
        width=9)
    D = Decimal{19,2,Int128}
    limits = Parquet.Limits(max_decimal_bytes=8)
    readvalues = DecimalConversionProbe{Vector{UInt8}}(1_000_000)
    writevalues = DecimalConversionProbe{D}(1_000_000)

    decimalreadallocation(element,
        DecimalConversionProbe{Vector{UInt8}}(1), limits)
    decimalwriteallocation(element, DecimalConversionProbe{D}(1), limits)
    GC.gc()
    readerror, readallocated = decimalreadallocation(element, readvalues, limits)
    writeerror, writeallocated = decimalwriteallocation(element, writevalues, limits)
    for error in (readerror, writeerror)
        @test error isa Parquet.LimitError
        @test error.resource == :decimal_bytes
        @test error.requested == 9
        @test error.maximum == 8
    end
    @test readallocated < 100_000
    @test writeallocated < 100_000

    stringlimits = Parquet.Limits(max_decimal_bytes=9, max_string_bytes=8)
    readerror, readallocated = decimalreadallocation(element, readvalues,
        stringlimits)
    writeerror, writeallocated = decimalwriteallocation(element, writevalues,
        stringlimits)
    for error in (readerror, writeerror)
        @test error isa Parquet.LimitError
        @test error.resource == :string_bytes
        @test error.requested == 9
        @test error.maximum == 8
    end
    @test readallocated < 100_000
    @test writeallocated < 100_000

    exactlimits = Parquet.Limits(max_decimal_bytes=9, max_string_bytes=9)
    raw = UInt8[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01]
    logical = Parquet._logicalvalues(element, Vector{UInt8}[raw]; limits=exactlimits)
    @test logical == D[reinterpret(D, Int128(1))]
    @test Parquet._physicalvalues(element, logical; limits=exactlimits) ==
        Vector{UInt8}[raw]
end

@testset "DECIMAL conversion allocations" begin
    # Wire conversion is machine integer arithmetic: no per-value heap object.
    bytes = fill(0xa5, 16)
    Parquet._decimalfrombytes(Int128, bytes, "value")
    Parquet._twoscomplementwidth(Int128(-129))
    GC.gc()
    @test (@allocated Parquet._decimalfrombytes(Int128, bytes, "value")) == 0
    @test (@allocated Parquet._twoscomplementwidth(Int128(-129))) == 0

    element = decimaltestelement("value", DMD.Type.INT64, 18, 4)
    D = Decimal64{4}
    raw = Int64[index for index in 1:1024]
    Parquet._logicalvalues(element, raw)
    GC.gc()
    allocated = @allocated Parquet._logicalvalues(element, raw)
    # One dense output array, not one object per value.
    @test allocated < 8 * length(raw) + 4096
    @test Parquet._logicalvalues(element, raw) isa Vector{D}
end
