using Test

const DMD = Parquet.Metadata

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

@testset "DECIMAL metadata validation" begin
    for (physical, precision, width) in (
            (DMD.Type.INT32, 9, nothing),
            (DMD.Type.INT64, 18, nothing),
            (DMD.Type.BYTE_ARRAY, 100, nothing),
            (DMD.Type.FIXED_LEN_BYTE_ARRAY, 9, 4))
        element = decimaltestelement("value", physical, precision, 2; width=width)
        @test Parquet._decimallogicalkind(element) === :decimal
        @test Parquet._decimallogicaleltype(:decimal) === Parquet.Decimal
    end
    legacy = decimaltestelement("legacy", DMD.Type.INT64, 18, 4; modern=false)
    @test Parquet._decimallogicalkind(legacy) === :decimal
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
end

@testset "DECIMAL exact values" begin
    element = decimaltestelement("value", DMD.Type.BYTE_ARRAY, 20, 4)
    cases = (
        (UInt8[0x00], big(0)),
        (UInt8[0x7f], big(127)),
        (UInt8[0x00, 0x80], big(128)),
        (UInt8[0x80], big(-128)),
        (UInt8[0xff, 0x7f], big(-129)),
        (UInt8[0xff], big(-1)),
    )
    for (bytes, unscaled) in cases
        value = Parquet._fromparquetdecimal(element, bytes, Parquet.Limits())
        @test value == Parquet.Decimal(unscaled, 4)
        @test Parquet._toparquetdecimal(element, value, Parquet.Limits()) == bytes
    end
    for (unscaled, width) in (
            (big(-129), 2),
            (big(-128), 1),
            (big(-1), 1),
            (big(0), 1),
            (big(127), 1),
            (big(128), 2),
        )
        @test Parquet._twoscomplementwidth(unscaled) == width
    end
    value = Parquet.Decimal(big"12345678901234567890", 4)
    @test copy(value) == value
    @test isequal(copy(value), value)
    @test hash(copy(value)) == hash(value)
    @test sprint(show, value) == "Parquet.Decimal(12345678901234567890, 4)"
    @test_throws ArgumentError Parquet.Decimal(1, -1)
end

@testset "DECIMAL bulk two's-complement conversion" begin
    for width in (1, 2, 3, 8, 17, 256, 4096)
        bits = 8 * width
        minimum = -(big(1) << (bits - 1))
        maximum = (big(1) << (bits - 1)) - 1
        values = (minimum, minimum + 1, big(-1), big(0), big(1),
            maximum - 1, maximum)
        for value in values
            encoded = Parquet._totwoscomplement(value, width)
            @test length(encoded) == width
            @test Parquet._fromtwoscomplement(encoded) == value
        end
        @test_throws ArgumentError Parquet._totwoscomplement(minimum - 1, width)
        @test_throws ArgumentError Parquet._totwoscomplement(maximum + 1, width)
    end

    storage = UInt8[0x00, 0xaa, 0x80, 0xbb]
    noncontiguous = @view storage[1:2:4]
    @test Parquet._fromtwoscomplement(noncontiguous) == 128
    @test storage == UInt8[0x00, 0xaa, 0x80, 0xbb]
    @test_throws Parquet.FormatError Parquet._fromtwoscomplement(UInt8[])
    @test Parquet._totwoscomplement(big(-1), 8) == fill(UInt8(0xff), 8)
    @test Parquet._totwoscomplement(big(128), 8) ==
        UInt8[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80]
end

@testset "DECIMAL physical round trips" begin
    int32 = decimaltestelement("int32", DMD.Type.INT32, 9, 2)
    int64 = decimaltestelement("int64", DMD.Type.INT64, 18, 6)
    fixed = decimaltestelement("fixed", DMD.Type.FIXED_LEN_BYTE_ARRAY, 9, 2; width=4)
    for (element, raw, expected) in (
            (int32, Int32(-12345), Parquet.Decimal(-12345, 2)),
            (int64, Int64(123456789012345678),
                Parquet.Decimal(123456789012345678, 6)),
            (fixed, UInt8[0xff, 0xff, 0xcf, 0xc7], Parquet.Decimal(-12345, 2)))
        decoded = Parquet._fromparquetdecimal(element, raw, Parquet.Limits())
        @test decoded == expected
        @test Parquet._toparquetdecimal(element, decoded, Parquet.Limits()) == raw
    end
    @test Parquet._toparquetdecimal(fixed, Parquet.Decimal(128, 2), Parquet.Limits()) ==
        UInt8[0x00, 0x00, 0x00, 0x80]
    @test_throws ArgumentError Parquet._toparquetdecimal(
        int32, Parquet.Decimal(1, 3), Parquet.Limits())
    @test_throws ArgumentError Parquet._toparquetdecimal(
        int32, Parquet.Decimal(1_000_000_000, 2), Parquet.Limits())
    @test_throws Parquet.FormatError Parquet._fromparquetdecimal(
        fixed, UInt8[0x01], Parquet.Limits())
    @test_throws Parquet.FormatError Parquet._fromparquetdecimal(
        int32, Int64(1), Parquet.Limits())
end

@testset "DECIMAL resource bounds" begin
    element = decimaltestelement("value", DMD.Type.BYTE_ARRAY, 20, 2)
    limits = Parquet.Limits(max_string_bytes=1)
    @test_throws Parquet.LimitError Parquet._fromparquetdecimal(
        element, UInt8[0x00, 0x01], limits)
    @test_throws Parquet.LimitError Parquet._toparquetdecimal(
        element, Parquet.Decimal(128, 2), limits)

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

    large = Parquet.Decimal(big(1) << 31, 2)
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
        fixed, Parquet.Decimal(1, 2), decimal_limits)
    exact = Parquet.Limits(max_decimal_bytes=2, max_string_bytes=2)
    @test Parquet._fromparquetdecimal(element, UInt8[0x00, 0x80], exact) ==
        Parquet.Decimal(128, 2)
    @test Parquet._toparquetdecimal(element, Parquet.Decimal(128, 2), exact) ==
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
    limits = Parquet.Limits(max_decimal_bytes=8)
    readvalues = DecimalConversionProbe{Vector{UInt8}}(1_000_000)
    writevalues = DecimalConversionProbe{Parquet.Decimal}(1_000_000)

    decimalreadallocation(element,
        DecimalConversionProbe{Vector{UInt8}}(1), limits)
    decimalwriteallocation(element,
        DecimalConversionProbe{Parquet.Decimal}(1), limits)
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
    @test logical == Parquet.Decimal[Parquet.Decimal(1, 2)]
    @test Parquet._physicalvalues(element, logical; limits=exactlimits) ==
        Vector{UInt8}[raw]
end

@testset "DECIMAL practical linear scaling" begin
    smallwidth = 4 * 1024
    largewidth = 16 * 1024
    smallbytes = fill(UInt8(0x55), smallwidth)
    largebytes = fill(UInt8(0x55), largewidth)
    smallvalue = (big(1) << (8 * smallwidth - 2)) + 123
    largevalue = (big(1) << (8 * largewidth - 2)) + 123
    Parquet._fromtwoscomplement(smallbytes)
    Parquet._totwoscomplement(smallvalue, smallwidth)
    GC.gc()
    smalldecode = @allocated Parquet._fromtwoscomplement(smallbytes)
    largedecode = @allocated Parquet._fromtwoscomplement(largebytes)
    smallencode = @allocated Parquet._totwoscomplement(smallvalue, smallwidth)
    largeencode = @allocated Parquet._totwoscomplement(largevalue, largewidth)
    @test largedecode <= 5 * smalldecode
    @test largeencode <= 5 * smallencode

    negative = -largevalue
    Parquet._twoscomplementwidth(negative)
    Parquet._totwoscomplement(negative, largewidth)
    GC.gc()
    widthallocated = @allocated Parquet._twoscomplementwidth(negative)
    negativeallocated = @allocated Parquet._totwoscomplement(
        negative, largewidth)
    @test widthallocated < 10_000
    @test negativeallocated <= largeencode + 100_000

    practicalwidth = 256 * 1024
    practicalbytes = fill(UInt8(0x55), practicalwidth)
    practicalexpected = Parquet._fromtwoscomplement(practicalbytes)
    practicalvalue = (big(1) << (8 * practicalwidth - 2)) + 123
    elapsed = @elapsed begin
        @test Parquet._fromtwoscomplement(practicalbytes) == practicalexpected
        @test Parquet._fromtwoscomplement(
            Parquet._totwoscomplement(practicalvalue, practicalwidth)) == practicalvalue
    end
    @test elapsed < 5.0
end
