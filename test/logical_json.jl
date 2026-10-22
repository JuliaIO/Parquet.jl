function jsonbytes(value::AbstractString)
    return collect(codeunits(value))
end

function jsonlogicalelement()
    return Parquet.Metadata.SchemaElement(name="json",
        type_=Parquet.Metadata.Type.BYTE_ARRAY,
        repetition_type=Parquet.Metadata.FieldRepetitionType.OPTIONAL,
        logicalType=Parquet.Metadata.LogicalType(JSON=Parquet.Metadata.JsonType()))
end

@noinline function jsonwritehitslimit(element, value, limits)
    try
        Parquet._binaryphysicalvalue(element, value; limits=limits)
    catch err
        err isa Parquet.LimitError || rethrow()
        return true
    end
    return false
end

@testset "JSON complete syntax validation" begin
    valid = (
        "null",
        "true",
        "false",
        "0",
        "-0",
        "1234567890",
        "-12.5e+10",
        "1E-2",
        "\"\"",
        "\"escape: \\\" \\\\ \\/ \\b \\f \\n \\r \\t\"",
        "\"raw UTF-8: λ\"",
        "\"\\u03bb\"",
        "\"\\ud834\\udd1e\"",
        "\"\\ud800\"",
        "\"\\ud800\\u0041\"",
        "\"\\udc00\"",
        "[]",
        "{}",
        "[null,true,false,0,\"x\",[],{}]",
        "{\"a\":1,\"b\":[2,3],\"c\":{\"d\":false}}",
        " \t\r\n { \"duplicate\": 1, \"duplicate\": 2 } \n",
    )
    for document in valid
        bytes = jsonbytes(document)
        @test Parquet._validatejson(bytes, Parquet.Limits(),
            Parquet.FormatError) === nothing
        @test Parquet.JSONValue(bytes).bytes == bytes
    end

    invalid = (
        "",
        "   ",
        "nil",
        "tru",
        "falsee",
        "+1",
        "01",
        "-01",
        "1.",
        ".1",
        "1e",
        "1e+",
        "NaN",
        "Infinity",
        "null null",
        "[",
        "[1,]",
        "[,1]",
        "[1 2]",
        "{",
        "{\"a\"}",
        "{\"a\":}",
        "{\"a\":1,}",
        "{a:1}",
        "\"unterminated",
        "\"bad \\x escape\"",
        "\"bad \\u12xz escape\"",
        "\"line\nfeed\"",
    )
    for document in invalid
        bytes = jsonbytes(document)
        @test_throws Parquet.FormatError Parquet._validatejson(
            bytes, Parquet.Limits(), Parquet.FormatError)
        @test_throws ArgumentError Parquet.JSONValue(bytes)
    end
    @test_throws Parquet.FormatError Parquet._validatejson(
        UInt8[0x22, 0xff, 0x22], Parquet.Limits(), Parquet.FormatError)
    @test_throws Parquet.FormatError Parquet._validatejson(
        UInt8[0xef, 0xbb, 0xbf, 0x6e, 0x75, 0x6c, 0x6c],
        Parquet.Limits(), Parquet.FormatError)
end

@testset "JSON read, write, mutation, and limits" begin
    element = jsonlogicalelement()
    bytes = jsonbytes("{\"nested\":[1,{\"ok\":true}]}")
    value = Parquet._binarylogicalvalue(element, bytes)
    @test value == Parquet.JSONValue(bytes)
    @test value.bytes isa Base.CodeUnits{UInt8,String}
    @test Parquet._binaryphysicalvalue(element, value) == bytes
    originalhash = hash(value)
    @test_throws CanonicalIndexError setindex!(value.bytes, 0x5d, length(value.bytes))
    @test hash(value) == originalhash
    unchecked = Parquet.JSONValue(jsonbytes("{"), Val(:validated))
    @test_throws ArgumentError Parquet._binaryphysicalvalue(element, unchecked)
    @test_throws Parquet.FormatError Parquet._binarylogicalvalue(
        element, jsonbytes("{\"broken\":]"))

    depthtwo = jsonbytes("[[0]]")
    depththree = jsonbytes("[[[0]]]")
    depthlimit = Parquet.Limits(max_metadata_depth=2)
    @test Parquet._binarylogicalvalue(element, depthtwo; limits=depthlimit) isa
        Parquet.JSONValue
    @test_throws Parquet.LimitError Parquet._binarylogicalvalue(
        element, depththree; limits=depthlimit)
    @test_throws Parquet.LimitError Parquet._validatejson(
        depththree, depthlimit, ArgumentError)

    oneitem = Parquet.Limits(max_container_elements=1)
    @test Parquet._binarylogicalvalue(element, jsonbytes("[1]"); limits=oneitem) isa
        Parquet.JSONValue
    @test_throws Parquet.LimitError Parquet._binarylogicalvalue(
        element, jsonbytes("[1,2]"); limits=oneitem)
    @test_throws Parquet.LimitError Parquet._binarylogicalvalue(
        element, jsonbytes("{\"a\":1,\"b\":2}"); limits=oneitem)
    @test_throws Parquet.LimitError Parquet._binarylogicalvalue(
        element, jsonbytes("null"); limits=Parquet.Limits(max_string_bytes=3))
    @test_throws Parquet.LimitError Parquet.JSONValue(
        jsonbytes("null"); limits=Parquet.Limits(max_string_bytes=3))
    @test Parquet.JSONValue(jsonbytes("null");
        limits=Parquet.Limits(max_string_bytes=4)).bytes == jsonbytes("null")

    large = vcat(jsonbytes("[[\""), fill(UInt8(0x61), 256 * 1024),
        jsonbytes("\"]]"))
    largevalue = Parquet.JSONValue(large)
    earlydepth = Parquet.Limits(max_metadata_depth=1,
        max_string_bytes=length(large))
    @test jsonwritehitslimit(element, largevalue, earlydepth)
    allocations = @allocated jsonwritehitslimit(element, largevalue, earlydepth)
    @test allocations < length(large) ÷ 4
end
