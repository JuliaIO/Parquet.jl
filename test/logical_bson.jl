function bsonle32(value::Integer)
    raw = reinterpret(UInt32, Int32(value))
    return UInt8[UInt8(raw & 0xff), UInt8((raw >> 8) & 0xff),
        UInt8((raw >> 16) & 0xff), UInt8(raw >> 24)]
end

function bsonle64(value::UInt64)
    return UInt8[UInt8((value >> shift) & 0xff) for shift in 0:8:56]
end

function bsoncstring(value::AbstractString)
    bytes = collect(codeunits(value))
    push!(bytes, 0x00)
    return bytes
end

function bsonstring(value::AbstractString)
    bytes = collect(codeunits(value))
    return vcat(bsonle32(length(bytes) + 1), bytes, UInt8[0x00])
end

function bsonelement(type::Integer, key::AbstractString, payload=UInt8[])
    return vcat(UInt8[UInt8(type)], bsoncstring(key), payload)
end

function bsondocument(elements...)
    body = UInt8[]
    for element in elements
        append!(body, element)
    end
    return vcat(bsonle32(length(body) + 5), body, UInt8[0x00])
end

function bsonarray(count::Int)
    body = UInt8[]
    for index in 0:(count - 1)
        push!(body, 0x0a)
        append!(body, codeunits(string(index)))
        push!(body, 0x00)
    end
    return vcat(bsonle32(length(body) + 5), body, UInt8[0x00])
end

function bsonlogicalelement()
    return Parquet.Metadata.SchemaElement(name="bson",
        type_=Parquet.Metadata.Type.BYTE_ARRAY,
        repetition_type=Parquet.Metadata.FieldRepetitionType.OPTIONAL,
        logicalType=Parquet.Metadata.LogicalType(BSON=Parquet.Metadata.BsonType()))
end

@noinline function bsonwritehitslimit(element, value, limits)
    try
        Parquet._binaryphysicalvalue(element, value; limits=limits)
    catch err
        err isa Parquet.LimitError || rethrow()
        return true
    end
    return false
end

function representativebson()
    nested = bsondocument(bsonelement(0x0a, "null"))
    array = bsondocument(bsonelement(0x10, "0", bsonle32(1)),
        bsonelement(0x0a, "1"))
    binary = vcat(bsonle32(3), UInt8[0x00, 0x01, 0x02, 0x03])
    oldbinary = vcat(bsonle32(7), UInt8[0x02], bsonle32(3),
        UInt8[0x04, 0x05, 0x06])
    regex = vcat(bsoncstring("a.*"), bsoncstring("im"))
    scope = bsondocument(bsonelement(0x10, "x", bsonle32(1)))
    code = bsonstring("return x")
    codewithscope = vcat(bsonle32(4 + length(code) + length(scope)), code, scope)
    return bsondocument(
        bsonelement(0x01, "double", bsonle64(reinterpret(UInt64, 1.5))),
        bsonelement(0x02, "string", bsonstring("λ")),
        bsonelement(0x03, "document", nested),
        bsonelement(0x04, "array", array),
        bsonelement(0x05, "binary", binary),
        bsonelement(0x05, "oldbinary", oldbinary),
        bsonelement(0x06, "undefined"),
        bsonelement(0x07, "objectid", collect(UInt8, 1:12)),
        bsonelement(0x08, "boolean", UInt8[0x01]),
        bsonelement(0x09, "datetime", zeros(UInt8, 8)),
        bsonelement(0x0a, "null"),
        bsonelement(0x0b, "regex", regex),
        bsonelement(0x0c, "dbpointer", vcat(bsonstring("namespace"), zeros(UInt8, 12))),
        bsonelement(0x0d, "javascript", bsonstring("return 1")),
        bsonelement(0x0e, "symbol", bsonstring("symbol")),
        bsonelement(0x0f, "scope", codewithscope),
        bsonelement(0x10, "int32", bsonle32(-1)),
        bsonelement(0x11, "timestamp", zeros(UInt8, 8)),
        bsonelement(0x12, "int64", bsonle64(typemax(UInt64))),
        bsonelement(0x13, "decimal128", zeros(UInt8, 16)),
        bsonelement(0x7f, "maxkey"),
        bsonelement(0xff, "minkey"),
    )
end

@testset "BSON 1.1 element structure" begin
    bytes = representativebson()
    @test Parquet._validatebson(bytes, Parquet.Limits(),
        Parquet.FormatError) === nothing
    @test Parquet.BSONValue(bytes).bytes == bytes
    element = bsonlogicalelement()
    tagged = Parquet._binarylogicalvalue(element, bytes)
    @test tagged == Parquet.BSONValue(bytes)
    @test tagged.bytes isa Base.CodeUnits{UInt8,String}
    @test Parquet._binaryphysicalvalue(element, tagged) == bytes
    opaque = bsondocument(bsonelement(0x05, "binary",
        vcat(bsonle32(2), UInt8[0x00, 0xff, 0xfe])))
    @test collect(Parquet.BSONValue(opaque).bytes) == opaque
    for subtype in UInt8[0x09, 0x80, 0xff]
        binary = bsondocument(bsonelement(0x05, "binary",
            vcat(bsonle32(0), subtype)))
        @test Parquet._validatebson(binary, Parquet.Limits(),
            Parquet.FormatError) === nothing
    end
end

@testset "BSON malformed documents" begin
    empty = bsondocument()
    invalid = Vector{UInt8}[]
    push!(invalid, UInt8[])
    push!(invalid, UInt8[0x04, 0x00, 0x00, 0x00])
    push!(invalid, vcat(bsonle32(6), UInt8[0x00]))
    push!(invalid, vcat(bsonle32(5), UInt8[0x00, 0x00]))
    missingterminator = copy(empty)
    missingterminator[end] = 0x01
    push!(invalid, missingterminator)
    oversized = copy(empty)
    oversized[1:4] = bsonle32(6)
    push!(invalid, oversized)
    push!(invalid, bsondocument(UInt8[0x00]))
    push!(invalid, bsondocument(UInt8[0x20, 0x00]))
    push!(invalid, bsondocument(UInt8[0x0a, 0x61]))
    push!(invalid, bsondocument(UInt8[0x0a, 0xff, 0x00]))
    push!(invalid, bsondocument(bsonelement(0x01, "double", zeros(UInt8, 7))))
    push!(invalid, bsondocument(bsonelement(0x08, "bool", UInt8[0x02])))
    push!(invalid, bsondocument(bsonelement(0x02, "string", bsonle32(0))))
    push!(invalid, bsondocument(bsonelement(0x02, "string",
        vcat(bsonle32(2), UInt8[0x61, 0x62]))))
    push!(invalid, bsondocument(bsonelement(0x02, "string",
        vcat(bsonle32(2), UInt8[0xff, 0x00]))))
    push!(invalid, bsondocument(bsonelement(0x05, "binary",
        vcat(bsonle32(-1), UInt8[0x00]))))
    push!(invalid, bsondocument(bsonelement(0x05, "binary",
        vcat(bsonle32(0), UInt8[0x0a]))))
    push!(invalid, bsondocument(bsonelement(0x05, "binary",
        vcat(bsonle32(0), UInt8[0x7f]))))
    push!(invalid, bsondocument(bsonelement(0x05, "oldbinary",
        vcat(bsonle32(7), UInt8[0x02], bsonle32(2), UInt8[1, 2, 3]))))
    push!(invalid, bsondocument(bsonelement(0x0b, "regex",
        vcat(bsoncstring("a"), bsoncstring("mi")))))
    push!(invalid, bsondocument(bsonelement(0x0b, "regex",
        vcat(bsoncstring("a"), bsoncstring("q")))))
    badarray = bsondocument(bsonelement(0x10, "1", bsonle32(1)))
    push!(invalid, bsondocument(bsonelement(0x04, "array", badarray)))
    leadingzeroarray = bsondocument(bsonelement(0x0a, "00"))
    push!(invalid, bsondocument(bsonelement(0x04, "array", leadingzeroarray)))
    nondigitarray = bsondocument(bsonelement(0x0a, "x"))
    push!(invalid, bsondocument(bsonelement(0x04, "array", nondigitarray)))
    overflowarray = bsondocument(bsonelement(0x0a, repeat("9", 32)))
    push!(invalid, bsondocument(bsonelement(0x04, "array", overflowarray)))
    badnested = copy(empty)
    badnested[1:4] = bsonle32(6)
    push!(invalid, bsondocument(bsonelement(0x03, "nested", badnested)))
    scope = bsondocument()
    code = bsonstring("x")
    badscope = vcat(bsonle32(4 + length(code) + length(scope) + 1), code, scope)
    push!(invalid, bsondocument(bsonelement(0x0f, "scope", badscope)))
    for bytes in invalid
        @test_throws Parquet.FormatError Parquet._validatebson(
            bytes, Parquet.Limits(), Parquet.FormatError)
        @test_throws ArgumentError Parquet.BSONValue(bytes)
    end
end

@testset "BSON read, write, mutation, and limits" begin
    element = bsonlogicalelement()
    bytes = representativebson()
    value = Parquet._binarylogicalvalue(element, bytes)
    originalhash = hash(value)
    @test_throws CanonicalIndexError setindex!(value.bytes, 0x01, length(value.bytes))
    @test hash(value) == originalhash
    unchecked = Parquet.BSONValue(UInt8[0x05, 0x00, 0x00, 0x00, 0x01],
        Val(:validated))
    @test_throws ArgumentError Parquet._binaryphysicalvalue(element, unchecked)
    malformed = copy(bytes)
    malformed[1:4] = bsonle32(length(bytes) - 1)
    @test_throws Parquet.FormatError Parquet._binarylogicalvalue(element, malformed)

    depthtwo = bsondocument(bsonelement(0x03, "child", bsondocument()))
    depththree = bsondocument(bsonelement(0x03, "child",
        bsondocument(bsonelement(0x03, "child", bsondocument()))))
    depthlimit = Parquet.Limits(max_metadata_depth=2)
    @test Parquet._binarylogicalvalue(element, depthtwo; limits=depthlimit) isa
        Parquet.BSONValue
    @test_throws Parquet.LimitError Parquet._binarylogicalvalue(
        element, depththree; limits=depthlimit)

    oneitem = Parquet.Limits(max_container_elements=1)
    @test Parquet._binarylogicalvalue(element,
        bsondocument(bsonelement(0x0a, "a")); limits=oneitem) isa Parquet.BSONValue
    @test_throws Parquet.LimitError Parquet._binarylogicalvalue(element,
        bsondocument(bsonelement(0x0a, "a"), bsonelement(0x0a, "b"));
        limits=oneitem)
    @test_throws Parquet.LimitError Parquet._binarylogicalvalue(
        element, bsondocument(); limits=Parquet.Limits(max_string_bytes=4))
    @test_throws Parquet.LimitError Parquet.BSONValue(
        bsondocument(); limits=Parquet.Limits(max_string_bytes=4))
    @test Parquet.BSONValue(bsondocument();
        limits=Parquet.Limits(max_string_bytes=5)).bytes == bsondocument()

    largepayload = fill(UInt8(0xff), 256 * 1024)
    binary = vcat(bsonle32(length(largepayload)), UInt8[0x00], largepayload)
    large = bsondocument(bsonelement(0x0a, "first"),
        bsonelement(0x05, "second", binary))
    largevalue = Parquet.BSONValue(large)
    earlyelement = Parquet.Limits(max_container_elements=1,
        max_string_bytes=length(large))
    @test bsonwritehitslimit(element, largevalue, earlyelement)
    allocations = @allocated bsonwritehitslimit(element, largevalue, earlyelement)
    @test allocations < length(large) ÷ 4
end

@testset "BSON array key allocation" begin
    count = 4096
    array = bsondocument(bsonelement(0x04, "array", bsonarray(count)))
    limits = Parquet.Limits(max_container_elements=count + 1)
    Parquet._validatebson(array, limits, Parquet.FormatError)
    allocations = @allocated Parquet._validatebson(array, limits,
        Parquet.FormatError)
    @test allocations < count
end
