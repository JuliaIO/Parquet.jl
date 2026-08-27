using Dates
using Test
using UUIDs

function rewritelogicalschema(replacement::Function, bytes::Vector{UInt8},
    index::Int=2)
    file = Parquet.File(bytes)
    metadata = Parquet.Thrift.decode(file.footer.bytes, Parquet.Metadata.FileMetaData)
    prefix = bytes[1:Int(file.footer.offset)]
    close(file)
    schema = copy(metadata.schema)
    schema[index] = replacement(schema[index])
    updated = Parquet.Metadata.FileMetaData(
        version=metadata.version,
        schema=schema,
        num_rows=metadata.num_rows,
        row_groups=metadata.row_groups,
        key_value_metadata=metadata.key_value_metadata,
        created_by=metadata.created_by,
        column_orders=metadata.column_orders,
        encryption_algorithm=metadata.encryption_algorithm,
        footer_signing_key_metadata=metadata.footer_signing_key_metadata,
        unknown_fields=metadata.unknown_fields,
    )
    footer = Parquet.Thrift.encode(updated)
    output = vcat(prefix, footer)
    Parquet._writelittle!(output, UInt32(length(footer)))
    append!(output, Parquet.PARQUET_MAGIC)
    return output
end

function withlogical(element::Parquet.Metadata.SchemaElement; logical,
    converted=element.converted_type, precision=element.precision,
    scale=element.scale)
    return Parquet.Metadata.SchemaElement(
        type_=element.type_,
        type_length=element.type_length,
        repetition_type=element.repetition_type,
        name=element.name,
        num_children=element.num_children,
        converted_type=converted,
        scale=scale,
        precision=precision,
        field_id=element.field_id,
        logicalType=logical,
        unknown_fields=element.unknown_fields,
    )
end

@testset "temporal and integer writer round trips" begin
    times = Union{Missing,Time}[
        Time(0), missing, Time(23, 59, 59, 999, 999, 999)]
    datetimes = DateTime[
        DateTime(1969, 12, 31, 23, 59, 59, 999),
        DateTime(1970, 1, 1),
        DateTime(2000, 2, 29, 12, 34, 56, 789),
    ]
    micros = Union{Missing,Parquet.Timestamp{:micros}}[
        Parquet.Timestamp(typemin(Int64), :micros, true),
        missing,
        Parquet.Timestamp(typemax(Int64), :micros, true),
    ]
    nanos = Parquet.Timestamp{:nanos}[
        Parquet.Timestamp(-1, :nanos, false),
        Parquet.Timestamp(0, :nanos, false),
        Parquet.Timestamp(1, :nanos, false),
    ]
    input = (
        i8=Int8[-128, 0, 127],
        u8=UInt8[0, 127, 255],
        i16=Int16[-32768, 0, 32767],
        u16=UInt16[0, 32767, 65535],
        u32=UInt32[0, 0x80000000, 0xffffffff],
        u64=UInt64[0, 0x8000000000000000, 0xffffffffffffffff],
        times=times,
        datetimes=datetimes,
        micros=micros,
        nanos=nanos,
    )
    for pageversion in (:v1, :v2)
        bytes = Parquet._encodefile(input; pageversion=pageversion,
            encoding=:delta_binary_packed)
        table = Parquet.Table(bytes)
        for name in keys(input)
            @test isequal(getproperty(table.columns, name), getproperty(input, name))
        end
        close(table)

        file = Parquet.File(bytes)
        metadata = Parquet.Thrift.decode(file.footer.bytes, Parquet.Metadata.FileMetaData)
        close(file)
        schema = metadata.schema
        @test all(element -> element.logicalType.INTEGER !== nothing, schema[2:7])
        @test schema[8].logicalType.TIME.unit.NANOS !== nothing
        @test !schema[8].logicalType.TIME.isAdjustedToUTC
        @test schema[8].converted_type === nothing
        @test schema[9].logicalType.TIMESTAMP.unit.MILLIS !== nothing
        @test !schema[9].logicalType.TIMESTAMP.isAdjustedToUTC
        @test schema[9].converted_type ==
            Parquet.Metadata.ConvertedType.TIMESTAMP_MILLIS
        @test schema[10].logicalType.TIMESTAMP.unit.MICROS !== nothing
        @test schema[10].logicalType.TIMESTAMP.isAdjustedToUTC
        @test schema[10].converted_type ==
            Parquet.Metadata.ConvertedType.TIMESTAMP_MICROS
        @test schema[11].logicalType.TIMESTAMP.unit.NANOS !== nothing
        @test !schema[11].logicalType.TIMESTAMP.isAdjustedToUTC
        @test schema[11].converted_type === nothing
    end

    @test_throws ArgumentError Parquet._encodefile(
        (value=Parquet.Timestamp{:micros}[],))
    @test_throws ArgumentError Parquet._encodefile((value=[
        Parquet.Timestamp(0, :nanos, true),
        Parquet.Timestamp(1, :nanos, false),
    ],))
end

@testset "binary logical writer round trips" begin
    uuids = Union{Missing,UUID}[
        UUID("00112233-4455-6677-8899-aabbccddeeff"),
        missing,
        UUID("ffffffff-ffff-ffff-ffff-ffffffffffff"),
    ]
    halves = Union{Missing,Float16}[
        reinterpret(Float16, UInt16(0x0000)),
        reinterpret(Float16, UInt16(0x8000)),
        reinterpret(Float16, UInt16(0x7e01)),
    ]
    json = Union{Missing,Parquet.JSONValue}[
        Parquet.JSONValue(codeunits("{\"a\":1}")),
        missing,
        Parquet.JSONValue(codeunits("[1,null,3]")),
    ]
    bson = Union{Missing,Parquet.BSONValue}[
        Parquet.BSONValue(hex2bytes("0c0000001061000100000000")),
        missing,
        Parquet.BSONValue(hex2bytes("090000000a00ff0000")),
    ]
    intervals = Union{Missing,Parquet.Interval}[
        Parquet.Interval(1, 2, 3),
        missing,
        Parquet.Interval(typemax(UInt32), 0, 86_400_000),
    ]
    nulls = Missing[missing, missing, missing]
    input = (; uuids, halves, json, bson, intervals, nulls)
    for pageversion in (:v1, :v2)
        bytes = Parquet._encodefile(input; pageversion=pageversion, codec=:zstd)
        table = Parquet.Table(bytes)
        @test isequal(table.columns.uuids, uuids)
        @test reinterpret(UInt16, collect(skipmissing(table.columns.halves))) ==
            reinterpret(UInt16, collect(skipmissing(halves)))
        @test isequal(table.columns.json, json)
        @test isequal(table.columns.bson, bson)
        @test isequal(table.columns.intervals, intervals)
        @test isequal(table.columns.nulls, nulls)
        close(table)

        file = Parquet.File(bytes)
        metadata = Parquet.Thrift.decode(file.footer.bytes, Parquet.Metadata.FileMetaData)
        close(file)
        schema = metadata.schema
        @test schema[2].logicalType.UUID !== nothing
        @test schema[2].type_length == 16
        @test schema[3].logicalType.FLOAT16 !== nothing
        @test schema[3].type_length == 2
        @test schema[4].logicalType.JSON !== nothing
        @test schema[4].converted_type == Parquet.Metadata.ConvertedType.JSON
        @test schema[5].logicalType.BSON !== nothing
        @test schema[5].converted_type == Parquet.Metadata.ConvertedType.BSON
        @test schema[6].logicalType === nothing
        @test schema[6].converted_type == Parquet.Metadata.ConvertedType.INTERVAL
        @test schema[6].type_length == 12
        @test schema[7].logicalType.UNKNOWN !== nothing
        @test schema[7].type_ == Parquet.Metadata.Type.INT32
    end
end

@testset "DECIMAL writer inference and round trips" begin
    small = Union{Missing,Parquet.Decimal}[
        Parquet.Decimal(12345, 2), missing, Parquet.Decimal(-99999, 2)]
    medium = Parquet.Decimal[
        Parquet.Decimal(123456789012345678, 6),
        Parquet.Decimal(-1, 6),
        Parquet.Decimal(0, 6),
    ]
    wide = Parquet.Decimal[
        Parquet.Decimal(big"12345678901234567890", 4),
        Parquet.Decimal(big"-12345678901234567890", 4),
        Parquet.Decimal(0, 4),
    ]
    input = (; small, medium, wide)
    for pageversion in (:v1, :v2)
        bytes = Parquet._encodefile(input; pageversion=pageversion,
            encoding=(small=:delta_binary_packed, medium=:delta_binary_packed,
                wide=:delta_byte_array))
        table = Parquet.Table(bytes)
        @test isequal(table.columns.small, small)
        @test table.columns.medium == medium
        @test table.columns.wide == wide
        close(table)

        file = Parquet.File(bytes)
        metadata = Parquet.Thrift.decode(file.footer.bytes, Parquet.Metadata.FileMetaData)
        close(file)
        @test [element.type_ for element in metadata.schema[2:end]] == [
            Parquet.Metadata.Type.INT32,
            Parquet.Metadata.Type.INT64,
            Parquet.Metadata.Type.FIXED_LEN_BYTE_ARRAY,
        ]
        @test [element.precision for element in metadata.schema[2:end]] ==
            Int32[5, 18, 20]
        @test [element.scale for element in metadata.schema[2:end]] ==
            Int32[2, 6, 4]
        @test all(element -> element.logicalType.DECIMAL !== nothing,
            metadata.schema[2:end])
        @test all(element -> element.converted_type ==
            Parquet.Metadata.ConvertedType.DECIMAL, metadata.schema[2:end])
    end

    @test_throws ArgumentError Parquet._encodefile(
        (value=Parquet.Decimal[],))
    @test_throws ArgumentError Parquet._encodefile(
        (value=Union{Missing,Parquet.Decimal}[missing],))
    @test_throws ArgumentError Parquet._encodefile(
        (value=[Parquet.Decimal(1, 1), Parquet.Decimal(1, 2)],))
end

@testset "schema-bearing Table exact logical rewrite" begin
    timestampbytes = Parquet._encodefile(
        (value=DateTime[DateTime(1970), DateTime(2000, 1, 1)],))
    timestampbytes = rewritelogicalschema(timestampbytes) do element
        logical = Parquet.Metadata.LogicalType(
            TIMESTAMP=Parquet.Metadata.TimestampType(
                isAdjustedToUTC=true,
                unit=Parquet.Metadata.TimeUnit(
                    MILLIS=Parquet.Metadata.MilliSeconds()),
            ),
        )
        return withlogical(element; logical=logical,
            converted=Parquet.Metadata.ConvertedType.TIME_MICROS)
    end
    table = Parquet.Table(timestampbytes)
    rewritten = Parquet._encodefile(table)
    close(table)
    file = Parquet.File(rewritten)
    metadata = Parquet.Thrift.decode(file.footer.bytes, Parquet.Metadata.FileMetaData)
    close(file)
    @test metadata.schema[2].logicalType.TIMESTAMP.isAdjustedToUTC
    @test metadata.schema[2].converted_type ==
        Parquet.Metadata.ConvertedType.TIME_MICROS

    timebytes = Parquet._encodefile(
        (value=Time[Time(Dates.Nanosecond(1_000))],))
    timebytes = rewritelogicalschema(timebytes) do element
        logical = Parquet.Metadata.LogicalType(
            TIME=Parquet.Metadata.TimeType(
                isAdjustedToUTC=true,
                unit=Parquet.Metadata.TimeUnit(
                    MICROS=Parquet.Metadata.MicroSeconds()),
            ),
        )
        return withlogical(element; logical=logical,
            converted=Parquet.Metadata.ConvertedType.TIME_MICROS)
    end
    table = Parquet.Table(timebytes)
    @test table.columns.value == [Time(Dates.Nanosecond(1_000_000))]
    rewritten = Parquet._encodefile(table)
    close(table)
    file = Parquet.File(rewritten)
    metadata = Parquet.Thrift.decode(file.footer.bytes, Parquet.Metadata.FileMetaData)
    close(file)
    @test metadata.schema[2].logicalType.TIME.unit.MICROS !== nothing
    @test metadata.schema[2].logicalType.TIME.isAdjustedToUTC
    @test metadata.schema[2].converted_type == Parquet.Metadata.ConvertedType.TIME_MICROS

    decimalbytes = Parquet._encodefile(
        (value=Parquet.Decimal[Parquet.Decimal(12345, 2)],))
    decimalbytes = rewritelogicalschema(decimalbytes) do element
        logical = Parquet.Metadata.LogicalType(
            DECIMAL=Parquet.Metadata.DecimalType(scale=Int32(2), precision=Int32(9)))
        return withlogical(element; logical=logical,
            converted=Parquet.Metadata.ConvertedType.DATE,
            precision=Int32(99), scale=Int32(99))
    end
    table = Parquet.Table(decimalbytes)
    rewritten = Parquet._encodefile(table)
    close(table)
    file = Parquet.File(rewritten)
    metadata = Parquet.Thrift.decode(file.footer.bytes, Parquet.Metadata.FileMetaData)
    close(file)
    @test metadata.schema[2].precision == 99
    @test metadata.schema[2].scale == 99
    @test metadata.schema[2].logicalType.DECIMAL.precision == 9
    @test metadata.schema[2].logicalType.DECIMAL.scale == 2
    @test metadata.schema[2].converted_type == Parquet.Metadata.ConvertedType.DATE

    enumbytes = Parquet._encodefile((value=["alpha", "beta"],))
    enumbytes = rewritelogicalschema(enumbytes) do element
        logical = Parquet.Metadata.LogicalType(ENUM=Parquet.Metadata.EnumType())
        return withlogical(element; logical=logical,
            converted=Parquet.Metadata.ConvertedType.UTF8)
    end
    table = Parquet.Table(enumbytes)
    @test table.columns.value == ["alpha", "beta"]
    rewritten = Parquet._encodefile(table)
    close(table)
    file = Parquet.File(rewritten)
    metadata = Parquet.Thrift.decode(file.footer.bytes, Parquet.Metadata.FileMetaData)
    close(file)
    @test metadata.schema[2].logicalType.ENUM !== nothing
    @test metadata.schema[2].converted_type == Parquet.Metadata.ConvertedType.UTF8

    legacystring = Parquet._encodefile((value=["legacy"],))
    legacystring = rewritelogicalschema(legacystring) do element
        return withlogical(element; logical=nothing,
            converted=Parquet.Metadata.ConvertedType.UTF8)
    end
    table = Parquet.Table(legacystring)
    @test table.columns.value == ["legacy"]
    rewritten = Parquet._encodefile(table)
    close(table)
    file = Parquet.File(rewritten)
    metadata = Parquet.Thrift.decode(file.footer.bytes, Parquet.Metadata.FileMetaData)
    close(file)
    @test metadata.schema[2].logicalType === nothing
    @test metadata.schema[2].converted_type == Parquet.Metadata.ConvertedType.UTF8

    modernstring = rewritelogicalschema(legacystring) do element
        logical = Parquet.Metadata.LogicalType(STRING=Parquet.Metadata.StringType())
        return withlogical(element; logical=logical,
            converted=Parquet.Metadata.ConvertedType.DATE)
    end
    table = Parquet.Table(modernstring)
    rewritten = Parquet._encodefile(table)
    close(table)
    file = Parquet.File(rewritten)
    metadata = Parquet.Thrift.decode(file.footer.bytes, Parquet.Metadata.FileMetaData)
    close(file)
    @test metadata.schema[2].logicalType.STRING !== nothing
    @test metadata.schema[2].converted_type == Parquet.Metadata.ConvertedType.DATE

    raw = (Parquet.Thrift.RawField(42, Parquet.Thrift.I32, UInt8[0x02]),)
    source = Parquet.Metadata.SchemaElement(
        type_=Parquet.Metadata.Type.BYTE_ARRAY,
        type_length=Int32(7),
        repetition_type=Parquet.Metadata.FieldRepetitionType.OPTIONAL,
        name="preserved",
        num_children=Int32(0),
        converted_type=Parquet.Metadata.ConvertedType.DATE,
        scale=Int32(9),
        precision=Int32(9),
        field_id=Int32(17),
        logicalType=Parquet.Metadata.LogicalType(
            STRING=Parquet.Metadata.StringType()),
        unknown_fields=raw,
    )
    canonical = Parquet._canonicalwriteelement(source)
    @test canonical.type_ == source.type_
    @test canonical.type_length == source.type_length
    @test canonical.repetition_type == source.repetition_type
    @test canonical.name == source.name
    @test canonical.num_children == source.num_children
    @test canonical.field_id == source.field_id
    @test canonical.unknown_fields == source.unknown_fields
    @test canonical.scale === nothing
    @test canonical.precision === nothing
    @test canonical.converted_type == Parquet.Metadata.ConvertedType.UTF8

    opaquebytes = Parquet._encodefile(
        (value=Vector{UInt8}[collect(codeunits("future"))],))
    opaque = Parquet.Metadata.LogicalType(unknown_fields=(
        Parquet.Thrift.RawField(127, Parquet.Thrift.STRUCT, UInt8[0x00]),
    ))
    opaquebytes = rewritelogicalschema(opaquebytes) do element
        return withlogical(element; logical=opaque,
            converted=Parquet.Metadata.ConvertedType.UTF8)
    end
    table = Parquet.Table(opaquebytes)
    @test table.columns.value == Vector{UInt8}[collect(codeunits("future"))]
    rewritten = Parquet._encodefile(table)
    close(table)
    file = Parquet.File(rewritten)
    metadata = Parquet.Thrift.decode(file.footer.bytes, Parquet.Metadata.FileMetaData)
    close(file)
    @test metadata.schema[2].logicalType == opaque
    @test metadata.schema[2].converted_type == Parquet.Metadata.ConvertedType.UTF8
end

@testset "unsupported temporal units stay explicit at the Table boundary" begin
    bytes = Parquet._encodefile((value=Int64[1],))
    unknownunit = Parquet.Metadata.TimeUnit(unknown_fields=(
        Parquet.Thrift.RawField(42, Parquet.Thrift.STRUCT, UInt8[0x00]),
    ))
    unknown = rewritelogicalschema(bytes) do element
        logical = Parquet.Metadata.LogicalType(
            TIMESTAMP=Parquet.Metadata.TimestampType(
                isAdjustedToUTC=true, unit=unknownunit))
        return withlogical(element; logical=logical)
    end
    file = Parquet.File(unknown)
    close(file)
    @test_throws Parquet.UnsupportedFeatureError Parquet.Table(unknown)

    empty = rewritelogicalschema(bytes) do element
        logical = Parquet.Metadata.LogicalType(
            TIMESTAMP=Parquet.Metadata.TimestampType(
                isAdjustedToUTC=true, unit=Parquet.Metadata.TimeUnit()))
        return withlogical(element; logical=logical)
    end
    file = Parquet.File(empty)
    close(file)
    @test_throws Parquet.FormatError Parquet.Table(empty)
end
