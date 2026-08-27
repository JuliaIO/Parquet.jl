using Random

if !@isdefined(TH)
    const TH = Parquet.Thrift
end
if !@isdefined(MD)
    const MD = Parquet.Metadata
end
if !isdefined(Parquet, :readpage)
    Base.include(Parquet, joinpath(@__DIR__, "..", "src", "page.jl"))
end
if !isdefined(Parquet, :readcolumn)
    Base.include(Parquet, joinpath(@__DIR__, "..", "src", "column.jl"))
end

const COLUMN_CORPUS = get(ENV, "PARQUET_TESTING_DIR", joinpath(@__DIR__, "parquet-testing"))

function columncorpus(parts...)
    return joinpath(COLUMN_CORPUS, "data", parts...)
end

function columnleaf(type; repetition=MD.FieldRepetitionType.REQUIRED, width=nothing)
    return MD.SchemaElement(name="value", type_=type, repetition_type=repetition,
        type_length=width === nothing ? nothing : Int32(width))
end

function columnplain(values; width=nothing)
    eltype(values) == Vector{UInt8} || return Parquet.encode_plain(values)
    width === nothing && return Parquet.encode_plain_byte_array(values)
    matrix = isempty(values) ? Matrix{UInt8}(undef, width, 0) : reduce(hcat, values)
    return Parquet.encode_plain_fixed(matrix)
end

function columnlevels(levels, maxlevel::Int)
    return Parquet.encode_hybrid(UInt64.(levels), Parquet._levelbitwidth(maxlevel); length_prefix=true)
end

function columnv1(count::Integer; encoding=MD.Encoding.PLAIN,
    levelencoding=MD.Encoding.RLE, repetitionencoding=MD.Encoding.RLE)
    return MD.DataPageHeader(num_values=Int32(count), encoding=encoding, definition_level_encoding=levelencoding,
        repetition_level_encoding=repetitionencoding)
end

function columnpage(payload::Vector{UInt8}; type=MD.PageType.DATA_PAGE, v1=nothing,
    index=nothing, dict=nothing, v2=nothing,
    crc=:valid, compressed=length(payload), uncompressed=compressed)
    crcvalue = crc === :valid ? reinterpret(Int32, Parquet.pagechecksum(payload)) : crc === :none ? nothing : Int32(crc)
    header = MD.PageHeader(type_=type, uncompressed_page_size=Int32(uncompressed), compressed_page_size=Int32(compressed),
        crc=crcvalue, data_page_header=v1, index_page_header=index,
        dictionary_page_header=dict, data_page_header_v2=v2)
    return vcat(TH.encode(header), payload)
end

# A V1 data page: optional definition levels then PLAIN values for the present slots.
function datapage(values; levels=nothing, maxlevel=0, width=nothing, extra=UInt8[], crc=:valid,
    encoding=MD.Encoding.PLAIN, levelencoding=MD.Encoding.RLE, repetitions=nothing,
    maxrepetition=0, repetitionencoding=MD.Encoding.RLE)
    count = levels === nothing ?
        (repetitions === nothing ? length(values) : length(repetitions)) : length(levels)
    repetitions === nothing || length(repetitions) == count ||
        throw(ArgumentError("repetition count differs from entry count"))
    repetition = repetitions === nothing ? UInt8[] : columnlevels(repetitions, maxrepetition)
    definition = levels === nothing ? UInt8[] : columnlevels(levels, maxlevel)
    payload = vcat(repetition, definition, columnplain(values; width=width), extra)
    header = columnv1(count; encoding=encoding, levelencoding=levelencoding,
        repetitionencoding=repetitionencoding)
    return columnpage(payload; v1=header, crc=crc)
end

function datapagev2(values; levels=nothing, maxlevel=0, width=nothing,
    repetition=UInt8[], definition=nothing, encoding=MD.Encoding.PLAIN,
    nulls=nothing, rows=nothing, is_compressed=false, extra=UInt8[], crc=:valid)
    valuecount = levels === nothing ? length(values) : length(levels)
    definitions = if definition !== nothing
        definition
    elseif levels === nothing || maxlevel == 0
        UInt8[]
    else
        Parquet.encode_hybrid(UInt64.(levels), Parquet._levelbitwidth(maxlevel))
    end
    data = columnplain(values; width=width)
    payload = vcat(repetition, definitions, data, extra)
    missingcount = something(nulls, levels === nothing ? 0 : count(level -> level != maxlevel, levels))
    header = MD.DataPageHeaderV2(
        num_values=Int32(valuecount),
        num_nulls=Int32(missingcount),
        num_rows=Int32(something(rows, valuecount)),
        encoding=encoding,
        definition_levels_byte_length=Int32(length(definitions)),
        repetition_levels_byte_length=Int32(length(repetition)),
        is_compressed=is_compressed,
    )
    return columnpage(payload; type=MD.PageType.DATA_PAGE_V2, v2=header, crc=crc)
end

function encodedpage(payload::Vector{UInt8}, valuecount::Integer, encoding; v2::Bool=false,
    levels=nothing, maxlevel::Int=0, repetitions=nothing, maxrepetition::Int=0,
    rows=nothing)
    if !v2
        repetition = repetitions === nothing ? UInt8[] :
            columnlevels(repetitions, maxrepetition)
        definitions = levels === nothing ? UInt8[] : columnlevels(levels, maxlevel)
        return columnpage(vcat(repetition, definitions, payload);
            v1=columnv1(valuecount; encoding=encoding))
    end
    repetition = repetitions === nothing ? UInt8[] :
        Parquet.encode_hybrid(UInt64.(repetitions), Parquet._levelbitwidth(maxrepetition))
    definitions = levels === nothing ? UInt8[] :
        Parquet.encode_hybrid(UInt64.(levels), Parquet._levelbitwidth(maxlevel))
    nulls = levels === nothing ? 0 : count(level -> level != maxlevel, levels)
    rowcount = something(rows, repetitions === nothing ? valuecount : count(iszero, repetitions))
    header = MD.DataPageHeaderV2(num_values=Int32(valuecount), num_nulls=Int32(nulls),
        num_rows=Int32(rowcount), encoding=encoding,
        definition_levels_byte_length=Int32(length(definitions)),
        repetition_levels_byte_length=Int32(length(repetition)), is_compressed=false)
    return columnpage(vcat(repetition, definitions, payload);
        type=MD.PageType.DATA_PAGE_V2, v2=header)
end

function rawv2page(payload::Vector{UInt8}; values=1, nulls=0, rows=values,
    definition=0, repetition=0, encoding=MD.Encoding.PLAIN, is_compressed=false,
    compressed=length(payload), uncompressed=compressed, crc=:valid)
    header = MD.DataPageHeaderV2(num_values=Int32(values), num_nulls=Int32(nulls),
        num_rows=Int32(rows), encoding=encoding,
        definition_levels_byte_length=Int32(definition),
        repetition_levels_byte_length=Int32(repetition), is_compressed=is_compressed)
    return columnpage(payload; type=MD.PageType.DATA_PAGE_V2, v2=header,
        compressed=compressed, uncompressed=uncompressed, crc=crc)
end

function columnroot(children::Int)
    return MD.SchemaElement(name="root", num_children=Int32(children))
end

# Build a complete single-column file: magic, pages, footer, footer length, magic.
function syntheticfile(pages::Vector{Vector{UInt8}}, leaf::MD.SchemaElement; num_values::Integer,
    codec=MD.CompressionCodec.UNCOMPRESSED, group=nothing, data_page_offset=4,
    index_page_offset=nothing, dictionary_page_offset=nothing,
    total=nothing, file_path=nothing, crypto=nothing, encryptedmeta=nothing, type=leaf.type_, path=nothing,
    extrachunks=0, rows=num_values, schemaelements=nothing)
    body = isempty(pages) ? UInt8[] : vcat(pages...)
    size = something(total, length(body))
    columnpath = something(path, group === nothing ? ["value"] : [group.name, "value"])
    md = MD.ColumnMetaData(type_=type, encodings=[MD.Encoding.PLAIN, MD.Encoding.RLE], path_in_schema=columnpath,
        codec=codec, num_values=Int64(num_values), total_uncompressed_size=Int64(size), total_compressed_size=Int64(size),
        data_page_offset=Int64(data_page_offset),
        index_page_offset=index_page_offset,
        dictionary_page_offset=dictionary_page_offset)
    chunk = MD.ColumnChunk(meta_data=md, file_path=file_path, crypto_metadata=crypto, encrypted_column_metadata=encryptedmeta)
    elements = something(schemaelements,
        group === nothing ? [columnroot(1), leaf] : [columnroot(1), group, leaf])
    rowgroup = MD.RowGroup(columns=fill(chunk, 1 + extrachunks),
        total_byte_size=Int64(size), num_rows=Int64(rows))
    meta = MD.FileMetaData(version=Int32(1), schema=elements, num_rows=Int64(rows),
        row_groups=[rowgroup])
    footer = TH.encode(meta)
    magic = UInt8[0x50, 0x41, 0x52, 0x31]
    bytes = vcat(magic, body, footer, reinterpret(UInt8, [htol(UInt32(length(footer)))]), magic)
    return bytes, meta, Parquet.Schema(meta)
end

function readsynthetic(pages, leaf; limits=Parquet.Limits(), kwargs...)
    bytes, meta, schema = syntheticfile(pages, leaf; kwargs...)
    file = Parquet.File(bytes)
    values = Parquet.readcolumn(file, meta, schema, 1, 1; limits=limits)
    close(file)
    return values
end

function readsyntheticstream(pages, leaf; expected_rows=nothing,
    limits=Parquet.Limits(), kwargs...)
    bytes, meta, schema = syntheticfile(pages, leaf; kwargs...)
    file = Parquet.File(bytes)
    stream = Parquet.readleafstream(file, meta, schema, 1, 1;
        expected_rows=expected_rows, limits=limits)
    close(file)
    return stream
end

function listleafschema(type; width=nothing)
    leaf = MD.SchemaElement(name="element", type_=type,
        repetition_type=MD.FieldRepetitionType.OPTIONAL,
        type_length=width === nothing ? nothing : Int32(width))
    root = columnroot(1)
    outer = MD.SchemaElement(name="items",
        repetition_type=MD.FieldRepetitionType.OPTIONAL, num_children=Int32(1))
    repeated = MD.SchemaElement(name="list",
        repetition_type=MD.FieldRepetitionType.REPEATED, num_children=Int32(1))
    return leaf, [root, outer, repeated, leaf]
end

function columnbitpacked(values, maxlevel::Int)
    width = Parquet._levelbitwidth(maxlevel)
    output = zeros(UInt8, cld(length(values) * width, 8))
    for (index, rawvalue) in enumerate(values)
        value = UInt64(rawvalue)
        for bit in 0:(width - 1)
            source = width - bit - 1
            iszero(value & (UInt64(1) << source)) && continue
            absolute = (index - 1) * width + bit
            output[(absolute >> 3) + 1] |= UInt8(1) << (7 - (absolute & 7))
        end
    end
    return output
end

@testset "leaf stream invariants" begin
    repetition = UInt64[0, 0, 0, 0, 1, 1, 0]
    definition = UInt64[0, 1, 2, 3, 2, 3, 3]
    values = Int32[0, -1, 11016]
    stream = Parquet.LeafStream(repetition, definition, values, 1, 3;
        expected_rows=5)
    @test stream.repetition === repetition
    @test stream.definition === definition
    @test stream.values === values
    @test length(stream) == 7 && !isempty(stream)
    @test isempty(Parquet.LeafStream(UInt64[], UInt64[], Int32[], 1, 3;
        expected_rows=0))
    @test_throws Parquet.FormatError Parquet.LeafStream(UInt64[0], UInt64[],
        Int32[], 1, 3)
    @test_throws Parquet.FormatError Parquet.LeafStream(UInt64[0], UInt64[3],
        Int32[], 1, 3)
    @test_throws Parquet.FormatError Parquet.LeafStream(UInt64[0], UInt64[4],
        Int32[1], 1, 3)
    @test_throws Parquet.FormatError Parquet.LeafStream(UInt64[2], UInt64[3],
        Int32[1], 1, 3)
    @test_throws Parquet.FormatError Parquet.LeafStream(UInt64[1], UInt64[3],
        Int32[1], 1, 3)
    @test_throws Parquet.FormatError Parquet.LeafStream(UInt64[0, 1], UInt64[0, 0],
        Int32[], 1, 3)
    @test_throws Parquet.FormatError Parquet.LeafStream(repetition, definition,
        values, 1, 3; expected_rows=4)
    @test_throws Parquet.FormatError Parquet.LeafStream(UInt64[], UInt64[],
        Int32[], -1, 3)
end

@testset "nested V1 leaf streams" begin
    leaf, elements = listleafschema(MD.Type.INT32)
    path = ["items", "list", "element"]
    repetition = [0, 0, 0, 0, 1, 1, 0]
    definition = [0, 1, 2, 3, 2, 3, 3]
    values = Int32[0, -1, 11016]
    page = datapage(values; levels=definition, maxlevel=3,
        repetitions=repetition, maxrepetition=1)
    stream = readsyntheticstream([page], leaf; num_values=7, rows=5,
        path=path, schemaelements=elements, expected_rows=5)
    @test stream.repetition == repetition
    @test stream.definition == definition
    @test stream.values == values
    @test_throws Parquet.FormatError readsynthetic([page], leaf; num_values=7,
        rows=5, path=path, schemaelements=elements)

    packedpayload = vcat(columnbitpacked(repetition, 1),
        columnbitpacked(definition, 3), columnplain(values))
    packedheader = columnv1(7; levelencoding=MD.Encoding.BIT_PACKED,
        repetitionencoding=MD.Encoding.BIT_PACKED)
    packedpage = columnpage(packedpayload; v1=packedheader)
    packed = readsyntheticstream([packedpage], leaf; num_values=7, rows=5,
        path=path, schemaelements=elements, expected_rows=5)
    @test packed.repetition == repetition
    @test packed.definition == definition
    @test packed.values == values

    pages = [
        datapage(Int32[1, 2]; levels=[3, 3], maxlevel=3,
            repetitions=[0, 1], maxrepetition=1),
        datapage(Int32[3, 4]; levels=[3, 3], maxlevel=3,
            repetitions=[1, 0], maxrepetition=1),
    ]
    split = readsyntheticstream(pages, leaf; num_values=4, rows=2,
        path=path, schemaelements=elements, expected_rows=2)
    @test split.repetition == [0, 1, 1, 0]
    @test split.definition == fill(UInt64(3), 4)
    @test split.values == Int32[1, 2, 3, 4]

    badstart = datapage(Int32[1]; levels=[3], maxlevel=3,
        repetitions=[1], maxrepetition=1)
    @test_throws Parquet.FormatError readsyntheticstream([badstart], leaf;
        num_values=1, rows=0, path=path, schemaelements=elements)
    @test_throws Parquet.FormatError readsyntheticstream([page], leaf;
        num_values=7, rows=5, path=path, schemaelements=elements, expected_rows=4)
end

@testset "nested V2 leaf streams" begin
    leaf, elements = listleafschema(MD.Type.INT32)
    path = ["items", "list", "element"]
    repetition = [0, 0, 0, 0, 1, 1, 0]
    definition = [0, 1, 2, 3, 2, 3, 3]
    values = Int32[0, -1, 11016]
    repetitionbytes = Parquet.encode_hybrid(UInt64.(repetition), 1)
    page = datapagev2(values; levels=definition, maxlevel=3,
        repetition=repetitionbytes, rows=5)
    stream = readsyntheticstream([page], leaf; num_values=7, rows=5,
        path=path, schemaelements=elements, expected_rows=5)
    @test stream.repetition == repetition
    @test stream.definition == definition
    @test stream.values == values

    firstrepetition = Parquet.encode_hybrid(UInt64[0, 1], 1)
    secondrepetition = Parquet.encode_hybrid(UInt64[0, 1, 1], 1)
    pages = [
        datapagev2(Int32[1, 2]; levels=[3, 3], maxlevel=3,
            repetition=firstrepetition, rows=1),
        datapagev2(Int32[3, 4, 5]; levels=[3, 3, 3], maxlevel=3,
            repetition=secondrepetition, rows=1),
    ]
    split = readsyntheticstream(pages, leaf; num_values=5, rows=2,
        path=path, schemaelements=elements, expected_rows=2)
    @test split.repetition == [0, 1, 0, 1, 1]
    @test split.values == Int32[1, 2, 3, 4, 5]

    nonzero = Parquet.encode_hybrid(UInt64[1], 1)
    badstart = datapagev2(Int32[1]; levels=[3], maxlevel=3,
        repetition=nonzero, rows=0)
    @test_throws Parquet.FormatError readsyntheticstream([badstart], leaf;
        num_values=1, rows=0, path=path, schemaelements=elements)
    wrongrows = datapagev2(Int32[1, 2]; levels=[3, 3], maxlevel=3,
        repetition=firstrepetition, rows=2)
    @test_throws Parquet.FormatError readsyntheticstream([wrongrows], leaf;
        num_values=2, rows=2, path=path, schemaelements=elements)
    wrongnulls = datapagev2(Int32[1]; levels=[3, 2], maxlevel=3,
        repetition=firstrepetition, rows=1, nulls=0)
    @test_throws Parquet.FormatError readsyntheticstream([wrongnulls], leaf;
        num_values=2, rows=1, path=path, schemaelements=elements)
    trailingrepetition = datapagev2(Int32[1, 2]; levels=[3, 3], maxlevel=3,
        repetition=vcat(firstrepetition, UInt8[0x00]), rows=1)
    @test_throws Parquet.FormatError readsyntheticstream([trailingrepetition], leaf;
        num_values=2, rows=1, path=path, schemaelements=elements)
    @test_throws Parquet.FormatError readsyntheticstream([page], leaf;
        num_values=7, rows=5, path=path, schemaelements=elements, expected_rows=4)
end

@testset "nested leaf value encodings" begin
    repetition = [0, 0, 0, 1]
    definition = [3, 0, 3, 3]
    cases = (
        (Int32[1, -2, 3], MD.Encoding.DELTA_BINARY_PACKED,
            Parquet.encode_delta_binary_packed(Int32[1, -2, 3]), MD.Type.INT32, nothing),
        (Int64[1, -2, 3], MD.Encoding.DELTA_BINARY_PACKED,
            Parquet.encode_delta_binary_packed(Int64[1, -2, 3]), MD.Type.INT64, nothing),
        ([UInt8[0x61], UInt8[], UInt8[0x62, 0x63]],
            MD.Encoding.DELTA_LENGTH_BYTE_ARRAY,
            Parquet.encode_delta_length_byte_array(
                [UInt8[0x61], UInt8[], UInt8[0x62, 0x63]]),
            MD.Type.BYTE_ARRAY, nothing),
        ([collect(codeunits(value)) for value in ("prefix-a", "prefix-b", "prefix-bb")],
            MD.Encoding.DELTA_BYTE_ARRAY,
            Parquet.encode_delta_byte_array(["prefix-a", "prefix-b", "prefix-bb"]),
            MD.Type.BYTE_ARRAY, nothing),
        (Float32[-0.0, 1.5, Inf], MD.Encoding.BYTE_STREAM_SPLIT,
            Parquet.encode_byte_stream_split(Float32[-0.0, 1.5, Inf]),
            MD.Type.FLOAT, nothing),
        (Bool[true, false, true], MD.Encoding.RLE,
            Parquet.encode_hybrid(UInt64[1, 0, 1], 1; length_prefix=true),
            MD.Type.BOOLEAN, nothing),
    )
    for v2 in (false, true), (expected, encoding, payload, type, width) in cases
        leaf, elements = listleafschema(type; width=width)
        page = encodedpage(payload, 4, encoding; v2=v2, levels=definition,
            maxlevel=3, repetitions=repetition, maxrepetition=1)
        stream = readsyntheticstream([page], leaf; num_values=4, rows=3,
            path=["items", "list", "element"], schemaelements=elements,
            expected_rows=3)
        @test stream.repetition == repetition
        @test stream.definition == definition
        if expected isa Vector{Float32}
            @test reinterpret(UInt32, stream.values) == reinterpret(UInt32, expected)
        else
            @test stream.values == expected
        end
    end

    fixedvalues = UInt8[1 4 7; 2 5 8; 3 6 9]
    expectedfixed = [fixedvalues[:, index] for index in axes(fixedvalues, 2)]
    for encoding in (MD.Encoding.DELTA_BYTE_ARRAY, MD.Encoding.BYTE_STREAM_SPLIT),
            v2 in (false, true)
        payload = encoding == MD.Encoding.DELTA_BYTE_ARRAY ?
            Parquet.encode_delta_byte_array_fixed(fixedvalues) :
            Parquet.encode_byte_stream_split_fixed(fixedvalues)
        leaf, elements = listleafschema(MD.Type.FIXED_LEN_BYTE_ARRAY; width=3)
        page = encodedpage(payload, 4, encoding; v2=v2, levels=definition,
            maxlevel=3, repetitions=repetition, maxrepetition=1)
        stream = readsyntheticstream([page], leaf; num_values=4, rows=3,
            path=["items", "list", "element"], schemaelements=elements,
            expected_rows=3)
        @test stream.values == expectedfixed
    end

    dictionaryvalues = Int32[10, 20]
    dictionarypayload = columnplain(dictionaryvalues)
    dictionaryheader = MD.DictionaryPageHeader(num_values=Int32(2),
        encoding=MD.Encoding.PLAIN)
    dictionary = columnpage(dictionarypayload; type=MD.PageType.DICTIONARY_PAGE,
        dict=dictionaryheader)
    indices = vcat(UInt8[0x01], Parquet.encode_hybrid(UInt64[1, 0, 1], 1))
    leaf, elements = listleafschema(MD.Type.INT32)
    for v2 in (false, true)
        data = encodedpage(indices, 4, MD.Encoding.RLE_DICTIONARY; v2=v2,
            levels=definition, maxlevel=3, repetitions=repetition, maxrepetition=1)
        stream = readsyntheticstream([dictionary, data], leaf; num_values=4, rows=3,
            path=["items", "list", "element"], schemaelements=elements,
            expected_rows=3, dictionary_page_offset=Int64(4),
            data_page_offset=4 + length(dictionary))
        @test stream.values == Int32[20, 10, 20]
    end
end

@testset "leaf stream materialized ownership" begin
    nvalues = 100
    wide = fill(UInt8(0x78), 4000)
    tiny = UInt8[0x61]
    dictionarypayload = columnplain([wide, tiny])
    dictionaryheader = MD.DictionaryPageHeader(num_values=Int32(2),
        encoding=MD.Encoding.PLAIN)
    dictionary = columnpage(dictionarypayload;
        type=MD.PageType.DICTIONARY_PAGE, dict=dictionaryheader)
    indices = vcat(UInt8[0x01],
        Parquet.encode_hybrid(fill(UInt64(1), nvalues), 1))
    data = encodedpage(indices, nvalues, MD.Encoding.RLE_DICTIONARY)
    leaf = columnleaf(MD.Type.BYTE_ARRAY)
    offset = 4 + length(dictionary)
    limits = Parquet.Limits(max_materialized_bytes=100_000)
    values = readsynthetic([dictionary, data], leaf; num_values=nvalues,
        dictionary_page_offset=Int64(4), data_page_offset=offset,
        limits=limits)
    @test values == fill(tiny, nvalues)

    bytes, metadata, schema = syntheticfile([dictionary, data], leaf;
        num_values=nvalues, dictionary_page_offset=Int64(4),
        data_page_offset=offset)
    file = Parquet.File(bytes)
    budget = Parquet._LiveByteBudget(Parquet.Limits())
    stream = Parquet.readleafstream(file, metadata, schema, 1, 1;
        budget=budget)
    retained = Parquet._leafretainedbytes(Vector{UInt8}, nvalues,
        stream.values)
    @test Parquet._budgetused(budget) == retained
    close(file)

    optional = columnleaf(MD.Type.BYTE_ARRAY;
        repetition=MD.FieldRepetitionType.OPTIONAL)
    optionalpage = datapage([UInt8[0x61], UInt8[0x62, 0x63]];
        levels=[1, 0, 1], maxlevel=1)
    bytes, metadata, schema = syntheticfile([optionalpage], optional;
        num_values=3)
    file = Parquet.File(bytes)
    flatbudget = Parquet._LiveByteBudget(Parquet.Limits())
    output = Parquet.readcolumn(file, metadata, schema, 1, 1;
        budget=flatbudget)
    @test isequal(output,
        Union{Missing,Vector{UInt8}}[UInt8[0x61], missing,
            UInt8[0x62, 0x63]])
    expected = Parquet._materializedarraybytes(eltype(output), length(output))
    expected += Parquet._leafchildbytes(Vector{UInt8},
        collect(skipmissing(output)))
    @test Parquet._budgetused(flatbudget) == expected
    close(file)

    delta = Parquet.encode_delta_binary_packed(Int32[1])
    malformed = encodedpage(vcat(delta, UInt8[0x00]), 1,
        MD.Encoding.DELTA_BINARY_PACKED)
    bytes, metadata, schema = syntheticfile([malformed],
        columnleaf(MD.Type.INT32); num_values=1)
    file = Parquet.File(bytes)
    failurebudget = Parquet._LiveByteBudget(Parquet.Limits())
    @test_throws Parquet.FormatError Parquet.readleafstream(file, metadata,
        schema, 1, 1; budget=failurebudget)
    @test Parquet._budgetused(failurebudget) == 0
    close(file)

    deltalimits = Parquet.Limits(max_materialized_bytes=
        Parquet._materializedarraybytes(Int32, 1) - 1)
    function rejectintdelta()
        deltabudget = Parquet._LiveByteBudget(deltalimits)
        @test_throws Parquet.LimitError Parquet._decodeencodedvalues(Int32,
            MD.Encoding.DELTA_BINARY_PACKED, delta, 1, nothing, 1,
            deltalimits, deltabudget)
        @test Parquet._budgetused(deltabudget) == 0
        return
    end
    rejectintdelta()
    GC.gc()
    @test @allocated(rejectintdelta()) < 10_000
end

@testset "required flat PLAIN columns" begin
    int32 = columnleaf(MD.Type.INT32)
    values = readsynthetic([datapage(Int32[1, -2, 3])], int32; num_values=3)
    @test values == Int32[1, -2, 3] && values isa Vector{Int32}
    two = readsynthetic([datapage(Int32[1, 2]), datapage(Int32[3]), datapage(Int32[4, 5, 6])], int32; num_values=6)
    @test two == Int32[1, 2, 3, 4, 5, 6]
    @test readsynthetic(Vector{UInt8}[], int32; num_values=0,
        data_page_offset=0) == Int32[]
    @test readsynthetic([datapage(Int32[])], int32; num_values=0) == Int32[]
    @test readsynthetic([datapage(Int32[]), datapage(Int32[9])], int32; num_values=1) == Int32[9]
    @test readsynthetic([datapage(Int32[5]; crc=:none)], int32; num_values=1) == Int32[5]
    # parquet-mr marks the absent level stream of required columns as BIT_PACKED
    @test readsynthetic([datapage(Int32[5]; levelencoding=MD.Encoding.BIT_PACKED)], int32; num_values=1) == Int32[5]
    bools = Bool[true, false, true, true, false, false, false, true, true]
    @test readsynthetic([datapage(bools)], columnleaf(MD.Type.BOOLEAN); num_values=9) == bools
    @test readsynthetic([datapage(Int64[typemin(Int64), 0, typemax(Int64)])], columnleaf(MD.Type.INT64); num_values=3) == Int64[typemin(Int64), 0, typemax(Int64)]
    floats = Float32[-0.0, NaN, Inf, 1.5]
    decoded = readsynthetic([datapage(floats)], columnleaf(MD.Type.FLOAT); num_values=4)
    @test reinterpret(UInt32, decoded) == reinterpret(UInt32, floats)
    doubles = [1.0, -2.5, NaN]
    @test isequal(readsynthetic([datapage(doubles)], columnleaf(MD.Type.DOUBLE); num_values=3), doubles)
    strings = [UInt8[], collect(codeunits("parquet")), UInt8[0x00, 0xff]]
    @test readsynthetic([datapage(strings)], columnleaf(MD.Type.BYTE_ARRAY); num_values=3) == strings
    fixed = [UInt8[1, 2, 3], UInt8[4, 5, 6]]
    decodedfixed = readsynthetic([datapage(fixed; width=3)], columnleaf(MD.Type.FIXED_LEN_BYTE_ARRAY; width=3); num_values=2)
    @test decodedfixed == fixed && decodedfixed isa Vector{Vector{UInt8}}
end

@testset "optional flat PLAIN columns" begin
    optional = MD.FieldRepetitionType.OPTIONAL
    int32 = columnleaf(MD.Type.INT32; repetition=optional)
    values = readsynthetic([datapage(Int32[10, 20, 30]; levels=[1, 0, 1, 1, 0], maxlevel=1)], int32; num_values=5)
    @test isequal(values, Union{Missing,Int32}[10, missing, 20, 30, missing]) && values isa Vector{Union{Missing,Int32}}
    allnull = readsynthetic([datapage(Int32[]; levels=[0, 0, 0], maxlevel=1)], int32; num_values=3)
    @test all(ismissing, allnull) && length(allnull) == 3
    allpresent = readsynthetic([datapage(Int32[1, 2]; levels=[1, 1], maxlevel=1)], int32; num_values=2)
    @test isequal(allpresent, Union{Missing,Int32}[1, 2])
    pages = [datapage(Int32[1]; levels=[0, 1], maxlevel=1), datapage(Int32[]; levels=[0], maxlevel=1), datapage(Int32[2, 3]; levels=[1, 1], maxlevel=1)]
    @test isequal(readsynthetic(pages, int32; num_values=5), Union{Missing,Int32}[missing, 1, missing, 2, 3])
    @test isequal(readsynthetic([datapage(Int32[]; levels=Int[], maxlevel=1)], int32; num_values=0), Union{Missing,Int32}[])
    bools = readsynthetic([datapage(Bool[true, false, true]; levels=[1, 0, 1, 1], maxlevel=1)], columnleaf(MD.Type.BOOLEAN; repetition=optional); num_values=4)
    @test isequal(bools, Union{Missing,Bool}[true, missing, false, true])
    strings = readsynthetic([datapage([UInt8[0x61], UInt8[]]; levels=[0, 1, 1], maxlevel=1)], columnleaf(MD.Type.BYTE_ARRAY; repetition=optional); num_values=3)
    @test isequal(strings, Union{Missing,Vector{UInt8}}[missing, UInt8[0x61], UInt8[]])
    fixed = readsynthetic([datapage([UInt8[9, 9]]; levels=[1, 0], maxlevel=1, width=2)], columnleaf(MD.Type.FIXED_LEN_BYTE_ARRAY; repetition=optional, width=2); num_values=2)
    @test isequal(fixed, Union{Missing,Vector{UInt8}}[UInt8[9, 9], missing])
    doubles = readsynthetic([datapage([NaN, 0.5]; levels=[1, 0, 1], maxlevel=1)], columnleaf(MD.Type.DOUBLE; repetition=optional); num_values=3)
    @test isequal(doubles, Union{Missing,Float64}[NaN, missing, 0.5])
    # a flat leaf under an optional group has max definition level 2
    group = MD.SchemaElement(name="g", repetition_type=optional, num_children=Int32(1))
    nested = readsynthetic([datapage(Int32[7, 8]; levels=[2, 1, 0, 2], maxlevel=2)], int32; num_values=4, group=group)
    @test isequal(nested, Union{Missing,Int32}[7, missing, missing, 8])
    @test_throws Parquet.FormatError readsynthetic([datapage(Int32[7]; levels=[2, 3], maxlevel=2)], int32; num_values=2, group=group)
    @test_throws Parquet.FormatError readsynthetic([datapage(Int32[7]; levels=[3], maxlevel=2)], int32; num_values=1, group=group)
    @test_throws Parquet.FormatError readsynthetic([datapage(Int32[7]; levels=[1], maxlevel=1, levelencoding=MD.Encoding.BIT_PACKED)], int32; num_values=1)
    @test_throws Parquet.FormatError readsynthetic([datapage(Int32[7, 8]; levels=[1, 0], maxlevel=1)], int32; num_values=2)
    @test_throws Parquet.FormatError readsynthetic([datapage(Int32[]; levels=[1], maxlevel=1)], int32; num_values=1)
    @test_throws Parquet.FormatError readsynthetic([datapage(Int32[1]; levels=[1], maxlevel=1, extra=UInt8[0x00])], int32; num_values=1)
    levelsonly = columnpage(columnlevels([1], 1)[1:(end - 1)]; v1=columnv1(1))
    @test_throws Parquet.FormatError readsynthetic([levelsonly], int32; num_values=1)
end

@testset "flat Data Page V2 columns" begin
    required = columnleaf(MD.Type.INT32)
    optional = columnleaf(MD.Type.INT32; repetition=MD.FieldRepetitionType.OPTIONAL)
    @test readsynthetic([datapagev2(Int32[1, -2, 3])], required; num_values=3) == Int32[1, -2, 3]
    values = readsynthetic([datapagev2(Int32[10, 20]; levels=[1, 0, 1], maxlevel=1)],
        optional; num_values=3)
    @test isequal(values, Union{Missing,Int32}[10, missing, 20])
    allnull = readsynthetic([datapagev2(Int32[]; levels=[0, 0], maxlevel=1)],
        optional; num_values=2)
    @test isequal(allnull, Union{Missing,Int32}[missing, missing])
    @test readsynthetic([datapagev2(Int32[])], required; num_values=0) == Int32[]
    zero_levels = Parquet.encode_hybrid(zeros(UInt64, 3), 0)
    redundant = datapagev2(Int32[4, 5, 6]; repetition=zero_levels, definition=zero_levels)
    @test readsynthetic([redundant], required; num_values=3) == Int32[4, 5, 6]
    absentdefault = datapagev2(Int32[9]; is_compressed=nothing)
    @test readsynthetic([absentdefault], required; num_values=1) == Int32[9]

    raw = Parquet.encode_plain(Int32[7, 8])
    compressed = Parquet.compress(MD.CompressionCodec.SNAPPY, raw)
    compressedheader = MD.DataPageHeaderV2(num_values=Int32(2), num_nulls=Int32(0),
        num_rows=Int32(2), encoding=MD.Encoding.PLAIN,
        definition_levels_byte_length=Int32(0), repetition_levels_byte_length=Int32(0))
    compressedpage = columnpage(compressed; type=MD.PageType.DATA_PAGE_V2,
        v2=compressedheader, uncompressed=length(raw))
    @test readsynthetic([compressedpage], required; num_values=2,
        codec=MD.CompressionCodec.SNAPPY) == Int32[7, 8]
    uncompressedheader = MD.DataPageHeaderV2(num_values=Int32(2), num_nulls=Int32(0),
        num_rows=Int32(2), encoding=MD.Encoding.PLAIN,
        definition_levels_byte_length=Int32(0), repetition_levels_byte_length=Int32(0),
        is_compressed=false)
    uncompressedpage = columnpage(raw; type=MD.PageType.DATA_PAGE_V2, v2=uncompressedheader)
    @test readsynthetic([uncompressedpage], required; num_values=2,
        codec=MD.CompressionCodec.SNAPPY) == Int32[7, 8]
    emptyheader = MD.DataPageHeaderV2(num_values=Int32(0), num_nulls=Int32(0),
        num_rows=Int32(0), encoding=MD.Encoding.PLAIN,
        definition_levels_byte_length=Int32(0), repetition_levels_byte_length=Int32(0))
    emptypage = columnpage(UInt8[]; type=MD.PageType.DATA_PAGE_V2, v2=emptyheader)
    @test readsynthetic([emptypage], required; num_values=0,
        codec=MD.CompressionCodec.SNAPPY) == Int32[]
end

@testset "page value encoding dispatch" begin
    bytearray = columnleaf(MD.Type.BYTE_ARRAY)
    fixed = columnleaf(MD.Type.FIXED_LEN_BYTE_ARRAY; width=3)
    cases = (
        (Int32[1, 2, -5, 8], MD.Encoding.DELTA_BINARY_PACKED,
            Parquet.encode_delta_binary_packed(Int32[1, 2, -5, 8]), columnleaf(MD.Type.INT32)),
        (Int64[typemin(Int64), -1, 0, typemax(Int64)], MD.Encoding.DELTA_BINARY_PACKED,
            Parquet.encode_delta_binary_packed(Int64[typemin(Int64), -1, 0, typemax(Int64)]),
            columnleaf(MD.Type.INT64)),
        ([UInt8[0x61], UInt8[0x62, 0x63], UInt8[]], MD.Encoding.DELTA_LENGTH_BYTE_ARRAY,
            Parquet.encode_delta_length_byte_array([UInt8[0x61], UInt8[0x62, 0x63], UInt8[]]), bytearray),
        ([collect(codeunits(value)) for value in ("prefix-a", "prefix-b", "prefix-bb")],
            MD.Encoding.DELTA_BYTE_ARRAY,
            Parquet.encode_delta_byte_array(["prefix-a", "prefix-b", "prefix-bb"]), bytearray),
        (Float32[-0.0, 1.5, Inf], MD.Encoding.BYTE_STREAM_SPLIT,
            Parquet.encode_byte_stream_split(Float32[-0.0, 1.5, Inf]), columnleaf(MD.Type.FLOAT)),
    )
    for v2 in (false, true), (expected, encoding, payload, leaf) in cases
        page = encodedpage(payload, length(expected), encoding; v2=v2)
        actual = readsynthetic([page], leaf; num_values=length(expected))
        if expected isa Vector{Float32}
            @test reinterpret(UInt32, actual) == reinterpret(UInt32, expected)
        else
            @test actual == expected
        end
    end
    fixedvalues = UInt8[1 4; 2 5; 3 6]
    for (encoding, payload) in (
            (MD.Encoding.DELTA_BYTE_ARRAY, Parquet.encode_delta_byte_array_fixed(fixedvalues)),
            (MD.Encoding.BYTE_STREAM_SPLIT, Parquet.encode_byte_stream_split_fixed(fixedvalues)))
        for v2 in (false, true)
            page = encodedpage(payload, 2, encoding; v2=v2)
            @test readsynthetic([page], fixed; num_values=2) == [UInt8[1, 2, 3], UInt8[4, 5, 6]]
        end
    end
    bools = Bool[true, false, true, true, false]
    rle = Parquet.encode_hybrid(UInt64.(bools), 1; length_prefix=true)
    for v2 in (false, true)
        page = encodedpage(rle, length(bools), MD.Encoding.RLE; v2=v2)
        @test readsynthetic([page], columnleaf(MD.Type.BOOLEAN); num_values=length(bools)) == bools
    end
    optional = columnleaf(MD.Type.INT32; repetition=MD.FieldRepetitionType.OPTIONAL)
    values = Int32[10, 20, 30]
    levels = [1, 0, 1, 1]
    page = encodedpage(Parquet.encode_delta_binary_packed(values), 4,
        MD.Encoding.DELTA_BINARY_PACKED; v2=true, levels=levels, maxlevel=1)
    @test isequal(readsynthetic([page], optional; num_values=4),
        Union{Missing,Int32}[10, missing, 20, 30])
end

@testset "Data Page V2 malformed input" begin
    required = columnleaf(MD.Type.INT32)
    optional = columnleaf(MD.Type.INT32; repetition=MD.FieldRepetitionType.OPTIONAL)
    value = Parquet.encode_plain(Int32[1])
    @test_throws Parquet.FormatError readsynthetic([rawv2page(value; values=1, rows=0)],
        required; num_values=1)
    @test_throws Parquet.FormatError readsynthetic([rawv2page(value; values=1, nulls=1)],
        required; num_values=1)
    definitions = Parquet.encode_hybrid(UInt64[1, 0], 1)
    mismatch = rawv2page(vcat(definitions, value); values=2, nulls=0, rows=2,
        definition=length(definitions))
    @test_throws Parquet.FormatError readsynthetic([mismatch], optional; num_values=2)
    trailinglevels = rawv2page(vcat(definitions, UInt8[0x00], value); values=2,
        nulls=1, rows=2, definition=length(definitions) + 1)
    @test_throws Parquet.FormatError readsynthetic([trailinglevels], optional; num_values=2)
    badrepetition = rawv2page(vcat(UInt8[0x00], value); repetition=1)
    @test_throws Parquet.FormatError readsynthetic([badrepetition], required; num_values=1)
    trailingvalue = rawv2page(vcat(value, UInt8[0x00]))
    @test_throws Parquet.FormatError readsynthetic([trailingvalue], required; num_values=1)
    shortvalue = rawv2page(value[1:(end - 1)]; uncompressed=length(value) - 1)
    @test_throws Parquet.FormatError readsynthetic([shortvalue], required; num_values=1)
    corruptcompressed = rawv2page(UInt8[0xff]; is_compressed=true, uncompressed=4)
    @test_throws Parquet.FormatError readsynthetic([corruptcompressed], required;
        num_values=1, codec=MD.CompressionCodec.SNAPPY)
    wrongtype = rawv2page(Parquet.encode_delta_binary_packed(Int32[1]);
        encoding=MD.Encoding.DELTA_BINARY_PACKED)
    @test_throws Parquet.FormatError readsynthetic([wrongtype],
        columnleaf(MD.Type.FLOAT); num_values=1)
    unknown = rawv2page(value; encoding=MD.Encoding.T(42))
    @test_throws Parquet.FormatError readsynthetic([unknown], required; num_values=1)
    corruptcrc = datapagev2(Int32[1])
    corruptcrc[end] ⊻= 0x01
    @test_throws Parquet.FormatError readsynthetic([corruptcrc], required; num_values=1)
end

@testset "exact consumption and unsupported pages" begin
    int32 = columnleaf(MD.Type.INT32)
    @test_throws Parquet.FormatError readsynthetic([datapage(Int32[1]; extra=UInt8[0x00])], int32; num_values=1)
    short = columnpage(Parquet.encode_plain(Int32[1, 2])[1:(end - 1)]; v1=columnv1(2))
    @test_throws Parquet.FormatError readsynthetic([short], int32; num_values=2)
    @test_throws Parquet.FormatError readsynthetic([datapage(Int32[1])], int32; num_values=1, codec=MD.CompressionCodec.SNAPPY)
    @test_throws Parquet.FormatError readsynthetic([datapage(Int32[1])], int32; num_values=1, codec=MD.CompressionCodec.T(42))
    for encoding in (MD.Encoding.PLAIN_DICTIONARY, MD.Encoding.RLE_DICTIONARY, MD.Encoding.DELTA_BINARY_PACKED, MD.Encoding.T(10))
        @test_throws Parquet.FormatError readsynthetic([datapage(Int32[1]; encoding=encoding)], int32; num_values=1)
    end
    dictionary = columnpage(Parquet.encode_plain(Int32[1]); type=MD.PageType.DICTIONARY_PAGE,
        dict=MD.DictionaryPageHeader(num_values=Int32(1), encoding=MD.Encoding.PLAIN))
    data = datapage(Int32[1])
    @test readsynthetic([dictionary, data], int32; num_values=1,
        data_page_offset=4 + length(dictionary), dictionary_page_offset=4) ==
        Int32[1]
    @test_throws Parquet.FormatError readsynthetic([data, dictionary], int32;
        num_values=1, data_page_offset=4,
        dictionary_page_offset=4 + length(data))
    v2 = columnpage(Parquet.encode_plain(Int32[1]); type=MD.PageType.DATA_PAGE_V2,
        v2=MD.DataPageHeaderV2(num_values=Int32(1), num_nulls=Int32(0), num_rows=Int32(1), encoding=MD.Encoding.PLAIN,
            definition_levels_byte_length=Int32(0), repetition_levels_byte_length=Int32(0)))
    @test readsynthetic([v2], int32; num_values=1) == Int32[1]
    # index pages and unknown page types are framed, CRC-checked, and skipped
    index = columnpage(UInt8[0x00]; type=MD.PageType.INDEX_PAGE,
        index=MD.IndexPageHeader())
    @test readsynthetic([index, data, index], int32; num_values=1,
        data_page_offset=4 + length(index), index_page_offset=4) == Int32[1]
    unknown = columnpage(UInt8[0x01, 0x02]; type=MD.PageType.T(9))
    @test readsynthetic([datapage(Int32[4]), unknown], int32;
        num_values=1) == Int32[4]
    badindex = copy(index)
    badindex[end] ⊻= 0x01
    @test_throws Parquet.FormatError readsynthetic([badindex, data], int32;
        num_values=1, data_page_offset=4 + length(index),
        index_page_offset=4)
    truncatedindex = columnpage(UInt8[0x00];
        type=MD.PageType.INDEX_PAGE, index=MD.IndexPageHeader(),
        compressed=2)
    @test_throws Parquet.FormatError readsynthetic([truncatedindex, data],
        int32; num_values=1, data_page_offset=4 + length(truncatedindex),
        index_page_offset=4)
    mismatch = columnpage(Parquet.encode_plain(Int32[1]); v1=columnv1(1), uncompressed=5)
    @test_throws Parquet.FormatError readsynthetic([mismatch], int32; num_values=1)
    corrupt = datapage(Int32[1, 2])
    corrupt[end] ⊻= 0x01
    @test_throws Parquet.FormatError readsynthetic([corrupt], int32; num_values=2)
end

@testset "physical page frame limits and cleanup" begin
    int32 = columnleaf(MD.Type.INT32)
    index = columnpage(UInt8[0x10]; type=MD.PageType.INDEX_PAGE,
        index=MD.IndexPageHeader())
    unknown = columnpage(UInt8[0x20]; type=MD.PageType.T(9))
    dictionary = columnpage(Parquet.encode_plain(Int32[9]);
        type=MD.PageType.DICTIONARY_PAGE,
        dict=MD.DictionaryPageHeader(num_values=Int32(1),
            encoding=MD.Encoding.PLAIN))
    for data in (datapage(Int32[1]), datapagev2(Int32[1]))
        plainframes = [index, unknown, data, unknown]
        dataoffset = 4 + length(index) + length(unknown)
        exact = Parquet.Limits(max_container_elements=4)
        @test readsynthetic(plainframes, int32; num_values=1,
            data_page_offset=dataoffset, index_page_offset=4,
            limits=exact) == Int32[1]
        failure = try
            readsynthetic(plainframes, int32; num_values=1,
                data_page_offset=dataoffset, index_page_offset=4,
                limits=Parquet.Limits(max_container_elements=3))
            nothing
        catch err
            err
        end
        @test failure isa Parquet.LimitError
        @test failure.resource == :container_elements
        @test failure.requested == 4
        @test failure.maximum == 3

        dictionaryframes = [dictionary, index, unknown, data, unknown]
        indexoffset = 4 + length(dictionary)
        dataoffset = indexoffset + length(index) + length(unknown)
        @test readsynthetic(dictionaryframes, int32; num_values=1,
            dictionary_page_offset=4, index_page_offset=indexoffset,
            data_page_offset=dataoffset,
            limits=Parquet.Limits(max_container_elements=5)) == Int32[1]
        failure = try
            readsynthetic(dictionaryframes, int32; num_values=1,
                dictionary_page_offset=4, index_page_offset=indexoffset,
                data_page_offset=dataoffset,
                limits=Parquet.Limits(max_container_elements=4))
            nothing
        catch err
            err
        end
        @test failure isa Parquet.LimitError
        @test failure.requested == 5
        @test failure.maximum == 4
    end

    frames = [dictionary, index, unknown, datapage(Int32[1]), unknown]
    bytes, metadata, schema = syntheticfile(frames, int32; num_values=1,
        dictionary_page_offset=4,
        index_page_offset=4 + length(dictionary),
        data_page_offset=4 + length(dictionary) + length(index) +
            length(unknown))
    limits = Parquet.Limits(max_container_elements=4)
    budget = Parquet._LiveByteBudget(limits)
    Parquet._reservearray!(budget, UInt8, 0)
    entry = Parquet._budgetused(budget)
    file = Parquet.File(bytes)
    try
        failure = try
            Parquet.readcolumn(file, metadata, schema, 1, 1;
                limits=limits, budget=budget)
            nothing
        catch err
            err
        end
        @test failure isa Parquet.LimitError
        @test failure.requested == 5
        @test Parquet._budgetused(budget) == entry
    finally
        close(file)
    end

    emptybytes, emptymetadata, emptyschema = syntheticfile(Vector{UInt8}[],
        int32;
        num_values=0, data_page_offset=0, total=0, rows=0)
    emptyfile = Parquet.File(emptybytes)
    try
        stream = Parquet.readleafstream(emptyfile, emptymetadata,
            emptyschema, 1, 1; limits=Parquet.Limits(
                max_container_elements=0))
        @test isempty(stream)
    finally
        close(emptyfile)
    end

    emptydictionary = columnpage(UInt8[];
        type=MD.PageType.DICTIONARY_PAGE,
        dict=MD.DictionaryPageHeader(num_values=Int32(0),
            encoding=MD.Encoding.PLAIN))
    @test isempty(readsynthetic([emptydictionary], int32; num_values=0,
        rows=0, data_page_offset=0, dictionary_page_offset=4,
        limits=Parquet.Limits(max_container_elements=1)))
    dictionarylimit = try
        readsynthetic([emptydictionary], int32; num_values=0, rows=0,
            data_page_offset=0, dictionary_page_offset=4,
            limits=Parquet.Limits(max_container_elements=0))
        nothing
    catch err
        err
    end
    @test dictionarylimit isa Parquet.LimitError
    @test dictionarylimit.requested == 1
    @test dictionarylimit.maximum == 0

    negativedictionary = columnpage(UInt8[];
        type=MD.PageType.DICTIONARY_PAGE,
        dict=MD.DictionaryPageHeader(num_values=Int32(-1),
            encoding=MD.Encoding.PLAIN))
    negativeerror = try
        readsynthetic([negativedictionary], int32; num_values=0, rows=0,
            data_page_offset=0, dictionary_page_offset=4,
            limits=Parquet.Limits(max_container_elements=0))
        nothing
    catch err
        err
    end
    @test negativeerror isa Parquet.FormatError
    @test negativeerror.message == "negative page value count"

    invaliddictionary = columnpage(Parquet.encode_plain(Int32[9, 10]);
        type=MD.PageType.DICTIONARY_PAGE,
        dict=MD.DictionaryPageHeader(num_values=Int32(2),
            encoding=MD.Encoding.RLE))
    invaliderror = try
        readsynthetic([invaliddictionary], int32; num_values=0, rows=0,
            data_page_offset=0, dictionary_page_offset=4,
            limits=Parquet.Limits(max_container_elements=1))
        nothing
    catch err
        err
    end
    @test invaliderror isa Parquet.FormatError
    @test occursin("is not PLAIN", invaliderror.message)

    oversizeddictionary = columnpage(Parquet.encode_plain(Int32[9, 10]);
        type=MD.PageType.DICTIONARY_PAGE,
        dict=MD.DictionaryPageHeader(num_values=Int32(2),
            encoding=MD.Encoding.PLAIN))
    entryerror = try
        readsynthetic([oversizeddictionary], int32; num_values=0, rows=0,
            data_page_offset=0, dictionary_page_offset=4,
            limits=Parquet.Limits(max_container_elements=1))
        nothing
    catch err
        err
    end
    @test entryerror isa Parquet.LimitError
    @test entryerror.resource == :container_elements
    @test entryerror.requested == 2
    @test entryerror.maximum == 1

    corrupt = copy(unknown)
    corrupt[end] ⊻= 0x01
    checksumframes = [datapage(Int32[1]), corrupt]
    checksumerror = try
        readsynthetic(checksumframes, int32; num_values=1,
            limits=Parquet.Limits(max_container_elements=1))
        nothing
    catch err
        err
    end
    @test checksumerror isa Parquet.FormatError

    mismatchedheader = columnpage(UInt8[];
        type=MD.PageType.INDEX_PAGE, index=MD.IndexPageHeader(),
        v1=columnv1(0))
    mismatcherror = try
        readsynthetic([datapage(Int32[1]), mismatchedheader], int32;
            num_values=1,
            limits=Parquet.Limits(max_container_elements=1))
        nothing
    catch err
        err
    end
    @test mismatcherror isa Parquet.FormatError

    truncated = columnpage(UInt8[0x01]; type=MD.PageType.T(9),
        compressed=2)
    truncationerror = try
        readsynthetic([datapage(Int32[1]), truncated], int32;
            num_values=1,
            limits=Parquet.Limits(max_container_elements=1))
        nothing
    catch err
        err
    end
    @test truncationerror isa Parquet.FormatError

    duplicateerror = try
        readsynthetic([dictionary, dictionary, datapage(Int32[1])], int32;
            num_values=1, dictionary_page_offset=4,
            data_page_offset=4 + 2 * length(dictionary),
            limits=Parquet.Limits(max_container_elements=1))
        nothing
    catch err
        err
    end
    @test duplicateerror isa Parquet.FormatError

    falseoffsetframes = [index, unknown, datapage(Int32[1])]
    falseoffset = try
        readsynthetic(falseoffsetframes, int32; num_values=1,
            index_page_offset=4, data_page_offset=4 + length(index),
            limits=Parquet.Limits(max_container_elements=1))
        nothing
    catch err
        err
    end
    @test falseoffset isa Parquet.FormatError

    malformed = columnpage(UInt8[]; v1=columnv1(-1))
    malformederror = try
        readsynthetic([index, malformed], int32; num_values=0, rows=0,
            index_page_offset=4, data_page_offset=4 + length(index),
            limits=Parquet.Limits(max_container_elements=1))
        nothing
    catch err
        err
    end
    @test malformederror isa Parquet.FormatError

    invalidcompressed = columnpage(UInt8[0xff]; v1=columnv1(1))
    compressedframes = [index, invalidcompressed]
    limitfirst = try
        readsynthetic(compressedframes, int32; num_values=1,
            index_page_offset=4, data_page_offset=4 + length(index),
            codec=MD.CompressionCodec.SNAPPY,
            limits=Parquet.Limits(max_container_elements=1))
        nothing
    catch err
        err
    end
    @test limitfirst isa Parquet.LimitError
    @test limitfirst.requested == 2
    payloaderror = try
        readsynthetic(compressedframes, int32; num_values=1,
            index_page_offset=4, data_page_offset=4 + length(index),
            codec=MD.CompressionCodec.SNAPPY,
            limits=Parquet.Limits(max_container_elements=2))
        nothing
    catch err
        err
    end
    @test payloaderror isa Parquet.FormatError

    pagebyteserror = try
        readsynthetic([index, datapage(Int32[1])], int32; num_values=1,
            index_page_offset=4, data_page_offset=4 + length(index),
            limits=Parquet.Limits(max_container_elements=1,
                max_page_bytes=3))
        nothing
    catch err
        err
    end
    @test pagebyteserror isa Parquet.LimitError
    @test pagebyteserror.resource == :page_bytes
    @test pagebyteserror.requested == 4

    bytes, metadata, schema = syntheticfile([datapage(Int32[1])], int32;
        num_values=1)
    chunk = metadata.row_groups[1].columns[1]
    src = Parquet.source(bytes)
    for offset in (typemax(UInt128), big(1) << 100, -(big(1) << 100))
        error = try
            Parquet.readleafstream(src, chunk, schema.leaves[1], offset)
            nothing
        catch err
            err
        end
        @test error isa ArgumentError
        @test error.msg == "footer offset does not fit Int64"
    end
end

@testset "column chunk bounds and counts" begin
    int32 = columnleaf(MD.Type.INT32)
    page = datapage(Int32[1, 2, 3])
    @test_throws Parquet.FormatError readsynthetic([page], int32; num_values=2)
    @test_throws Parquet.FormatError readsynthetic([page], int32; num_values=4)
    @test_throws Parquet.FormatError readsynthetic([page, page], int32; num_values=3)
    @test_throws Parquet.FormatError readsynthetic([page], int32; num_values=3, total=length(page) + 1)
    @test_throws Parquet.FormatError readsynthetic([page], int32; num_values=3, total=length(page) - 1)
    @test_throws Parquet.FormatError readsynthetic([page], int32; num_values=3, total=-1)
    @test_throws Parquet.FormatError readsynthetic([page], int32; num_values=-1)
    @test_throws Parquet.FormatError readsynthetic([page], int32; num_values=3, data_page_offset=3)
    @test_throws Parquet.FormatError readsynthetic([page], int32; num_values=3, data_page_offset=5)
    @test_throws Parquet.FormatError readsynthetic([page], int32; num_values=3, data_page_offset=length(page) + 4)
    @test_throws Parquet.FormatError readsynthetic([page], int32; num_values=3, data_page_offset=typemax(Int64) - 1)
    @test readsynthetic([page], int32; num_values=3, dictionary_page_offset=Int64(0)) == Int32[1, 2, 3]
    @test_throws Parquet.FormatError readsynthetic([page], int32; num_values=3, dictionary_page_offset=Int64(4 + length(page) + 10))
    @test_throws Parquet.FormatError readsynthetic([page], int32; num_values=3, dictionary_page_offset=Int64(2))
    @test_throws Parquet.FormatError readsynthetic([page], int32; num_values=3, dictionary_page_offset=Int64(-1))
    @test_throws Parquet.FormatError readsynthetic([page], int32; num_values=3, data_page_offset=-4)
    # chunk range policy: the dictionary offset (if any) starts the chunk; data_page_offset is 0 or inside it
    function chunkmeta(; data, index=nothing, dictionary=nothing, size=20)
        return MD.ColumnMetaData(type_=MD.Type.INT32, encodings=[MD.Encoding.PLAIN], path_in_schema=["value"],
            codec=MD.CompressionCodec.UNCOMPRESSED, num_values=Int64(0), total_uncompressed_size=Int64(size),
            total_compressed_size=Int64(size), data_page_offset=Int64(data),
            index_page_offset=index, dictionary_page_offset=dictionary)
    end
    @test Parquet._chunkrange(chunkmeta(; data=4), Int64(100)) == (4, 24)
    @test Parquet._chunkrange(chunkmeta(; data=0, dictionary=Int64(4)), Int64(100)) == (4, 24)
    @test Parquet._chunkrange(chunkmeta(; data=10, dictionary=Int64(4)), Int64(100)) == (4, 24)
    @test_throws Parquet.FormatError Parquet._chunkrange(
        chunkmeta(; data=24, dictionary=Int64(4)), Int64(100))
    @test Parquet._chunkrange(chunkmeta(; data=8, dictionary=Int64(0)), Int64(100)) == (8, 28)
    @test Parquet._chunkrange(chunkmeta(; data=0, size=0), Int64(100)) == (0, 0)
    @test Parquet._chunkrange(chunkmeta(; data=0, dictionary=Int64(0),
        size=0), Int64(100)) == (0, 0)
    @test_throws Parquet.FormatError Parquet._chunkrange(
        chunkmeta(; data=1, size=0), Int64(100))
    @test_throws Parquet.FormatError Parquet._chunkrange(
        chunkmeta(; data=4, size=0), Int64(100))
    @test_throws Parquet.FormatError Parquet._chunkrange(
        chunkmeta(; data=0, dictionary=Int64(4), size=0), Int64(100))
    @test_throws Parquet.FormatError Parquet._chunkrange(
        chunkmeta(; data=0, index=Int64(4), size=0), Int64(100))
    @test_throws Parquet.FormatError Parquet._chunkrange(chunkmeta(; data=25, dictionary=Int64(4)), Int64(100))
    @test_throws Parquet.FormatError Parquet._chunkrange(chunkmeta(; data=4, dictionary=Int64(8)), Int64(100))
    @test_throws Parquet.FormatError Parquet._chunkrange(chunkmeta(; data=4, dictionary=Int64(-8)), Int64(100))
    @test_throws Parquet.FormatError Parquet._chunkrange(chunkmeta(; data=-4), Int64(100))
    @test_throws Parquet.FormatError Parquet._chunkrange(chunkmeta(; data=0), Int64(100))
    @test_throws Parquet.FormatError Parquet._chunkrange(chunkmeta(; data=4), Int64(23))
    @test_throws Parquet.FormatError Parquet._chunkrange(chunkmeta(; data=4), typemin(Int64))
    @test_throws Parquet.FormatError Parquet._chunkrange(chunkmeta(; data=typemax(Int64)), Int64(100))
    @test_throws Parquet.FormatError Parquet._chunkrange(chunkmeta(; data=4, size=typemax(Int64)), Int64(100))
    @test_throws Parquet.FormatError Parquet._chunkrange(chunkmeta(; data=typemax(Int64), dictionary=Int64(4)), Int64(100))
    # hostile footer offsets and sizes never widen the range past [4, footer) and never raise overflow errors
    rng = MersenneTwister(77)
    extremes = Int64[typemin(Int64), -1, 0, 1, 3, 4, 5, 99, 100, 101, typemax(Int64) - 1, typemax(Int64)]
    for trial in 1:3000
        pick = () -> rand(rng, Bool) ? rand(rng, extremes) : rand(rng, Int64)
        dictionary = rand(rng, Bool) ? nothing : pick()
        md = chunkmeta(; data=pick(), dictionary=dictionary, size=pick())
        result = try
            Parquet._chunkrange(md, Int64(100))
        catch err
            err
        end
        if result isa Tuple
            start, stop = result
            @test start <= stop && (start == stop || (start >= 4 && stop <= 100))
        else
            @test result isa Parquet.FormatError
        end
    end
    bytes, meta, schema = syntheticfile([page], int32; num_values=3)
    chunk = meta.row_groups[1].columns[1]
    src = Parquet.source(bytes)
    @test Parquet.readcolumn(src, chunk, schema.leaves[1], 4 + length(page)) == Int32[1, 2, 3]
    @test_throws Parquet.FormatError Parquet.readcolumn(src, chunk, schema.leaves[1], 4 + length(page) - 1)
    @test_throws Parquet.FormatError Parquet.readcolumn(src, chunk, schema.leaves[1], length(bytes) + 1)
    file = Parquet.File(bytes)
    @test_throws ArgumentError Parquet.readcolumn(file, meta, schema, 2, 1)
    @test_throws ArgumentError Parquet.readcolumn(file, meta, schema, 1, 2)
    @test_throws ArgumentError Parquet.readcolumn(file, meta, schema, 0, 1)
    close(file)
    _, wide, wideschema = syntheticfile([page], int32; num_values=3, extrachunks=1)
    file = Parquet.File(bytes)
    @test_throws Parquet.FormatError Parquet.readcolumn(file, wide, wideschema, 1, 1)
    close(file)
end

@testset "column chunk metadata validation" begin
    int32 = columnleaf(MD.Type.INT32)
    page = datapage(Int32[1])
    @test_throws Parquet.FormatError readsynthetic([page], int32; num_values=1, file_path="other.parquet")
    @test_throws Parquet.FormatError readsynthetic([page], int32; num_values=1, crypto=MD.ColumnCryptoMetaData(ENCRYPTION_WITH_FOOTER_KEY=MD.EncryptionWithFooterKey()))
    @test_throws Parquet.FormatError readsynthetic([page], int32; num_values=1, encryptedmeta=UInt8[0x01])
    @test_throws Parquet.FormatError readsynthetic([page], int32; num_values=1, type=MD.Type.INT64)
    @test_throws Parquet.FormatError readsynthetic([page], int32; num_values=1, path=["other"])
    @test_throws Parquet.FormatError readsynthetic([page], columnleaf(MD.Type.INT96); num_values=1)
    repeated = columnleaf(MD.Type.INT32; repetition=MD.FieldRepetitionType.REPEATED)
    @test_throws Parquet.FormatError readsynthetic([page], repeated; num_values=1)
    bytes, meta, schema = syntheticfile([page], int32; num_values=1)
    nometa = MD.ColumnChunk()
    @test_throws Parquet.FormatError Parquet.readcolumn(Parquet.source(bytes), nometa, schema.leaves[1], 4 + length(page))
    @test_throws Parquet.LimitError readsynthetic([page], int32; num_values=1, limits=Parquet.Limits(max_container_elements=0))
    @test_throws Parquet.LimitError readsynthetic([page], int32; num_values=1, limits=Parquet.Limits(max_page_bytes=3))
    @test_throws Parquet.LimitError readsynthetic([page], int32; num_values=1, limits=Parquet.Limits(max_page_header_bytes=2))
    @test readsynthetic([page], int32; num_values=1, limits=Parquet.Limits(max_page_bytes=4)) == Int32[1]
end

@testset "column mutation fuzz" begin
    int32 = columnleaf(MD.Type.INT32; repetition=MD.FieldRepetitionType.OPTIONAL)
    pages = [datapage(Int32[1, 2]; levels=[1, 0, 1], maxlevel=1), datapage(Int32[3]; levels=[1, 0], maxlevel=1)]
    bytes, meta, schema = syntheticfile(pages, int32; num_values=5)
    region = 5:(4 + sum(length, pages))
    rng = MersenneTwister(4242)
    outcomes = Set{Symbol}()
    for trial in 1:800
        mutated = copy(bytes)
        for _ in 1:rand(rng, 1:3)
            mutated[rand(rng, region)] = rand(rng, UInt8)
        end
        file = Parquet.File(mutated)
        result = try
            Parquet.readcolumn(file, meta, schema, 1, 1)
            :ok
        catch err
            err
        end
        close(file)
        if result === :ok
            push!(outcomes, :ok)
        else
            @test result isa Union{Parquet.FormatError,Parquet.LimitError}
            push!(outcomes, nameof(typeof(result)))
        end
    end
    @test :FormatError in outcomes
end

function corpuscolumn(path::String, column::Int)
    file = Parquet.File(path)
    meta = TH.decode(file.footer.bytes, MD.FileMetaData)
    schema = Parquet.Schema(meta)
    values = Parquet.readcolumn(file, meta, schema, 1, column)
    close(file)
    return values, meta
end

# Row-weighted digest used to compare against the pyarrow oracle (see the handoff notes).
function oracledigest(values)
    digest = UInt64(0)
    nulls = Int[]
    for (index, value) in enumerate(values)
        if value === missing
            push!(nulls, index)
            continue
        end
        word = value isa Vector{UInt8} ? foldl((acc, byte) -> (acc << 8) | UInt64(byte), value; init=UInt64(0)) :
            value isa Bool ? UInt64(value) : reinterpret(UInt64, Int64(value))
        digest += UInt64(index) * word
    end
    return digest, nulls
end

@testset "official flat PLAIN fixtures" begin
    if !isdir(columncorpus())
        @warn "parquet-testing corpus not found; skipping column corpus tests" COLUMN_CORPUS
    else
        a, meta = corpuscolumn(columncorpus("datapage_v1-uncompressed-checksum.parquet"), 1)
        b, _ = corpuscolumn(columncorpus("datapage_v1-uncompressed-checksum.parquet"), 2)
        @test a isa Vector{Int32} && length(a) == 5120 && sum(Int64, a) == 43118090240 && oracledigest(a)[1] == 454385326080
        @test a[1:4] == Int32[50462976, 117835012, 185207048, 252579084] && a[(end - 1):end] == Int32[84281096, 16909060]
        @test extrema(a) == (-2122153084, 2138996092)
        @test b isa Vector{Int32} && length(b) == 5120 && sum(Int64, b) == 129016125440 && oracledigest(b)[1] == 378853655132160
        @test b[1:2] == Int32[1734763876, 1802135912] && b[end] == -1684366952 && extrema(b) == (-2088599168, 2138996092)
        chunks = meta.row_groups[1].columns
        @test [chunk.meta_data.encodings for chunk in chunks] == [[MD.Encoding.RLE, MD.Encoding.PLAIN], [MD.Encoding.RLE, MD.Encoding.PLAIN]]
        @test [chunk.meta_data.data_page_offset for chunk in chunks] == [4, 20540]
        @test all(chunk -> chunk.meta_data.dictionary_page_offset === nothing && chunk.meta_data.statistics === nothing, chunks)
        @test all(chunk -> chunk.meta_data.encoding_stats == [MD.PageEncodingStats(page_type=MD.PageType.DATA_PAGE, encoding=MD.Encoding.PLAIN, count=Int32(2))], chunks)
        @test startswith(meta.created_by, "parquet-mr version 1.13.0-SNAPSHOT")
        @test_throws Parquet.FormatError corpuscolumn(columncorpus("datapage_v1-corrupt-checksum.parquet"), 1)
        @test_throws Parquet.FormatError corpuscolumn(columncorpus("datapage_v1-corrupt-checksum.parquet"), 2)
        nullable, nullmeta = corpuscolumn(columncorpus("int32_with_null_pages.parquet"), 1)
        digest, nulls = oracledigest(nullable)
        @test nullable isa Vector{Union{Missing,Int32}} && length(nullable) == 1000 && length(nulls) == 275
        @test digest == 18446743417863037648 && sum(nulls) == 92306 && nulls[1:10] == [5, 14, 34, 47, 57, 67, 80, 83, 102, 116]
        @test sum(Int64, skipmissing(nullable)) == -12383254597
        @test isequal(nullable[1:5], Union{Missing,Int32}[-654807448, -465559769, -34563097, 398454479, missing])
        @test [count(ismissing, nullable[((page - 1) * 100 + 1):(page * 100)]) for page in 1:10] == [8, 55, 100, 52, 16, 12, 5, 7, 8, 12]
        @test nullmeta.row_groups[1].columns[1].meta_data.statistics.null_count == 275
        fixed, fixedmeta = corpuscolumn(columncorpus("fixed_length_byte_array.parquet"), 1)
        digest, nulls = oracledigest(fixed)
        @test fixed isa Vector{Union{Missing,Vector{UInt8}}} && length(fixed) == 1000 && length(nulls) == 105
        @test digest == 148827896 && sum(nulls) == 43965
        integers = [foldl((acc, byte) -> (acc << 8) | Int(byte), value; init=0) for value in skipmissing(fixed)]
        @test integers[1:6] == [1000, 990, 989, 988, 987, 986] && integers[(end - 2):end] == [3, 2, 1] && sum(integers) == 439360
        @test all(integers[i] > integers[i + 1] for i in 1:(length(integers) - 1))
        @test all(value -> value === missing || length(value) == 4, fixed)
        @test [count(ismissing, fixed[((page - 1) * 100 + 1):(page * 100)]) for page in 1:10] == [9, 9, 19, 10, 13, 11, 11, 8, 9, 6]
        bson, _ = corpuscolumn(columncorpus("bson.parquet"), 1)
        @test isequal(bson, Union{Missing,Vector{UInt8}}[hex2bytes("0c0000001061000100000000"), hex2bytes("0f000000106100010000000a620000"), missing])
        json, _ = corpuscolumn(columncorpus("json.parquet"), 1)
        @test isequal(json, Union{Missing,Vector{UInt8}}[collect(codeunits("{\"a\":1}")), collect(codeunits("{\"a\":1,\"b\":null}")), collect(codeunits("[1,null,3]")), missing])
        binary, _ = corpuscolumn(columncorpus("binary.parquet"), 1)
        @test isequal(binary, Union{Missing,Vector{UInt8}}[[UInt8(i)] for i in 0:11])
        bools, _ = corpuscolumn(columncorpus("alltypes_plain.parquet"), 2)
        @test isequal(bools, Union{Missing,Bool}[true, false, true, false, true, false, true, false])
        floats, _ = corpuscolumn(columncorpus("floating_orders_nan_count.parquet"), 1)
        @test floats isa Vector{Float32} && length(floats) == 10
        # parquet-cpp-arrow 17 writes data_page_offset = 0 for chunks that hold only a dictionary page
        emptyfile = Parquet.File(columncorpus("column_chunk_key_value_metadata.parquet"))
        emptymeta = TH.decode(emptyfile.footer.bytes, MD.FileMetaData)
        emptychunks = emptymeta.row_groups[1].columns
        @test emptymeta.num_rows == 0 && [chunk.meta_data.data_page_offset for chunk in emptychunks] == [0, 0]
        @test [Parquet._chunkrange(chunk.meta_data, emptyfile.footer.offset) for chunk in emptychunks] == [(4, 18), (97, 111)]
        @test Parquet.readcolumn(emptyfile, emptymeta, Parquet.Schema(emptymeta), 1, 1) == Int32[]
        close(emptyfile)
        # seeded mutations inside the first column chunk of the checksum fixture
        path = columncorpus("datapage_v1-uncompressed-checksum.parquet")
        original = read(path)
        file = Parquet.File(path)
        meta = TH.decode(file.footer.bytes, MD.FileMetaData)
        close(file)
        schema = Parquet.Schema(meta)
        md = meta.row_groups[1].columns[1].meta_data
        region = (md.data_page_offset + 1):(md.data_page_offset + md.total_compressed_size)
        rng = MersenneTwister(99)
        for trial in 1:200
            mutated = copy(original)
            for _ in 1:rand(rng, 1:2)
                mutated[rand(rng, region)] = rand(rng, UInt8)
            end
            mutatedfile = Parquet.File(mutated)
            result = try
                Parquet.readcolumn(mutatedfile, meta, schema, 1, 1)
                :ok
            catch err
                err
            end
            close(mutatedfile)
            @test result === :ok || result isa Union{Parquet.FormatError,
                Parquet.LimitError}
        end
    end
end

@testset "official V2 and encoded flat fixtures" begin
    if !isdir(columncorpus())
        @warn "parquet-testing corpus not found; skipping V2 column corpus tests" COLUMN_CORPUS
    else
        gzip = Parquet.Table(columncorpus("concatenated_gzip_members.parquet"))
        @test gzip.columns.long_col == collect(Int64, 1:513)
        close(gzip)

        booleans = Parquet.Table(columncorpus("rle_boolean_encoding.parquet"))
        booleanvalues = booleans.columns.datatype_boolean
        @test length(booleanvalues) == 68
        @test findall(ismissing, booleanvalues) == [3, 16, 24, 39, 49, 61]
        @test isequal(booleanvalues[1:8], Union{Missing,Bool}[true, false, missing, true,
            true, false, false, true])
        close(booleans)

        for fixture in ("page_v2_empty_compressed.parquet",
                "datapage_v2_empty_datapage.snappy.parquet")
            empty = Parquet.Table(columncorpus(fixture))
            @test all(ismissing, first(values(empty.columns)))
            close(empty)
        end

        required = Parquet.Table(columncorpus("delta_encoding_required_column.parquet"))
        firstrequired = first(values(required.columns))
        @test firstrequired[1:5] == Int32[105, 104, 103, 102, 101]
        @test firstrequired[(end - 2):end] == Int32[3, 2, 1]
        close(required)
        optional = Parquet.Table(columncorpus("delta_encoding_optional_column.parquet"))
        firstoptional = first(values(optional.columns))
        @test isequal(firstoptional[1:5], Union{Missing,Int64}[100, 99, 98, 97, 96])
        @test isequal(firstoptional[(end - 2):end], Union{Missing,Int64}[3, 2, 1])
        close(optional)

        delta = Parquet.Table(columncorpus("delta_byte_array.parquet"))
        @test delta.columns.c_customer_id[1:3] ==
            ["AAAAAAAAIODAAAAA", "AAAAAAAAHODAAAAA", "AAAAAAAAGODAAAAA"]
        @test delta.columns.c_customer_id[(end - 2):end] ==
            ["AAAAAAAADAAAAAAA", "AAAAAAAACAAAAAAA", "AAAAAAAABAAAAAAA"]
        close(delta)
        lengths = Parquet.Table(columncorpus("delta_length_byte_array.parquet"))
        @test lengths.columns.FRUIT ==
            ["apple_banana_mango$((index - 1)^2)" for index in 1:1000]
        close(lengths)

        packed = Parquet.Table(columncorpus("delta_binary_packed.parquet"))
        @test all(==(Int64(6374628540732951412)), packed.columns.bitwidth0)
        @test packed.columns.bitwidth1[1:5] == Int64[0, -1, -1, -1, -1]
        @test packed.columns.bitwidth1[(end - 2):end] == Int64[-102, -103, -104]
        close(packed)

        split = Parquet.Table(columncorpus("byte_stream_split_extended.gzip.parquet"))
        for (plain, encoded) in ((:float_plain, :float_byte_stream_split),
                (:double_plain, :double_byte_stream_split),
                (:int32_plain, :int32_byte_stream_split),
                (:int64_plain, :int64_byte_stream_split),
                (:flba5_plain, :flba5_byte_stream_split))
            @test isequal(getproperty(split.columns, plain), getproperty(split.columns, encoded))
        end
        close(split)

        path = columncorpus("datapage_v2.snappy.parquet")
        file = Parquet.File(path)
        metadata = TH.decode(copy(file.footer.bytes), MD.FileMetaData)
        schema = Parquet.Schema(metadata)
        abc = collect(codeunits("abc"))
        @test isequal(Parquet.readcolumn(file, metadata, schema, 1, 1),
            Union{Missing,Vector{UInt8}}[abc, abc, abc, missing, abc])
        @test Parquet.readcolumn(file, metadata, schema, 1, 2) == Int32[1, 2, 3, 4, 5]
        @test Parquet.readcolumn(file, metadata, schema, 1, 3) == [2.0, 3.0, 4.0, 5.0, 2.0]
        @test Parquet.readcolumn(file, metadata, schema, 1, 4) == Bool[1, 1, 1, 0, 1]
        close(file)
    end
end
