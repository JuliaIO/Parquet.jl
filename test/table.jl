using Tables
using Dates

function tablereplace(value; overrides...)
    names = fieldnames(typeof(value))
    values = map(names) do name
        return get(overrides, name, getproperty(value, name))
    end
    return typeof(value)(values...)
end

function tablerewritefooter(bytes::Vector{UInt8}, metadata)
    file = Parquet.File(bytes)
    prefix = collect(@view bytes[1:Int(file.footer.offset)])
    close(file)
    footer = Parquet.Thrift.encode(metadata)
    output = copy(prefix)
    append!(output, footer)
    Parquet._writelittle!(output, UInt32(length(footer)))
    append!(output, Parquet.PARQUET_MAGIC)
    return output
end

function tableprematerialized(bytes::Vector{UInt8};
        limits::Parquet.Limits=Parquet.Limits())
    budget = Parquet._LiveByteBudget(limits)
    Parquet._reserveobjects!(budget, 2)
    file = Parquet.File(bytes; limits=limits, budget=budget)
    try
        metadata = Parquet._readfilemetadata(file, limits, budget)
        Parquet.Schema(metadata; limits=limits, budget=budget)
        return Parquet._budgetused(budget)
    finally
        close(file)
    end
end

function tabledeepstruct(depth::Int)
    depth >= 1 || throw(ArgumentError("deep table depth must be positive"))
    values::AbstractVector = Int32[7]
    for level in depth:-1:1
        child = level == depth ? "value" : "level_$(level + 1)"
        values = Parquet.StructVector(String[child], AbstractVector[values];
            rows=1)
    end
    return values
end

function tableintervalresult(bytes::Vector{UInt8}, limits::Parquet.Limits)
    budget = Parquet._LiveByteBudget(limits)
    Parquet._reserveobjects!(budget, 2)
    file = Parquet.File(bytes; limits=limits, budget=budget)
    try
        metadata = Parquet._readfilemetadata(file, limits, budget)
        schema = Parquet.Schema(metadata; limits=limits, budget=budget)
        baseline = Parquet._budgetused(budget)
        preflight = Parquet._preflightoffsetindexdeclarations(file, metadata,
            schema, limits)
        result = try
            Parquet._validatepageindexdeclarationoverlaps!(file, metadata,
                schema, preflight, budget)
        catch err
            err
        end
        return result, preflight, baseline, Parquet._budgetused(budget)
    finally
        close(file)
    end
end

mutable struct TableCallbackSentinel <: Exception
    id::Int
end

mutable struct TableCallbackSource <: Parquet.AbstractSource
    bytes::Vector{UInt8}
    failread::Int
    reads::Int
    closes::Int
    sentinel::TableCallbackSentinel
end

mutable struct TableThrowingCloseSource <: Parquet.AbstractSource
    bytes::Vector{UInt8}
    failread::Int
    reads::Int
    closes::Int
    readsentinel::TableCallbackSentinel
    closesentinel::TableCallbackSentinel
end

function Parquet.sourcelength(source::TableCallbackSource)
    return Int64(length(source.bytes))
end

function Parquet.readrange(source::TableCallbackSource, offset::Integer,
        count::Integer)
    source.reads += 1
    source.reads == source.failread && throw(source.sentinel)
    first = Int(offset) + 1
    return @view source.bytes[first:(first + Int(count) - 1)]
end

function Parquet.close!(source::TableCallbackSource)
    source.closes += 1
    return
end

function Parquet.sourcelength(source::TableThrowingCloseSource)
    return Int64(length(source.bytes))
end

function Parquet.readrange(source::TableThrowingCloseSource, offset::Integer,
        count::Integer)
    source.reads += 1
    source.reads == source.failread && throw(source.readsentinel)
    first = Int(offset) + 1
    return @view source.bytes[first:(first + Int(count) - 1)]
end

function Parquet.close!(source::TableThrowingCloseSource)
    source.closes += 1
    throw(source.closesentinel)
end

@testset "flat Tables facade" begin
    input = (
        id=Int64[1, 2, 3],
        flag=Bool[true, false, true],
        score=Union{Missing,Float64}[1.5, missing, -2.0],
        name=["alpha", "βeta", ""],
        label=Union{Missing,String}["first", missing, "κ"],
    )
    bytes = Parquet._encodefile(input)
    table = Parquet.Table(bytes)
    @test length(table) == 3
    @test Tables.istable(typeof(table))
    @test Tables.columnaccess(typeof(table))
    @test Tables.columnnames(table) == (:id, :flag, :score, :name, :label)
    @test Tables.schema(table).types == (Int64, Bool, Union{Missing,Float64}, Parquet.DataStrings.DataString, Union{Missing,Parquet.DataStrings.DataString})
    columns = Tables.columntable(table)
    @test columns.id == input.id
    @test columns.flag == input.flag
    @test isequal(columns.score, input.score)
    @test columns.name == input.name
    @test isequal(columns.label, input.label)
    close(table)
    close(table)
end

@testset "Apache optional LIST fixture" begin
    corpus = get(ENV, "PARQUET_TESTING_DIR", joinpath(@__DIR__, "parquet-testing"))
    fixture = joinpath(corpus, "data", "list_columns.parquet")
    if isfile(fixture)
        table = Parquet.Table(fixture)
        expectedintegers = Union{Missing,Vector{Union{Missing,Int64}}}[
            Union{Missing,Int64}[1, 2, 3],
            Union{Missing,Int64}[missing, 1],
            Union{Missing,Int64}[4],
        ]
        expectedstrings = Union{Missing,Vector{Union{Missing,String}}}[
            Union{Missing,String}["abc", "efg", "hij"],
            missing,
            Union{Missing,String}["efg", missing, "hij", "xyz"],
        ]
        @test isequal(table.columns.int64_list, expectedintegers)
        @test isequal(table.columns.utf8_list, expectedstrings)
        close(table)
    else
        @info "parquet-testing corpus not found; skipping LIST Tables fixture" corpus
    end
end

@testset "DATE and optional LIST Tables facade" begin
    dates = Union{Missing,Date}[Date(1969, 12, 31), missing, Date(1970, 1, 1),
        Date(2000, 2, 29)]
    for pageversion in (:v1, :v2)
        table = Parquet.Table(Parquet._encodefile((dates=dates,);
            pageversion=pageversion, codec=:snappy))
        @test Tables.schema(table).types == (Union{Missing,Date},)
        @test isequal(table.columns.dates, dates)
        close(table)
    end

    days = Union{Missing,Vector{Union{Missing,Date}}}[
        missing,
        Union{Missing,Date}[],
        Union{Missing,Date}[missing],
        Union{Missing,Date}[Date(1970, 1, 1), missing, Date(1969, 12, 31)],
        Union{Missing,Date}[Date(2000, 2, 29)],
    ]
    for pageversion in (:v1, :v2)
        table = Parquet.Table(Parquet._encodefile((id=Int32[1, 2, 3, 4, 5], days=days);
            pageversion=pageversion, codec=:snappy))
        @test Tables.schema(table).types ==
            (Int32, Union{Missing,Parquet.ListValue{Union{Missing,Date}}})
        @test table.columns.id == Int32[1, 2, 3, 4, 5]
        @test isequal(table.columns.days, days)
        close(table)
    end
end

@testset "flat Tables facade validation" begin
    bytes = Parquet._encodefile((a=Int32[1, 2],))
    file = Parquet.File(bytes)
    metadata = Parquet.Thrift.decode(copy(file.footer.bytes), Parquet.Metadata.FileMetaData)
    close(file)
    badmetadata = Parquet.Metadata.FileMetaData(
        version=metadata.version,
        schema=metadata.schema,
        num_rows=Int64(3),
        row_groups=metadata.row_groups,
        created_by=metadata.created_by,
    )
    footer = Parquet.Thrift.encode(badmetadata)
    bad = vcat(bytes[1:(Int(metadata.row_groups[1].total_byte_size) + 4)], footer,
        reinterpret(UInt8, [htol(UInt32(length(footer)))]), Parquet.PARQUET_MAGIC)
    @test_throws Parquet.FormatError Parquet.Table(bad)
    @test_throws Parquet.LimitError Parquet.Table(bytes; limits=Parquet.Limits(max_container_elements=1))
    element = Parquet.Metadata.SchemaElement(
        name="bad",
        type_=Parquet.Metadata.Type.BYTE_ARRAY,
        repetition_type=Parquet.Metadata.FieldRepetitionType.REQUIRED,
        logicalType=Parquet.Metadata.LogicalType(STRING=Parquet.Metadata.StringType()),
    )
    node = Parquet.SchemaNode(element, ["bad"], Int16(0), Int16(0), Int32(1), Parquet.SchemaNode[])
    @test_throws Parquet.FormatError Parquet._tablevalues(node, [UInt8[0xff]])
end

@testset "table name validation rollback and precedence" begin
    required = Parquet.Metadata.FieldRepetitionType.REQUIRED
    for (names, message) in ((("duplicate", "duplicate"), "unique"),
            (("valid", "nul\0name"), "containing NUL"))
        elements = Parquet.Metadata.SchemaElement[
            Parquet.Metadata.SchemaElement(name="root", num_children=Int32(2)),
            Parquet.Metadata.SchemaElement(name=names[1],
                type_=Parquet.Metadata.Type.INT32, repetition_type=required),
            Parquet.Metadata.SchemaElement(name=names[2],
                type_=Parquet.Metadata.Type.INT32, repetition_type=required),
        ]
        schema = Parquet.Schema(elements)
        limits = Parquet.Limits(max_schema_name_bytes=0)
        budget = Parquet._LiveByteBudget(limits)
        Parquet._reserve!(budget, Int64(64))
        before = Parquet._internedschemanamebytes()
        error = try
            Parquet._tablenames(schema, limits, budget)
            nothing
        catch err
            err
        end
        @test error isa Parquet.UnsupportedFeatureError
        @test occursin(message, error.message)
        @test Parquet._budgetused(budget) == 64
        @test Parquet._internedschemanamebytes() == before
    end

    unique = "__parquet_table_name_limit_rollback__"
    elements = Parquet.Metadata.SchemaElement[
        Parquet.Metadata.SchemaElement(name="root", num_children=Int32(1)),
        Parquet.Metadata.SchemaElement(name=unique,
            type_=Parquet.Metadata.Type.INT32, repetition_type=required),
    ]
    schema = Parquet.Schema(elements)
    limits = Parquet.Limits(max_schema_name_bytes=0)
    budget = Parquet._LiveByteBudget(limits)
    Parquet._reserve!(budget, Int64(64))
    before = Parquet._internedschemanamebytes()
    @test_throws Parquet.LimitError Parquet._tablenames(schema, limits,
        budget)
    @test Parquet._budgetused(budget) == 64
    @test Parquet._internedschemanamebytes() == before
end

@testset "table metadata preflight precedes nested allocation" begin
    bytes = Parquet._encodefile((value=Int32[1],))
    file = Parquet.File(bytes)
    metadata = Parquet.Thrift.decode(file.footer.bytes,
        Parquet.Metadata.FileMetaData)
    footeroffset = file.footer.offset
    close(file)
    group = only(metadata.row_groups)
    chunk = only(group.columns)
    column = something(chunk.meta_data)
    negativeindexmetadata = tablereplace(metadata;
        row_groups=[tablereplace(group;
            columns=[tablereplace(chunk; offset_index_offset=Int64(-1),
                offset_index_length=Int32(1))])])
    sortingnegative = Parquet.Metadata.SortingColumn(column_idx=Int32(-1),
        descending=false, nulls_first=false)
    sortingpast = Parquet.Metadata.SortingColumn(
        column_idx=Int32(length(group.columns)), descending=false,
        nulls_first=false)
    malformed = (
        tablereplace(metadata; row_groups=[tablereplace(group;
            num_rows=Int64(-1))]),
        tablereplace(metadata; row_groups=[tablereplace(group;
            total_byte_size=Int64(-1))]),
        tablereplace(metadata; row_groups=[tablereplace(group;
            total_compressed_size=Int64(-1))]),
        tablereplace(metadata; row_groups=[tablereplace(group;
            file_offset=Int64(-1))]),
        tablereplace(metadata; row_groups=[tablereplace(group;
            file_offset=Int64(1))]),
        tablereplace(metadata; row_groups=[tablereplace(group;
            file_offset=footeroffset + 1)]),
        tablereplace(metadata; row_groups=[tablereplace(group;
            ordinal=Int16(-1))]),
        tablereplace(metadata; row_groups=[tablereplace(group;
            sorting_columns=[sortingnegative])]),
        tablereplace(metadata; row_groups=[tablereplace(group;
            sorting_columns=[sortingpast])]),
        tablereplace(metadata; row_groups=[tablereplace(group;
            columns=[tablereplace(chunk; file_offset=Int64(-1))])]),
        tablereplace(metadata; row_groups=[tablereplace(group;
            columns=[tablereplace(chunk; meta_data=tablereplace(column;
                total_compressed_size=Int64(-1)))])]),
        tablereplace(metadata; row_groups=[tablereplace(group;
            columns=[tablereplace(chunk; meta_data=tablereplace(column;
                data_page_offset=Int64(-1)))])]),
        tablereplace(metadata; row_groups=[tablereplace(group;
            columns=[tablereplace(chunk; offset_index_length=nothing)])]),
        tablereplace(metadata; row_groups=[tablereplace(group;
            columns=[tablereplace(chunk; column_index_offset=Int64(4),
                column_index_length=nothing)])]),
        tablereplace(metadata; row_groups=[tablereplace(group;
            columns=[tablereplace(chunk; offset_index_offset=nothing,
                offset_index_length=nothing, column_index_offset=Int64(4),
                column_index_length=Int32(1))])]),
        negativeindexmetadata,
        tablereplace(metadata; row_groups=[tablereplace(group;
            columns=[tablereplace(chunk; offset_index_length=Int32(0))])]),
        tablereplace(metadata; row_groups=[tablereplace(group;
            columns=[tablereplace(chunk;
                offset_index_offset=typemax(Int64),
                offset_index_length=Int32(1))])]),
        tablereplace(metadata; row_groups=[tablereplace(group;
            columns=[tablereplace(chunk; offset_index_offset=footeroffset,
                offset_index_length=Int32(1))])]),
        tablereplace(metadata; row_groups=[tablereplace(group;
            columns=[tablereplace(chunk; column_index_offset=Int64(-1),
                column_index_length=Int32(1))])]),
        tablereplace(metadata; row_groups=[tablereplace(group;
            columns=[tablereplace(chunk; column_index_offset=Int64(4),
                column_index_length=Int32(0))])]),
        tablereplace(metadata; row_groups=[tablereplace(group;
            columns=[tablereplace(chunk;
                column_index_offset=typemax(Int64),
                column_index_length=Int32(1))])]),
        tablereplace(metadata; row_groups=[tablereplace(group;
            columns=[tablereplace(chunk; column_index_offset=footeroffset,
                column_index_length=Int32(1))])]),
        tablereplace(metadata; row_groups=[tablereplace(group;
            columns=[tablereplace(chunk; meta_data=tablereplace(column;
                bloom_filter_offset=Int64(-1)))])]),
        tablereplace(metadata; row_groups=[tablereplace(group;
            columns=[tablereplace(chunk; meta_data=tablereplace(column;
                bloom_filter_length=Int32(1)))])]),
        tablereplace(metadata; row_groups=[tablereplace(group;
            columns=[tablereplace(chunk; meta_data=tablereplace(column;
                bloom_filter_offset=Int64(4),
                bloom_filter_length=Int32(0)))])]),
        tablereplace(metadata; row_groups=[tablereplace(group;
            columns=[tablereplace(chunk; meta_data=tablereplace(column;
                bloom_filter_offset=typemax(Int64),
                bloom_filter_length=Int32(1)))])]),
        tablereplace(metadata; row_groups=[tablereplace(group;
            columns=[tablereplace(chunk; meta_data=tablereplace(column;
                bloom_filter_offset=footeroffset,
                bloom_filter_length=Int32(1)))])]),
    )
    messages = ("negative row count", "negative total byte size",
        "negative compressed byte size", "negative file offset",
        "outside the file body", "outside the file body", "negative ordinal",
        "sorting column index", "sorting column index", "negative file offset",
        "negative column chunk size", "negative data page offset",
        "offset-index offset and length", "column-index offset and length",
        "required offset index", "offset-index offset", "offset-index length",
        "overflows Int64", "extends past the footer", "column-index offset",
        "column-index length", "overflows Int64", "extends past the footer",
        "bloom-filter offset",
        "bloom-filter length is present without its offset",
        "bloom-filter length must be positive", "overflows Int64",
        "extends past the footer")
    for (hostilemetadata, message) in zip(malformed, messages)
        hostile = tablerewritefooter(bytes, hostilemetadata)
        maximum = tableprematerialized(hostile)
        error = try
            Parquet.Table(hostile; limits=Parquet.Limits(
                max_materialized_bytes=maximum))
            nothing
        catch err
            err
        end
        @test error isa Parquet.FormatError
        @test occursin(message, sprint(showerror, error))
    end

    maximum = tableprematerialized(bytes)
    pageindexerror = try
        Parquet.Table(bytes; limits=Parquet.Limits(
            max_materialized_bytes=maximum,
            max_page_index_bytes=Int64(something(
                chunk.offset_index_length)) - 1))
        nothing
    catch err
        err
    end
    @test pageindexerror isa Parquet.LimitError
    @test pageindexerror.resource == :page_index_bytes

    hostile = tablerewritefooter(bytes, negativeindexmetadata)
    source = TableCallbackSource(hostile, 0, 0, 0,
        TableCallbackSentinel(5))
    sourceerror = try
        Parquet.Table(source; limits=Parquet.Limits(
            max_materialized_bytes=tableprematerialized(hostile)))
        nothing
    catch err
        err
    end
    @test sourceerror isa Parquet.FormatError
    @test source.reads == 3
    @test source.closes == 1

    legacychunk = tablereplace(chunk; offset_index_offset=nothing,
        offset_index_length=nothing, meta_data=tablereplace(column;
            bloom_filter_offset=Int64(something(chunk.offset_index_offset)),
            bloom_filter_length=nothing))
    legacybloom = tablereplace(metadata;
        row_groups=[tablereplace(group; columns=[legacychunk])])
    legacytable = Parquet.Table(tablerewritefooter(bytes, legacybloom))
    @test legacytable.columns.value == Int32[1]
    close(legacytable)

    zerogroup = tablereplace(metadata;
        row_groups=[tablereplace(group; file_offset=Int64(0))])
    zerotable = Parquet.Table(tablerewritefooter(bytes, zerogroup))
    @test zerotable.columns.value == Int32[1]
    close(zerotable)

    twobytes = Parquet._encodefile((left=Int32[1], right=Int32[2]))
    twofile = Parquet.File(twobytes)
    twometadata = Parquet.Thrift.decode(twofile.footer.bytes,
        Parquet.Metadata.FileMetaData)
    close(twofile)
    twogroup = only(twometadata.row_groups)
    firstchunk, secondchunk = twogroup.columns
    firstcolumn = something(firstchunk.meta_data)
    secondcolumn = something(secondchunk.meta_data)
    for bloomlength in (Int32(1), nothing)
        bloomchunk = tablereplace(secondchunk;
            meta_data=tablereplace(secondcolumn;
                bloom_filter_offset=firstcolumn.data_page_offset,
                bloom_filter_length=bloomlength))
        bloommetadata = tablereplace(twometadata;
            row_groups=[tablereplace(twogroup;
                columns=[firstchunk, bloomchunk])])
        bloomhostile = tablerewritefooter(twobytes, bloommetadata)
        generous = Parquet.Limits(max_materialized_bytes=1_000_000_000)
        direct, bloompreflight, bloombaseline, bloomafter =
            tableintervalresult(bloomhostile, generous)
        @test direct isa Parquet.FormatError
        @test bloombaseline == bloomafter
        bloomscratch = Parquet._materializedarraybytes(
            Parquet._PageIndexInterval, bloompreflight.intervalcount)
        bloomerror = try
            Parquet.Table(bloomhostile; limits=Parquet.Limits(
                max_materialized_bytes=bloombaseline + bloomscratch))
            nothing
        catch err
            err
        end
        @test bloomerror isa Parquet.FormatError
        @test occursin("storage ranges overlap", sprint(showerror, bloomerror))
    end
end

@testset "deep page-index overlap preflight and scratch rollback" begin
    depth = 256
    wide = Parquet.Limits(max_metadata_depth=depth + 4,
        max_materialized_bytes=1_000_000_000)
    bytes = Parquet._encodefile((deep=tabledeepstruct(depth),); limits=wide)
    file = Parquet.File(bytes)
    metadata = Parquet.Thrift.decode(file.footer.bytes,
        Parquet.Metadata.FileMetaData)
    close(file)
    group = only(metadata.row_groups)
    chunk = only(group.columns)
    column = something(chunk.meta_data)
    overlapchunk = tablereplace(chunk;
        offset_index_offset=column.data_page_offset,
        offset_index_length=chunk.offset_index_length)
    hostilemetadata = tablereplace(metadata;
        row_groups=[tablereplace(group; columns=[overlapchunk])])
    hostile = tablerewritefooter(bytes, hostilemetadata)
    baselimits = Parquet.Limits(max_metadata_depth=depth + 4,
        max_materialized_bytes=1_000_000_000)
    baseline = tableprematerialized(hostile; limits=baselimits)
    scratch = Parquet._materializedarraybytes(Parquet._PageIndexInterval, 2)

    exactlimits = Parquet.Limits(max_metadata_depth=depth + 4,
        max_materialized_bytes=baseline + scratch)
    exact, preflight, exactbaseline, exactafter =
        tableintervalresult(hostile, exactlimits)
    @test preflight.intervalcount == 2
    @test !preflight.overlaps_validated
    @test exact isa Parquet.FormatError
    @test occursin("storage ranges overlap", sprint(showerror, exact))
    @test exactbaseline == exactafter == baseline

    lowlimits = Parquet.Limits(max_metadata_depth=depth + 4,
        max_materialized_bytes=baseline + scratch - 1)
    low, _, lowbaseline, lowafter = tableintervalresult(hostile, lowlimits)
    @test low isa Parquet.LimitError
    @test low.resource == :materialized_bytes
    @test lowbaseline == lowafter == baseline

    valid, _, validbaseline, validafter = tableintervalresult(bytes,
        exactlimits)
    @test valid isa Parquet._PageIndexRangePreflight
    @test valid.overlaps_validated
    @test validbaseline == validafter

    tableerror = try
        Parquet.Table(hostile; limits=exactlimits)
        nothing
    catch err
        err
    end
    @test tableerror isa Parquet.FormatError
    @test occursin("storage ranges overlap", sprint(showerror, tableerror))
end

@testset "Table source exception identity and ownership" begin
    bytes = Parquet._encodefile((value=Int32[1],))
    failedsentinel = TableCallbackSentinel(1)
    failed = TableCallbackSource(bytes, 1, 0, 0, failedsentinel)
    failederror = try
        Parquet.Table(failed)
        nothing
    catch err
        err
    end
    @test failederror === failedsentinel
    @test failed.reads == 1
    @test failed.closes == 0

    adoptedsentinel = TableCallbackSentinel(2)
    adopted = TableCallbackSource(bytes, 4, 0, 0, adoptedsentinel)
    adoptederror = try
        Parquet.Table(adopted)
        nothing
    catch err
        err
    end
    @test adoptederror === adoptedsentinel
    @test adopted.reads == 4
    @test adopted.closes == 1

    successful = TableCallbackSource(bytes, 0, 0, 0,
        TableCallbackSentinel(3))
    table = Parquet.Table(successful)
    @test table.columns.value == Int32[1]
    @test successful.closes == 0
    close(table)
    @test successful.closes == 1

    readsentinel = TableCallbackSentinel(4)
    closesentinel = TableCallbackSentinel(5)
    throwingclose = TableThrowingCloseSource(bytes, 4, 0, 0, readsentinel,
        closesentinel)
    throwingerror = try
        Parquet.Table(throwingclose)
        nothing
    catch err
        err
    end
    @test throwingerror === readsentinel
    @test throwingclose.reads == 4
    @test throwingclose.closes == 1
end

@testset "fixed-width Tables schema preservation" begin
    required = NTuple{3,UInt8}[(0x01, 0x02, 0x03), (0x04, 0x05, 0x06)]
    optional = Union{Missing,NTuple{2,UInt8}}[(0x07, 0x08), missing]
    input = (required=required, optional=optional)
    expected_required = Vector{UInt8}[UInt8[1, 2, 3], UInt8[4, 5, 6]]
    expected_optional = Union{Missing,Vector{UInt8}}[UInt8[7, 8], missing]
    table = Parquet.Table(Parquet._encodefile(input))
    @test table.columns.required == expected_required
    @test isequal(table.columns.optional, expected_optional)
    @test table.columns.required isa Parquet.FixedByteArrayVector
    @test table.columns.required.width == 3
    @test table.columns.optional.width == 2
    @test collect(table.columns.required) == expected_required
    @test copy(table.columns.required) == expected_required
    table.columns.required[1] = UInt8[9, 9, 9]
    @test table.columns.required[1] == UInt8[9, 9, 9]
    table.columns.required[1] = expected_required[1]
    @test_throws ArgumentError setindex!(table.columns.required, UInt8[1], 1)
    @test Tables.schema(table).types ==
        (Vector{UInt8}, Union{Missing,Vector{UInt8}})
    rewritten = Parquet._encodefile(table)
    close(table)

    file = Parquet.File(rewritten)
    metadata = Parquet.Thrift.decode(file.footer.bytes, Parquet.Metadata.FileMetaData)
    @test [element.type_ for element in metadata.schema[2:end]] ==
        fill(Parquet.Metadata.Type.FIXED_LEN_BYTE_ARRAY, 2)
    @test [element.type_length for element in metadata.schema[2:end]] == [3, 2]
    close(file)
    table = Parquet.Table(rewritten)
    @test table.columns.required == expected_required
    @test isequal(table.columns.optional, expected_optional)
    close(table)

    short = Parquet.Table(Parquet._encodefile(input))
    pop!(short.columns.required[1])
    @test_throws ArgumentError Parquet._encodefile(short)
    close(short)
    long = Parquet.Table(Parquet._encodefile(input))
    push!(long.columns.required[1], 0xff)
    @test_throws ArgumentError Parquet._encodefile(long)
    close(long)
end

@testset "fixed-width Tables schema limit" begin
    bytes = Parquet._encodefile((value=NTuple{1,UInt8}[],))
    file = Parquet.File(bytes)
    metadata = Parquet.Thrift.decode(file.footer.bytes, Parquet.Metadata.FileMetaData)
    prefix = bytes[1:Int(file.footer.offset)]
    close(file)
    schema = copy(metadata.schema)
    element = schema[2]
    schema[2] = Parquet.Metadata.SchemaElement(
        type_=element.type_,
        type_length=typemax(Int32),
        repetition_type=element.repetition_type,
        name=element.name,
    )
    hostile = Parquet.Metadata.FileMetaData(
        version=metadata.version,
        schema=schema,
        num_rows=metadata.num_rows,
        row_groups=metadata.row_groups,
        created_by=metadata.created_by,
    )
    footer = Parquet.Thrift.encode(hostile)
    trailer = UInt8[]
    Parquet._writelittle!(trailer, UInt32(length(footer)))
    append!(trailer, Parquet.PARQUET_MAGIC)
    input = vcat(prefix, footer, trailer)
    error = try
        Parquet.Table(input; limits=Parquet.Limits(max_string_bytes=64))
        nothing
    catch err
        err
    end
    @test error isa Parquet.LimitError
    @test error.resource == :string_bytes
    @test error.requested == typemax(Int32)
    @test error.maximum == 64
end

@testset "flat Tables corpus facade" begin
    corpus = get(ENV, "PARQUET_TESTING_DIR", joinpath(@__DIR__, "parquet-testing"))
    fixture = joinpath(corpus, "data", "datapage_v1-uncompressed-checksum.parquet")
    corrupt = joinpath(corpus, "data", "datapage_v1-corrupt-checksum.parquet")
    if isfile(fixture)
        table = Parquet.Table(fixture)
        columns = Tables.columntable(table)
        @test keys(columns) == (:a, :b)
        @test length(table) == 5120
        @test sum(Int64, columns.a) == 43118090240
        @test sum(Int64, columns.b) == 129016125440
        close(table)
        @test_throws Parquet.FormatError Parquet.Table(corrupt)
        encrypted = joinpath(corpus, "data", "encrypt_columns_plaintext_footer.parquet.encrypted")
        if isfile(encrypted)
            error = try
                Parquet.Table(encrypted)
            catch err
                err
            end
            @test error isa Parquet.FormatError
            @test occursin("plaintext-footer encryption", sprint(showerror, error))
        end
    else
        @info "parquet-testing corpus not found; skipping Tables corpus facade" corpus
    end
end

# Rebuilds a one-chunk file around its first data page with the page header, chunk,
# row group, and file all claiming `claim` values and rows.
function tableclaimfile(bytes::Vector{UInt8}, claim::Integer)
    metadata = wpmetadata(bytes)
    group = only(metadata.row_groups)
    chunk = only(group.columns)
    md = chunk.meta_data
    file = Parquet.File(bytes)
    header, payload = try
        start, stop = Parquet._chunkrange(md, file.footer.offset)
        frame = Parquet.readpage(file.source, start, stop, Parquet.Limits())
        frame.header, collect(frame.payload)
    finally
        close(file)
    end
    header = wpreplace(header; data_page_header=wpreplace(
        header.data_page_header; num_values=Int32(claim)))
    page = vcat(WPTH.encode(header), payload)
    md = wpreplace(md; num_values=Int64(claim), data_page_offset=Int64(4),
        dictionary_page_offset=nothing, total_compressed_size=Int64(length(page)),
        total_uncompressed_size=Int64(length(page)))
    group = wpreplace(group; num_rows=Int64(claim),
        columns=[wpreplace(chunk; meta_data=md, file_offset=Int64(4))],
        total_byte_size=Int64(length(page)),
        total_compressed_size=Int64(length(page)), file_offset=Int64(4))
    footer = WPTH.encode(wpreplace(metadata; num_rows=Int64(claim),
        row_groups=[group]))
    output = vcat(Parquet.PARQUET_MAGIC, page, footer)
    Parquet._writelittle!(output, UInt32(length(footer)))
    return append!(output, Parquet.PARQUET_MAGIC)
end

@testset "file-declared counts are checked against their bytes" begin
    function claimresult(bytes)
        read() = try
            close(Parquet.Table(bytes))
            nothing
        catch err
            err
        end
        read()
        allocated = @allocated(error = read())
        return error, allocated
    end
    claim = typemax(Int32)
    for source in ((x=Int64[1, 2, 3],), (x=Union{Missing,Int64}[1, missing, 3],))
        for pageversion in (:v1,)
            bytes = Parquet._encodefile(source; pageversion=pageversion,
                pageindex=false, statistics=false, checksum=false)
            # The page, its chunk, and the file all claim 2^31 - 1 values.
            error, allocated = claimresult(tableclaimfile(bytes, claim))
            @test error isa Parquet.FormatError
            @test allocated < 64 * 2^20

            # Only the footer claims more rows and values than the pages hold.
            metadata = wpmetadata(bytes)
            group = only(metadata.row_groups)
            chunk = only(group.columns)
            for (rows, values) in ((3, 2^40), (2^40, 2^40))
                changed = wpreplace(metadata; num_rows=Int64(rows),
                    row_groups=[wpreplace(group; num_rows=Int64(rows),
                        columns=[wpreplace(chunk; meta_data=wpreplace(
                            chunk.meta_data; num_values=Int64(values)))])])
                error, allocated = claimresult(wprewritefooter(bytes, changed))
                @test error isa Parquet.FormatError
                @test allocated < 64 * 2^20
            end
        end
    end
end

mutable struct TableScanSource <: Parquet.AbstractSource
    bytes::Vector{UInt8}
    reads::Vector{Tuple{Int64,Int64}}
end

Parquet.sourcelength(source::TableScanSource) = Int64(length(source.bytes))

function Parquet.readrange(source::TableScanSource, offset::Integer, count::Integer)
    push!(source.reads, (Int64(offset), Int64(count)))
    return source.bytes[(offset + 1):(offset + count)]
end

@testset "scan selects columns without reading the others" begin
    decimals = Parquet.LogicalColumn(Union{Missing,Parquet.Decimal}[
        Parquet.Decimal(125, 2), missing, Parquet.Decimal(-5, 2), Parquet.Decimal(0, 2)],
        :decimal; precision=5, scale=2)
    source = (id=Int32[1, 2, 3, 4], name=["w", "x", "y", "z"],
        items=[Int64[1], Int64[], Int64[2, 3], Int64[4]],
        point=NamedTuple{(:x, :y),Tuple{Float64,Union{Missing,Float64}}}[
            (x=1.0, y=missing), (x=2.0, y=2.5), (x=3.0, y=missing), (x=4.0, y=4.5)],
        amount=decimals)
    bytes = Parquet._encodefile(source; rowgroupsize=2)
    full = Parquet.Table(bytes)
    amounts = full.columns.amount
    close(full)

    table = Parquet.Table(bytes; scan=Tables.Scan(select=(:items, :id)))
    try
        @test keys(table.columns) == (:items, :id)
        @test table.rows == 4
        @test collect.(table.columns.items) == source.items
        @test table.columns.id == source.id
        @test [element.name for element in table.metadata.schema] ==
            ["schema", "items", "list", "element", "id"]
        @test all(group -> length(group.columns) == 2, table.metadata.row_groups)
    finally
        close(table)
    end

    # Filter columns are read and dropped; renames carry into the stored schema, so a
    # rewrite keeps the column's annotations.
    request = Tables.Scan(select=(:amount => :cents, :point), offset=1, limit=2,
        filter=Tables.col(:id) >= 2)
    table = Parquet.Table(bytes; scan=request)
    try
        @test keys(table.columns) == (:cents, :point)
        @test table.rows == 2
        @test isequal(table.columns.cents, amounts[3:4])
        @test isequal([NamedTuple(value) for value in table.columns.point],
            [(x=3.0, y=missing), (x=4.0, y=4.5)])
        io = IOBuffer()
        Parquet.write(io, table)
        rewritten = Parquet.Table(take!(io))
        try
            @test keys(rewritten.columns) == (:cents, :point)
            element = rewritten.metadata.schema[2]
            @test element.name == "cents" && element.logicalType.DECIMAL.precision == 5
            @test isequal(rewritten.columns.cents, table.columns.cents)
        finally
            close(rewritten)
        end
    finally
        close(table)
    end

    for (request, rows) in ((Tables.Scan(select=()), 4),
            (Tables.Scan(select=(), offset=3), 1),
            (Tables.Scan(select=(), filter=Tables.col(:id) > 1), 3))
        table = Parquet.Table(bytes; scan=request)
        @test isempty(table.columns) && table.rows == rows
        close(table)
    end
    table = Parquet.Table(bytes; scan=Tables.Scan(select=Tables.Not(r"^(name|point)$")))
    @test keys(table.columns) == (:id, :items, :amount)
    close(table)
    @test_throws ArgumentError Parquet.Table(bytes;
        scan=Tables.Scan(select=(:id => Float64,)))
    @test_throws ArgumentError Parquet.Table(bytes; scan=Tables.Scan(select=(:nope,)))

    # Only the selected chunks are fetched, each in one read.
    plain = Parquet._encodefile(source; rowgroupsize=2, pagesize=8, pageindex=false)
    metadata = wpmetadata(plain)
    tracked = TableScanSource(plain, Tuple{Int64,Int64}[])
    table = Parquet.Table(tracked; scan=Tables.Scan(select=(:name,)))
    try
        @test table.columns.name == source.name
    finally
        close(table)
    end
    footer = length(plain) - 8 - Int(Parquet._readu32le(plain, length(plain) - 7))
    for group in metadata.row_groups, (index, chunk) in enumerate(group.columns)
        start, stop = Parquet._chunkrange(chunk.meta_data, Int64(footer))
        reads = count(read -> read[1] < stop && read[1] + read[2] > start,
            tracked.reads)
        @test reads == (index == 2 ? 1 : 0)
    end

    # A damaged chunk of an unselected column does not affect the selection.
    damaged = copy(plain)
    chunk = first(metadata.row_groups).columns[1]
    start, stop = Parquet._chunkrange(chunk.meta_data, Int64(footer))
    damaged[(start + 1):stop] .= 0x00
    @test_throws Parquet.FormatError Parquet.Table(damaged)
    table = Parquet.Table(damaged; scan=Tables.Scan(select=Tables.Not(:id)))
    @test table.columns.name == source.name
    close(table)
end
