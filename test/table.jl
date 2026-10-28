using Tables
using Dates
import DataAPI
import Random

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

# Bytes a read charges for the footer metadata of `bytes`, the first thing it decodes.
function tableprematerialized(bytes::Vector{UInt8})
    budget = Parquet._ByteBudget(Parquet.Limits(max_materialized_bytes=typemax(Int64) - 1))
    Parquet._decodefooter(Parquet.readfooter(Parquet.source(bytes)), Parquet.Limits(), budget)
    return budget.used
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
    @test Tables.rowcount(table) == Parquet.DataAPI.nrow(table) == 3
    @test length(table) == Parquet.DataAPI.ncol(table) == 5
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
    # A Table is a Tables.AbstractColumns: columns by property, index, and name.
    @test table isa Tables.AbstractColumns
    @test table.id == table[:id] == table[1] == Tables.getcolumn(table, :id) ==
        Tables.getcolumn(table, Int64, 1, :id) == input.id
    @test propertynames(table) == keys(input)
    @test isequal(collect(table), collect(values(Tables.columns(table))))
    close(table)
    close(table)
end

@testset "encrypted files are refused as unsupported" begin
    datadir = joinpath(get(ENV, "PARQUET_TESTING_DIR",
        joinpath(@__DIR__, "parquet-testing")), "data")
    files = isdir(datadir) ? filter(f -> endswith(f, ".parquet.encrypted"),
        readdir(datadir)) : String[]
    isempty(files) && @info "parquet-testing corpus not found; skipping encrypted files"
    for file in files
        error = try
            Parquet.Table(joinpath(datadir, file))
            nothing
        catch err
            err
        end
        @test error isa Parquet.UnsupportedFeatureError
        @test occursin("encrypted", error.message)
    end
end

@testset "Table and File display never print buffers" begin
    rows = 100_000
    bytes = Parquet._encodefile((id=collect(Int64, 1:rows),
        name=["n$(i % 7)" for i in 1:rows]))
    table = Parquet.Table(bytes)
    @test sprint(show, table) == "Parquet.Table(100000 rows, 2 columns)"
    text = sprint(show, MIME("text/plain"), table)
    @test startswith(text, "Parquet.Table with 100000 rows, 2 columns, and schema:\n")
    @test occursin(":id", text) && occursin(":name", text) && length(text) < 200
    close(table)
    file = Parquet.File(bytes)
    @test sprint(show, file) == "Parquet.File(100000 rows, 2 columns, 1 row groups)"
    @test sprint(show, MIME("text/plain"), file) == sprint(show, file)
    close(file)
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
        @test isequal(Parquet.Tables.columns(table).int64_list, expectedintegers)
        @test isequal(Parquet.Tables.columns(table).utf8_list, expectedstrings)
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
        @test isequal(Parquet.Tables.columns(table).dates, dates)
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
        @test Parquet.Tables.columns(table).id == Int32[1, 2, 3, 4, 5]
        @test isequal(Parquet.Tables.columns(table).days, days)
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
    @test_throws Parquet.FormatError Parquet._logicalvalues(node, [UInt8[0xff]])
end

@testset "repeated column names read as name_1, name_2" begin
    bytes = take!(Parquet.write(IOBuffer(), (a=[1, 2], b=["x", "y"], c=[[3.0], [4.0]])))
    n = length(bytes)
    start = n - 8 - Int(Parquet._readu32le(bytes, n - 7))
    metadata = Parquet.Thrift.decode(view(bytes, (start + 1):(n - 8)),
        Parquet.Metadata.FileMetaData)
    # As pyarrow writes a table whose columns share a name: every chunk path says "a".
    rename(element) = element.name in ("b", "c") ? Parquet._withfields(element; name="a") :
        element
    groups = map(metadata.row_groups) do group
        Parquet._withfields(group; columns=map(group.columns) do chunk
            path = copy(chunk.meta_data.path_in_schema)
            path[1] = "a"
            Parquet._withfields(chunk;
                meta_data=Parquet._withfields(chunk.meta_data; path_in_schema=path))
        end)
    end
    footer = Parquet.Thrift.encode(Parquet._withfields(metadata;
        schema=map(rename, metadata.schema), row_groups=groups, key_value_metadata=nothing))
    repeated = vcat(bytes[1:start], footer,
        reinterpret(UInt8, [htol(UInt32(length(footer)))]), b"PAR1")
    table = Parquet.Table(repeated)
    @test Tables.columnnames(table) == (:a, :a_1, :a_2)
    @test table.a == [1, 2] && table.a_1 == ["x", "y"] && table.a_2 == [[3.0], [4.0]]
    @test Parquet.Table(repeated; select=[2]).a_1 == ["x", "y"]
    @test Parquet.Table(repeated; select=[:a_1, :a],
        filter=Parquet.colcmp(==, Parquet.col(:a_1), "y")).a == [2]
    @test Tables.columnnames(Parquet.File(repeated)[1]) == (:a, :a_1, :a_2)
    @test Tables.columnnames(Parquet.Table(take!(Parquet.write(IOBuffer(), table)))) ==
        (:a, :a_1, :a_2)
end

@testset "an annotation the physical type cannot carry reads as that type" begin
    # As parquet-testing's int32_with_uuid_logical_type.parquet: INT32 annotated UUID.
    bytes = take!(Parquet.write(IOBuffer(), (good=Int32[1, 2, 3], bad=Int32[4, 5, 6])))
    metadata = Parquet.File(bytes).metadata
    uuid = Parquet.Metadata.LogicalType(UUID=Parquet.Metadata.UUIDType())
    schema = [element.name == "bad" ? Parquet._withfields(element; logicalType=uuid) :
        element for element in metadata.schema]
    annotated = tablerewritefooter(bytes, Parquet._withfields(metadata; schema=schema))
    table = Parquet.Table(annotated)
    @test table.bad == Int32[4, 5, 6] && eltype(table.bad) === Int32
    # Its statistics are not trusted: they are absent and never prune.
    @test only(Parquet.statistics(Parquet.File(annotated), "bad")).min === nothing
    @test Parquet.Table(annotated;
        filter=Parquet.colcmp(==, Parquet.col(:bad), Int32(5))).good == Int32[2]
    # A rewrite drops the annotation.
    rewritten = Parquet.File(take!(Parquet.write(IOBuffer(), table)))
    @test rewritten.metadata.schema[3].logicalType === nothing
    @test Parquet.Table(rewritten).bad == Int32[4, 5, 6]
end

@testset "table name validation rollback and precedence" begin
    required = Parquet.Metadata.FieldRepetitionType.REQUIRED
    for (names, message) in ((("valid", "nul\0name"), "containing NUL"),)
        elements = Parquet.Metadata.SchemaElement[
            Parquet.Metadata.SchemaElement(name="root", num_children=Int32(2)),
            Parquet.Metadata.SchemaElement(name=names[1],
                type_=Parquet.Metadata.Type.INT32, repetition_type=required),
            Parquet.Metadata.SchemaElement(name=names[2],
                type_=Parquet.Metadata.Type.INT32, repetition_type=required),
        ]
        schema = Parquet.Schema(elements)
        limits = Parquet.Limits(max_schema_name_bytes=0)
        before = Parquet._internedschemanamebytes()
        error = try
            Parquet._tablenames(schema, limits)
            nothing
        catch err
            err
        end
        @test error isa Parquet.UnsupportedFeatureError
        @test occursin(message, error.message)
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
    before = Parquet._internedschemanamebytes()
    @test_throws Parquet.LimitError Parquet._tablenames(schema, limits)
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
    )
    messages = ("negative row count", "negative total byte size",
        "negative compressed byte size", "negative file offset",
        "outside the file body", "outside the file body",
        "sorting column index", "sorting column index", "negative file offset",
        "negative column chunk size", "negative data page offset")
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

    legacychunk = tablereplace(chunk; offset_index_offset=nothing,
        offset_index_length=nothing, meta_data=tablereplace(column;
            bloom_filter_offset=Int64(something(chunk.offset_index_offset)),
            bloom_filter_length=nothing))
    legacybloom = tablereplace(metadata;
        row_groups=[tablereplace(group; columns=[legacychunk])])
    legacytable = Parquet.Table(tablerewritefooter(bytes, legacybloom))
    @test Parquet.Tables.columns(legacytable).value == Int32[1]
    close(legacytable)

    zerogroup = tablereplace(metadata;
        row_groups=[tablereplace(group; file_offset=Int64(0))])
    zerotable = Parquet.Table(tablerewritefooter(bytes, zerogroup))
    @test Parquet.Tables.columns(zerotable).value == Int32[1]
    close(zerotable)
end

@testset "Table source exception identity and ownership" begin
    # A caller's source is read (the footer with one tail read, then the column
    # chunks) and never closed; its errors propagate unchanged.
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

    datasentinel = TableCallbackSentinel(2)
    datafailure = TableCallbackSource(bytes, 2, 0, 0, datasentinel)
    dataerror = try
        Parquet.Table(datafailure)
        nothing
    catch err
        err
    end
    @test dataerror === datasentinel
    @test datafailure.reads == 2
    @test datafailure.closes == 0

    successful = TableCallbackSource(bytes, 0, 0, 0,
        TableCallbackSentinel(3))
    table = Parquet.Table(successful)
    @test Parquet.Tables.columns(table).value == Int32[1]
    @test successful.reads == 2
    close(table)
    @test successful.closes == 0

    throwingclose = TableThrowingCloseSource(bytes, 2, 0, 0, TableCallbackSentinel(4),
        TableCallbackSentinel(5))
    @test_throws TableCallbackSentinel Parquet.Table(throwingclose)
    @test throwingclose.closes == 0
end

@testset "fixed-width Tables schema preservation" begin
    required = NTuple{3,UInt8}[(0x01, 0x02, 0x03), (0x04, 0x05, 0x06)]
    optional = Union{Missing,NTuple{2,UInt8}}[(0x07, 0x08), missing]
    input = (required=required, optional=optional)
    table = Parquet.Table(Parquet._encodefile(input))
    # FIXED_LEN_BYTE_ARRAY without an annotation reads back as the NTuples written.
    @test table.required isa Vector{NTuple{3,UInt8}}
    @test table.required == required
    @test isequal(table.optional, optional)
    @test Tables.schema(table).types == (NTuple{3,UInt8}, Union{Missing,NTuple{2,UInt8}})
    rewritten = Parquet._encodefile(table)
    close(table)

    file = Parquet.File(rewritten)
    metadata = Parquet.Thrift.decode(file.footer.bytes, Parquet.Metadata.FileMetaData)
    @test [element.type_ for element in metadata.schema[2:end]] ==
        fill(Parquet.Metadata.Type.FIXED_LEN_BYTE_ARRAY, 2)
    @test [element.type_length for element in metadata.schema[2:end]] == [3, 2]
    close(file)
    table = Parquet.Table(rewritten)
    @test table.required == required
    @test isequal(table.optional, optional)
    close(table)

    # Up to 256 bytes a fixed width reads as NTuples; past that, as DataBytes.
    widest = [Tuple(fill(UInt8(i), 256)) for i in 1:2]
    widesttable = Parquet.Table(Parquet._encodefile((widest=widest,)))
    @test widesttable.widest isa Vector{NTuple{256,UInt8}}
    @test widesttable.widest == widest
    wide = [Tuple(fill(UInt8(i), 257)) for i in 1:2]
    widetable = Parquet.Table(Parquet._encodefile((wide=wide,)))
    @test widetable.wide isa Parquet.DataStrings.BytesVector
    @test [collect(value) for value in widetable.wide] == [collect(value) for value in wide]
    close(widetable)
end

@testset "ecosystem element types" begin
    DT = Parquet.Durations
    MD = Parquet.Metadata
    stamps = [DT.Timestamp{Dates.Microsecond}(Dates.UTInstant(Dates.Microsecond(
        Dates.value(DateTime(2025, 1, day) - DateTime(1970)) * 1000))) for day in 1:4]
    bytes = Parquet._encodefile((ts=stamps, raw=[UInt8[i, i] for i in 1:4],
        long=[fill(UInt8(i), 20) for i in 1:4]))
    table = Parquet.Table(bytes)
    @test table.ts == stamps
    @test table.raw isa Parquet.DataStrings.BytesVector{Parquet.DataStrings.DataBytes}
    @test table.raw == [UInt8[i, i] for i in 1:4]
    @test table.long == [fill(UInt8(i), 20) for i in 1:4]
    close(table)
    # A DateTime literal filters a local timestamp column.
    filtered = Parquet.Table(bytes; scan=Tables.Scan(
        filter=Tables.colcmp(>=, Tables.col(:ts), DateTime(2025, 1, 3))))
    @test filtered.ts == stamps[3:4]
    close(filtered)

    # Nested binary leaves are BytesVectors too.
    nested = Parquet.Table(Parquet._encodefile((lists=[[UInt8[1], fill(UInt8(2), 30)],
        Vector{UInt8}[]],)))
    @test nested.lists[1] == [UInt8[1], fill(UInt8(2), 30)]
    @test nested.lists.values isa Parquet.DataStrings.BytesVector
    close(nested)
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
        @test Tables.rowcount(table) == 5120
        @test sum(Int64, columns.a) == 43118090240
        @test sum(Int64, columns.b) == 129016125440
        close(table)
        # Page CRCs are verified only on request.
        @test Tables.rowcount(Parquet.Table(corrupt)) == 5120
        @test_throws Parquet.FormatError Parquet.Table(corrupt; checksum=true)
        @test_throws Parquet.FormatError Parquet.Dataset([corrupt]; checksum=true) |>
            Parquet.Table
        @test_throws Parquet.FormatError Parquet.Table(corrupt; checksum=true,
            filter=Parquet.col(:a) > 0)
        encrypted = joinpath(corpus, "data", "encrypt_columns_plaintext_footer.parquet.encrypted")
        if isfile(encrypted)
            error = try
                Parquet.Table(encrypted)
            catch err
                err
            end
            @test error isa Parquet.UnsupportedFeatureError
            @test occursin("plaintext footer", sprint(showerror, error))
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
    amounts = Parquet.Tables.columns(full).amount
    close(full)

    table = Parquet.Table(bytes; scan=Tables.Scan(select=(:items, :id)))
    try
        @test keys(Parquet.Tables.columns(table)) == (:items, :id)
        @test Parquet._tablerows(table) == 4
        @test collect.(Parquet.Tables.columns(table).items) == source.items
        @test Parquet.Tables.columns(table).id == source.id
        @test [element.name for element in Parquet._tablemetadata(table).schema] ==
            ["schema", "items", "list", "element", "id"]
        @test all(group -> length(group.columns) == 2, Parquet._tablemetadata(table).row_groups)
    finally
        close(table)
    end

    # Filter columns are read and dropped; renames carry into the stored schema, so a
    # rewrite keeps the column's annotations.
    request = Tables.Scan(select=(:amount => :cents, :point), offset=1, limit=2,
        filter=Tables.col(:id) >= 2)
    table = Parquet.Table(bytes; scan=request)
    try
        @test keys(Parquet.Tables.columns(table)) == (:cents, :point)
        @test Parquet._tablerows(table) == 2
        @test isequal(Parquet.Tables.columns(table).cents, amounts[3:4])
        @test isequal([NamedTuple(value) for value in Parquet.Tables.columns(table).point],
            [(x=3.0, y=missing), (x=4.0, y=4.5)])
        io = IOBuffer()
        Parquet.write(io, table)
        rewritten = Parquet.Table(take!(io))
        try
            @test keys(Parquet.Tables.columns(rewritten)) == (:cents, :point)
            element = Parquet._tablemetadata(rewritten).schema[2]
            @test element.name == "cents" && element.logicalType.DECIMAL.precision == 5
            @test isequal(Parquet.Tables.columns(rewritten).cents, Parquet.Tables.columns(table).cents)
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
        @test isempty(Parquet.Tables.columns(table)) && Parquet._tablerows(table) == rows
        close(table)
    end
    table = Parquet.Table(bytes; scan=Tables.Scan(select=Tables.Not(r"^(name|point)$")))
    @test keys(Parquet.Tables.columns(table)) == (:id, :items, :amount)
    close(table)
    @test_throws ArgumentError Parquet.Table(bytes;
        scan=Tables.Scan(select=(:id => Float64,)))
    @test_throws ArgumentError Parquet.Table(bytes; scan=Tables.Scan(select=(:nope,)))

    # A source that is not in memory: one read of its end for the footer, then the
    # selected chunks. They lie less than 64 KiB apart here, so one read covers them
    # all, and it starts and ends with selected chunks.
    for pageindex in (true, false)
        paged = Parquet._encodefile(source; rowgroupsize=2, pagesize=8,
            pageindex=pageindex)
        tracked = TableScanSource(paged, Tuple{Int64,Int64}[])
        table = Parquet.Table(tracked; scan=Tables.Scan(select=(:name,)))
        @test Parquet.Tables.columns(table).name == source.name
        @test length(tracked.reads) == 2
        data = tracked.reads[2]
        footer = length(paged) - 8 -
            Int(Parquet._readu32le(paged, length(paged) - 7))
        selected = [Parquet._chunkrange(group.columns[2].meta_data, Int64(footer))
            for group in wpmetadata(paged).row_groups]
        @test data[1] == first(first(selected))
        @test data[1] + data[2] == last(last(selected))
    end
    plain = Parquet._encodefile(source; rowgroupsize=2, pagesize=8, pageindex=false)
    metadata = wpmetadata(plain)
    footer = length(plain) - 8 - Int(Parquet._readu32le(plain, length(plain) - 7))

    # A damaged chunk of an unselected column does not affect the selection.
    damaged = copy(plain)
    chunk = first(metadata.row_groups).columns[1]
    start, stop = Parquet._chunkrange(chunk.meta_data, Int64(footer))
    damaged[(start + 1):stop] .= 0x00
    @test_throws Parquet.FormatError Parquet.Table(damaged)
    table = Parquet.Table(damaged; scan=Tables.Scan(select=Tables.Not(:id)))
    @test Parquet.Tables.columns(table).name == source.name
    close(table)
end

@testset "parallel and sequential reads agree" begin
    # Reads decode one task per column chunk; ntasks=1 decodes them in order on the
    # calling task, and a byte cap is shared by all tasks.
    capped = Parquet.Limits(max_materialized_bytes=typemax(Int64) - 1)
    rows = 1_000
    source = (id=collect(Int64, 1:rows), name=["n$(i % 17)" for i in 1:rows],
        score=Union{Missing,Float64}[isodd(i) ? i / 2 : missing for i in 1:rows],
        tags=[["t$(j)" for j in 1:(i % 3)] for i in 1:rows],
        point=[(x=i, y=-i) for i in 1:rows])
    flat = (id=source.id, name=source.name, score=source.score)
    for table in (flat, source)
        bytes = Parquet._encodefile(table; rowgroupsize=150, pagesize=512,
            codec=:snappy)
        parallel = Parquet.Table(bytes; limits=capped)
        ordered = Parquet.Table(bytes; ntasks=1)
        try
            @test keys(Parquet.Tables.columns(parallel)) == keys(Parquet.Tables.columns(ordered)) == keys(table)
            for name in keys(table)
                @test isequal(collect(getproperty(Parquet.Tables.columns(parallel), name)),
                    collect(getproperty(Parquet.Tables.columns(ordered), name)))
            end
            @test Parquet.Tables.columns(parallel).id == source.id
        finally
            close(parallel)
            close(ordered)
        end
    end
    bytes = Parquet._encodefile(source; rowgroupsize=150, pagesize=512,
        codec=:snappy)

    # A failing chunk reports its own error type either way.
    metadata = wpmetadata(bytes)
    footer = length(bytes) - 8 - Int(Parquet._readu32le(bytes, length(bytes) - 7))
    chunk = metadata.row_groups[3].columns[2]
    start, stop = Parquet._chunkrange(chunk.meta_data, Int64(footer))
    damaged = copy(bytes)
    damaged[(start + 1):stop] .= 0x00
    @test_throws Parquet.FormatError Parquet.Table(damaged)
    @test_throws Parquet.FormatError Parquet.Table(damaged; ntasks=1)
    @test_throws Parquet.FormatError Parquet.Table(damaged; limits=capped)
    @test_throws ArgumentError Parquet.Table(bytes; ntasks=0)
end

@testset "read kernels: storage-integer decimals, PLAIN strings, nullable pages" begin
    MD = Parquet.Metadata
    decimal(p, s) = element -> withlogical(element;
        logical=MD.LogicalType(DECIMAL=MD.DecimalType(scale=Int32(s), precision=Int32(p))),
        converted=MD.ConvertedType.DECIMAL, precision=Int32(p), scale=Int32(s))
    asstring(element) = withlogical(element;
        logical=MD.LogicalType(STRING=MD.StringType()), converted=MD.ConvertedType.UTF8)

    # INT64 DECIMAL values copy in bulk into Int64 storage and convert into Int32 storage.
    # A value with more digits than the precision reads as stored when the storage holds it.
    for pageversion in (:v1, :v2), dictionary in (false, true)
        values = Int64[1, -99_999, typemin(Int64), typemax(Int64)]
        wide = rewritelogicalschema(decimal(10, 2),
            Parquet._encodefile((d=values,); pageversion, dictionary))
        table = Parquet.Table(wide)
        @test table.d == reinterpret.(Parquet.DataDecimals.Decimal{10,2,Int64}, values)
        close(table)
        values = Int64[1, -99_999, 100_000, typemin(Int32)]
        narrow = rewritelogicalschema(decimal(5, 2),
            Parquet._encodefile((d=values,); pageversion, dictionary))
        @test Parquet.Table(narrow).d ==
            reinterpret.(Parquet.DataDecimals.Decimal{5,2,Int32}, Int32.(values))
        toolarge = rewritelogicalschema(decimal(5, 2),
            Parquet._encodefile((d=Int64[1, typemax(Int32) + 1],); pageversion, dictionary))
        @test_throws Parquet.UnsupportedFeatureError Parquet.Table(toolarge)
    end

    # PLAIN STRING pages: inline (up to 12 bytes) and view values, ASCII and not,
    # length prefixes of 128 and more, compressed pages (kept as decoded) and
    # uncompressed ones (copied out of the source).
    strings = ["", "short", "exactly12byt", "thirteen byte", "é", "x"^127, "y"^128,
        "naïve café, long enough to be a view", "z"^300, "日本語のテキストです"]
    for codec in (:uncompressed, :snappy), pageversion in (:v1, :v2)
        table = Parquet.Table(Parquet._encodefile((s=strings, o=Union{Missing,String}[
            isodd(i) ? missing : value for (i, value) in enumerate(strings)]);
            codec, pageversion))
        @test table.s == strings
        @test isequal(table.o, [isodd(i) ? missing : v for (i, v) in enumerate(strings)])
        close(table)
    end
    for values in ([UInt8[0xff]], [codeunits("ascii"), UInt8[0xc3, 0x28]],
            [codeunits("x"^200), UInt8[0xe2, 0x82]])
        bytes = rewritelogicalschema(asstring,
            Parquet._encodefile((s=[collect(v) for v in values],)))
        @test_throws Parquet.FormatError Parquet.Table(bytes)
    end

    # Optional fixed-width pages with nulls, PLAIN and dictionary, V1 and V2.
    rng = Random.Xoshiro(3)
    for T in (Int32, Int64, Float32, Float64), dictionary in (false, true),
            pageversion in (:v1, :v2)
        values = Union{Missing,T}[rand(rng) < 0.3 ? missing : T(rand(rng, 1:50))
            for _ in 1:2_000]
        bytes = Parquet._encodefile((x=values,); dictionary, pageversion, pagesize=256)
        table = Parquet.Table(bytes)
        @test isequal(table.x, values)
        close(table)
    end
end

@testset "scan reorders and renames without copying columns" begin
    bytes = Parquet._encodefile((a=collect(1:5), b=collect(6:10), c=collect(11:15)))
    table = Parquet.Table(bytes; scan=Tables.Scan(select=[:c, :a => :x]))
    @test Tables.columnnames(table) == (:c, :x)
    @test table.c == 11:15 && table.x == 1:5
    @test Parquet._tablemetadata(table).schema[2].name == "c"
    @test Parquet._tablemetadata(table).schema[3].name == "x"
    # Column vectors come from the read itself, not a second copy.
    large = Parquet._encodefile((a=collect(1:100_000), b=collect(1:100_000),
        c=collect(1:100_000)))
    inorder, reorder = Tables.Scan(select=[:a, :c]), Tables.Scan(select=[:c, :a])
    Parquet.Table(large; scan=inorder, ntasks=1); Parquet.Table(large; scan=reorder, ntasks=1)
    whole = @allocated Parquet.Table(large; scan=inorder, ntasks=1)
    reordered = @allocated Parquet.Table(large; scan=reorder, ntasks=1)
    @test reordered < whole + 100_000
    twice = Parquet.Table(bytes; scan=Tables.Scan(select=[:a, :a => :y]))
    @test twice.a == twice.y && twice.a !== twice.y
end

@testset "fixed-width DECIMAL pages of up to 8 bytes" begin
    # pyarrow stores every decimal as FIXED_LEN_BYTE_ARRAY (DECIMAL(15, 2) in 7 bytes);
    # values of up to 8 bytes decode with one load each, and a page's last values,
    # whose 8-byte load would run past the page, take the per-value path.
    MD = Parquet.Metadata
    rng = Random.Xoshiro(11)
    for width in 1:8
        precision = min(18, floor(Int, (8width - 1) * log10(2)))
        bound = Int64(10)^precision
        values = Int64[rand(rng, (-bound + 1):(bound - 1)) for _ in 1:300]
        values[1:4] .= (bound - 1, -(bound - 1), 0, -1)
        encode(x) = ntuple(i -> UInt8((x >> (8 * (width - i))) & 0xff), width)
        for pageversion in (:v1, :v2), dictionary in (false, true)
            bytes = rewritelogicalschema(Parquet._encodefile(
                (d=[encode(x) for x in values],); pageversion, dictionary, pagesize=512)) do element
                withlogical(element; logical=MD.LogicalType(DECIMAL=MD.DecimalType(
                    scale=Int32(2), precision=Int32(precision))),
                    converted=MD.ConvertedType.DECIMAL, precision=Int32(precision),
                    scale=Int32(2))
            end
            table = Parquet.Table(bytes)
            @test [Parquet.DataDecimals.unscaled(x) for x in table.d] == values
        end
        # A value at 10^precision, past the declared precision, reads as stored.
        over = rewritelogicalschema(Parquet._encodefile((d=[encode(Int64(1)), encode(bound)],))) do element
            withlogical(element; logical=MD.LogicalType(DECIMAL=MD.DecimalType(
                scale=Int32(2), precision=Int32(precision))),
                converted=MD.ConvertedType.DECIMAL, precision=Int32(precision), scale=Int32(2))
        end
        bound < Int64(2)^(8width - 1) &&
            @test [Parquet.DataDecimals.unscaled(x) for x in Parquet.Table(over).d] == [1, bound]
    end
    # A value its storage integer cannot hold (DECIMAL(3, 0) is stored in Int32), first on
    # the one-load path, then last, on the per-value path.
    encode5(x) = ntuple(i -> UInt8((x >> (8 * (5 - i))) & 0xff), 5)
    for values in (Int64[2^35, 1], Int64[1, 2^35])
        toolarge = rewritelogicalschema(Parquet._encodefile((d=encode5.(values),);
                dictionary=false)) do element
            withlogical(element; logical=MD.LogicalType(DECIMAL=MD.DecimalType(
                scale=Int32(0), precision=Int32(3))),
                converted=MD.ConvertedType.DECIMAL, precision=Int32(3), scale=Int32(0))
        end
        @test_throws Parquet.UnsupportedFeatureError Parquet.Table(toolarge)
    end
end

@testset "stringtype=String and copy(table)" begin
    source = (s=["a", "a string longer than twelve bytes"], ms=["x", missing],
        e=Parquet.LogicalColumn(["lo", "hi"], :enum), l=[["p", "q"], String[]],
        st=[(x=1, y="a"), (x=2, y="b")], m=[["k" => "v"], Pair{String,String}[]],
        ln=[["a", missing], missing], b=[UInt8[1], UInt8[2]])
    bytes = take!(Parquet.write(IOBuffer(), source))
    for ntasks in (1, 4)
        t = Parquet.Table(bytes; stringtype=String, ntasks=ntasks)
        @test t.s isa Vector{String} && t.s == source.s
        @test t.ms isa Vector{Union{Missing,String}} && isequal(t.ms, source.ms)
        @test t.e isa Vector{String} && t.e == ["lo", "hi"]
        @test eltype(t.l) == Parquet.ListValue{String} && t.l == source.l
        @test eltype(t.st) == @NamedTuple{x::Int64, y::String} && t.st == source.st
        @test eltype(t.m) == Parquet.MapValue{String,String} && t.m[1] == Dict("k" => "v")
        @test eltype(t.ln) == Union{Missing,Parquet.ListValue{Union{Missing,String}}}
        @test t.b isa Parquet.DataStrings.BytesVector
        @test Tables.schema(Parquet.File(bytes; stringtype=String)) == Tables.schema(t)
        # Filtered reads, and reads with a nested column, take it too.
        @test Parquet.Table(bytes; stringtype=String, ntasks=ntasks,
            filter=Parquet.colcmp(==, Parquet.col(:s), "a")).s == ["a"]
        filtered = Parquet.Table(bytes; stringtype=String, ntasks=ntasks, select=[:s, :l],
            filter=Parquet.colcmp(==, Parquet.col(:s), "a"))
        @test eltype(filtered.s) == String && eltype(filtered.l) == Parquet.ListValue{String}
    end
    lake = mktempdir()
    Parquet.write(lake, (k=["x", "y"], v=["a", "b"]); partitionby=:k)
    t = Parquet.Table(lake; stringtype=String)
    @test t.k isa Vector{String} && t.v isa Vector{String}
    @test_throws ArgumentError Parquet.Table(bytes; stringtype=Symbol)

    t = Parquet.Table(bytes)
    c = copy(t)
    @test c isa Parquet.Table && Tables.schema(c) == Tables.schema(t)
    @test c.l isa Vector && c.st isa Vector && c.l == t.l && c.st == t.st
    c.s[1] = "changed"
    push!(c.l, c.l[1])
    @test t.s[1] == "a" && length(t.l) == 2
end

@testset "metadata display and typed statistics" begin
    source = (id=[1, 2, 3, 4], name=["b", "a", missing, "d"],
        day=[Date(2024, 1, 2), Date(2024, 1, 1), Date(2025, 3, 1), Date(2023, 5, 5)],
        tags=[[1], [2], Int[], [3]])
    bytes = take!(Parquet.write(IOBuffer(), source; rowgroupsize=2,
        metadata=Dict("owner" => "me")))
    file = Parquet.File(bytes)
    text = sprint(show, MIME"text/plain"(), Parquet.metadata(file))
    @test startswith(text, "Parquet file metadata: 4 rows in 2 row groups, 4 columns")
    @test occursin("columns: id, name, day, tags", text) && occursin("owner", text) &&
        occursin("created by: Parquet.jl", text) && length(text) < 400
    @test Parquet.statistics(file, :id) ==
        [(rows=2, nulls=0, min=1, max=2), (rows=2, nulls=0, min=3, max=4)]
    @test Parquet.statistics(file, "name") ==
        [(rows=2, nulls=0, min="a", max="b"), (rows=2, nulls=1, min="d", max="d")]
    @test Parquet.statistics(file, 3)[2] ==
        (rows=2, nulls=0, min=Date(2023, 5, 5), max=Date(2025, 3, 1))
    @test_throws ArgumentError Parquet.statistics(file, :tags)
    @test_throws ArgumentError Parquet.statistics(file, :nope)
    @test_throws ArgumentError Parquet.statistics(file, 5)
    close(file)
    bare = Parquet.File(take!(Parquet.write(IOBuffer(), source; statistics=false)))
    @test Parquet.statistics(bare, :id) == [(rows=4, nulls=nothing, min=nothing, max=nothing)]
    close(bare)
end

@testset "select and drop: functions, and errors in the keywords' terms" begin
    bytes = take!(Parquet.write(IOBuffer(), (a=[1], b=[2], c=[3])))
    @test Tables.columnnames(Parquet.Table(bytes; select=(i, name) -> name != :b)) == (:a, :c)
    @test Tables.columnnames(Parquet.Table(bytes; drop=(i, name) -> i == 1)) == (:b, :c)
    @test Tables.columnnames(Parquet.File(bytes; select=(i, name) -> i > 1)) == (:b, :c)
    lake = mktempdir()
    Parquet.write(joinpath(lake, "x.parquet"), (a=[1], b=[2]))
    @test Tables.columnnames(Parquet.Table(lake; select=(i, name) -> name === :b)) == (:b,)
    for (keywords, needle) in (((select=[:nope],), "select names no column :nope"),
            ((select=:nope,), "select names no column :nope"),
            ((select=[5],), "select names no column 5"),
            ((select=r"zz",), "select names no column r\"zz\""),
            ((drop=[:nope],), "drop names no column :nope"))
        err = try
            Parquet.Table(bytes; keywords...)
        catch e
            e
        end
        @test err isa ArgumentError && occursin(needle, err.msg) &&
            occursin("the columns are a, b, c", err.msg) && !occursin("validate", err.msg) &&
            !occursin("Not(", err.msg)
    end
end

@testset "Table and File container conveniences" begin
    path = tempname() * ".parquet"
    Parquet.write(path, (a=[1, 2, 3], b=["x", "y", "z"]); rowgroupsize=2)
    table = Parquet.Table(path)
    @test names(table) == ["a", "b"] && table["b"] == ["x", "y", "z"]
    file = Parquet.File(path; select=[:b])
    @test names(file) == ["b"]
    @test Parquet.Table(file).b == ["x", "y", "z"]
    @test_throws ArgumentError Parquet.Table(file; select=[:a])
    @test file[end].b == ["z"] && file[begin].b == ["x", "y"] && lastindex(file) == 2
    close(file)
    err = try
        file[1]
    catch e
        e
    end
    @test err isa ArgumentError && occursin("Parquet.File was closed", err.msg)
    rm(path)
end

@testset "page errors name the row group and column" begin
    source = (a=collect(1:1000), b=[string(i) for i in 1:1000], l=[[i] for i in 1:1000])
    bytes = take!(Parquet.write(IOBuffer(), source; rowgroupsize=500, compress=:zstd,
        checksum=true))
    md = Parquet.metadata(Parquet.File(bytes))
    for (column, path) in ((2, "b"), (3, "l.list.element"))
        chunk = md.row_groups[2].columns[column].meta_data
        corrupt = copy(bytes)
        offset = chunk.data_page_offset + chunk.total_compressed_size ÷ 2
        corrupt[offset:(offset + 7)] .= 0x5a
        for keywords in ((;), (ntasks=1,), (filter=Parquet.col(:a) > 400,))
            err = try
                Parquet.Table(corrupt; checksum=true, keywords...)
            catch e
                e
            end
            @test err isa Parquet.FormatError &&
                startswith(err.message, "row group 2, column $path: ")
        end
    end
end

@testset "128-bit integers" begin
    # Polars 1.43.2 stores Int128/UInt128 as FIXED_LEN_BYTE_ARRAY(16) that its ARROW:schema
    # calls Int(128): little-endian bytes.
    fixture(name) = Parquet.Table(joinpath(@__DIR__, "fixtures", name))
    @test isequal(fixture("polars_int128.parquet").i, Union{Missing,Int128}[1, -1,
        Int128(2)^100, typemin(Int128), typemax(Int128), missing, 0])
    @test isequal(fixture("polars_uint128.parquet").u,
        Union{Missing,UInt128}[1, typemax(UInt128), missing])
    # Parquet.jl writes them the same way, marked with Arrow.jl's JuliaLang.(U)Int128
    # extension, and reads them back as written, flat and nested.
    source = (i=Int128[1, -1, typemin(Int128), typemax(Int128), Int128(2)^100],
        u=UInt128[0, 1, typemax(UInt128), 3, 5],
        m=Union{Missing,Int128}[missing, 7, -7, missing, 0],
        l=[Int128[1, 2], Int128[], Int128[-3], Int128[], Int128[4]],
        s=[(a=Int128(k), b=k) for k in 1:5])
    bytes = take!(Parquet.write(IOBuffer(), source; colmetadata=Dict(:i => ["unit" => "n"])))
    table = Parquet.Table(bytes)
    for name in keys(source)
        @test isequal(collect(Tables.getcolumn(table, name)), getproperty(source, name))
    end
    @test eltype(table.l) == Parquet.ListValue{Int128} && eltype(table.m) == Union{Missing,Int128}
    md = Parquet.metadata(Parquet.File(bytes))
    @test md.schema[2].type_ == Parquet.Metadata.Type.FIXED_LEN_BYTE_ARRAY &&
        md.schema[2].type_length == 16 && md.schema[2].logicalType === nothing
    arrow = Parquet._decodearrowschema(only(entry.value for entry in md.key_value_metadata
        if entry.key == "ARROW:schema"), Parquet.Limits())
    @test ("ARROW:extension:name" => "JuliaLang.UInt128") in arrow[2].metadata
    # The extension is the column's type, not metadata.
    @test DataAPI.colmetadata(table, :i) == Dict("unit" => "n")
    # Statistics order the bytes, so they bound no integers and prune nothing.
    @test Parquet.statistics(Parquet.File(bytes), :i) ==
        [(rows=5, nulls=0, min=nothing, max=nothing)]
    @test Parquet.Table(bytes; filter=Parquet.col(:i) > 1).i == [typemax(Int128), Int128(2)^100]
    @test Parquet.Table(bytes; filter=Parquet.col(:u) < 4, select=[:u]).u == UInt128[0, 1, 3]
    @test isequal(Parquet.Table(take!(Parquet.write(IOBuffer(), table))).m, source.m)
end

@testset "row group ordinals do not matter" begin
    # parquet-java writes RowGroup.ordinal as an i16, negative past 32,767 row groups.
    bytes = take!(Parquet.write(IOBuffer(), (x=collect(1:6),); rowgroupsize=2))
    n = length(bytes)
    start = n - 8 - Int(Parquet._readu32le(bytes, n - 7))
    metadata = Parquet.Thrift.decode(view(bytes, (start + 1):(n - 8)),
        Parquet.Metadata.FileMetaData)
    groups = [Parquet._withfields(group; ordinal=Int16(-32768 + index))
        for (index, group) in enumerate(metadata.row_groups)]
    wrapped = tablerewritefooter(bytes, Parquet._withfields(metadata; row_groups=groups))
    @test Parquet.Table(wrapped).x == 1:6
    @test Parquet.File(wrapped)[3].x == [5, 6]
end
