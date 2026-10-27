using Tables
using Dates
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
    )
    messages = ("negative row count", "negative total byte size",
        "negative compressed byte size", "negative file offset",
        "outside the file body", "outside the file body", "negative ordinal",
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

    # Past 1024 bytes a fixed width reads as DataBytes.
    wide = [Tuple(fill(UInt8(i), 1025)) for i in 1:2]
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
        @test_throws Parquet.FormatError Parquet.Table(corrupt)
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

    # INT64 DECIMAL values copy in bulk and are then checked against the precision.
    D = Parquet.DataDecimals.Decimal{5,2,Int64}
    for pageversion in (:v1, :v2), dictionary in (false, true)
        good = rewritelogicalschema(decimal(5, 2),
            Parquet._encodefile((d=Int64[1, -99_999, 99_999],); pageversion, dictionary))
        table = Parquet.Table(good)
        @test table.d == [reinterpret(D, Int64(v)) for v in (1, -99_999, 99_999)]
        close(table)
        bad = rewritelogicalschema(decimal(5, 2),
            Parquet._encodefile((d=Int64[1, 100_000],); pageversion, dictionary))
        @test_throws Parquet.FormatError Parquet.Table(bad)
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
        # A value at 10^precision is past the declared precision.
        over = rewritelogicalschema(Parquet._encodefile((d=[encode(Int64(1)), encode(bound)],))) do element
            withlogical(element; logical=MD.LogicalType(DECIMAL=MD.DecimalType(
                scale=Int32(2), precision=Int32(precision))),
                converted=MD.ConvertedType.DECIMAL, precision=Int32(precision), scale=Int32(2))
        end
        bound < Int64(2)^(8width - 1) && @test_throws Parquet.FormatError Parquet.Table(over)
    end
end
