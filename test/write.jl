using Dates
using SHA

struct DeferredWriterName <: AbstractString
    converted::Base.RefValue{Bool}
    bytes::Int
end

function Base.ncodeunits(name::DeferredWriterName)
    return name.bytes
end

function Base.String(name::DeferredWriterName)
    name.converted[] = true
    return "deferred"
end

function writtenpages(bytes::Vector{UInt8}, column::Int)
    file = Parquet.File(bytes)
    metadata = TH.decode(file.footer.bytes, MD.FileMetaData)
    chunk = metadata.row_groups[1].columns[column].meta_data
    start, stop = Parquet._chunkrange(chunk, file.footer.offset)
    pages = NamedTuple[]
    position = start
    while position < stop
        frame = Parquet.readpage(file.source, position, stop, Parquet.Limits())
        push!(pages, (header=frame.header, payload=collect(frame.payload)))
        position = Parquet.pageend(frame)
    end
    close(file)
    return pages, chunk
end

struct FooterTestValue{B<:AbstractVector{UInt8}}
    bytes::B
end

function TH.encode!(writer::TH.Writer, value::FooterTestValue)
    append!(writer.buffer, value.bytes)
    return
end

mutable struct FooterPhaseValue
    actions::Vector{Any}
    calls::Int
end

function TH.encode!(writer::TH.Writer, value::FooterPhaseValue)
    value.calls += 1
    action = value.actions[value.calls]
    action isa Exception && throw(action)
    append!(writer.buffer, action)
    return
end

struct FooterVirtualBytes <: AbstractVector{UInt8}
    count::Int
end

Base.size(bytes::FooterVirtualBytes) = (bytes.count,)
Base.getindex(::FooterVirtualBytes, ::Int) =
    throw(AssertionError("virtual footer bytes must not be materialized"))

struct FooterOverflowValue end

function TH.encode!(writer::TH.Writer, ::FooterOverflowValue)
    append!(writer.buffer, FooterVirtualBytes(typemax(Int)))
    push!(writer.buffer, UInt8(0))
    return
end

function minimalfootermetadata(; unknown_fields=())
    return MD.FileMetaData(
        version=Int32(1),
        schema=MD.SchemaElement[MD.SchemaElement(name="schema",
            num_children=Int32(0))],
        num_rows=Int64(0),
        row_groups=MD.RowGroup[],
        created_by="footer-test",
        unknown_fields=unknown_fields,
    )
end

function boundedfooter(value; limits=Parquet.Limits())
    budget = Parquet._LiveByteBudget(limits)
    bytes, charge = Parquet._writeencodefooter(value, limits, budget)
    @test Parquet._budgetused(budget) == charge
    Parquet._release!(budget, charge)
    @test Parquet._budgetused(budget) == 0
    return bytes
end

@testset "exact bounded footer encoding" begin
    metadata = minimalfootermetadata()
    dynamic = TH.encode(metadata)
    exact = Int64(length(dynamic))
    valuecharge = Parquet._materializedarraybytes(UInt8, exact)
    controlcharge = Parquet._WRITE_FOOTER_CONTROL_BYTES
    entry = Int64(17)
    maximum = entry + controlcharge + valuecharge
    limits = Parquet.Limits(max_footer_bytes=exact,
        max_materialized_bytes=maximum)
    budget = Parquet._LiveByteBudget(limits)
    Parquet._reserve!(budget, entry)
    bytes, charge = Parquet._writeencodefooter(metadata, limits, budget)
    @test bytes == dynamic
    @test charge == valuecharge
    @test Parquet._budgetused(budget) == entry + charge
    Parquet._release!(budget, charge)
    @test Parquet._budgetused(budget) == entry

    footerbudget = Parquet._LiveByteBudget(Parquet.Limits())
    Parquet._reserve!(footerbudget, entry)
    footererr = try
        Parquet._writeencodefooter(metadata,
            Parquet.Limits(max_footer_bytes=exact - 1), footerbudget)
        nothing
    catch caught
        caught
    end
    @test footererr isa Parquet.LimitError
    @test footererr.resource == :footer_bytes
    @test footererr.requested == exact
    @test footererr.maximum == exact - 1
    @test Parquet._budgetused(footerbudget) == entry

    tightlimits = Parquet.Limits(max_footer_bytes=exact,
        max_materialized_bytes=maximum - 1)
    tightbudget = Parquet._LiveByteBudget(tightlimits)
    Parquet._reserve!(tightbudget, entry)
    materialerr = try
        Parquet._writeencodefooter(metadata, tightlimits, tightbudget)
        nothing
    catch caught
        caught
    end
    @test materialerr isa Parquet.LimitError
    @test materialerr.resource == :materialized_bytes
    @test materialerr.requested == maximum
    @test materialerr.maximum == maximum - 1
    @test Parquet._budgetused(tightbudget) == entry

    verbatim = TH.RawField(Int16(10), TH.I32, Int16(6), Int8(2),
        UInt8[TH.I32, 0x14, 0x02])
    preserved = minimalfootermetadata(unknown_fields=(verbatim,))
    preservedbytes = boundedfooter(preserved)
    @test preservedbytes == TH.encode(preserved)
    @test preservedbytes[(end - 3):end] == UInt8[TH.I32, 0x14, 0x02, TH.STOP]
    synthesized = TH.RawField(Int16(10), TH.I32, Int16(5), Int8(2),
        UInt8[TH.I32, 0x14, 0x02])
    canonical = minimalfootermetadata(unknown_fields=(synthesized,))
    canonicalbytes = boundedfooter(canonical)
    @test canonicalbytes == TH.encode(canonical)
    @test canonicalbytes[(end - 2):end] == UInt8[0x45, 0x02, TH.STOP]

    nestedraw = TH.RawField(Int16(11), TH.I32, Int16(5), Int8(2),
        UInt8[TH.I32, 0x16, 0x0e])
    topraw = TH.RawField(Int16(12), TH.I32, Int16(6), Int8(2),
        UInt8[TH.I32, 0x18, 0x12])
    nestedmetadata = MD.FileMetaData(
        version=Int32(1),
        schema=MD.SchemaElement[MD.SchemaElement(name="schema",
            num_children=Int32(0), unknown_fields=(nestedraw,))],
        num_rows=Int64(0),
        row_groups=MD.RowGroup[],
        created_by="footer-test",
        unknown_fields=(topraw,),
    )
    original = TH.encode(nestedmetadata)
    decoded = TH.decode(original, MD.FileMetaData)
    @test only(decoded.schema).unknown_fields == [nestedraw]
    @test decoded.unknown_fields == [topraw]
    @test TH.encode(decoded) == original
    @test boundedfooter(decoded) == original

    rowgroup = MD.RowGroup(columns=MD.ColumnChunk[], total_byte_size=Int64(0),
        num_rows=Int64(0))
    large = MD.FileMetaData(version=Int32(1), schema=metadata.schema,
        num_rows=Int64(0), row_groups=fill(rowgroup, 4096),
        created_by=metadata.created_by)
    largedynamic = TH.encode(large)
    largeexact = Int64(length(largedynamic))
    largebytes = boundedfooter(large; limits=Parquet.Limits(
        max_footer_bytes=largeexact,
        max_materialized_bytes=Parquet._WRITE_FOOTER_CONTROL_BYTES +
            Parquet._materializedarraybytes(UInt8, largeexact)))
    @test largebytes == largedynamic

    for (actions, expected) in (
            (Any[UInt8[1], UInt8[1, 2]], AssertionError),
            (Any[UInt8[1, 2], UInt8[1]], AssertionError))
        phase = FooterPhaseValue(actions, 0)
        phasebudget = Parquet._LiveByteBudget(Parquet.Limits())
        Parquet._reserve!(phasebudget, entry)
        @test_throws expected Parquet._writeencodefooter(phase,
            Parquet.Limits(), phasebudget)
        @test Parquet._budgetused(phasebudget) == entry
    end
    sentinel = ErrorException("footer second-pass sentinel")
    throwing = FooterPhaseValue(Any[UInt8[1], sentinel], 0)
    throwbudget = Parquet._LiveByteBudget(Parquet.Limits())
    Parquet._reserve!(throwbudget, entry)
    thrown = try
        Parquet._writeencodefooter(throwing, Parquet.Limits(), throwbudget)
        nothing
    catch caught
        caught
    end
    @test thrown === sentinel
    @test Parquet._budgetused(throwbudget) == entry

    countsentinel = ErrorException("footer count-pass sentinel")
    countthrowing = FooterPhaseValue(Any[countsentinel], 0)
    countbudget = Parquet._LiveByteBudget(Parquet.Limits())
    Parquet._reserve!(countbudget, entry)
    countthrown = try
        Parquet._writeencodefooter(countthrowing, Parquet.Limits(), countbudget)
        nothing
    catch caught
        caught
    end
    @test countthrown === countsentinel
    @test Parquet._budgetused(countbudget) == entry

    wiremaximum = Int64(typemax(UInt32))
    boundary = FooterTestValue(FooterVirtualBytes(Int(wiremaximum)))
    boundarylimits = Parquet.Limits(max_footer_bytes=wiremaximum - 1)
    boundarybudget = Parquet._LiveByteBudget(boundarylimits)
    Parquet._reserve!(boundarybudget, entry)
    boundaryerr = try
        Parquet._writeencodefooter(boundary, boundarylimits, boundarybudget)
        nothing
    catch caught
        caught
    end
    @test boundaryerr isa Parquet.LimitError
    @test boundaryerr.resource == :footer_bytes
    @test boundaryerr.requested == wiremaximum
    @test boundaryerr.maximum == wiremaximum - 1
    @test Parquet._budgetused(boundarybudget) == entry

    wireexact = wiremaximum + 1
    virtual = FooterTestValue(FooterVirtualBytes(Int(wireexact)))
    for maximum in (wireexact - 1, typemax(Int64))
        virtualbudget = Parquet._LiveByteBudget(Parquet.Limits())
        Parquet._reserve!(virtualbudget, entry)
        virtualerr = try
            Parquet._writeencodefooter(virtual,
                Parquet.Limits(max_footer_bytes=maximum), virtualbudget)
            nothing
        catch caught
            caught
        end
        @test virtualerr isa ArgumentError
        @test virtualerr isa ArgumentError &&
            virtualerr.msg == "Parquet footer exceeds UInt32 bytes"
        @test Parquet._budgetused(virtualbudget) == entry
    end

    for limits in (Parquet.Limits(),
            Parquet.Limits(max_footer_bytes=typemax(Int64)))
        overflowbudget = Parquet._LiveByteBudget(limits)
        Parquet._reserve!(overflowbudget, entry)
        overflowerr = try
            Parquet._writeencodefooter(FooterOverflowValue(), limits,
                overflowbudget)
            nothing
        catch caught
            caught
        end
        @test overflowerr isa ArgumentError
        @test overflowerr isa ArgumentError &&
            overflowerr.msg == "Parquet footer exceeds UInt32 bytes"
        @test Parquet._budgetused(overflowbudget) == entry
    end
end

@testset "footer limit leaves public destinations unchanged" begin
    table = (value=Int32[1, 2],)
    reference = Parquet._encodefile(table)
    file = Parquet.File(reference)
    exact = Int64(length(file.footer.bytes))
    close(file)
    exactio = IOBuffer()
    Parquet.write(exactio, table;
        limits=Parquet.Limits(max_footer_bytes=exact))
    @test take!(exactio) == reference
    limits = Parquet.Limits(max_footer_bytes=exact - 1)
    sentinel = UInt8[0xde, 0xad, 0xbe, 0xef]
    io = IOBuffer()
    Base.write(io, sentinel)
    err = try
        Parquet.write(io, table; limits=limits)
        nothing
    catch caught
        caught
    end
    @test err isa Parquet.LimitError
    @test err.resource == :footer_bytes
    @test err.requested == exact
    @test take!(io) == sentinel
    mktempdir() do directory
        existing = joinpath(directory, "existing.parquet")
        Base.write(existing, sentinel)
        @test_throws Parquet.LimitError Parquet.write(existing, table;
            limits=limits)
        @test read(existing) == sentinel
        missing = joinpath(directory, "missing.parquet")
        @test_throws Parquet.LimitError Parquet.write(missing, table;
            limits=limits)
        @test !ispath(missing)
    end
end

@testset "writer allocation accounting" begin
    duplicatebudget = Parquet._LiveByteBudget(Parquet.Limits())
    @test_throws ArgumentError Parquet._validatewritecolumnnames(
        Symbol[:duplicate, :duplicate], duplicatebudget)
    @test Parquet._budgetused(duplicatebudget) == 0

    invalidbudget = Parquet._LiveByteBudget(Parquet.Limits())
    @test_throws ArgumentError Parquet._validatewritecolumnnames(
        Any[1], invalidbudget)
    @test Parquet._budgetused(invalidbudget) == 0

    converted = Ref(false)
    deferred = DeferredWriterName(converted, 10_000)
    preflightbudget = Parquet._LiveByteBudget(
        Parquet.Limits(max_materialized_bytes=512))
    @test_throws Parquet.LimitError Parquet._validatewritecolumnnames(
        AbstractString[deferred], preflightbudget)
    @test !converted[]
    @test Parquet._budgetused(preflightbudget) == 0

    source = Parquet._writecolumn(:value, Int32[1])
    field = Parquet._writefieldplan(source)
    elements = MD.SchemaElement[
        MD.SchemaElement(name="schema", num_children=Int32(1)),
        field.schema...,
    ]
    schema = Parquet.Schema(elements)
    leafbudget = Parquet._LiveByteBudget(Parquet.Limits())
    leaves, leafcharge = Parquet._writeplanleaves(
        Parquet.WriteFieldPlan[field], schema, 1, Parquet.Limits(),
        leafbudget)
    prefixcharge = Parquet._materializedproduct(3,
        Parquet._materializedarraybytes(Int64, 2))
    @test length(leaves) == 1
    @test leafcharge == Parquet._writeleafplanbytes(schema.leaves) +
        prefixcharge
    @test Parquet._budgetused(leafbudget) == leafcharge
    Parquet._release!(leafbudget, leafcharge)
    @test Parquet._budgetused(leafbudget) == 0

    zerocolumn = Parquet._writecolumn(:value, Int32[])
    zerofield = Parquet._writefieldplan(zerocolumn)
    zerobudget = Parquet._LiveByteBudget(Parquet.Limits())
    zeroplan = Parquet._writeplan(Parquet.WriteFieldPlan[zerofield], 0,
        Parquet.Limits(), zerobudget)
    onecolumn = Parquet._writecolumn(:value, Int32[1])
    onefield = Parquet._writefieldplan(onecolumn)
    onebudget = Parquet._LiveByteBudget(Parquet.Limits())
    oneplan = Parquet._writeplan(Parquet.WriteFieldPlan[onefield], 1,
        Parquet.Limits(), onebudget)
    rowgroupcharge = Parquet._materializedarraybytes(
        Parquet.WriteRowGroupPlan, 1) - Parquet._materializedarraybytes(
        Parquet.WriteRowGroupPlan, 0)
    @test isempty(zeroplan.rowgroups)
    @test length(oneplan.rowgroups) == 1
    @test Parquet._budgetused(onebudget) - Parquet._budgetused(zerobudget) ==
        Parquet._writeleafplanbytes(oneplan.schema.leaves) + rowgroupcharge +
        prefixcharge
end

@testset "file writer schema ownership" begin
    days = Union{Missing,Vector{Union{Missing,Date}}}[
        missing,
        Union{Missing,Date}[Date(2020, 1, 2), missing],
    ]
    input = (id=Int32[1, 2], days=days)
    plan = Parquet._writeplan(input)
    @test plan.rows == 2
    @test length(plan.rowgroups) == 1
    @test plan.elements[1].num_children == 2
    @test length(plan.schema.root.children) == 2
    @test [element.name for element in plan.elements] ==
        ["schema", "id", "days", "list", "element"]
    rowgroup = only(plan.rowgroups)
    @test rowgroup.rows == plan.rows
    @test [leaf.ordinal for leaf in rowgroup.leaves] == Int32[1, 2]
    @test [leaf.path for leaf in rowgroup.leaves] ==
        [["id"], ["days", "list", "element"]]
    @test [leaf.path for leaf in rowgroup.leaves] ==
        [leaf.path for leaf in plan.schema.leaves]
    @test all(isempty(leaf.column.schema) for leaf in rowgroup.leaves)
    @test all(leaf.column.path == leaf.path for leaf in rowgroup.leaves)
    bytes = Parquet._encodefile(input)
    file = Parquet.File(bytes)
    metadata = TH.decode(file.footer.bytes, MD.FileMetaData)
    @test metadata.schema == plan.elements
    @test metadata.num_rows == plan.rows
    @test metadata.row_groups[1].num_rows == rowgroup.rows
    @test [chunk.meta_data.path_in_schema for chunk in metadata.row_groups[1].columns] ==
        [leaf.path for leaf in rowgroup.leaves]
    close(file)

    leftsource = Parquet._writecolumn(:left, Int32[1, 2])
    rightsource = Parquet._writecolumn(:right, Float64[1, 2])
    left = Parquet._withoutcolumnschema(leftsource, ["pair", "left"])
    right = Parquet._withoutcolumnschema(rightsource, ["pair", "right"])
    group = MD.SchemaElement(name="pair", num_children=Int32(2),
        repetition_type=MD.FieldRepetitionType.REQUIRED)
    field = Parquet.WriteFieldPlan(
        MD.SchemaElement[group, only(leftsource.schema), only(rightsource.schema)],
        Parquet.WriteColumn[left, right])
    grouped = Parquet._writeplan(Parquet.WriteFieldPlan[field], 2, Parquet.Limits())
    @test grouped.elements[1].num_children == 1
    @test length(grouped.schema.root.children) == 1
    @test length(grouped.schema.leaves) == 2
    @test [leaf.ordinal for leaf in only(grouped.rowgroups).leaves] == Int32[1, 2]
    @test [leaf.path for leaf in only(grouped.rowgroups).leaves] ==
        [["pair", "left"], ["pair", "right"]]
    @test_throws Parquet.UnsupportedFeatureError Parquet._validatewritechoicecount(
        only(grouped.rowgroups).leaves,
        Parquet.WriteEncodingChoice[Parquet.WriteEncodingChoice(nothing, false)])
    incomplete = Parquet.WriteFieldPlan(field.schema, Parquet.WriteColumn[left])
    @test_throws ArgumentError Parquet._writeplan(
        Parquet.WriteFieldPlan[incomplete], 2, Parquet.Limits())

    emptyplan = Parquet._writeplan((value=Int32[],))
    @test emptyplan.rows == 0
    @test isempty(emptyplan.rowgroups)
    @test emptyplan.elements[1].num_children == 1
    for pageversion in (:v1, :v2)
        emptybytes = Parquet._encodefile((value=Int32[],); pageversion=pageversion)
        emptyfile = Parquet.File(emptybytes)
        emptymetadata = TH.decode(emptyfile.footer.bytes, MD.FileMetaData)
        @test emptymetadata.num_rows == 0
        @test isempty(emptymetadata.row_groups)
        @test [element.name for element in emptymetadata.schema] == ["schema", "value"]
        close(emptyfile)
        emptytable = Parquet.Table(emptybytes)
        @test emptytable.columns.value == Int32[]
        close(emptytable)
    end
    @test_throws ArgumentError Parquet._encodefile((value=Int32[],);
        encoding=(unknown=:plain,))
end

@testset "writer output is deterministic with row-group offsets" begin
    flat = (
        id=Int32[1, 2, 3],
        label=Union{Missing,String}["a", missing, "b"],
    )
    @test bytes2hex(sha256(Parquet._encodefile(flat; statistics=false))) ==
        "bd0f5655e9f9aca2a1aa5f1d2721d9ea8f5ca3c9a2714385cca49edfcb7c0256"
    @test bytes2hex(sha256(Parquet._encodefile(flat; pageindex=false,
        statistics=false))) ==
        "2d33126eb969251001cb972c25a054f27274de3e6b3684dec5a7fa0a12d9beb4"
    dictionary = (
        id=fill(Int64(7), 64),
        flag=Union{Missing,Bool}[isodd(index) ? true : missing for index in 1:64],
    )
    @test bytes2hex(sha256(Parquet._encodefile(dictionary;
        pageversion=:v2, dictionary=true, statistics=false))) ==
        "4c65c27c3b2eeb850b74ea6c5e76454a44480bcb774724c88bdad955b10a87e5"
    @test bytes2hex(sha256(Parquet._encodefile(dictionary;
        pageversion=:v2, dictionary=true, pageindex=false,
        statistics=false))) ==
        "979f70b637a8225075e6822836e1bd4d3041fa105a87155a3d4583ac1a108996"
    lists = (
        days=Union{Missing,Vector{Union{Missing,Date}}}[
            missing,
            Union{Missing,Date}[],
            Union{Missing,Date}[Date(2020, 1, 2), missing],
        ],
    )
    @test bytes2hex(sha256(Parquet._encodefile(lists;
        pageversion=:v2, encoding=:plain, statistics=false))) ==
        "d7a97e35b231befee376faf76c9ee8fb01ffcae60631b7113bd029ac803168b7"
    @test bytes2hex(sha256(Parquet._encodefile(lists;
        pageversion=:v2, encoding=:plain, pageindex=false,
        statistics=false))) ==
        "c9ba5e006a12d3873e98b8b7948f6c35d2e090b1d3d09ac9036895d892e48afd"
end

@testset "PLAIN V1 writer metadata" begin
    raw = Vector{UInt8}[UInt8[0x00, 0xff], UInt8[], UInt8[0x41]]
    table = (
        i32=Int32[1, -2, 3],
        i64=Int64[typemin(Int64), 0, typemax(Int64)],
        flag=Bool[true, false, true],
        f32=Float32[1.5, -0.0, Inf],
        f64=Float64[NaN, 2.5, -Inf],
        text=["alpha", "", "κ"],
        raw=raw,
        optional=Union{Missing,Int32}[1, missing, -3],
    )
    bytes = Parquet._encodefile(table)
    @test bytes[1:4] == Parquet.PARQUET_MAGIC
    @test bytes[(end - 3):end] == Parquet.PARQUET_MAGIC
    file = Parquet.File(bytes)
    metadata = Parquet.Thrift.decode(copy(file.footer.bytes), Parquet.Metadata.FileMetaData)
    @test metadata.version == 1
    @test metadata.num_rows == 3
    @test metadata.created_by == "Parquet.jl version 1.0.0-DEV"
    @test length(metadata.row_groups) == 1
    @test length(metadata.schema) == 9
    @test [element.name for element in metadata.schema[2:end]] == collect(String.(keys(table)))
    @test metadata.schema[7].logicalType.STRING !== nothing
    @test metadata.schema[7].converted_type == Parquet.Metadata.ConvertedType.UTF8
    @test metadata.schema[end].repetition_type == Parquet.Metadata.FieldRepetitionType.OPTIONAL
    group = metadata.row_groups[1]
    @test group.num_rows == 3
    @test group.total_byte_size == group.total_compressed_size
    @test all(chunk.file_offset == 0 for chunk in group.columns)
    @test all(chunk.meta_data.codec == Parquet.Metadata.CompressionCodec.UNCOMPRESSED for chunk in group.columns)
    @test all(chunk.meta_data.num_values == 3 for chunk in group.columns)
    @test all(chunk.meta_data.dictionary_page_offset === nothing for chunk in group.columns)
    close(file)
end


@testset "DATE and canonical optional LIST writer" begin
    days = Union{Missing,Vector{Union{Missing,Date}}}[
        missing,
        Union{Missing,Date}[],
        Union{Missing,Date}[missing],
        Union{Missing,Date}[Date(1970, 1, 1), missing, Date(1969, 12, 31)],
        Union{Missing,Date}[Date(2000, 2, 29)],
    ]
    expectedrepetition = UInt64[0, 0, 0, 0, 1, 1, 0]
    expecteddefinition = UInt64[0, 1, 2, 3, 2, 3, 3]
    expectedphysical = Int32[0, -1, 11016]
    input = (id=Int32[1, 2, 3, 4, 5], days=days)
    for pageversion in (:v1, :v2)
        bytes = Parquet._encodefile(input; pageversion=pageversion)
        file = Parquet.File(bytes)
        metadata = TH.decode(file.footer.bytes, MD.FileMetaData)
        @test [element.name for element in metadata.schema] ==
            ["schema", "id", "days", "list", "element"]
        @test metadata.schema[3].logicalType.LIST !== nothing
        @test metadata.schema[3].converted_type == MD.ConvertedType.LIST
        @test metadata.schema[4].repetition_type == MD.FieldRepetitionType.REPEATED
        @test metadata.schema[5].logicalType.DATE !== nothing
        @test metadata.schema[5].converted_type == MD.ConvertedType.DATE
        close(file)

        pages, chunk = writtenpages(bytes, 2)
        @test chunk.path_in_schema == ["days", "list", "element"]
        @test chunk.num_values == 7
        @test length(pages) == 1
        page = only(pages)
        payload = page.payload
        if pageversion === :v1
            header = page.header.data_page_header
            @test header.num_values == 7
            repetition, position = Parquet.decode_hybrid(payload, 7, 1;
                length_prefix=true)
            definition, position = Parquet.decode_hybrid(payload, 7, 2;
                offset=position, length_prefix=true)
            physical, position = Parquet.decode_plain(Int32, payload, 3;
                offset=position)
            @test repetition == expectedrepetition
            @test definition == expecteddefinition
            @test physical == expectedphysical
            @test position == length(payload) + 1
        else
            header = page.header.data_page_header_v2
            @test header.num_values == 7
            @test header.num_rows == 5
            @test header.num_nulls == 4
            repetitionlength = Int(header.repetition_levels_byte_length)
            definitionlength = Int(header.definition_levels_byte_length)
            repetitionbytes = @view payload[1:repetitionlength]
            definitionstart = repetitionlength + 1
            definitionstop = repetitionlength + definitionlength
            definitionbytes = @view payload[definitionstart:definitionstop]
            valuebytes = @view payload[(definitionstop + 1):end]
            repetition, repetitionposition = Parquet.decode_hybrid(
                repetitionbytes, 7, 1)
            definition, definitionposition = Parquet.decode_hybrid(
                definitionbytes, 7, 2)
            physical, valueposition = Parquet.decode_plain(Int32, valuebytes, 3)
            @test repetition == expectedrepetition
            @test definition == expecteddefinition
            @test physical == expectedphysical
            @test repetitionposition == length(repetitionbytes) + 1
            @test definitionposition == length(definitionbytes) + 1
            @test valueposition == length(valuebytes) + 1
        end
        table = Parquet.Table(bytes)
        @test table.columns.id == input.id
        @test isequal(table.columns.days, days)
        close(table)
    end

    for pageversion in (:v1, :v2), encoding in (:plain, :delta_binary_packed)
        bytes = Parquet._encodefile((days=days,); pageversion=pageversion,
            encoding=encoding, codec=:snappy)
        table = Parquet.Table(bytes)
        @test isequal(table.columns.days, days)
        close(table)
    end

    repeated = fill(Union{Missing,Date}[Date(2000, 2, 29), Date(2000, 2, 29)], 256)
    for pageversion in (:v1, :v2)
        bytes = Parquet._encodefile((days=repeated,); pageversion=pageversion,
            dictionary=true, codec=:snappy)
        _, chunk = writtenpages(bytes, 1)
        @test chunk.dictionary_page_offset !== nothing
        @test MD.Encoding.RLE_DICTIONARY in chunk.encodings
        table = Parquet.Table(bytes)
        @test isequal(table.columns.days, repeated)
        close(table)
    end

    dates = Union{Missing,Date}[Date(1969, 12, 31), missing, Date(2000, 2, 29)]
    bytes = Parquet._encodefile((date=dates,); pageversion=:v2)
    file = Parquet.File(bytes)
    metadata = TH.decode(file.footer.bytes, MD.FileMetaData)
    @test metadata.schema[2].type_ == MD.Type.INT32
    @test metadata.schema[2].logicalType.DATE !== nothing
    @test metadata.schema[2].converted_type == MD.ConvertedType.DATE
    close(file)
    table = Parquet.Table(bytes)
    @test isequal(table.columns.date, dates)
    close(table)

    invalid = Vector{Union{Missing,Date,Int32}}[
        Union{Missing,Date,Int32}[Date(2000, 1, 1), Int32(1)],
    ]
    @test_throws ArgumentError Parquet._encodefile((days=invalid,))
    @test_throws Parquet.LimitError Parquet._encodefile(
        (days=[Date[Date(2000, 1, 1), Date(2000, 1, 2)]],);
        limits=Parquet.Limits(max_container_elements=1))
end

@testset "PLAIN V1 writer page bytes" begin
    table = (required=Int32[10, 20, 30], optional=Union{Missing,Int32}[missing, 2, missing])
    bytes = Parquet._encodefile(table)
    file = Parquet.File(bytes)
    metadata = Parquet.Thrift.decode(copy(file.footer.bytes), Parquet.Metadata.FileMetaData)
    group = metadata.row_groups[1]
    required = group.columns[1].meta_data
    start = Int(required.data_page_offset) + 1
    reader = Parquet.Thrift.Reader(bytes, start, length(bytes))
    header = Parquet.Thrift.decode(reader, Parquet.Metadata.PageHeader)
    @test header.type_ == Parquet.Metadata.PageType.DATA_PAGE
    @test header.data_page_header.encoding == Parquet.Metadata.Encoding.PLAIN
    payloadstart = start + Parquet.Thrift.consumed(reader)
    payload = @view bytes[payloadstart:(payloadstart + header.compressed_page_size - 1)]
    Parquet.verifypagechecksum(header.crc, payload)
    @test Parquet.decode_plain(Int32, payload, 3) == (Int32[10, 20, 30], length(payload) + 1)

    optional = group.columns[2].meta_data
    start = Int(optional.data_page_offset) + 1
    reader = Parquet.Thrift.Reader(bytes, start, length(bytes))
    header = Parquet.Thrift.decode(reader, Parquet.Metadata.PageHeader)
    payloadstart = start + Parquet.Thrift.consumed(reader)
    payload = @view bytes[payloadstart:(payloadstart + header.compressed_page_size - 1)]
    levels, position = Parquet.decode_hybrid(payload, 3, 1; length_prefix=true)
    values, position = Parquet.decode_plain(Int32, payload, 1; offset=position)
    @test levels == UInt64[0, 1, 0]
    @test values == Int32[2]
    @test position == length(payload) + 1
    close(file)
end

@testset "PLAIN V1 writer validation" begin
    @test_throws ArgumentError Parquet._encodefile(NamedTuple())
    @test_throws ArgumentError Parquet._encodefile((a=Int32[1], b=Int32[1, 2]))
    @test_throws ArgumentError Parquet._encodefile((a=Int128[1, 2],))
    @test_throws Parquet.LimitError Parquet._encodefile((a=Int32[1, 2],); limits=Parquet.Limits(max_container_elements=1))
    splitbytes = Parquet._encodefile((a=fill("large", 10),);
        limits=Parquet.Limits(max_page_bytes=10))
    splitpages, _ = writtenpages(splitbytes, 1)
    @test length(splitpages) == 10
    splittable = Parquet.Table(splitbytes)
    @test splittable.columns.a == fill("large", 10)
    close(splittable)
    @test_throws Parquet.LimitError Parquet._encodefile((a=["large"],);
        limits=Parquet.Limits(max_page_bytes=8))
    @test_throws Parquet.LimitError Parquet._encodefile((a=["large"],); limits=Parquet.Limits(max_string_bytes=4))
    bytes = Parquet._encodefile((a=Int32[1, 2],); checksum=false)
    file = Parquet.File(bytes)
    metadata = Parquet.Thrift.decode(copy(file.footer.bytes), Parquet.Metadata.FileMetaData)
    start = Int(metadata.row_groups[1].columns[1].meta_data.data_page_offset) + 1
    reader = Parquet.Thrift.Reader(bytes, start, length(bytes))
    header = Parquet.Thrift.decode(reader, Parquet.Metadata.PageHeader)
    @test header.crc === nothing
    close(file)
end

@testset "compressed V1 writer" begin
    input = (
        id=repeat(Int32[1, 2, 3, 2], 64),
        name=repeat(["alpha", "beta", "alpha", "gamma"], 64),
        optional=Union{Missing,Float64}[index % 3 == 0 ? missing : index / 10 for index in 1:256],
    )
    codecs = (
        (:uncompressed, MD.CompressionCodec.UNCOMPRESSED),
        (:snappy, MD.CompressionCodec.SNAPPY),
        (:gzip, MD.CompressionCodec.GZIP),
        (:brotli, MD.CompressionCodec.BROTLI),
        (:zstd, MD.CompressionCodec.ZSTD),
        (:lz4_raw, MD.CompressionCodec.LZ4_RAW),
    )
    for (name, codec) in codecs
        bytes = Parquet._encodefile(input; codec=name, dictionary=true)
        @test bytes == Parquet._encodefile(input; codec=name, dictionary=true)
        table = Parquet.Table(bytes)
        @test table.columns.id == input.id
        @test table.columns.name == input.name
        @test isequal(table.columns.optional, input.optional)
        close(table)
        file = Parquet.File(bytes)
        metadata = TH.decode(file.footer.bytes, MD.FileMetaData)
        group = metadata.row_groups[1]
        @test all(chunk.meta_data.codec == codec for chunk in group.columns)
        @test group.total_byte_size == sum(chunk.meta_data.total_uncompressed_size for chunk in group.columns)
        @test group.total_compressed_size == sum(chunk.meta_data.total_compressed_size for chunk in group.columns)
        @test group.total_compressed_size == sum(group.columns) do chunk
            start, stop = Parquet._chunkrange(chunk.meta_data, file.footer.offset)
            return stop - start
        end
        close(file)
    end
    @test Parquet._encodefile(input; codec=:SNAPPY) == Parquet._encodefile(input; codec="snappy")
    @test Parquet.Table(Parquet._encodefile(input; codec=:gzip, compressionlevel=9)).columns.id == input.id
    @test_throws ArgumentError Parquet._encodefile(input; codec=:unknown)
    @test_throws ArgumentError Parquet._encodefile(input; codec=:lz4)
    @test_throws ArgumentError Parquet._encodefile(input; codec=:lzo)
    @test_throws ArgumentError Parquet._encodefile(input; codec=1)
    @test_throws ArgumentError Parquet._encodefile(input; compressionlevel=1)
    @test_throws ArgumentError Parquet._encodefile(input; codec=:snappy, compressionlevel=1)
    @test_throws ArgumentError Parquet._encodefile(input; codec=:gzip, compressionlevel=10)
    @test_throws Parquet.LimitError Parquet._encodefile((value=Int32[1],); codec=:snappy,
        limits=Parquet.Limits(max_page_bytes=4))
end

function publicwritebytes(table; kwargs...)
    io = IOBuffer()
    Parquet.write(io, table; kwargs...)
    return take!(io)
end

@testset "PLAIN V2 writer" begin
    input = (
        required=fill(Int32(7), 256),
        optional=Union{Missing,Int32}[isodd(index) ? 7 : missing for index in 1:256],
    )
    codecs = (
        (:uncompressed, MD.CompressionCodec.UNCOMPRESSED),
        (:snappy, MD.CompressionCodec.SNAPPY),
        (:gzip, MD.CompressionCodec.GZIP),
        (:brotli, MD.CompressionCodec.BROTLI),
        (:zstd, MD.CompressionCodec.ZSTD),
        (:lz4_raw, MD.CompressionCodec.LZ4_RAW),
    )
    for (name, codec) in codecs
        bytes = Parquet._encodefile(input; codec=name, pageversion=:v2)
        @test bytes == Parquet._encodefile(input; codec=name, pageversion=:v2)
        table = Parquet.Table(bytes)
        @test table.columns.required == input.required
        @test isequal(table.columns.optional, input.optional)
        close(table)
        for column in 1:2
            pages, metadata = writtenpages(bytes, column)
            @test length(pages) == 1
            header = pages[1].header
            data = header.data_page_header_v2
            @test header.type_ == MD.PageType.DATA_PAGE_V2
            @test header.data_page_header === nothing
            @test data.num_values == 256
            @test data.num_nulls == (column == 1 ? 0 : 128)
            @test data.num_rows == 256
            @test data.encoding == MD.Encoding.PLAIN
            @test data.repetition_levels_byte_length == 0
            @test data.definition_levels_byte_length == (column == 1 ? 0 : 33)
            @test data.is_compressed == (codec != MD.CompressionCodec.UNCOMPRESSED)
            @test header.compressed_page_size == length(pages[1].payload)
            @test metadata.codec == codec
            @test metadata.encoding_stats == [MD.PageEncodingStats(
                page_type=MD.PageType.DATA_PAGE_V2, encoding=MD.Encoding.PLAIN,
                count=Int32(1))]
        end
    end

    small = Parquet._encodefile((value=Int32[1],); codec=:snappy, pageversion=:v2)
    smallpages, smallmetadata = writtenpages(small, 1)
    smallheader = only(smallpages).header
    @test smallmetadata.codec == MD.CompressionCodec.SNAPPY
    @test smallheader.data_page_header_v2.is_compressed == false
    @test only(smallpages).payload == Parquet.encode_plain(Int32[1])

    allmissing = Union{Missing,Int32}[missing for _ in 1:8]
    missingbytes = Parquet._encodefile((value=allmissing,); codec=:snappy, pageversion=:v2)
    missingpages, _ = writtenpages(missingbytes, 1)
    missingheader = only(missingpages).header
    @test missingheader.data_page_header_v2.is_compressed == false
    @test missingheader.compressed_page_size == missingheader.uncompressed_page_size == 2
    missingtable = Parquet.Table(missingbytes)
    @test all(ismissing, missingtable.columns.value)
    close(missingtable)

    emptybytes = Parquet._encodefile((value=Int32[],); codec=:snappy, pageversion=:v2)
    emptyfile = Parquet.File(emptybytes)
    emptymetadata = TH.decode(emptyfile.footer.bytes, MD.FileMetaData)
    @test emptymetadata.num_rows == 0
    @test isempty(emptymetadata.row_groups)
    close(emptyfile)
    emptytable = Parquet.Table(emptybytes)
    @test isempty(emptytable.columns.value)
    close(emptytable)

    io = IOBuffer()
    Parquet.write(io, input; codec=:gzip, pageversion="V2")
    @test Parquet.Table(take!(io)).columns.required == input.required
    @test_throws ArgumentError Parquet._encodefile(input; pageversion=:v3)
    @test_throws ArgumentError Parquet._encodefile(input; pageversion=2)
end

function expectedtablevalues(values::AbstractVector{<:NTuple{N,UInt8}}) where {N}
    return Vector{UInt8}[collect(value) for value in values]
end

function expectedtablevalues(
    values::AbstractVector{Union{Missing,NTuple{N,UInt8}}}) where {N}
    return Union{Missing,Vector{UInt8}}[
        ismissing(value) ? missing : collect(value) for value in values]
end

function expectedtablevalues(values::AbstractVector)
    return values
end

function checkencodedfile(input::NamedTuple, encoding::MD.Encoding.T, pageversion::Symbol)
    bytes = Parquet._encodefile(input; encoding=encoding, pageversion=pageversion)
    table = Parquet.Table(bytes)
    for name in keys(input)
        expected = expectedtablevalues(getproperty(input, name))
        @test isequal(getproperty(table.columns, name), expected)
    end
    close(table)
    if isempty(first(values(input)))
        file = Parquet.File(bytes)
        metadata = TH.decode(file.footer.bytes, MD.FileMetaData)
        @test metadata.num_rows == 0
        @test isempty(metadata.row_groups)
        close(file)
        return
    end
    pagetype = pageversion === :v1 ? MD.PageType.DATA_PAGE : MD.PageType.DATA_PAGE_V2
    for (index, name) in enumerate(keys(input))
        pages, metadata = writtenpages(bytes, index)
        @test length(pages) == 1
        page = only(pages).header
        @test page.type_ == pagetype
        pageencoding = pageversion === :v1 ? page.data_page_header.encoding :
            page.data_page_header_v2.encoding
        @test pageencoding == encoding
        values = getproperty(input, name)
        optional = Missing <: eltype(values)
        expectedencodings = optional && encoding != MD.Encoding.RLE ?
            MD.Encoding.T[MD.Encoding.RLE, encoding] : MD.Encoding.T[encoding]
        @test metadata.encodings == expectedencodings
        @test metadata.dictionary_page_offset === nothing
        @test metadata.encoding_stats == MD.PageEncodingStats[
            MD.PageEncodingStats(page_type=pagetype, encoding=encoding, count=Int32(1)),
        ]
        if pageversion === :v2
            @test page.data_page_header_v2.num_values == length(values)
            @test page.data_page_header_v2.num_nulls == count(ismissing, values)
        end
    end
    return
end

function encodedvaluepayload(values::AbstractVector, encoding::MD.Encoding.T)
    column = Parquet._writecolumn(:value, values)
    return Parquet._encodedpayload(column, encoding, Parquet.Limits())
end

@testset "explicit writer value encodings" begin
    plain = (
        required=Int32[1, -2, 3, 0, 9, -10],
        optional=Union{Missing,Int32}[1, missing, -2, 0, missing, 4],
    )
    delta_binary = (
        i32=Int32[-20, -10, -9, 0, 100, 101],
        optional_i32=Union{Missing,Int32}[-20, missing, -9, 0, missing, 101],
        i64=Int64[-9_000_000_000, -8_000_000_000, 0, 1, 8_000_000_000, 8_000_000_001],
        optional_i64=Union{Missing,Int64}[missing, -8_000_000_000, 0, missing,
            8_000_000_000, 8_000_000_001],
    )
    delta_length = (
        text=["alpha", "", "κόσμος", "prefix-a", "prefix-b", "omega"],
        optional_text=Union{Missing,String}["alpha", missing, "", "prefix-a", missing, "omega"],
        raw=Vector{UInt8}[UInt8[0x00, 0xff], UInt8[], UInt8[0x41], UInt8[1, 2],
            UInt8[1, 3], UInt8[9]],
        optional_raw=Union{Missing,Vector{UInt8}}[UInt8[0x00, 0xff], missing, UInt8[],
            UInt8[1, 2], missing, UInt8[9]],
    )
    delta_byte = (
        text=["prefix-a", "prefix-ab", "prefix-b", "", "omega", "omega-2"],
        optional_text=Union{Missing,String}["prefix-a", missing, "prefix-b", "", missing,
            "omega-2"],
        raw=Vector{UInt8}[UInt8[1, 2, 3], UInt8[1, 2, 4], UInt8[1, 5], UInt8[],
            UInt8[9], UInt8[9, 2]],
        optional_raw=Union{Missing,Vector{UInt8}}[UInt8[1, 2, 3], missing, UInt8[1, 5],
            UInt8[], missing, UInt8[9, 2]],
    )
    byte_stream_split = (
        i32=Int32[typemin(Int32), -1, 0, 1, 2, typemax(Int32)],
        optional_i32=Union{Missing,Int32}[typemin(Int32), missing, 0, 1, missing, typemax(Int32)],
        i64=Int64[typemin(Int64), -1, 0, 1, 2, typemax(Int64)],
        optional_i64=Union{Missing,Int64}[typemin(Int64), missing, 0, 1, missing, typemax(Int64)],
        f32=Float32[-Inf, -0.0, 0.0, 1.5, 2.25, Inf],
        optional_f32=Union{Missing,Float32}[-Inf, missing, 0.0, 1.5, missing, Inf],
        f64=Float64[-Inf, -0.0, 0.0, 1.5, 2.25, Inf],
        optional_f64=Union{Missing,Float64}[-Inf, missing, 0.0, 1.5, missing, Inf],
    )
    rle = (
        required=Bool[true, true, false, false, true, false],
        optional=Union{Missing,Bool}[true, missing, false, false, missing, true],
    )
    cases = (
        (plain, MD.Encoding.PLAIN),
        (delta_binary, MD.Encoding.DELTA_BINARY_PACKED),
        (delta_length, MD.Encoding.DELTA_LENGTH_BYTE_ARRAY),
        (delta_byte, MD.Encoding.DELTA_BYTE_ARRAY),
        (byte_stream_split, MD.Encoding.BYTE_STREAM_SPLIT),
        (rle, MD.Encoding.RLE),
    )
    for pageversion in (:v1, :v2), (input, encoding) in cases
        checkencodedfile(input, encoding, pageversion)
    end

    expected = Parquet._encodefile(delta_binary; encoding=MD.Encoding.DELTA_BINARY_PACKED)
    @test expected == Parquet._encodefile(delta_binary; encoding=:DELTA_BINARY_PACKED)
    @test expected == Parquet._encodefile(delta_binary; encoding="delta_binary_packed")
    @test Parquet._encodefile(plain) == Parquet._encodefile(plain; encoding=nothing)
end

@testset "empty explicit writer value payloads" begin
    deltaempty = UInt8[0x80, 0x01, 0x04, 0x00, 0x00]
    for values in (Int32[], Int64[], Union{Missing,Int32}[missing, missing],
        Union{Missing,Int64}[missing, missing])
        @test encodedvaluepayload(values, MD.Encoding.DELTA_BINARY_PACKED) == deltaempty
    end
    for values in (String[], Vector{UInt8}[], Union{Missing,String}[missing, missing],
        Union{Missing,Vector{UInt8}}[missing, missing])
        @test encodedvaluepayload(values, MD.Encoding.DELTA_LENGTH_BYTE_ARRAY) == deltaempty
        @test encodedvaluepayload(values, MD.Encoding.DELTA_BYTE_ARRAY) ==
            vcat(deltaempty, deltaempty)
    end
    for T in (Int32, Int64, Float32, Float64)
        @test isempty(encodedvaluepayload(T[], MD.Encoding.BYTE_STREAM_SPLIT))
        allnull = Union{Missing,T}[missing, missing]
        @test isempty(encodedvaluepayload(allnull, MD.Encoding.BYTE_STREAM_SPLIT))
    end
    @test encodedvaluepayload(Bool[], MD.Encoding.RLE) == zeros(UInt8, 4)
    allnull = Union{Missing,Bool}[missing, missing]
    @test encodedvaluepayload(allnull, MD.Encoding.RLE) == zeros(UInt8, 4)
end

@testset "explicit writer value encoding validation" begin
    physicals = (
        (MD.Type.BOOLEAN, (value=Bool[true],)),
        (MD.Type.INT32, (value=Int32[1],)),
        (MD.Type.INT64, (value=Int64[1],)),
        (MD.Type.FLOAT, (value=Float32[1],)),
        (MD.Type.DOUBLE, (value=Float64[1],)),
        (MD.Type.BYTE_ARRAY, (value=["one"],)),
    )
    encodings = (
        MD.Encoding.PLAIN,
        MD.Encoding.DELTA_BINARY_PACKED,
        MD.Encoding.DELTA_LENGTH_BYTE_ARRAY,
        MD.Encoding.DELTA_BYTE_ARRAY,
        MD.Encoding.BYTE_STREAM_SPLIT,
        MD.Encoding.RLE,
    )
    for encoding in encodings, (physical, input) in physicals
        valid = encoding == MD.Encoding.PLAIN ||
            encoding == MD.Encoding.DELTA_BINARY_PACKED &&
                physical in (MD.Type.INT32, MD.Type.INT64) ||
            encoding in (MD.Encoding.DELTA_LENGTH_BYTE_ARRAY, MD.Encoding.DELTA_BYTE_ARRAY) &&
                physical == MD.Type.BYTE_ARRAY ||
            encoding == MD.Encoding.BYTE_STREAM_SPLIT &&
                physical in (MD.Type.INT32, MD.Type.INT64, MD.Type.FLOAT, MD.Type.DOUBLE) ||
            encoding == MD.Encoding.RLE && physical == MD.Type.BOOLEAN
        valid || @test_throws ArgumentError Parquet._encodefile(input; encoding=encoding)
    end
    for encoding in (MD.Encoding.PLAIN_DICTIONARY, MD.Encoding.RLE_DICTIONARY,
        MD.Encoding.BIT_PACKED, MD.Encoding.T(Int32(99)), :unknown, 5)
        @test_throws ArgumentError Parquet._encodefile((value=Int32[1],); encoding=encoding)
    end
    @test_throws ArgumentError Parquet._encodefile((value=Int32[1],);
        encoding=MD.Encoding.PLAIN, dictionary=true)
    mixed = (valid=fill(Int32(1), 100), invalid=fill("x", 100))
    @test_throws ArgumentError Parquet._encodefile(mixed;
        encoding=MD.Encoding.DELTA_BINARY_PACKED,
        limits=Parquet.Limits(max_page_bytes=1))
    io = IOBuffer()
    Parquet.write(io, (value=Int32[1],); encoding=:plain)
    table = Parquet.Table(take!(io))
    @test table.columns.value == Int32[1]
    close(table)
end

@testset "public per-column writer encoding policy" begin
    plain = (value=Int32[1, -2, 3],)
    expected = Parquet._encodefile(plain)
    @test expected == publicwritebytes(plain; encoding=nothing)
    @test expected == publicwritebytes(plain; encoding=:PLAIN)
    @test expected == publicwritebytes(plain; encoding="plain")
    @test expected == publicwritebytes(plain; encoding=:value => :plain)
    @test expected == publicwritebytes(plain; encoding="value" => "PLAIN")
    @test expected == publicwritebytes(plain; encoding=(value=:plain,))
    @test expected == publicwritebytes(plain; encoding=Dict("value" => :plain))

    repeated = (value=fill("same", 128),)
    dictionary = Parquet._encodefile(repeated; dictionary=true)
    @test dictionary == publicwritebytes(repeated; encoding=:dictionary)
    @test dictionary == publicwritebytes(repeated;
        encoding=:value => "DICTIONARY")

    count = 128
    input = (
        id=Int32.(1:count),
        text=["prefix-$(index)" for index in 1:count],
        score=Float64.(1:count) ./ 10,
        active=[isodd(index) for index in 1:count],
        fixed=fill((0x01, 0x02, 0x03, 0x04), count),
        plainvalue=fill(Int64(7), count),
        category=fill("category", count),
    )
    policy = (
        id=:delta_binary_packed,
        text="DELTA_BYTE_ARRAY",
        score=:byte_stream_split,
        active=:rle,
        fixed=:byte_stream_split,
        plainvalue=:plain,
    )
    expectedencodings = (
        MD.Encoding.DELTA_BINARY_PACKED,
        MD.Encoding.DELTA_BYTE_ARRAY,
        MD.Encoding.BYTE_STREAM_SPLIT,
        MD.Encoding.RLE,
        MD.Encoding.BYTE_STREAM_SPLIT,
        MD.Encoding.PLAIN,
    )
    for pageversion in (:v1, :v2)
        io = IOBuffer()
        Parquet.write(io, input; dictionary=true, encoding=policy,
            pageversion=pageversion)
        bytes = take!(io)
        table = Parquet.Table(bytes)
        for name in keys(input)
            expected = expectedtablevalues(getproperty(input, name))
            @test isequal(getproperty(table.columns, name), expected)
        end
        close(table)
        for (index, encoding) in enumerate(expectedencodings)
            pages, metadata = writtenpages(bytes, index)
            @test metadata.encodings == [encoding]
            @test metadata.dictionary_page_offset === nothing
            @test length(pages) == 1
        end
        pages, metadata = writtenpages(bytes, length(input))
        @test metadata.encodings == [MD.Encoding.PLAIN, MD.Encoding.RLE,
            MD.Encoding.RLE_DICTIONARY]
        @test metadata.dictionary_page_offset !== nothing
        @test [page.header.type_ for page in pages] ==
            [MD.PageType.DICTIONARY_PAGE,
                pageversion === :v1 ? MD.PageType.DATA_PAGE : MD.PageType.DATA_PAGE_V2]
        @test metadata.encoding_stats == MD.PageEncodingStats[
            MD.PageEncodingStats(page_type=MD.PageType.DICTIONARY_PAGE,
                encoding=MD.Encoding.PLAIN, count=Int32(1)),
            MD.PageEncodingStats(
                page_type=pageversion === :v1 ? MD.PageType.DATA_PAGE :
                    MD.PageType.DATA_PAGE_V2,
                encoding=MD.Encoding.RLE_DICTIONARY, count=Int32(1)),
        ]
    end

    partial = (id=Int32[1, 2, 3], label=["a", "b", "c"])
    bytes = Parquet._encodefile(partial; encoding=(id=:delta_binary_packed,))
    @test writtenpages(bytes, 1)[2].encodings == [MD.Encoding.DELTA_BINARY_PACKED]
    @test writtenpages(bytes, 2)[2].encodings == [MD.Encoding.PLAIN]
    firstorder = Dict{Any,Any}(:id => :delta_binary_packed, :label => :delta_byte_array)
    secondorder = Dict{Any,Any}(:label => :delta_byte_array, :id => :delta_binary_packed)
    @test Parquet._encodefile(partial; encoding=firstorder) ==
        Parquet._encodefile(partial; encoding=secondorder)

    @test_throws ArgumentError Parquet._encodefile(plain;
        encoding=(unknown=:plain,))
    duplicate = Dict{Any,Any}(:value => :plain, "value" => :plain)
    @test_throws ArgumentError Parquet._encodefile(plain; encoding=duplicate)
    @test Parquet._encodefile(plain; encoding=Dict(1 => :plain)) ==
        Parquet._encodefile(plain; encoding=(value=:plain,))
    @test_throws ArgumentError Parquet._encodefile(plain; encoding=(value=nothing,))
    @test_throws ArgumentError Parquet._encodefile(plain; encoding=(value=1,))
    @test_throws ArgumentError Parquet._encodefile((value=["one"],);
        encoding=(value=:delta_binary_packed,))
    @test_throws ArgumentError Parquet._encodefile(plain;
        dictionary=true, encoding=:plain)
    @test_throws ArgumentError Parquet._encodefile(plain;
        encoding=(value=:rle_dictionary,))
    @test_throws ArgumentError Parquet._encodefile(plain;
        encoding=(value=:bit_packed,))
    @test_throws ArgumentError Parquet._encodefile(plain; encoding=(:plain,))

    io = IOBuffer()
    @test_throws ArgumentError Parquet.write(io, plain; encoding=(typo=:plain,))
    @test isempty(take!(io))
    mktempdir() do directory
        path = joinpath(directory, "invalid.parquet")
        @test_throws ArgumentError Parquet.write(path, plain;
            encoding=(value=:delta_byte_array,))
        @test !ispath(path)
    end
end

@testset "FIXED_LEN_BYTE_ARRAY writer" begin
    required = NTuple{3,UInt8}[
        (0x01, 0x02, 0x03),
        (0x01, 0x02, 0x04),
        (0x09, 0x08, 0x07),
    ]
    optional = Union{Missing,NTuple{3,UInt8}}[
        (0x01, 0x02, 0x03),
        missing,
        (0x09, 0x08, 0x07),
    ]
    input = (required=required, optional=optional)
    for pageversion in (:v1, :v2), encoding in (
            MD.Encoding.PLAIN, MD.Encoding.DELTA_BYTE_ARRAY,
            MD.Encoding.BYTE_STREAM_SPLIT)
        checkencodedfile(input, encoding, pageversion)
        bytes = Parquet._encodefile(input; encoding=encoding, pageversion=pageversion)
        file = Parquet.File(bytes)
        metadata = TH.decode(file.footer.bytes, MD.FileMetaData)
        @test all(element -> element.type_ == MD.Type.FIXED_LEN_BYTE_ARRAY,
            metadata.schema[2:end])
        @test all(element -> element.type_length == 3, metadata.schema[2:end])
        @test all(chunk -> chunk.meta_data.type_ == MD.Type.FIXED_LEN_BYTE_ARRAY,
            metadata.row_groups[1].columns)
        close(file)
    end

    matrix = UInt8[0x01 0x01 0x09; 0x02 0x02 0x08; 0x03 0x04 0x07]
    @test encodedvaluepayload(required, MD.Encoding.PLAIN) == collect(vec(matrix))
    @test encodedvaluepayload(required, MD.Encoding.DELTA_BYTE_ARRAY) ==
        Parquet.encode_delta_byte_array_fixed(matrix)
    @test encodedvaluepayload(required, MD.Encoding.BYTE_STREAM_SPLIT) ==
        Parquet.encode_byte_stream_split_fixed(matrix)

    for pageversion in (:v1, :v2), encoding in (
            MD.Encoding.PLAIN, MD.Encoding.DELTA_BYTE_ARRAY,
            MD.Encoding.BYTE_STREAM_SPLIT)
        empty = NTuple{4,UInt8}[]
        checkencodedfile((value=empty,), encoding, pageversion)
        allnull = Union{Missing,NTuple{2,UInt8}}[missing, missing]
        checkencodedfile((value=allnull,), encoding, pageversion)
    end
    @test isempty(encodedvaluepayload(NTuple{4,UInt8}[], MD.Encoding.PLAIN))
    @test isempty(encodedvaluepayload(NTuple{4,UInt8}[], MD.Encoding.BYTE_STREAM_SPLIT))
    @test encodedvaluepayload(NTuple{4,UInt8}[], MD.Encoding.DELTA_BYTE_ARRAY) ==
        Parquet.encode_delta_byte_array_fixed(Matrix{UInt8}(undef, 4, 0))

    repeated = (value=fill((0x01, 0x02, 0x03, 0x04), 128),)
    dictionary = Parquet._encodefile(repeated; dictionary=true)
    _, metadata = writtenpages(dictionary, 1)
    @test metadata.dictionary_page_offset !== nothing
    @test metadata.encodings == [MD.Encoding.PLAIN, MD.Encoding.RLE,
        MD.Encoding.RLE_DICTIONARY]
    table = Parquet.Table(dictionary)
    @test table.columns.value == expectedtablevalues(repeated.value)
    close(table)

    @test_throws ArgumentError Parquet._encodefile((value=NTuple{0,UInt8}[()],))
    @test_throws ArgumentError Parquet._encodefile((value=[(UInt8(1), Int8(2))],))
    fixed = (value=NTuple{3,UInt8}[(0x01, 0x02, 0x03)],)
    for encoding in (MD.Encoding.DELTA_BINARY_PACKED,
            MD.Encoding.DELTA_LENGTH_BYTE_ARRAY, MD.Encoding.RLE)
        @test_throws ArgumentError Parquet._encodefile(fixed; encoding=encoding)
    end
    @test_throws Parquet.LimitError Parquet._encodefile(fixed;
        limits=Parquet.Limits(max_string_bytes=2))
    @test_throws Parquet.LimitError Parquet._encodefile(fixed;
        limits=Parquet.Limits(max_page_bytes=2))
end
