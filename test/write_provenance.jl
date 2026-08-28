using Test

const WPMD = Parquet.Metadata
const WPTH = Parquet.Thrift
const WRITE_PROVENANCE_CORPUS = get(ENV, "PARQUET_TESTING_DIR",
    joinpath(@__DIR__, "parquet-testing"))

struct WPNullKeyVector <: AbstractVector{String}
    values::Vector{String}
end

function Base.IndexStyle(::Type{WPNullKeyVector})
    return IndexLinear()
end

function Base.size(values::WPNullKeyVector)
    return size(values.values)
end

function Base.getindex(values::WPNullKeyVector, index::Int)
    return index == firstindex(values.values) ? missing : values.values[index]
end

function wpfixture(name::AbstractString)
    return joinpath(WRITE_PROVENANCE_CORPUS, "data", name)
end

function wpreplace(value; replacements...)
    names = fieldnames(typeof(value))
    fields = map(names) do name
        return haskey(replacements, name) ? replacements[name] : getfield(value, name)
    end
    return typeof(value)(fields...)
end

function wpmetadata(bytes::AbstractVector{UInt8})
    file = Parquet.File(bytes)
    try
        return WPTH.decode(copy(file.footer.bytes), WPMD.FileMetaData)
    finally
        close(file)
    end
end

function wprewritefooter(bytes::Vector{UInt8}, metadata::WPMD.FileMetaData)
    file = Parquet.File(bytes)
    offset = try
        file.footer.offset
    finally
        close(file)
    end
    output = copy(bytes[1:Int(offset)])
    footer = WPTH.encode(metadata)
    append!(output, footer)
    Parquet._writelittle!(output, UInt32(length(footer)))
    append!(output, Parquet.PARQUET_MAGIC)
    return output
end

function wprawi32(id::Integer, value::Integer)
    writer = WPTH.Writer()
    WPTH.writei32!(writer, Int32(value))
    return WPTH.RawField(id, WPTH.I32, writer.buffer)
end

function wpschemaequal(left, right)
    length(left) == length(right) || return false
    for index in eachindex(left, right)
        Parquet._provenanceexact(left[index], right[index]) || return false
    end
    return true
end

function wpsemantic(value)
    ismissing(value) && return missing
    if value isa Parquet.StructValue
        output = Pair{String,Any}[]
        sizehint!(output, length(value))
        for index in 1:length(value)
            push!(output, value.names[index] => wpsemantic(value[index]))
        end
        return output
    end
    if value isa Parquet.MapValue
        output = Pair{Any,Any}[]
        sizehint!(output, length(value))
        for pair in value
            push!(output, wpsemantic(pair.first) => wpsemantic(pair.second))
        end
        return output
    end
    if value isa Parquet.ListValue
        output = []
        sizehint!(output, length(value))
        for item in value
            push!(output, wpsemantic(item))
        end
        return output
    end
    return value
end

function wpsemanticcolumns(table::Parquet.Table)
    names = keys(table.columns)
    columns = map(values(table.columns)) do column
        return Any[wpsemantic(value) for value in column]
    end
    return NamedTuple{names}(columns)
end

function wpexactrewrite(table::Parquet.Table; kwargs...)
    expected_schema = table.metadata.schema
    expected_values = wpsemanticcolumns(table)
    output = Parquet._encodefile(table; kwargs...)
    metadata = wpmetadata(output)
    @test wpschemaequal(metadata.schema, expected_schema)
    rewritten = Parquet.Table(output)
    try
        @test isequal(wpsemanticcolumns(rewritten), expected_values)
        @test rewritten.rows == table.rows
    finally
        close(rewritten)
    end
    return output
end

function wpprovenancesource()
    structs = Union{Missing,NamedTuple{(:id,:name),Tuple{Int32,String}}}[
        missing,
        (id=Int32(1), name="one"),
        (id=Int32(2), name="two"),
    ]
    lists = Union{Missing,Vector{Union{Missing,Int32}}}[
        missing,
        Union{Missing,Int32}[],
        Union{Missing,Int32}[Int32(1), missing],
    ]
    maps = Union{Missing,Dict{String,Union{Missing,Int32}}}[
        missing,
        Dict{String,Union{Missing,Int32}}(),
        Dict{String,Union{Missing,Int32}}("a" => Int32(1), "b" => missing),
    ]
    return (; s=structs, l=lists, m=maps)
end

function wpfreshtable()
    return Parquet.Table(Parquet._encodefile(wpprovenancesource()))
end

function wpreplacecolumn!(table::Parquet.Table, name::Symbol, replacement)
    names = keys(table.columns)
    columns = ntuple(length(names)) do index
        return names[index] == name ? replacement : values(table.columns)[index]
    end
    updated = NamedTuple{names}(columns)
    typeof(updated) === typeof(table.columns) || throw(ArgumentError(
        "replacement changed the concrete table column type"))
    table.columns = updated
    return table
end

function wprejects(f)
    error = try
        f()
        nothing
    catch err
        err
    end
    @test error !== nothing
    @test error isa Union{ArgumentError,Parquet.FormatError,Parquet.LimitError}
    return error
end

function wpdeepstructtable(depth::Int, rows::Int)
    depth >= 1 || throw(ArgumentError("deep struct depth must be positive"))
    rows in (0, 1) || throw(ArgumentError("deep struct rows must be zero or one"))
    required = WPMD.FieldRepetitionType.REQUIRED
    elements = Vector{WPMD.SchemaElement}(undef, depth + 2)
    elements[1] = WPMD.SchemaElement(name="schema",
        num_children=Int32(1))
    for level in 1:depth
        name = level == 1 ? "deep" : "level_$level"
        elements[level + 1] = WPMD.SchemaElement(name=name,
            repetition_type=required, num_children=Int32(1))
    end
    elements[end] = WPMD.SchemaElement(name="value",
        type_=WPMD.Type.INT32, repetition_type=required)
    limits = Parquet.Limits(max_metadata_depth=depth + 2,
        max_container_elements=max(10_000, depth + 2))
    schema = Parquet.Schema(elements; limits=limits)
    column::AbstractVector = iszero(rows) ? Int32[] : Int32[7]
    for level in depth:-1:1
        childname = level == depth ? "value" : "level_$(level + 1)"
        column = Parquet.StructVector(String[childname],
            AbstractVector[column]; rows=rows)
    end
    seed = Parquet._encodefile((seed=Int32[1],))
    file = Parquet.File(seed)
    metadata = WPTH.decode(copy(file.footer.bytes), WPMD.FileMetaData)
    metadata = wpreplace(metadata; schema=elements, num_rows=Int64(rows),
        row_groups=WPMD.RowGroup[])
    table = Parquet.Table(file, metadata, schema, (; deep=column), rows, false)
    return table, limits
end

function wpdeepstructkeytable(depth::Int)
    depth >= 1 || throw(ArgumentError("deep key depth must be positive"))
    required = WPMD.FieldRepetitionType.REQUIRED
    repeated = WPMD.FieldRepetitionType.REPEATED
    elements = Vector{WPMD.SchemaElement}(undef, depth + 4)
    elements[1] = WPMD.SchemaElement(name="schema", num_children=Int32(1))
    elements[2] = WPMD.SchemaElement(name="m",
        repetition_type=required, num_children=Int32(1),
        converted_type=WPMD.ConvertedType.MAP,
        logicalType=WPMD.LogicalType(MAP=WPMD.MapType()))
    elements[3] = WPMD.SchemaElement(name="key_value",
        repetition_type=repeated, num_children=Int32(1))
    for level in 1:depth
        name = level == 1 ? "key" : "level_$level"
        elements[level + 3] = WPMD.SchemaElement(name=name,
            repetition_type=required, num_children=Int32(1))
    end
    elements[end] = WPMD.SchemaElement(name="value",
        type_=WPMD.Type.INT32, repetition_type=required)
    limits = Parquet.Limits(max_metadata_depth=depth + 4,
        max_container_elements=max(10_000, depth + 4))
    schema = Parquet.Schema(elements; limits=limits)
    key::AbstractVector = Int32[7]
    for level in depth:-1:1
        childname = level == depth ? "value" : "level_$(level + 1)"
        key = Parquet.StructVector(String[childname], AbstractVector[key];
            rows=1)
    end
    column = Parquet.MapVector(Int32[0, 1], key, nothing)
    seed = Parquet._encodefile((seed=Int32[1],))
    file = Parquet.File(seed)
    metadata = WPMD.FileMetaData(version=Int32(1), schema=elements,
        num_rows=Int64(1), row_groups=WPMD.RowGroup[])
    table = Parquet.Table(file, metadata, schema, (; m=column), 1, false)
    return table, limits
end

function wpbindingpass(table, semantic, limits, budget, topology)
    before = Parquet._budgetused(budget)
    bindings = Parquet._provenancebindings(table, semantic, limits, budget,
        topology)
    count = length(bindings)
    Parquet._release!(budget, Parquet._budgetused(budget) - before)
    return count
end

function wpprovenanceallocations(name::Symbol)
    source = NamedTuple{(name,)}((Int32[1],))
    table = Parquet.Table(Parquet._encodefile(source))
    try
        limits = Parquet.Limits()
        budget = Parquet._LiveByteBudget(limits)
        _, schema = Parquet._provenancefreshschema(table, limits, budget)
        semantic = Parquet._nestedplan(schema; limits=limits, budget=budget)
        topology = Parquet._provenancetopology(table, limits, budget)
        wpbindingpass(table, semantic, limits, budget, topology)
        GC.gc()
        bindingbytes = @allocated wpbindingpass(table, semantic, limits, budget,
            topology)
        bindings = Parquet._provenancebindings(table, semantic, limits, budget,
            topology)
        before = Parquet._budgetused(budget)
        Parquet._provenancevalidatetop(table, semantic, bindings, 1, limits,
            topology, budget)
        Parquet._budgetused(budget) == before || throw(AssertionError(
            "provenance validation retained scratch budget"))
        GC.gc()
        validationbytes = @allocated Parquet._provenancevalidatetop(table,
            semantic, bindings, 1, limits, topology, budget)
        Parquet._budgetused(budget) == before || throw(AssertionError(
            "provenance validation retained measured scratch budget"))
        return (; bindingbytes, validationbytes)
    finally
        close(table)
    end
end

function wpcustomschemafile()
    labels = Union{Missing,Vector{Union{Missing,String}}}[
        missing,
        Union{Missing,String}[],
        Union{Missing,String}["alpha", missing],
    ]
    amounts = Union{Missing,Parquet.Decimal}[
        Parquet.Decimal(123, 2), missing, Parquet.Decimal(-4, 2)]
    decimal = Parquet.LogicalColumn(amounts, :decimal; precision=9, scale=2)
    bytes = Parquet._encodefile((; labels, amount=decimal))
    metadata = wpmetadata(bytes)
    schema = copy(metadata.schema)
    root = schema[1]
    schema[1] = wpreplace(root; name="provenance root", field_id=Int32(101),
        unknown_fields=(root.unknown_fields..., wprawi32(90, 900)))
    labelsindex = findfirst(element -> element.name == "labels", schema)
    amountindex = findfirst(element -> element.name == "amount", schema)
    stringindex = findfirst(element -> element.type_ == WPMD.Type.BYTE_ARRAY &&
        element.logicalType !== nothing && element.logicalType.STRING !== nothing, schema)
    for (index, fieldid, rawid) in ((labelsindex, 102, 91),
            (stringindex, 103, 92), (amountindex, 104, 93))
        element = schema[index]
        schema[index] = wpreplace(element; field_id=Int32(fieldid),
            unknown_fields=(element.unknown_fields..., wprawi32(rawid, rawid * 10)))
    end
    custom = wpreplace(metadata; schema=schema)
    return wprewritefooter(bytes, custom)
end

@testset "schema-bearing writer exact provenance" begin
    bytes = wpcustomschemafile()
    table = Parquet.Table(bytes)
    expected = table.metadata.schema
    @test expected[1].name == "provenance root"
    @test expected[1].field_id == Int32(101)
    @test !isempty(expected[1].unknown_fields)
    list = only(filter(element -> element.name == "labels", expected))
    @test list.converted_type == WPMD.ConvertedType.LIST
    @test list.logicalType.LIST !== nothing
    string = only(filter(element -> element.type_ == WPMD.Type.BYTE_ARRAY &&
        element.logicalType !== nothing && element.logicalType.STRING !== nothing,
        expected))
    @test string.converted_type == WPMD.ConvertedType.UTF8
    decimal = only(filter(element -> element.name == "amount", expected))
    @test decimal.converted_type == WPMD.ConvertedType.DECIMAL
    @test decimal.logicalType.DECIMAL.precision == Int32(9)
    @test decimal.logicalType.DECIMAL.scale == Int32(2)
    @test decimal.precision == Int32(9)
    @test decimal.scale == Int32(2)
    close(table)
    @test @atomic table.closed
    for pageversion in (:v1, :v2), codec in (:uncompressed, :snappy)
        output = wpexactrewrite(table; pageversion=pageversion, codec=codec)
        @test wpschemaequal(wpmetadata(output).schema, expected)
    end
    io = IOBuffer()
    Parquet.write(io, table; pageversion=:v2, codec=:zstd)
    @test wpschemaequal(wpmetadata(take!(io)).schema, expected)
end

@testset "schema-bearing writer preserves file key-value metadata" begin
    bytes = Parquet._encodefile((value=Int32[1],))
    original = wpmetadata(bytes)
    keyvalue = WPMD.KeyValue(key="ARROW:schema", value="opaque-schema",
        unknown_fields=[wprawi32(90, 900)])
    bytes = wprewritefooter(bytes, wpreplace(original;
        key_value_metadata=[keyvalue]))
    table = Parquet.Table(bytes)
    expected = only(table.metadata.key_value_metadata)
    output = try
        error = try
            Parquet._encodefile(table;
                limits=Parquet.Limits(max_string_bytes=5))
            nothing
        catch err
            err
        end
        @test error isa Parquet.LimitError
        @test error.resource == :string_bytes
        Parquet._encodefile(table)
    finally
        close(table)
    end
    rewritten = wpmetadata(output)
    @test rewritten.key_value_metadata !== nothing
    actual = only(rewritten.key_value_metadata)
    @test actual.key == expected.key
    @test actual.value == expected.value
    @test Parquet._provenanceexact(actual.unknown_fields,
        expected.unknown_fields)
end

@testset "schema-bearing writer selectors" begin
    path = wpfixture("list_columns.parquet")
    if isfile(path)
        table = Parquet.Table(path)
        expected = table.metadata.schema
        policies = (
            Dict(("int64_list", "list", "item") => :delta_binary_packed,
                ("utf8_list", "list", "item") => :delta_byte_array),
            Dict(1 => :plain),
            Dict("int64_list" => :plain),
        )
        for policy in policies
            output = Parquet._encodefile(table; encoding=policy, pageversion=:v2)
            @test wpschemaequal(wpmetadata(output).schema, expected)
        end
        @test_throws ArgumentError Parquet._encodefile(table;
            encoding=Dict(("missing", "path") => :plain))
        close(table)
        ambiguous = Parquet.Table(wpfixture("nullable.impala.parquet"))
        try
            @test_throws ArgumentError Parquet._encodefile(ambiguous;
                encoding=Dict("nested_struct" => :plain))
        finally
            close(ambiguous)
        end
    else
        @info "skipping provenance selector corpus tests" root=WRITE_PROVENANCE_CORPUS
    end
end

@testset "schema-bearing writer corpus layouts" begin
    fixtures = (
        "list_columns.parquet",
        "null_list.parquet",
        "datapage_v2.snappy.parquet",
        "old_list_structure.parquet",
        "nested_lists.snappy.parquet",
        "nested_maps.snappy.parquet",
        "repeated_primitive_no_list.parquet",
        "repeated_no_annotation.parquet",
        "nullable.impala.parquet",
        "nonnullable.impala.parquet",
        "map_no_value.parquet",
        "incorrect_map_schema.parquet",
        "nested_structs.rust.parquet",
    )
    if all(name -> isfile(wpfixture(name)), fixtures)
        for name in fixtures
            table = Parquet.Table(wpfixture(name))
            try
                wpexactrewrite(table; pageversion=:v1, codec=:uncompressed)
            finally
                close(table)
            end
        end
        for name in ("list_columns.parquet", "nested_maps.snappy.parquet",
                "nullable.impala.parquet")
            table = Parquet.Table(wpfixture(name))
            try
                wpexactrewrite(table; pageversion=:v2, codec=:zstd)
            finally
                close(table)
            end
        end
    else
        @info "skipping provenance layout corpus tests" root=WRITE_PROVENANCE_CORPUS
    end
end

@testset "schema-bearing writer zero rows" begin
    empty = (
        s=NamedTuple{(:id,),Tuple{Int32}}[],
        l=Vector{Union{Missing,Int32}}[],
        m=Dict{String,Union{Missing,Int32}}[],
    )
    table = Parquet.Table(Parquet._encodefile(empty))
    @test table.rows == 0
    for pageversion in (:v1, :v2)
        output = wpexactrewrite(table; pageversion=pageversion,
            codec=:uncompressed)
        metadata = wpmetadata(output)
        @test metadata.num_rows == 0
        @test isempty(metadata.row_groups)
    end
    close(table)
end

@testset "schema-bearing writer rejects stored-schema tampering" begin
    table = wpfreshtable()
    changed = copy(table.metadata.schema)
    changed[1] = wpreplace(changed[1]; field_id=Int32(777))
    table.metadata = wpreplace(table.metadata; schema=changed)
    wprejects(() -> Parquet._encodefile(table))
    close(table)

    table = wpfreshtable()
    changed = copy(table.metadata.schema)
    leafindex = findfirst(element -> element.name == "id", changed)
    changed[leafindex] = wpreplace(changed[leafindex]; type_=WPMD.Type.INT64)
    table.metadata = wpreplace(table.metadata; schema=changed)
    wprejects(() -> Parquet._encodefile(table))
    close(table)

    table = wpfreshtable()
    changed = copy(table.metadata.schema)
    changed[1] = wpreplace(changed[1];
        num_children=changed[1].num_children + Int32(1))
    table.metadata = wpreplace(table.metadata; schema=changed)
    wprejects(() -> Parquet._encodefile(table))
    close(table)

    table = wpfreshtable()
    changed = copy(table.metadata.schema)
    changed[1] = wpreplace(changed[1]; field_id=Int32(778))
    changedmetadata = wpreplace(table.metadata; schema=changed)
    table.schema = Parquet.Schema(changedmetadata)
    wprejects(() -> Parquet._encodefile(table))
    close(table)

    table = Parquet.Table(wpcustomschemafile())
    changed = copy(table.metadata.schema)
    raw = only(changed[1].unknown_fields)
    changedraw = WPTH.RawField(raw.id, raw.type, Int16(raw.previd + 1),
        raw.headerlength, copy(raw.bytes))
    @test raw == changedraw
    @test !Parquet._provenanceexact(raw, changedraw)
    changed[1] = wpreplace(changed[1]; unknown_fields=[changedraw])
    table.metadata = wpreplace(table.metadata; schema=changed)
    wprejects(() -> Parquet._encodefile(table))
    close(table)
end

@testset "unknown-field provenance clone charge includes its vector" begin
    fields = WPTH.RawField[wprawi32(90, 900), wprawi32(91, 910)]
    expected = Parquet._materializedarraybytes(WPTH.RawField, length(fields))
    for field in fields
        fieldcharge = Parquet._materializedsum(
            Parquet._MATERIALIZED_OBJECT_BYTES,
            Parquet._materializedarraybytes(UInt8, length(field.bytes)))
        expected = Parquet._materializedsum(expected, fieldcharge)
    end
    @test Parquet._provenanceclonecharge(fields) == expected
end

@testset "schema-bearing writer rejects vector tampering" begin
    table = wpfreshtable()
    table.columns.s.names[1] = "changed"
    wprejects(() -> Parquet._encodefile(table))
    close(table)

    table = wpfreshtable()
    pop!(table.columns.s.children)
    wprejects(() -> Parquet._encodefile(table))
    close(table)

    table = wpfreshtable()
    table.columns.s.children[1] = Int64[1, 2]
    wprejects(() -> Parquet._encodefile(table))
    close(table)

    table = wpfreshtable()
    table.columns.s.ranks[1] = Int32(1)
    wprejects(() -> Parquet._encodefile(table))
    close(table)

    table = wpfreshtable()
    table.columns.s.ranks[3] = Int32(2)
    wprejects(() -> Parquet._encodefile(table))
    close(table)

    table = wpfreshtable()
    table.columns.l.offsets[1] = Int32(1)
    wprejects(() -> Parquet._encodefile(table))
    close(table)

    table = wpfreshtable()
    table.columns.l.offsets[end] = Int32(1)
    wprejects(() -> Parquet._encodefile(table))
    close(table)

    table = wpfreshtable()
    table.columns.l.offsets[2] = Int32(1)
    table.columns.l.offsets[3] = Int32(1)
    wprejects(() -> Parquet._encodefile(table))
    close(table)

    table = wpfreshtable()
    table.columns.m.offsets[1] = Int32(1)
    wprejects(() -> Parquet._encodefile(table))
    close(table)

    table = wpfreshtable()
    table.columns.m.offsets[end] = Int32(1)
    wprejects(() -> Parquet._encodefile(table))
    close(table)

    table = wpfreshtable()
    map = table.columns.m
    badmap = typeof(map)(copy(map.offsets), copy(map.validity),
        WPNullKeyVector(copy(map.keys)), copy(map.values))
    wpreplacecolumn!(table, :m, badmap)
    wprejects(() -> Parquet._encodefile(table))
    close(table)

    table = wpfreshtable()
    table.rows += 1
    wprejects(() -> Parquet._encodefile(table))
    close(table)
end

@testset "schema-bearing writer limits and cleanup" begin
    values = Union{Missing,NamedTuple{(:text,),Tuple{String}}}[
        (text=repeat("x", 32),), missing]
    table = Parquet.Table(Parquet._encodefile((; values)))
    limits = Parquet.Limits(max_string_bytes=16)
    budget = Parquet._LiveByteBudget(limits)
    Parquet._reserve!(budget, 64)
    before = Parquet._budgetused(budget)
    @test_throws Parquet.LimitError Parquet._provenancewritefields(
        table, limits, budget, nothing, false)
    @test Parquet._budgetused(budget) == before
    Parquet._release!(budget, before)
    @test Parquet._budgetused(budget) == 0

    tight = Parquet.Limits(max_materialized_bytes=512)
    tightbudget = Parquet._LiveByteBudget(tight)
    @test_throws Parquet.LimitError Parquet._provenancewritefields(
        table, tight, tightbudget, nothing, false)
    @test Parquet._budgetused(tightbudget) == 0
    @test_throws Parquet.LimitError Parquet._encodefile(table;
        limits=Parquet.Limits(max_container_elements=1))
    close(table)
end

@testset "schema-bearing writer charged name snapshot" begin
    short = wpprovenanceallocations(:x)
    long = wpprovenanceallocations(Symbol(repeat("x", 4096)))
    @test long.bindingbytes <= short.bindingbytes + 512
    @test long.validationbytes <= short.validationbytes + 512
end

@testset "schema-bearing writer iterative deep topology" begin
    depth = 4096
    for rows in (0, 1)
        table, limits = wpdeepstructtable(depth, rows)
        try
            output = Parquet._encodefile(table; limits=limits)
            metadata = wpmetadata(output)
            @test metadata.num_rows == rows
            @test length(metadata.schema) == depth + 2
            @test metadata.schema[2].name == "deep"
            @test metadata.schema[end].name == "value"
            error = try
                Parquet.Table(output; limits=limits)
                nothing
            catch err
                err
            end
            @test error isa Parquet.LimitError
            @test error.resource == :nested_read_depth
        finally
            close(table)
        end
    end

    table, limits = wpdeepstructtable(256, 1)
    try
        output = Parquet._encodefile(table; limits=limits)
        rewritten = Parquet.Table(output; limits=limits)
        try
            value = rewritten.columns.deep[1]
            for _ in 1:256
                value = value[1]
            end
            @test value == Int32(7)
        finally
            close(rewritten)
        end
        shallow = Parquet.Limits(max_metadata_depth=257,
            max_container_elements=limits.max_container_elements)
        budget = Parquet._LiveByteBudget(shallow)
        Parquet._reserve!(budget, Int64(64))
        error = try
            Parquet._provenancewritefields(table, shallow, budget, nothing,
                false)
            nothing
        catch err
            err
        end
        @test error isa Parquet.LimitError
        @test error.resource == :metadata_depth
        @test Parquet._budgetused(budget) == 64
    finally
        close(table)
    end
end

@testset "schema-bearing writer recursive key-only MAP" begin
    depth = 256
    table, limits = wpdeepstructkeytable(depth)
    try
        output = Parquet._encodefile(table; limits=limits)
        rewritten = Parquet.Table(output; limits=limits)
        try
            pair = only(only(rewritten.columns.m))
            @test ismissing(pair.second)
            value = pair.first
            for _ in 1:depth
                value = value[1]
            end
            @test value == Int32(7)
        finally
            close(rewritten)
        end
    finally
        close(table)
    end
end
