using Test
using Dates
import Tables

const WPMD = Parquet.Metadata
const WPTH = Parquet.Thrift
const WRITE_PROVENANCE_CORPUS = get(ENV, "PARQUET_TESTING_DIR",
    joinpath(@__DIR__, "parquet-testing"))

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

# The footer metadata of `bytes`, decoded without validating the schema, so files
# deeper than the default limits can be inspected.
function wpmetadata(bytes::AbstractVector{UInt8})
    footer = Parquet.readfooter(Parquet.source(Vector{UInt8}(bytes)))
    return WPTH.decode(copy(footer.bytes), WPMD.FileMetaData)
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

# Field-by-field equality that also compares the raw Compact Thrift header state of
# preserved unknown fields, which RawField's == ignores.
function wpexact(left::WPTH.RawField, right::WPTH.RawField)
    return left.id == right.id && left.type == right.type &&
        left.previd == right.previd && left.headerlength == right.headerlength &&
        left.bytes == right.bytes
end

function wpexact(left::AbstractVector, right::AbstractVector)
    length(left) == length(right) || return false
    return all(index -> wpexact(left[index], right[index]), eachindex(left, right))
end

function wpexact(left, right)
    typeof(left) === typeof(right) || return false
    T = typeof(left)
    isstructtype(T) && hasfield(T, :unknown_fields) || return isequal(left, right)
    return all(index -> wpexact(getfield(left, index), getfield(right, index)),
        1:fieldcount(T))
end

function wpschemaequal(left, right)
    return wpexact(left, right)
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
    names = keys(Parquet.Tables.columns(table))
    columns = map(values(Parquet.Tables.columns(table))) do column
        return Any[wpsemantic(value) for value in column]
    end
    return NamedTuple{names}(Tuple(columns))
end

function wpexactrewrite(table::Parquet.Table; kwargs...)
    expected_schema = Parquet._tablemetadata(table).schema
    expected_values = wpsemanticcolumns(table)
    output = Parquet._encodefile(table; kwargs...)
    metadata = wpmetadata(output)
    @test wpschemaequal(metadata.schema, expected_schema)
    rewritten = Parquet.Table(output)
    try
        @test isequal(wpsemanticcolumns(rewritten), expected_values)
        @test Parquet._tablerows(rewritten) == Parquet._tablerows(table)
    finally
        close(rewritten)
    end
    return output
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
    close(file)
    table = Parquet.Table(metadata, schema, [:deep], AbstractVector[column], rows)
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
    close(file)
    table = Parquet.Table(metadata, schema, [:m], AbstractVector[column], 1)
    return table, limits
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
    expected = Parquet._tablemetadata(table).schema
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
    for pageversion in (:v1, :v2), codec in (:uncompressed, :snappy)
        output = wpexactrewrite(table; pageversion=pageversion, codec=codec)
        @test wpschemaequal(wpmetadata(output).schema, expected)
    end
    io = IOBuffer()
    Parquet.write(io, table; pageversion=:v2, codec=:zstd)
    @test wpschemaequal(wpmetadata(take!(io)).schema, expected)
end

@testset "schema-bearing writer keeps key-value metadata except source schemas" begin
    bytes = Parquet._encodefile((value=Int32[1], other=Int64[2]))
    original = wpmetadata(bytes)
    kept = WPMD.KeyValue(key="source", value="etl-job-7",
        unknown_fields=[wprawi32(90, 900)])
    stale = [WPMD.KeyValue(key=key, value="opaque-schema") for key in
        ("ARROW:schema", "pandas", "org.apache.spark.sql.parquet.row.metadata")]
    bytes = wprewritefooter(bytes, wpreplace(original;
        key_value_metadata=[stale[1], kept, stale[2], stale[3]]))
    table = Parquet.Table(bytes)
    expected = Parquet._tablemetadata(table).key_value_metadata[2]
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
    # The stale ARROW:schema is dropped; the writer generates a fresh one last.
    @test [item.key for item in rewritten.key_value_metadata] == ["source", "ARROW:schema"]
    @test rewritten.key_value_metadata[2].value != "opaque-schema"
    actual = first(rewritten.key_value_metadata)
    @test actual.key == expected.key
    @test actual.value == expected.value
    @test wpexact(actual.unknown_fields, expected.unknown_fields)
    # A scan that renames and drops columns must not carry the source's schema.
    renamed = Parquet.Table(bytes; scan=Tables.Scan(select=(:other => :renamed,)))
    output = try
        Parquet._encodefile(renamed)
    finally
        close(renamed)
    end
    rewritten = wpmetadata(output)
    @test [element.name for element in rewritten.schema] == ["schema", "renamed"]
    @test [item.key for item in rewritten.key_value_metadata] == ["source", "ARROW:schema"]
    single = Parquet._encodefile((value=Int32[1],))
    onlystale = wprewritefooter(single,
        wpreplace(wpmetadata(single); key_value_metadata=stale))
    table = Parquet.Table(onlystale)
    try
        @test [item.key for item in wpmetadata(Parquet._encodefile(table)).key_value_metadata] ==
            ["ARROW:schema"]
    finally
        close(table)
    end
end

@testset "schema-bearing writer selectors" begin
    path = wpfixture("list_columns.parquet")
    if isfile(path)
        table = Parquet.Table(path)
        expected = Parquet._tablemetadata(table).schema
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
    @test Parquet._tablerows(table) == 0
    for pageversion in (:v1, :v2)
        output = wpexactrewrite(table; pageversion=pageversion,
            codec=:uncompressed)
        metadata = wpmetadata(output)
        @test metadata.num_rows == 0
        @test isempty(metadata.row_groups)
    end
    close(table)
end

@testset "schema-bearing writer limits" begin
    values = Union{Missing,NamedTuple{(:text,),Tuple{String}}}[
        (text=repeat("x", 32),), missing]
    table = Parquet.Table(Parquet._encodefile((; values)))
    try
        @test_throws Parquet.LimitError Parquet._encodefile(table;
            limits=Parquet.Limits(max_string_bytes=16))
        @test_throws Parquet.LimitError Parquet._encodefile(table;
            limits=Parquet.Limits(max_container_elements=1))
    finally
        close(table)
    end
end

# The first field of a struct row: a NamedTuple, or Pair rows past the typed depth.
wpfield(row::NamedTuple) = first(row)
wpfield(row::Parquet.StructValue) = first(row).second

@testset "schema-bearing writer deep structs" begin
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
            value = Parquet.Tables.columns(rewritten).deep[1]
            for _ in 1:256
                value = wpfield(value)
            end
            @test value == Int32(7)
        finally
            close(rewritten)
        end
        shallow = Parquet.Limits(max_metadata_depth=257,
            max_container_elements=limits.max_container_elements)
        error = try
            Parquet._encodefile(table; limits=shallow)
            nothing
        catch err
            err
        end
        @test error isa Parquet.LimitError
        @test error.resource == :metadata_depth
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
            pair = only(only(Parquet.Tables.columns(rewritten).m))
            @test ismissing(pair.second)
            value = pair.first
            for _ in 1:depth
                value = wpfield(value)
            end
            @test value == Int32(7)
        finally
            close(rewritten)
        end
    finally
        close(table)
    end
end
