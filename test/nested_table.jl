using Test

const NTMD = Parquet.Metadata
const NESTED_TABLE_CORPUS = get(ENV, "PARQUET_TESTING_DIR",
    joinpath(@__DIR__, "parquet-testing"))
const NESTED_TABLE_FIXTURES = (
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

function ntfixture(name::AbstractString)
    return joinpath(NESTED_TABLE_CORPUS, "data", name)
end

function ntreadroot(input; limits::Parquet.Limits=Parquet.Limits())
    budget = Parquet._LiveByteBudget(limits)
    file = Parquet.File(input; limits=limits, budget=budget)
    try
        metadata = Parquet._readfilemetadata(file, limits, budget)
        schema = Parquet.Schema(metadata; limits=limits, budget=budget)
        plan = Parquet._nestedplan(schema; limits=limits, budget=budget)
        root = Parquet._readnestedroot(file, metadata, schema, plan, limits,
            budget)
        return (; root, metadata, schema, plan, budget)
    finally
        close(file)
    end
end

function ntsemantic(value)
    ismissing(value) && return missing
    if value isa Parquet.StructValue
        output = Pair{String,Any}[]
        sizehint!(output, length(value))
        for index in 1:length(value)
            push!(output, value.names[index] => ntsemantic(value[index]))
        end
        return output
    end
    if value isa Parquet.MapValue
        output = Pair{Any,Any}[]
        sizehint!(output, length(value))
        for pair in value
            push!(output, ntsemantic(pair.first) => ntsemantic(pair.second))
        end
        return output
    end
    if value isa Parquet.ListValue
        output = []
        sizehint!(output, length(value))
        for item in value
            push!(output, ntsemantic(item))
        end
        return output
    end
    return value
end

function ntreplace(value; replacements...)
    names = fieldnames(typeof(value))
    values = map(names) do name
        return haskey(replacements, name) ? replacements[name] :
            getfield(value, name)
    end
    return typeof(value)(values...)
end

function ntshift(offset::Nothing, delta::Int64)
    return nothing
end

function ntshift(offset::Int64, delta::Int64)
    return Base.checked_add(offset, delta)
end

function ntshiftcolumn(chunk::NTMD.ColumnChunk, delta::Int64)
    md = something(chunk.meta_data)
    shiftedmd = ntreplace(md;
        data_page_offset=ntshift(md.data_page_offset, delta),
        index_page_offset=ntshift(md.index_page_offset, delta),
        dictionary_page_offset=ntshift(md.dictionary_page_offset, delta),
        bloom_filter_offset=ntshift(md.bloom_filter_offset, delta),
    )
    return ntreplace(chunk;
        file_offset=ntshift(chunk.file_offset, delta),
        meta_data=shiftedmd,
        offset_index_offset=ntshift(chunk.offset_index_offset, delta),
        column_index_offset=ntshift(chunk.column_index_offset, delta),
    )
end

function ntshiftrowgroup(group::NTMD.RowGroup, delta::Int64)
    columns = NTMD.ColumnChunk[
        ntshiftcolumn(column, delta) for column in group.columns]
    ordinal = group.ordinal === nothing ? nothing :
        Base.checked_add(group.ordinal, Int16(1))
    return ntreplace(group; columns=columns,
        file_offset=ntshift(group.file_offset, delta), ordinal=ordinal)
end

function ntduplicaterowgroup(path::AbstractString)
    source = read(path)
    file = Parquet.File(source)
    metadata = try
        Parquet.Thrift.decode(file.footer.bytes, NTMD.FileMetaData)
    finally
        close(file)
    end
    length(metadata.row_groups) == 1 || throw(ArgumentError(
        "nested row-group fixture must have one row group"))
    footeroffset = Int64(length(source) - 8 -
        Int(Parquet._readu32le(@view source[(end - 7):(end - 4)])))
    footeroffset >= 4 || throw(ArgumentError(
        "nested row-group fixture has no data region"))
    body = @view source[5:Int(footeroffset)]
    delta = Int64(length(body))
    firstgroup = only(metadata.row_groups)
    secondgroup = ntshiftrowgroup(firstgroup, delta)
    duplicated = ntreplace(metadata;
        num_rows=Base.checked_mul(metadata.num_rows, Int64(2)),
        row_groups=NTMD.RowGroup[firstgroup, secondgroup],
    )
    output = copy(source[1:Int(footeroffset)])
    append!(output, body)
    footer = Parquet.Thrift.encode(duplicated)
    append!(output, footer)
    Parquet._writelittle!(output, UInt32(length(footer)))
    append!(output, Parquet.PARQUET_MAGIC)
    return output, duplicated, delta
end

@testset "whole-table nested Apache corpus" begin
    datadir = joinpath(NESTED_TABLE_CORPUS, "data")
    if isdir(datadir)
        @test all(name -> isfile(ntfixture(name)), NESTED_TABLE_FIXTURES)

        lists = ntreadroot(ntfixture("list_columns.parquet"))
        @test lists.root.names == ["int64_list", "utf8_list"]
        @test length(lists.root) == 3
        @test isequal(ntsemantic(lists.root[1]["int64_list"]), [1, 2, 3])
        @test isequal(ntsemantic(lists.root[2]["int64_list"]), [missing, 1])
        @test lists.root[2]["utf8_list"] === missing
        @test isequal(ntsemantic(lists.root[3]["utf8_list"]),
            ["efg", missing, "hij", "xyz"])

        null_list = ntreadroot(ntfixture("null_list.parquet")).root
        @test null_list.names == ["emptylist"]
        @test length(null_list) == 1
        @test null_list[1]["emptylist"] !== missing
        @test isempty(null_list[1]["emptylist"])

        v2 = ntreadroot(ntfixture("datapage_v2.snappy.parquet")).root
        @test v2.names == ["a", "b", "c", "d", "e"]
        @test length(v2) == 5
        @test [v2[row]["b"] for row in 1:5] == Int64[1, 2, 3, 4, 5]
        @test isequal([v2[row]["a"] for row in 1:5],
            ["abc", "abc", "abc", missing, "abc"])
        @test isequal([ntsemantic(v2[row]["e"]) for row in 1:5],
            Any[[1, 2, 3], missing, missing, [1, 2, 3], [1, 2]])

        old = ntreadroot(ntfixture("old_list_structure.parquet")).root
        @test old.names == ["a"]
        @test length(old) == 1
        @test isequal(ntsemantic(old[1]["a"]), [[1, 2], [3, 4]])

        nestedlists = ntreadroot(
            ntfixture("nested_lists.snappy.parquet")).root
        @test nestedlists.names == ["a", "b"]
        @test length(nestedlists) == 3
        @test [nestedlists[row]["b"] for row in 1:3] == Int32[1, 1, 1]
        @test isequal(ntsemantic(nestedlists[1]["a"]),
            [[Any["a", "b"], Any["c"]], [missing, Any["d"]]])
        @test isequal(ntsemantic(nestedlists[3]["a"]),
            [[Any["a", "b"], Any["c", "d"], Any["e"]],
                [missing, Any["f"]]])

        nestedmaps = ntreadroot(
            ntfixture("nested_maps.snappy.parquet")).root
        @test nestedmaps.names == ["a", "b", "c"]
        @test length(nestedmaps) == 6
        @test [nestedmaps[row]["b"] for row in 1:6] == fill(Int32(1), 6)
        @test [nestedmaps[row]["c"] for row in 1:6] == fill(1.0, 6)
        @test isequal(ntsemantic(nestedmaps[1]["a"]),
            ["a" => Any[1 => true, 2 => false]])
        @test isequal(ntsemantic(nestedmaps[3]["a"]), ["c" => missing])
        @test isequal(ntsemantic(nestedmaps[4]["a"]), ["d" => Any[]])
        @test isequal(ntsemantic(nestedmaps[6]["a"]),
            ["f" => Any[3 => true, 4 => false, 5 => true]])

        repeated = ntreadroot(
            ntfixture("repeated_primitive_no_list.parquet")).root
        @test repeated.names ==
            ["Int32_list", "String_list", "group_of_lists"]
        @test length(repeated) == 4
        expectedints = Any[[0, 1, 2, 3], [], [4], [5, 6, 7, 8]]
        expectedstrings = Any[["foo", "zero", "one", "two"], ["three"],
            ["four"], ["five", "six", "seven", "eight"]]
        @test isequal([ntsemantic(repeated[row]["Int32_list"])
            for row in 1:4], expectedints)
        @test isequal([ntsemantic(repeated[row]["String_list"])
            for row in 1:4], expectedstrings)
        @test all(row -> isequal(
            ntsemantic(repeated[row]["Int32_list"]),
            ntsemantic(repeated[row]["group_of_lists"]["Int32_list_in_group"])),
            1:4)
        @test all(row -> isequal(
            ntsemantic(repeated[row]["String_list"]),
            ntsemantic(repeated[row]["group_of_lists"]["String_list_in_group"])),
            1:4)

        nullable = ntreadroot(ntfixture("nullable.impala.parquet")).root
        @test nullable.names == ["id", "int_array", "int_array_Array",
            "int_map", "int_Map_Array", "nested_struct"]
        @test length(nullable) == 7
        @test isequal(ntsemantic(nullable[1]["int_array"]), [1, 2, 3])
        @test isempty(nullable[3]["int_array"])
        @test nullable[4]["int_array"] === missing
        @test isempty(nullable[4]["int_array_Array"])
        @test nullable[5]["int_array_Array"] === missing
        @test isempty(nullable[3]["int_map"])
        @test nullable[6]["int_map"] === missing
        @test nullable[6]["nested_struct"] === missing
        @test nullable[7]["nested_struct"]["A"] == 7
        @test isequal(ntsemantic(nullable[7]["nested_struct"]["b"]),
            [2, 3, missing])

        nonnullable = ntreadroot(
            ntfixture("nonnullable.impala.parquet")).root
        @test nonnullable.names == ["ID", "Int_Array", "int_array_array",
            "Int_Map", "int_map_array", "nested_Struct"]
        @test length(nonnullable) == 1
        @test nonnullable[1]["ID"] == 8
        @test isequal(ntsemantic(nonnullable[1]["int_array_array"]),
            [[-1, -2], Any[]])
        @test isequal(ntsemantic(nonnullable[1]["Int_Map"]), ["k1" => -1])
        @test isempty(nonnullable[1]["nested_Struct"]["G"])

        novalue = ntreadroot(ntfixture("map_no_value.parquet")).root
        @test novalue.names == ["my_map", "my_map_no_v", "my_list"]
        @test length(novalue) == 3
        for row in 1:3
            firstkey = 3 * row - 2
            expectedkeys = collect(Int32(firstkey):Int32(firstkey + 2))
            @test [pair.first for pair in novalue[row]["my_map"]] ==
                expectedkeys
            @test all(pair -> ismissing(pair.second),
                novalue[row]["my_map"])
            @test isequal(ntsemantic(novalue[row]["my_map"]),
                ntsemantic(novalue[row]["my_map_no_v"]))
            @test collect(novalue[row]["my_list"]) == expectedkeys
        end

        incorrect = ntreadroot(
            ntfixture("incorrect_map_schema.parquet")).root
        @test incorrect.names == ["my_map"]
        @test length(incorrect) == 1
        @test length(incorrect[1]["my_map"]) == 2
        @test Parquet.maplookup(incorrect[1]["my_map"], "name") == "report"
        @test Parquet.maplookup(incorrect[1]["my_map"], "parent") ==
            "another"

        structs = ntreadroot(ntfixture("nested_structs.rust.parquet")).root
        @test length(structs) == 1
        @test length(structs.names) == 36
        @test structs.names[1:4] == ["roll_num", "PC_CUR", "CVA_2012",
            "CVA_2016"]
        @test structs[1]["roll_num"]["min"] == 190406409000602
        @test structs[1]["PC_CUR"]["max"] == 742
        @test structs[1]["CVA_2012"]["max"] == 32150509
        @test structs[1]["CVA_2016"]["max"] == 35195000
        @test structs[1]["BIA_3"]["count"] == 0
        @test structs[1]["count"]["sum"] == 495
    else
        @info "parquet-testing corpus not found; skipping whole-table nested fixtures" corpus=NESTED_TABLE_CORPUS
    end
end

@testset "whole-table nested row-group concatenation" begin
    fixture = ntfixture("list_columns.parquet")
    if isfile(fixture)
        bytes, metadata, delta = ntduplicaterowgroup(fixture)
        @test metadata.num_rows == 6
        @test length(metadata.row_groups) == 2
        firstgroup, secondgroup = metadata.row_groups
        @test secondgroup.num_rows == firstgroup.num_rows == 3
        for (first, second) in zip(firstgroup.columns, secondgroup.columns)
            @test second.file_offset == first.file_offset + delta
            @test second.meta_data.data_page_offset ==
                first.meta_data.data_page_offset + delta
            @test second.meta_data.dictionary_page_offset ==
                first.meta_data.dictionary_page_offset + delta
            @test second.offset_index_offset === nothing
            @test second.column_index_offset === nothing
            @test second.meta_data.bloom_filter_offset === nothing
        end
        result = ntreadroot(bytes)
        @test length(result.root) == 6
        @test [group.num_rows for group in result.metadata.row_groups] == [3, 3]
        @test all(row -> isequal(ntsemantic(result.root[row]),
            ntsemantic(result.root[row + 3])), 1:3)
        @test isequal([ntsemantic(result.root[row]["int64_list"])
            for row in 1:6],
            Any[[1, 2, 3], [missing, 1], [4], [1, 2, 3], [missing, 1], [4]])
    else
        @info "list_columns fixture not found; skipping nested row-group concatenation" corpus=NESTED_TABLE_CORPUS
    end
end

@testset "legacy zero footer row-count sentinel" begin
    fixture = ntfixture("repeated_no_annotation.parquet")
    if isfile(fixture)
        result = ntreadroot(fixture)
        @test result.metadata.num_rows == 0
        @test [group.num_rows for group in result.metadata.row_groups] == [6]
        @test length(result.root) == 6
        @test [result.root[row]["id"] for row in 1:6] == Int64[1, 2, 3, 4, 5, 6]
        @test result.root[1]["phoneNumbers"] === missing
        @test isempty(result.root[3]["phoneNumbers"]["phone"])
        @test result.root[4]["phoneNumbers"]["phone"][1]["number"] ==
            5555555555
        @test [phone["number"] for phone in
            result.root[6]["phoneNumbers"]["phone"]] ==
            Int64[1111111111, 2222222222, 3333333333]
        @test result.root[6]["phoneNumbers"]["phone"][2]["kind"] === missing
    else
        @info "legacy sentinel fixture not found; skipping nested row-count test" corpus=NESTED_TABLE_CORPUS
    end
end

@testset "whole-table nested materialization cleanup" begin
    fixture = ntfixture("list_columns.parquet")
    if isfile(fixture)
        setup = ntreadroot(fixture)
        count = length(setup.plan.leaves)
        initial = Parquet._materializedarraybytes(Parquet.LeafStream, count) +
            Parquet._materializedarraybytes(Int, count) +
            Parquet._materializedarraybytes(Int64, count)
        limits = Parquet.Limits(max_materialized_bytes=initial)
        budget = Parquet._LiveByteBudget(limits)
        file = Parquet.File(fixture)
        try
            error = try
                Parquet._readnestedroot(file, setup.metadata, setup.schema,
                    setup.plan, limits, budget)
                nothing
            catch err
                err
            end
            @test error isa Parquet.LimitError
            @test error.resource == :materialized_bytes
            @test error.requested > error.maximum == initial
            @test Parquet._budgetused(budget) == 0
        finally
            close(file)
        end
    else
        @info "list_columns fixture not found; skipping nested cleanup test" corpus=NESTED_TABLE_CORPUS
    end
end
