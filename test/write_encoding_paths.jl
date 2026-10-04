using Parquet
using Test

function selectorleaf(ordinal::Integer, path::Vector{String}, values::AbstractVector)
    source = Parquet._writecolumn(Symbol(last(path)), values)
    column = Parquet._withoutcolumnschema(source, path)
    return Parquet.WriteLeafPlan(Int32(ordinal), copy(path), column)
end

function assertencoding(choice::Parquet.WriteEncodingChoice, encoding;
    dictionary::Bool=false)
    @test choice.encoding == encoding
    @test choice.dictionary == dictionary
    return
end

@testset "nested writer leaf encoding selectors" begin
    MD = Parquet.Metadata
    leaves = Parquet.WriteLeafPlan[
        selectorleaf(1, ["id"], Int32[1]),
        selectorleaf(2, ["orders", "list", "element", "price"], Int64[2]),
        selectorleaf(3, ["single", "value"], ["three"]),
        selectorleaf(4, ["a.b"], Float64[4]),
        selectorleaf(5, ["a", "b"], Int32[5]),
    ]

    defaults = Parquet._writeencodingchoices(leaves, nothing, true)
    @test length(defaults) == length(leaves)
    @test all(choice -> choice.encoding === nothing && choice.dictionary, defaults)

    tablewide = Parquet._writeencodingchoices(leaves, :plain, false)
    @test length(tablewide) == length(leaves)
    @test all(choice -> choice.encoding == MD.Encoding.PLAIN &&
        !choice.dictionary, tablewide)
    @test_throws ArgumentError Parquet._writeencodingchoices(
        leaves, :plain, true)

    policies = Dict{Any,Any}(
        1 => :delta_binary_packed,
        (:orders, "list", :element, "price") => :delta_binary_packed,
        :single => :dictionary,
        "a.b" => :byte_stream_split,
        ("a", :b) => :plain,
    )
    choices = Parquet._writeencodingchoices(leaves, policies, false)
    assertencoding(choices[1], MD.Encoding.DELTA_BINARY_PACKED)
    assertencoding(choices[2], MD.Encoding.DELTA_BINARY_PACKED)
    assertencoding(choices[3], nothing; dictionary=true)
    assertencoding(choices[4], MD.Encoding.BYTE_STREAM_SPLIT)
    assertencoding(choices[5], MD.Encoding.PLAIN)

    flat = Parquet._writeencodingchoices(leaves, :id => :plain, false)
    assertencoding(flat[1], MD.Encoding.PLAIN)
    alias = Parquet._writeencodingchoices(leaves, "single" => :dictionary, false)
    assertencoding(alias[3], nothing; dictionary=true)
    exact = Parquet._writeencodingchoices(
        leaves, (:orders, :list, :element, :price) => :plain, false)
    assertencoding(exact[2], MD.Encoding.PLAIN)
    ordinal = Parquet._writeencodingchoices(leaves, Int32(5) => :plain, false)
    assertencoding(ordinal[5], MD.Encoding.PLAIN)

    duplicatebare = Dict{Any,Any}(:id => :plain, "id" => :dictionary)
    @test_throws ArgumentError Parquet._writeencodingchoices(
        leaves, duplicatebare, false)
    duplicatepath = Dict{Any,Any}(
        (:orders, :list, :element, :price) => :plain,
        ("orders", "list", "element", "price") => :dictionary,
    )
    @test_throws ArgumentError Parquet._writeencodingchoices(
        leaves, duplicatepath, false)
    doubleassignment = Dict{Any,Any}(
        1 => :plain,
        (:id,) => :delta_binary_packed,
    )
    @test_throws ArgumentError Parquet._writeencodingchoices(
        leaves, doubleassignment, false)

    @test_throws ArgumentError Parquet._writeencodingchoices(
        leaves, 0 => :plain, false)
    @test_throws ArgumentError Parquet._writeencodingchoices(
        leaves, -1 => :plain, false)
    @test_throws ArgumentError Parquet._writeencodingchoices(
        leaves, true => :plain, false)
    @test_throws ArgumentError Parquet._writeencodingchoices(
        leaves, 99 => :plain, false)
    @test_throws ArgumentError Parquet._writeencodingchoices(
        leaves, () => :plain, false)
    @test_throws ArgumentError Parquet._writeencodingchoices(
        leaves, (:orders, 1) => :plain, false)
    @test_throws ArgumentError Parquet._writeencodingchoices(
        leaves, 1.0 => :plain, false)
    @test_throws ArgumentError Parquet._writeencodingchoices(
        leaves, :unknown => :plain, false)
    @test_throws ArgumentError Parquet._writeencodingchoices(
        leaves, ("orders", "missing") => :plain, false)
    @test_throws ArgumentError Parquet._writeencodingchoices(
        leaves, (:single, :value) => :delta_binary_packed, false)

    multidotted = Parquet.WriteLeafPlan[
        selectorleaf(1, ["a", "b"], Int32[1]),
    ]
    @test_throws ArgumentError Parquet._writeencodingchoices(
        multidotted, "a.b" => :plain, false)

    multigroup = Parquet.WriteLeafPlan[
        selectorleaf(1, ["pair", "left"], Int32[1]),
        selectorleaf(2, ["pair", "right"], Int32[2]),
    ]
    @test_throws ArgumentError Parquet._writeencodingchoices(
        multigroup, :pair => :plain, false)
    pairchoices = Parquet._writeencodingchoices(
        multigroup, (:pair, :left) => :delta_binary_packed, true)
    assertencoding(pairchoices[1], MD.Encoding.DELTA_BINARY_PACKED)
    assertencoding(pairchoices[2], nothing; dictionary=true)

    duplicatepaths = Parquet.WriteLeafPlan[
        selectorleaf(6, ["duplicate", "value"], Int32[1]),
        selectorleaf(7, ["duplicate", "value"], Int32[2]),
    ]
    @test_throws ArgumentError Parquet._writeencodingchoices(
        duplicatepaths, (:duplicate, :value) => :plain, false)
    @test_throws ArgumentError Parquet._writeencodingchoices(
        duplicatepaths, :duplicate => :plain, false)
    duplicateordinal = Parquet._writeencodingchoices(
        duplicatepaths, 7 => :delta_binary_packed, false)
    assertencoding(duplicateordinal[1], nothing)
    assertencoding(duplicateordinal[2], MD.Encoding.DELTA_BINARY_PACKED)
end
