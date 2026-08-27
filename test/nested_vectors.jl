using Test

struct VirtualNestedVector{T} <: AbstractVector{T}
    length::Int
end

struct UnsupportedNestedKey
    values::Vector{Int}
end

mutable struct MutableNestedKey
    value::Int
end

struct ThrowNestedMetric{E} <: AbstractVector{Int32}
    error::E
end

struct MutatingNestedVector{T,F} <: AbstractVector{T}
    values::Vector{T}
    action::F
end

struct NonIntNestedLength <: AbstractVector{Int32} end

function Base.IndexStyle(::Type{<:VirtualNestedVector})
    return IndexLinear()
end

function Base.size(values::VirtualNestedVector)
    return (values.length,)
end

function Base.getindex(values::VirtualNestedVector{T}, index::Int) where {T}
    @boundscheck checkbounds(values, index)
    return zero(T)
end

function Base.size(values::ThrowNestedMetric)
    throw(values.error)
end

function Base.getindex(::ThrowNestedMetric, ::Int)
    return Int32(0)
end

function Base.IndexStyle(::Type{<:MutatingNestedVector})
    return IndexLinear()
end

function Base.size(values::MutatingNestedVector)
    return size(values.values)
end

function Base.getindex(values::MutatingNestedVector, index::Int)
    item = values.values[index]
    values.action()
    return item
end

function Base.IndexStyle(::Type{NonIntNestedLength})
    return IndexLinear()
end

function Base.size(::NonIntNestedLength)
    return (1,)
end

function Base.length(::NonIntNestedLength)
    return Int32(1)
end

function Base.getindex(::NonIntNestedLength, ::Int)
    return Int32(1)
end

function deepnestedlist(depth::Int)
    values = Int[1]
    for _ in 1:depth
        values = Parquet.ListVector(Int32[0, 1], values)
    end
    return values
end

function deepnestedmap(depth::Int)
    values = Int[1]
    for _ in 1:depth
        values = Parquet.MapVector(Int32[0, 1], Int[1], values)
    end
    return values
end

@testset "Nested list vectors" begin
    required = Parquet.ListVector(Int64[0, 2, 2, 3], Int32[10, 20, 30])
    @test required.offsets isa Vector{Int32}
    @test eltype(required) === Parquet.ListValue{Int32}
    @test fieldtype(typeof(required), :values) === AbstractVector{Int32}
    @test size(required) == (3,)
    @test collect(required[1]) == Int32[10, 20]
    @test isempty(required[2])
    @test collect(required[3]) == Int32[30]
    @test copy(required[1]) == Int32[10, 20]
    @test_throws Base.CanonicalIndexError setindex!(required, required[1], 1)
    @test_throws Base.CanonicalIndexError setindex!(required[1], Int32(1), 1)

    optional = Parquet.ListVector(Int32[0, 0, 0, 2], Int32[1, 2];
        validity=Bool[false, true, true])
    @test Missing <: eltype(optional)
    @test ismissing(optional[1])
    @test !ismissing(optional[2]) && isempty(optional[2])
    @test collect(optional[3]) == Int32[1, 2]

    child = Parquet.ListVector(Int32[0, 1, 2], Int32[4, 5])
    outer = Parquet.ListVector(Int32[0, 2], child)
    @test eltype(outer) === Parquet.ListValue{Parquet.ListValue{Int32}}
    shallow = collect(outer[1])
    @test shallow isa Vector{eltype(child)}
    @test shallow[1] == child[1]
    @test shallow[2] == child[2]

    deepnestedlist(1)
    deepresult = @timed deepnestedlist(128)
    typerepr = @timed sprint(show, typeof(deepresult.value))
    @test ncodeunits(typerepr.value) < 16 * 1024
    @test count("ListVector", typerepr.value) == 1
    @test deepresult.bytes < 64 * 1024 * 1024
    @test deepresult.time < 5.0
    @test typerepr.bytes < 4 * 1024 * 1024
    @test typerepr.time < 2.0

    @test_throws ArgumentError Parquet.ListVector(Int[1], Int[])
    @test_throws ArgumentError Parquet.ListVector(Int[0, 2, 1], Int[1])
    @test_throws ArgumentError Parquet.ListVector(Int[0, 2], Int[1])
    @test_throws ArgumentError Parquet.ListVector(Int[0, 0], Bool[], Int[])
    @test_throws ArgumentError Parquet.ListVector(Int[0, 1], Bool[false], Int[1])
    malformed = Parquet.ListValue(Int32[], 1, 1)
    @test_throws ArgumentError malformed[1]
    @test_throws ArgumentError Parquet.ListVector(Int32[0, 1],
        NonIntNestedLength())
    owner = Ref{Any}(nothing)
    backing = MutatingNestedVector(Int32[7], () -> begin
        empty!(owner[].values)
        return
    end)
    owner[] = backing
    @test_throws ArgumentError Parquet.ListValue(backing, 1, 1)[1]

    if Sys.WORD_SIZE == 64
        large = Int(typemax(Int32)) + 1
        virtual = VirtualNestedVector{UInt8}(large)
        wide = Parquet.ListVector(Int64[0, large], virtual)
        @test wide.offsets isa Vector{Int64}
        @test length(wide[1]) == large
    end
    @test Parquet._nestedindextype(typemax(Int32)) === Int32
    @test Parquet._nestedindextype(Int64(typemax(Int32)) + 1) === Int64
end

@testset "Nested struct vectors" begin
    required = Parquet.StructVector(["id", "name"],
        (Int32[1, 2], String["one", "two"]))
    firstvalue = required[1]
    @test eltype(required) <: Parquet.StructValue
    @test firstvalue[1] == Int32(1)
    @test firstvalue["name"] == "one"
    @test firstvalue[:name] == "one"
    @test collect(firstvalue) == Pair{String,Any}["id" => Int32(1), "name" => "one"]
    @test copy(firstvalue) == collect(firstvalue)
    @test NamedTuple(firstvalue) == (id=Int32(1), name="one")
    @test_throws KeyError firstvalue["absent"]
    @test_throws Base.CanonicalIndexError setindex!(required, firstvalue, 1)

    duplicate = Parquet.StructVector(["x", "x"], (Int[1], String["a"]))[1]
    @test duplicate[1] == 1
    @test duplicate[2] == "a"
    @test collect(duplicate) == Pair{String,Any}["x" => 1, "x" => "a"]
    @test_throws ArgumentError duplicate["x"]
    @test_throws ArgumentError NamedTuple(duplicate)

    optional = Parquet.StructVector(["id", "name"],
        Int32[0, 0, 1, 2, 2], (Int32[7, 8], String["a", "b"]))
    @test optional.ranks isa Vector{Int32}
    @test size(optional) == (4,)
    @test ismissing(optional[1])
    @test optional[2]["id"] == Int32(7)
    @test optional[3]["name"] == "b"
    @test ismissing(optional[4])

    empty = Parquet.StructVector(String[], (); rows=2)
    @test length(empty) == 2
    @test isempty(collect(empty[1]))
    @test NamedTuple(empty[1]) == NamedTuple()

    nulname = Parquet.StructVector(["a\0b"], (Int[1],))[1]
    @test nulname["a\0b"] == 1
    @test_throws ArgumentError NamedTuple(nulname)

    left = Parquet.StructVector(["a", "b"],
        (Float64[NaN], Union{Missing,Int}[missing]))[1]
    right = Parquet.StructVector(["a", "b"],
        (Float64[NaN], Union{Missing,Int}[missing]))[1]
    @test isequal(left, right)
    @test hash(left) == hash(right)
    @test (left == right) === false

    nested = Parquet.StructVector(["items"], (Parquet.ListVector(
        Int[0, 1], Int[9]),))
    copied = copy(nested[1])
    @test copied[1].second isa Parquet.ListValue
    @test collect(copied[1].second) == [9]

    width = 1_024
    widechildren = AbstractVector[Int[index] for index in 1:width]
    wide = Parquet.StructVector(["field_$index" for index in 1:width], widechildren)
    @test wide.children isa Vector{AbstractVector}
    @test fieldtype(typeof(wide), :children) === Vector{AbstractVector}
    @test typeof(wide[1]) === Parquet.StructValue
    @test fieldtype(Parquet.StructValue, :children) === Vector{AbstractVector}
    @test wide[1][width] == width

    @test_throws ArgumentError Parquet.StructVector(["a"], ())
    @test_throws ArgumentError Parquet.StructVector(String[], ())
    @test_throws ArgumentError Parquet.StructVector(String[], Int[0], ())
    @test_throws ArgumentError Parquet.StructVector(["a"], (Int[1],); rows=2)
    @test_throws ArgumentError Parquet.StructVector(["a"], Int[1, 1], (Int[],))
    @test_throws ArgumentError Parquet.StructVector(["a"], Int[0, 2], (Int[1, 2],))
    @test_throws ArgumentError Parquet.StructVector(["a"], Int[0, 1], (Int[],))
    malformed = Parquet.StructValue(["x"], AbstractVector[Int32[]], 1)
    @test_throws ArgumentError malformed[1]
    sentinel = ErrorException("unrelated child metric was called")
    selected = Parquet.StructValue(["x", "y"],
        AbstractVector[Int32[7], ThrowNestedMetric(sentinel)], 1)
    @test selected[1] == Int32(7)
    owner = Ref{Any}(nothing)
    child = MutatingNestedVector(Int32[7], () -> begin
        empty!(owner[].values)
        return
    end)
    owner[] = child
    value = Parquet.StructValue(["x"], AbstractVector[child], 1)
    @test_throws ArgumentError value[1]
    @test_throws ArgumentError Parquet.StructVector(["x"],
        AbstractVector[NonIntNestedLength()])
end

@testset "Nested map vectors" begin
    column = Parquet.MapVector(Int32[0, 3, 3], Int32[1, 2, 1],
        String["first", "middle", "last"])
    value = column[1]
    @test eltype(column) === Parquet.MapValue{Int32,String,true}
    @test fieldtype(typeof(column), :keys) === AbstractVector{Int32}
    @test fieldtype(typeof(column), :values) === Union{Nothing,AbstractVector{String}}
    @test collect(value) == Pair{Int32,String}[
        Int32(1) => "first", Int32(2) => "middle", Int32(1) => "last"]
    @test value[1] == (Int32(1) => "first")
    @test Parquet.maplookup(value, Int32(1)) == "last"
    @test Parquet.maplookup(value, Int32(3), "default") == "default"
    @test_throws KeyError Parquet.maplookup(value, Int32(3))
    @test Dict(value) == Dict(Int32(1) => "last", Int32(2) => "middle")
    @test isempty(column[2])
    @test copy(value) == collect(value)
    @test_throws Base.CanonicalIndexError setindex!(value, Int32(1) => "new", 1)

    deepnestedmap(1)
    deepresult = @timed deepnestedmap(128)
    typerepr = @timed sprint(show, typeof(deepresult.value))
    @test ncodeunits(typerepr.value) < 32 * 1024
    @test count("MapVector", typerepr.value) == 1
    @test deepresult.bytes < 96 * 1024 * 1024
    @test deepresult.time < 5.0
    @test typerepr.bytes < 4 * 1024 * 1024
    @test typerepr.time < 2.0

    omitted = Parquet.MapVector(Int[0, 2], String["a", "b"])
    @test eltype(omitted) === Parquet.MapValue{String,Missing,false}
    @test isequal(collect(omitted[1]),
        Pair{String,Missing}["a" => missing, "b" => missing])
    @test isequal(Dict(omitted[1]), Dict("a" => missing, "b" => missing))

    optional = Parquet.MapVector(Int[0, 0, 0, 1], Bool[false, true, true],
        String["a"], Int[1])
    @test ismissing(optional[1])
    @test !ismissing(optional[2]) && isempty(optional[2])
    @test collect(optional[3]) == ["a" => 1]

    booleankeys = Bool[true, false]
    booleanvalues = Int[10, 20]
    booleans = Parquet.MapVector(Int[0, 1, 2], booleankeys, booleanvalues)
    @test booleans.validity === nothing
    @test booleans.keys === booleankeys
    @test booleans.values === booleanvalues
    @test collect(booleans[1]) == [true => 10]
    @test collect(booleans[2]) == [false => 20]

    optionalkeyonly = Parquet.MapVector(Int[0, 0, 1], String["a"];
        validity=Bool[false, true])
    @test ismissing(optionalkeyonly[1])
    @test isequal(collect(optionalkeyonly[2]), Pair{String,Missing}["a" => missing])

    bytekeys = [UInt8[1], UInt8[1]]
    bytes = Parquet.MapVector(Int[0, 2], bytekeys, String["old", "new"])[1]
    dictionary = Dict(bytes)
    storedkey = only(keys(dictionary))
    @test storedkey !== bytekeys[1]
    @test storedkey !== bytekeys[2]
    bytekeys[1][1] = 2
    bytekeys[2][1] = 3
    @test dictionary[UInt8[1]] == "new"

    listkeycolumn = Parquet.ListVector(Int[0, 2], Int[1, 2])
    listkey = listkeycolumn[1]
    listdictionary = Dict(Parquet.MapVector(Int[0, 1], [listkey], ["value"])[1])
    @test listdictionary[listkey] == "value"
    storedlistkey = only(keys(listdictionary))
    listkeycolumn.values[1] = 9
    originallistkey = Parquet.ListVector(Int[0, 2], Int[1, 2])[1]
    @test listdictionary[originallistkey] == "value"
    @test !haskey(listdictionary, listkey)
    @test isequal(storedlistkey, originallistkey)
    @test hash(storedlistkey) == hash(originallistkey)
    @test_throws Base.CanonicalIndexError setindex!(storedlistkey, 3, 1)

    structchild = Int[1]
    structkey = Parquet.StructVector(["x"], (structchild,))[1]
    structdictionary = Dict(Parquet.MapVector(Int[0, 1], [structkey], ["value"])[1])
    structchild[1] = 9
    originalstructkey = Parquet.StructVector(["x"], (Int[1],))[1]
    @test structdictionary[originalstructkey] == "value"
    @test !haskey(structdictionary, structkey)

    tuplechild = Int[1]
    tuplekey = (Parquet.ListVector(Int[0, 1], tuplechild)[1], :tag)
    tupledictionary = Dict(Parquet.MapVector(Int[0, 1], [tuplekey], ["value"])[1])
    tuplechild[1] = 9
    originaltuplekey = (Parquet.ListVector(Int[0, 1], Int[1])[1], :tag)
    @test tupledictionary[originaltuplekey] == "value"

    namedchild = Int[1]
    namedkey = (items=Parquet.ListVector(Int[0, 1], namedchild)[1], tag=:tag)
    nameddictionary = Dict(Parquet.MapVector(Int[0, 1], [namedkey], ["value"])[1])
    namedchild[1] = 9
    originalnamedkey = (items=Parquet.ListVector(Int[0, 1], Int[1])[1], tag=:tag)
    @test nameddictionary[originalnamedkey] == "value"

    equalkeycolumn = Parquet.ListVector(Int[0, 2, 4], Int[1, 2, 1, 2])
    firstkey = equalkeycolumn[1]
    secondkey = equalkeycolumn[2]
    @test isequal(firstkey, secondkey)
    @test hash(firstkey) == hash(secondkey)
    equaldictionary = Dict(Parquet.MapVector(
        Int[0, 2], [firstkey, secondkey], ["old", "new"])[1])
    @test length(equaldictionary) == 1
    @test equaldictionary[firstkey] == "new"

    nullablekeycolumn = Parquet.ListVector(Int[0, 1], Union{Missing,Int}[missing])
    nullablekey = nullablekeycolumn[1]
    @test Dict(Parquet.MapVector(Int[0, 1], [nullablekey], [1])[1])[nullablekey] == 1

    bytechildren = [UInt8[1]]
    bytechild = Parquet.ListVector(Int[0, 1], bytechildren)[1]
    bytedictionary = Dict(Parquet.MapVector(Int[0, 1], [bytechild], [1])[1])
    bytechildren[1][1] = 2
    originalbytechild = Parquet.ListVector(Int[0, 1], [UInt8[1]])[1]
    @test bytedictionary[originalbytechild] == 1

    inner = Parquet.MapVector(Int[0, 1], Int[1], Int[2])[1]
    @test_throws ArgumentError Dict(Parquet.MapVector(Int[0, 1], [inner], Int[3])[1])
    @test_throws ArgumentError Dict(Parquet.MapVector(
        Int[0, 1], [UnsupportedNestedKey(Int[1])], Int[1])[1])
    @test_throws ArgumentError Dict(Parquet.MapVector(
        Int[0, 1], [MutableNestedKey(1)], Int[1])[1])

    @test_throws ArgumentError Parquet.MapVector(Int[0, 1], Union{Missing,Int}[1], Int[1])
    @test_throws ArgumentError Parquet.MapVector(Int[0, 1], Any[missing], Int[1])
    @test_throws ArgumentError Parquet.MapVector(Int[0, 1], Int[1], Int[])
    @test_throws ArgumentError Parquet.MapVector(Int[0, 2], Int[1], Int[1])
    @test_throws ArgumentError Parquet.MapVector(Int[0, 1], Bool[false], Int[1], Int[1])
    @test_throws ArgumentError Parquet.MapVector(Int32[0, 1],
        NonIntNestedLength(), Int32[2])
    @test_throws ArgumentError Parquet.MapVector(Int32[0, 1], Int32[1],
        NonIntNestedLength())

    attackkeys = Int32[1]
    values = MutatingNestedVector(Int32[7], () -> begin
        empty!(attackkeys)
        return
    end)
    view = Parquet.MapValue{Int32,Int32,true}(attackkeys, values, 1, 1)
    @test_throws ArgumentError view[1]

    owner = Ref{Any}(nothing)
    attackkeys = MutatingNestedVector(Int32[1], () -> begin
        empty!(owner[].values)
        return
    end)
    owner[] = attackkeys
    view = Parquet.MapValue{Int32,Missing,false}(attackkeys, nothing, 1, 1)
    @test_throws ArgumentError view[1]
end
