include(joinpath(@__DIR__, "..", "thrift", "generate.jl"))
using SHA

if !@isdefined(TH)
    const TH = Parquet.Thrift
end

const GENERATOR_TEST_IDL = """
namespace jl ParquetTest

enum Color {
  RED = 1;
  GREEN = 2,
  BLUE = 4
}

struct Inner {
  1: required i32 a
  2: optional string b;
}

union Choice {
  1: Inner one
  2: i64 two
}

struct Defaults {
  1: required i64 large = 7
  2: optional bool is_compressed = true
}

struct Empty {}

struct Both {
  1: optional Inner one
  2: optional i64 two
}

struct Other {
  3: optional i32 three
}

struct Mixed {
  2: optional i64 two
  3: optional i32 three
}

struct TwoUnknown {
  3: optional i32 three
  4: optional i32 four
}

struct Bits {
  1: required list<bool> bits
}

struct Painted {
  1: optional Color color
}

struct Everything {
  1: required bool flag
  2: optional bool maybe
  3: required byte small
  4: required i16 medium
  5: required i32 type
  6: required i64 large
  7: required double real
  8: required string text
  9: required binary blob
  10: required list<i32> ints
  11: optional set<string> names
  12: optional map<string, i64> counts
  13: optional list<list<bool>> matrix
  14: optional Color color
  15: optional Inner inner
  16: optional list<Inner> inners
  17: optional Choice choice
  18: optional map<i32, Inner> byid
  19: optional list<map<string, i32>> maps
}
"""

function generatedmodule(idl::String)
    mod = Module(:GeneratedThriftTest)
    Core.eval(mod, Expr(:const, Expr(:(=), :Thrift, TH)))
    Base.include_string(mod, ThriftGenerator.generate(idl; version="test", commit="none"))
    return Base.invokelatest(getfield, mod, :Metadata)
end

@testset "generator regeneration is deterministic and current" begin
    idlbytes = read(ThriftGenerator.IDL_PATH)
    gitblob = vcat(codeunits("blob $(length(idlbytes))\0"), idlbytes)
    @test bytes2hex(sha1(gitblob)) == "fe259d61bc470ade78bad48f5223a82598b91b59"
    @test bytes2hex(sha256(idlbytes)) == "53bb8fc9b96469d7ca694121ead839e449e5156d7bf79f0df728cdd72796df38"
    idl = String(idlbytes)
    generated = ThriftGenerator.generate(idl)
    @test generated == ThriftGenerator.generate(idl)
    @test generated == read(ThriftGenerator.OUTPUT_PATH, String)
    @test occursin("apache/parquet-format 2.13.0 (c47e2a66e88943fc46fde1b028a9432f14fdf5c0)", generated)
    @test occursin("# 1: optional Type type\n", generated)
    @test occursin("type_::Union{Nothing, Type.T}", generated)
    @test occursin("# 7: optional bool is_compressed = true\n", generated)
    @test !occursin("Dict{Symbol", generated)
    @test ThriftGenerator.main(["--check"]) == 0
    mangled = ThriftGenerator.generate("struct A { 1: required i32 end }")
    @test occursin("    end_::Int32  # 1: required i32 end\n", mangled)
    @test_throws ErrorException ThriftGenerator.generate("struct A { 1: required Missing a }")
    @test_throws ErrorException ThriftGenerator.generate("struct A { 1: required i32 a 1: required i32 b }")
    @test_throws ErrorException ThriftGenerator.generate("struct A { 1: required list<set<i32>> a }")
    @test_throws ErrorException ThriftGenerator.generate("service A {}")
end

@testset "generated code covers every Thrift type" begin
    G = generatedmodule(GENERATOR_TEST_IDL)
    x = G.Everything(flag=true, small=Int8(-3), medium=Int16(-300), type_=Int32(5), large=Int64(1) << 40, real=2.5,
        text="héllo", blob=UInt8[1, 2, 3], ints=Int32[1, -2, 3], names=["a", "b"], counts=["k" => Int64(1)],
        matrix=[[true, false], Bool[]], color=G.Color.BLUE, inner=G.Inner(a=Int32(1)),
        inners=[G.Inner(a=Int32(2), b="x")], choice=G.Choice(two=Int64(9)), byid=[Int32(1) => G.Inner(a=Int32(3))],
        maps=[["m" => Int32(4)], Pair{String,Int32}[]])
    bytes = TH.encode(x)
    y = TH.decode(bytes, G.Everything)
    @test isequal(x, y) && x == y && hash(x) == hash(y)
    @test TH.encode(y) == bytes
    @test y.maybe === nothing && y.type_ == 5 && y.names == ["a", "b"] && y.choice.two == 9
    @test fieldnames(G.Everything)[5] == :type_
    @test_throws UndefKeywordError G.Everything(flag=true)
    @test TH.decode(TH.encode(G.Empty()), G.Defaults) == G.Defaults(large=Int64(7), is_compressed=nothing)
    @test G.Defaults().large == 7 && G.Defaults().is_compressed === nothing
    @test TH.decode(UInt8[0x00], G.Defaults).large == 7
    @test TH.decode(TH.encode(G.Defaults(is_compressed=false)), G.Defaults).is_compressed === false
    @test_throws Parquet.FormatError TH.decode(UInt8[0x00], G.Inner)
    @test_throws UndefKeywordError G.Inner()
    @test G.Color.RED.value == 1 && G.Color.BLUE.value == 4
    @test TH.name(G.Color.GREEN) === :GREEN && TH.name(G.Color.T(9)) === nothing
    @test sprint(show, G.Color.RED) == "Color.RED" && sprint(show, G.Color.T(9)) == "Color.T(9)"
    @test TH.decode(TH.encode(G.Painted(color=G.Color.T(9))), G.Painted).color == G.Color.T(9)
    @test_throws InexactError G.Color.T(Int64(2)^40)
    bits = TH.encode(G.Bits(bits=[true]))
    @test bits == UInt8[0x19, 0x11, 0x01, 0x00]
    bits[3] = 0x00
    @test_throws Parquet.FormatError TH.decode(bits, G.Bits)
end

@testset "generated unions" begin
    G = generatedmodule(GENERATOR_TEST_IDL)
    @test G.Choice(two=Int64(1)).two == 1
    @test_throws ArgumentError G.Choice(one=G.Inner(a=Int32(1)), two=Int64(2))
    @test_throws ArgumentError G.Choice(two=Int64(2), unknown_fields=(TH.RawField(3, TH.I32, UInt8[0x02]),))
    @test_throws ArgumentError G.Choice(unknown_fields=(TH.RawField(3, TH.I32, UInt8[0x02]), TH.RawField(4, TH.I32, UInt8[0x02])))
    @test G.Choice() == G.Choice() && TH.decode(TH.encode(G.Choice()), G.Choice) == G.Choice()
    both = TH.encode(G.Both(one=G.Inner(a=Int32(1)), two=Int64(2)))
    @test_throws Parquet.FormatError TH.decode(both, G.Choice)
    other = TH.encode(G.Other(three=Int32(5)))
    choice = TH.decode(other, G.Choice)
    @test choice.one === nothing && choice.two === nothing
    @test length(choice.unknown_fields) == 1 && choice.unknown_fields[1].id == 3
    @test TH.encode(choice) == other
    mutablechoice = G.Choice(two=Int64(2))
    push!(mutablechoice.unknown_fields, TH.RawField(3, TH.I32, UInt8[0x02]))
    @test_throws ArgumentError TH.encode(mutablechoice)
    @test_throws Parquet.FormatError TH.decode(TH.encode(G.Mixed(two=Int64(1), three=Int32(2))), G.Choice)
    @test_throws Parquet.FormatError TH.decode(TH.encode(G.TwoUnknown(three=Int32(1), four=Int32(2))), G.Choice)
    wrapped = G.Everything(flag=false, small=Int8(0), medium=Int16(0), type_=Int32(0), large=Int64(0), real=0.0,
        text="", blob=UInt8[], ints=Int32[], choice=choice)
    @test TH.decode(TH.encode(wrapped), G.Everything).choice == choice
end
