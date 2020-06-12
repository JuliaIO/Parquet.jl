using Parquet
using Test

function test_codec()
    println("testing reading bitpacked run (old scheme)...")
    let data = UInt8[0x05, 0x39, 0x77]
        byte_width = Parquet.@bit2bytewidth(UInt8(3))
        typ = Parquet.@byt2itype(byte_width)
        #arr = Array{typ}(undef, 8)
        inp = Parquet.InputState(data, 0)
        out = Parquet.OutputState(typ, 8)
        Parquet.read_bitpacked_run_old(inp, out, Int32(8), UInt8(3))
        @test out.data == Int32[0:7;]
    end
    println("passed.")

    println("testing reading bitpacked run...")
    let data = UInt8[0x88, 0xc6, 0xfa]
        byte_width = Parquet.@bit2bytewidth(UInt8(3))
        typ = Parquet.@byt2itype(byte_width)
        #arr = Array{typ}(undef, 8)
        inp = Parquet.InputState(data, 0)
        out = Parquet.OutputState(typ, 8)
        Parquet.read_bitpacked_run(inp, out, 8, UInt8(3), byte_width)
        @test out.data == Int32[0:7;]
    end
    println("passed.")
end

test_codec()
