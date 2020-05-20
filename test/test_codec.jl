using Parquet
using Test

function test_codec()
    println("testing reading bitpacked run (old scheme)...")
    let data = UInt8[0x05, 0x39, 0x77]
        io = PipeBuffer(data)
        decoded = Parquet.read_bitpacked_run_old(io, Int32(8), Val{UInt8(3)}())
        @test decoded == Int32[0:7;]
    end
    println("passed.")

    println("testing reading bitpacked run...")
    let data = UInt8[0x88, 0xc6, 0xfa]
        io = PipeBuffer(data)
        decoded = Parquet.read_bitpacked_run(io, 8, Val{UInt8(3)}())
        @test decoded == Int32[0:7;]
    end
    println("passed.")
end

test_codec()
