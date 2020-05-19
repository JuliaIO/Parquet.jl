using Parquet
using Test

function test_codec()
    println("testing reading bitpacked run (old scheme)...")
    let data = UInt8[0x05, 0x39, 0x77]
        io = PipeBuffer(data)
        decoded = Parquet.read_bitpacked_run_old(io, 8, UInt8(3))
        @test decoded == Int32[0:7;]
    end
    println("passed.")

    println("testing reading bitpacked run...")
    let data = UInt8[0x88, 0xc6, 0xfa]
        io = PipeBuffer(data)
        bits = UInt8(3)
        byt = Parquet.bit2bytewidth(bits)
        itype = Parquet.byt2itype(byt)
        arr = Array{itype}(undef, 8)
        decoded = Parquet.read_bitpacked_run(io, 1, bits, byt, itype, arr)
        @test decoded == Int32[0:7;]
    end
    println("passed.")
end

test_codec()
