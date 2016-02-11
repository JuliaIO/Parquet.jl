using Parquet
using Base.Test

parcompat = joinpath(dirname(@__FILE__), "parquet-compatibility")
# look for parquet-compatibility in test folder, clone to tempdir if not found
if !isdir(parcompat)
    parcompat = joinpath(tempdir(), "parquet-compatibility")
    run(`git clone https://github.com/Parquet/parquet-compatibility.git $parcompat`)
end

function test_load(file)
    p = ParFile(joinpath(parcompat, file))
    println("loaded $file")
    @test isa(p.meta, Parquet.FileMetaData)

    rg = rowgroups(p)
    @test length(rg) > 0
    println("\tfound $(length(rg)) row groups")

    cc = columns(p, 1)
    println("\tfound $(length(cc)) column chunks in row group 1")

    cnames = colnames(rg[1])
    @test length(cnames) == length(cc)
    println("\tcolumns: $cnames")

    pg = pages(p, 1, 1)
    println("\tfound $(length(pg)) pages in column chunk 1 of row group 1")

    bpg = bytes(pg[1])
    println("\tread $(length(bpg)) bytes from page 1, column chunk 1 of row group 1")

    cc1 = columns(p, rg[1], cnames[1:2:3])
    @test length(cc) > length(cc1)
    @test length(rowgroups(p, [cnames[1], "xyz"])) == 0
    println("\tsuccess")
end

function test_all_pages(file)
    p = ParFile(joinpath(parcompat, file))
    println("loaded $file")
    @test isa(p.meta, Parquet.FileMetaData)

    rgs = rowgroups(p)
    @test length(rgs) > 0
    println("\tfound $(length(rgs)) row groups")

    for rg in rgs
        ccs = columns(p, rg)
        println("\treading row group with $(length(ccs)) column chunks")

        for cc in ccs
            pgs = pages(p, cc)
            println("\t\treading column chunk with $(length(pgs)) pages")

            for pg in pgs
                bpg = bytes(pg)
                println("\t\t\tread page with $(length(bpg)) bytes")
                if (pg.hdr._type == Parquet.PageType.DATA_PAGE) || (pg.hdr._type == Parquet.PageType.DATA_PAGE_V2)
                    println("\t\t\treading data page")
                    vals, defn_levels, repn_levels = values(p, pg)
                    println("\t\t\tread $(length(defn_levels)) defn_levels, $(length(repn_levels)) repn_levels, $(length(vals)) values")
                elseif pg.hdr._type == Parquet.PageType.DICTIONARY_PAGE
                    println("\t\t\treading dictionary page")
                    vals = values(p, pg)[1]
                    println("\t\t\tread $(length(vals)) values")
                elseif pg.hdr._type == Parquet.PageType.INDEX_PAGE
                    println("\t\t\tnot reading index page yet")
                else
                    error("unknown page type $(pg.hdr._type)")
                end
            end
        end
    end

    println("\tsuccess")
end

function test_codec()
    println("testing reading bitpacked run (old scheme)...")
    let data = UInt8[0x05, 0x39, 0x77]
        io = PipeBuffer(data)
        decoded = Parquet.read_bitpacked_run_old(io, 8, 3)
        @test decoded == Int32[0:7;]
    end
    println("passed.")

    println("testing reading bitpacked run...")
    let data = UInt8[0x88, 0xc6, 0xfa]
        io = PipeBuffer(data)
        decoded = Parquet.read_bitpacked_run(io, 1, 3)
        @test decoded == Int32[0:7;]
    end
    println("passed.")
end

for encformat in ("SNAPPY", "GZIP", "NONE")
    for fname in ("nation", "customer")
        test_load("parquet-testdata/impala/1.1.1-$encformat/$fname.impala.parquet")
    end
end

for encformat in ("SNAPPY", "GZIP", "NONE")
    for fname in ("nation", "customer")
        test_all_pages("parquet-testdata/impala/1.1.1-$encformat/$fname.impala.parquet")
    end
end

test_codec()
