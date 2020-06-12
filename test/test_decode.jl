using Parquet
using Test

function test_decode(file)
    p = ParFile(file)
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
            println("\t\treading column chunk with $(length(pgs)) pages, $(colname(cc))")
            println("\t\tre-reading column chunk for values. total $(length(pgs)) pages")
            jtype = Parquet.elemtype(Parquet.elem(schema(p), colname(cc)))

            ccpv = Parquet.ColumnChunkPageValues(p, cc, jtype)
            result = iterate(ccpv)
            valcount = repncount = defncount = 0
            while result !== nothing
                resultdata,nextpos = result
                valcount += resultdata.value.offset
                repncount += resultdata.repn_level.offset
                defncount += resultdata.defn_level.offset
                result = iterate(ccpv, nextpos)
            end

            println("\t\t\tread $valcount values, $defncount defn levels, $repncount repn levels")
        end
    end

    println("\tsuccess")
end

function test_decode_all_pages()
    testfolder = joinpath(@__DIR__, "parquet-compatibility")
    for encformat in ("SNAPPY", "GZIP", "NONE")
        for fname in ("nation", "customer")
            testfile = joinpath(testfolder, "parquet-testdata", "impala", "1.1.1-$encformat", "$fname.impala.parquet")
            test_decode(testfile)
        end
    end

    testfolder = joinpath(@__DIR__, "julia-parquet-compatibility")
    for encformat in ("ZSTD", "SNAPPY", "GZIP", "NONE")
        for fname in ("nation", "customer")
            testfile = joinpath(testfolder, "Parquet_Files", "$(encformat)_pandas_pyarrow_$(fname).parquet")
            test_decode(testfile)
        end
    end

    test_decode(joinpath(@__DIR__, "missingvalues", "synthetic_data.parquet"))
end

test_decode_all_pages()
