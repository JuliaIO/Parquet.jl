using Parquet
using Test

function test_decode(file, parcompat=joinpath(dirname(@__FILE__), "parquet-compatibility"))
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
            println("\t\treading column chunk with $(length(pgs)) pages, $(colname(cc))")

            for pg in pgs
                bpg = bytes(pg)
                println("\t\t\tread page with $(length(bpg)) bytes")
                if (pg.hdr._type == Parquet.PageType.DATA_PAGE) || (pg.hdr._type == Parquet.PageType.DATA_PAGE_V2)
                    #println("\t\t\treading data page")
                    vals, defn_levels, repn_levels = values(p, pg)
                    println("\t\t\tread data page, $(length(defn_levels)) defn_levels, $(length(repn_levels)) repn_levels, $(length(vals)) values")
                elseif pg.hdr._type == Parquet.PageType.DICTIONARY_PAGE
                    #println("\t\t\treading dictionary page")
                    vals = values(p, pg)[1]
                    println("\t\t\tread dictionary page, $(length(vals)) values")
                elseif pg.hdr._type == Parquet.PageType.INDEX_PAGE
                    println("\t\t\tnot reading index page yet")
                else
                    error("unknown page type $(pg.hdr._type)")
                end
            end

            println("\t\tre-reading column chunk for values. total $(length(pgs)) pages")
            #vals, defn_levels, repn_levels = values(p, cc)
            #println("\t\t\tread $(length(vals)) values, $(length(defn_levels)) defn levels, $(length(repn_levels)) repn levels")
        end
    end

    println("\tsuccess")
end

function test_decode_all_pages()
    for encformat in ("NONE", "SNAPPY", "GZIP", "ZSTD")
        for source in ("_pandas_pyarrow_",)
            for fname in ("nation", "customer")
                parquet_filename = "Parquet_Files/" * encformat * source * fname * ".parquet"
                test_decode(parquet_filename)
            end
        end
    end
end

test_decode_all_pages()
