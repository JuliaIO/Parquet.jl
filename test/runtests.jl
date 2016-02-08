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

for encformat in ("SNAPPY", "GZIP", "NONE")
    for fname in ("nation", "customer")
        test_load("parquet-testdata/impala/1.1.1-$encformat/$fname.impala.parquet")
    end
end
