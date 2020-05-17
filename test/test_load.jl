using Parquet
using Test
using Dates

function test_load(file::String)
    p = ParFile(file)
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
    @test length(rowgroups(p, [cnames[1], ["xyz"]])) == 0

    iob = IOBuffer()
    show(iob, p)
    sb = take!(iob)
    @test !isempty(sb)
    println("\t" * String(sb))

    println("\tsuccess")
end

function test_load_all_pages()
    testfolder = joinpath(@__DIR__, "parquet-compatibility")
    for encformat in ("SNAPPY", "GZIP", "NONE")
        for fname in ("nation", "customer")
            testfile = joinpath(testfolder, "parquet-testdata", "impala", "1.1.1-$encformat", "$fname.impala.parquet")
            test_load(testfile)
        end
    end

    testfolder = joinpath(@__DIR__, "julia-parquet-compatibility")
    for encformat in ("ZSTD", "SNAPPY", "GZIP", "NONE")
        for fname in ("nation", "customer")
            testfile = joinpath(testfolder, "Parquet_Files", "$(encformat)_pandas_pyarrow_$(fname).parquet")
            test_load(testfile)
        end
    end
end

function test_load_boolean_and_ts()
    println("testing booleans and timestamps...")
    p = ParFile(joinpath(@__DIR__, "booltest", "alltypes_plain.snappy.parquet"))

    rg = rowgroups(p)
    @test length(rg) == 1
    cc = columns(p, 1)
    @test length(cc) == 11
    cnames = colnames(rg[1])
    @test length(cnames) == length(cc)
    @test cnames[2] == ["bool_col"]
    pg = pages(p, 1, 1)
    @test length(pg) == 2

    rc = RecordCursor(p; rows=1:2, colnames=colnames(p))
    @test length(rc) == 2
    @test eltype(rc) == NamedTuple{(:id, :bool_col, :tinyint_col, :smallint_col, :int_col, :bigint_col, :float_col, :double_col, :date_string_col, :string_col, :timestamp_col),Tuple{Union{Missing, Int32},Union{Missing, Bool},Union{Missing, Int32},Union{Missing, Int32},Union{Missing, Int32},Union{Missing, Int64},Union{Missing, Float32},Union{Missing, Float64},Union{Missing, Array{UInt8,1}},Union{Missing, Array{UInt8,1}},Union{Missing, Int128}}}

    values = Any[]
    for rec in rc
       push!(values, rec)
    end

    @test [v.bool_col for v in values] == [true,false]
    @test [logical_timestamp(v.timestamp_col) for v in values] == [DateTime("2009-04-01T12:00:00"), DateTime("2009-04-01T12:01:00")]
    @test [logical_timestamp(v.timestamp_col; offset=Dates.Second(30)) for v in values] == [DateTime("2009-04-01T12:00:30"), DateTime("2009-04-01T12:01:30")]
    @test [logical_string(v.date_string_col) for v in values] == ["04/01/09", "04/01/09"]
    #dlm,headers=readdlm("booltest/alltypes.csv", ','; header=true)
    #@test [v.bool_col for v in values] == dlm[:,2]  # skipping for now as this needs additional dependency on DelimitedFiles
end

function test_load_nested()
    println("testing nested columns...")
    p = ParFile(joinpath(@__DIR__, "nested", "nested1.parquet"))

    @test nrows(p) == 100
    @test ncols(p) == 5

    rc = RecordCursor(p)
    @test length(rc) == 100
    @test eltype(rc) == NamedTuple{(:_adobe_corpnew,),Tuple{NamedTuple{(:id, :vocab, :frequency, :max_len, :reduced_max_len),Tuple{Union{Missing, Int32},Union{Missing, Array{UInt8,1}},Union{Missing, Int32},Union{Missing, Float64},Union{Missing, Int32}}}}}

    values = Any[]
    for rec in rc
        push!(values, rec)
    end

    v = values[1]._adobe_corpnew
    @test v.frequency == 3
    @test v.id == 1375
    @test v.max_len == 64192.0
    @test v.reduced_max_len == 64
    @test String(copy(v.vocab)) == "10385911_a"

    v = values[100]._adobe_corpnew
    @test v.frequency == 61322
    @test v.id == 724
    @test v.max_len == 64192.0
    @test v.reduced_max_len == 64
    @test String(copy(v.vocab)) == "12400277_a"
end

test_load_all_pages()
test_load_boolean_and_ts()
test_load_nested()
