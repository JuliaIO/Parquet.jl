using Parquet
using Test
using Dates
using Tables

function test_load(file::String)
    p = Parquet.File(file)
    @debug("load file", file)
    @test isa(p.meta, Parquet.FileMetaData)

    rgs = rowgroups(p)
    @test length(rgs) > 0

    cnames = colnames(p)
    @test length(cnames) > 0
    @debug("data columns", file, cnames)

    for rg in rgs
        ccs = columns(p, rg)
        @debug("reading row group", file, ncolumnchunks=length(ccs))

        for cc in ccs
            npages = 0
            ccp = Parquet.ColumnChunkPages(p, cc)
            result = iterate(ccp)
            npages = 0
            ncompressedbytes = 0
            nuncompressedbytes = 0
            while result !== nothing
                page,nextpos = result
                result = iterate(ccp, nextpos)
                npages += 1
                ncompressedbytes += Parquet.page_size(page.hdr)
                nuncompressedbytes += page.hdr.uncompressed_page_size
            end
            @test npages > 0
            @test ncompressedbytes > 0
            @test nuncompressedbytes >= 0
            @debug("read column chunk", file, npages, ncompressedbytes, nuncompressedbytes)
        end
    end

    @debug("done loading file", file, showbuffer=String(sb))

    iob = IOBuffer()
    show(iob, p)
    sb = take!(iob)
    @test !isempty(sb)
    @debug("parquet file show", file, showbuffer=String(sb))

    iob = IOBuffer()
    show(iob, p.meta)
    sb = take!(iob)
    @test !isempty(sb)
    @debug("parquet file metadata show", file, showbuffer=String(sb))

    show(iob, schema(p))
    sb = take!(iob)
    @test !isempty(sb)
    @debug("schema show", file, showbuffer=String(sb))

    nothing
end

function test_decode(file)
    p = Parquet.File(file)
    @debug("load file", file)
    @test isa(p.meta, Parquet.FileMetaData)

    rgs = rowgroups(p)
    @test length(rgs) > 0

    for rg in rgs
        ccs = columns(p, rg)
        @debug("reading row group", file, ncolumnchunks=length(ccs))

        for cc in ccs
            jtype = Parquet.elemtype(Parquet.elem(schema(p), colname(p,cc)))
            npages = 0
            ccpv = Parquet.ColumnChunkPageValues(p, cc, jtype)
            result = iterate(ccpv)
            valcount = repncount = defncount = 0
            while result !== nothing
                resultdata,nextpos = result
                valcount += resultdata.value.offset
                repncount += resultdata.repn_level.offset
                defncount += resultdata.defn_level.offset
                result = iterate(ccpv, nextpos)
                npages += 1
            end

            @debug("read", file, npages, valcount, defncount, repncount)
        end
    end
    nothing
end

function test_decode_all_pages()
    @testset "decode parquet-compatibility test files" begin
        testfolder = parcompat
        for encformat in ("SNAPPY", "GZIP", "NONE")
            for fname in ("nation", "customer")
                testfile = joinpath(testfolder, "parquet-testdata", "impala", "1.1.1-$encformat", "$fname.impala.parquet")
                test_decode(testfile)
            end
        end
    end

    @testset "decode julia-parquet-compatibility test files" begin
        testfolder = julia_parcompat
        for encformat in ("ZSTD", "SNAPPY", "GZIP", "NONE")
            for fname in ("nation", "customer")
                testfile = joinpath(testfolder, "Parquet_Files", "$(encformat)_pandas_pyarrow_$(fname).parquet")
                test_decode(testfile)
            end
        end
    end

    @testset "decode missingvalues test file" begin
        test_decode(joinpath(@__DIR__, "missingvalues", "synthetic_data.parquet"))
    end
end

function test_load_all_pages()
    @testset "load parquet-compatibility test files" begin
        testfolder = parcompat
        for encformat in ("SNAPPY", "GZIP", "NONE")
            for fname in ("nation", "customer")
                testfile = joinpath(testfolder, "parquet-testdata", "impala", "1.1.1-$encformat", "$fname.impala.parquet")
                test_load(testfile)
            end
        end
    end

    @testset "load julia-parquet-compatibility test files" begin
        testfolder = julia_parcompat
        for encformat in ("ZSTD", "SNAPPY", "GZIP", "NONE")
            for fname in ("nation", "customer")
                testfile = joinpath(testfolder, "Parquet_Files", "$(encformat)_pandas_pyarrow_$(fname).parquet")
                test_load(testfile)
            end
        end
    end
end

function test_load_boolean_and_ts()
    @testset "load booleans and timestamps" begin
        @debug("load booleans and timestamps")
        p = Parquet.File(joinpath(@__DIR__, "booltest", "alltypes_plain.snappy.parquet"))

        rg = rowgroups(p)
        @test length(rg) == 1
        cc = columns(p, 1)
        @test length(cc) == 11
        cnames = colnames(p)
        @test length(cnames) == length(cc)
        @test cnames[2] == ["bool_col"]

        rc = RecordCursor(p; rows=1:2, colnames=colnames(p))
        @test length(rc) == 2
        @test eltype(rc) == NamedTuple{(:id, :bool_col, :tinyint_col, :smallint_col, :int_col, :bigint_col, :float_col, :double_col, :date_string_col, :string_col, :timestamp_col),Tuple{Union{Missing, Int32},Union{Missing, Bool},Union{Missing, Int32},Union{Missing, Int32},Union{Missing, Int32},Union{Missing, Int64},Union{Missing, Float32},Union{Missing, Float64},Union{Missing, Array{UInt8,1}},Union{Missing, Array{UInt8,1}},Union{Missing, DateTime}}}

        values = collect(rc)
        @test [v.bool_col for v in values] == [true,false]
        @test [v.timestamp_col for v in values] == [DateTime("2009-04-01T12:00:00"), DateTime("2009-04-01T12:01:00")]

        cc = BatchedColumnsCursor(p)
        values, _state = iterate(cc)
        @test values.timestamp_col == [DateTime("2009-04-01T12:00:00"), DateTime("2009-04-01T12:01:00")]

        p = Parquet.File(joinpath(@__DIR__, "booltest", "alltypes_plain.snappy.parquet"); map_logical_types=Dict(["date_string_col"]=>(String,logical_string)))
        rc = RecordCursor(p; rows=1:2, colnames=colnames(p))
        values = collect(rc)
        @test [v.date_string_col for v in values] == ["04/01/09", "04/01/09"]

        cc = BatchedColumnsCursor(p)
        values, _state = iterate(cc)
        @test values.date_string_col == ["04/01/09", "04/01/09"]

        p = Parquet.File(joinpath(@__DIR__, "booltest", "alltypes_plain.snappy.parquet"); map_logical_types=Dict(["timestamp_col"]=>(DateTime,(v)->logical_timestamp(v; offset=Dates.Second(30)))))
        rc = RecordCursor(p; rows=1:2, colnames=colnames(p))
        values = collect(rc)
        @test [v.timestamp_col for v in values] == [DateTime("2009-04-01T12:00:30"), DateTime("2009-04-01T12:01:30")]

        cc = BatchedColumnsCursor(p)
        values, _state = iterate(cc)
        @test values.timestamp_col == [DateTime("2009-04-01T12:00:30"), DateTime("2009-04-01T12:01:30")]
        #dlm,headers=readdlm("booltest/alltypes.csv", ','; header=true)
        #@test [v.bool_col for v in values] == dlm[:,2]  # skipping for now as this needs additional dependency on DelimitedFiles
    end
end

function test_load_nested()
    @testset "load nested columns" begin
        @debug("load nested columns")
        p = Parquet.File(joinpath(@__DIR__, "nested", "nested1.parquet"))

        @test nrows(p) == 100
        @test ncols(p) == 5

        rc = RecordCursor(p)
        @test length(rc) == 100
        @test eltype(rc) == NamedTuple{(:_adobe_corpnew,),Tuple{NamedTuple{(:id, :vocab, :frequency, :max_len, :reduced_max_len),Tuple{Union{Missing, Int32},Union{Missing, String},Union{Missing, Int32},Union{Missing, Float64},Union{Missing, Int32}}}}}

        values = Any[]
        for rec in rc
            push!(values, rec)
        end

        v = values[1]._adobe_corpnew
        @test v.frequency == 3
        @test v.id == 1375
        @test v.max_len == 64192.0
        @test v.reduced_max_len == 64
        @test v.vocab == "10385911_a"

        v = values[100]._adobe_corpnew
        @test v.frequency == 61322
        @test v.id == 724
        @test v.max_len == 64192.0
        @test v.reduced_max_len == 64
        @test v.vocab == "12400277_a"

        p = Parquet.File(joinpath(@__DIR__, "nested", "nested.parq"))

        @test nrows(p) == 10
        @test ncols(p) == 1

        rc = RecordCursor(p)
        @test length(rc) == 10
        @test eltype(rc) == NamedTuple{(:nest,),Tuple{Union{Missing, NamedTuple{(:thing,),Tuple{Union{Missing, NamedTuple{(:list,),Tuple{Array{NamedTuple{(:element,),Tuple{Union{Missing, String}}},1}}}}}}}}}

        values = collect(rc)
        v = first(values)
        @test length(v.nest.thing.list) == 2
        @test v.nest.thing.list[1].element == "hi"
        v = last(values)
        @test length(v.nest.thing.list) == 2
        @test v.nest.thing.list[1].element == "world"
    end
end

function test_load_multiple_rowgroups()
    @testset "testing multiple rowgroups..." begin
        @debug("load multiple rowgroups")
        p = Parquet.File(joinpath(@__DIR__, "rowgroups", "multiple_rowgroups.parquet"))

        @test nrows(p) == 100
        @test ncols(p) == 12

        rc = RecordCursor(p)
        @test length(rc) == 100
        vals = collect(rc)
        @test length(vals) == 100
        @test vals[1].int64 == vals[51].int64
        @test vals[1].int32 == vals[51].int32

        cc = BatchedColumnsCursor(p)
        @test length(cc) == 2
        colvals = collect(cc)
        @test length(colvals) == 2
        @test length(colvals[1].int32) == 50
    end
end

function test_load_file()
    @testset "load a file" begin
        filename = joinpath(@__DIR__, "rowgroups", "multiple_rowgroups.parquet")

        table = read_parquet(filename)
        @test Tables.istable(table)
        @test Tables.columnaccess(table)
        @test Tables.schema(table).names == (:int32, :int64, :float32, :float64, :bool, :string, :int32m, :int64m, :float32m, :float64m, :boolm, :stringm)
        cols = Tables.columns(table)
        @test all([length(col)==100 for col in cols])   # all columns must be 100 rows long
        @test length(cols) == 12                        # 12 columns
        partitions = Tables.partitions(table)
        @test length(partitions) == 2
        partition_tables = collect(partitions)
        @test length(partition_tables) == 2
        @test Tables.istable(partition_tables[1])
        @test Tables.columnaccess(partition_tables[1])
        close(table)

        table = read_parquet(filename; rows=1:10)
        cols = Tables.columns(table)
        @test all([length(col)==10 for col in cols])    # all columns must be 100 rows long
        @test length(cols) == 12                        # 12 columns
        partitions = Tables.partitions(table)
        @test length(partitions) == 1
        @test length(collect(partitions)) == 1
        close(table)

        table = read_parquet(filename; rows=1:100, batchsize=10)
        cols = Tables.columns(table)
        @test all([length(col)==100 for col in cols])   # all columns must be 100 rows long
        @test length(cols) == 12                        # 12 columns
        partitions = Tables.partitions(table)
        @test length(partitions) == 10
        @test length(collect(partitions)) == 10

        iob = IOBuffer()
        show(iob, table)
        @test startswith(String(take!(iob)), "Parquet.Table(")
        close(table)

        # test loading table with separately specified schema
        parfile = joinpath(@__DIR__, "datasets", "bool_partition", "bool=False", "560bea059bf94bae9f785f9b4a455317.parquet")
        schemafile = joinpath(@__DIR__, "datasets", "bool_partition", "_common_metadata")
        schema = Tables.schema(read_parquet(schemafile))
        table = Parquet.Table(parfile, schema)
        cols = Tables.columns(table)
        @test length(cols) == 12
        @test all(ismissing.(cols.bool))
        close(table)

        # test loading table with custom missing partition column generator
        table = Parquet.Table(parfile, schema; column_generator=(t,c,l)->trues(l))
        cols = Tables.columns(table)
        @test sum(cols.bool) == 42
    end
end
  
function test_load_at_offset()
    @testset "load file at offset" begin
        testfolder = parcompat
        testfile = joinpath(testfolder, "parquet-testdata", "impala", "1.1.1-NONE", "customer.impala.parquet")
        parquet_file = Parquet.File(testfile)

        vals_20000_40000 = first(collect(Parquet.BatchedColumnsCursor(parquet_file; rows=20000:40000))).c_custkey
        vals_1_40000 = first(collect(Parquet.BatchedColumnsCursor(parquet_file; rows=1:40000))).c_custkey
        vals_2_40001 = first(collect(Parquet.BatchedColumnsCursor(parquet_file; rows=2:40001))).c_custkey

        @test vals_20000_40000 == vals_1_40000[20000:40000]
        @test vals_20000_40000 != vals_1_40000[1:20000]
        @test vals_2_40001[1:10000] == vals_1_40000[2:10001]
        @test vals_2_40001[1:10000] != vals_1_40000[1:10000]
    end
end

function test_mmap_mode()
    @testset "memory map modes" begin
        default_mode = Parquet._use_mmap[]
        for mode in (true, false)
            (mode === default_mode) && continue
            Parquet.use_mmap(mode)
            table = read_parquet(joinpath(@__DIR__, "rowgroups", "multiple_rowgroups.parquet"))
            cols = Tables.columns(table)
            @test all([length(col)==100 for col in cols])
            @test length(cols) == 12
            close(table)
        end
        Parquet.use_mmap(default_mode)
    end
end

function test_zero_rows()
    @testset "load file with no rows" begin
        table = read_parquet(joinpath(@__DIR__, "empty", "empty.parquet"))
        cols = Tables.columns(table)
        @test all([length(col)==0 for col in cols])
        @test length(cols) == 31
        partitions = Tables.partitions(table)
        @test length(partitions) == 0
        schema = Tables.schema(table)
        @test first(schema.types) == Union{Missing, Int64}
        @test last(schema.types) == Union{Missing, String}
    end
end

function test_dataset()
    @testset "load dataset" begin
        # load dataset partitioned by boolean column
        dataset_path = joinpath(@__DIR__, "datasets", "bool_partition")

        dataset = read_parquet(dataset_path)
        @test Tables.istable(dataset)
        @test Tables.columnaccess(dataset)
        @test Tables.schema(dataset).names == (:int32, :int64, :float32, :float64, :bool, :string, :int32m, :int64m, :float32m, :float64m, :boolm, :stringm)
        cols = Tables.columns(dataset)
        @test all([length(col)==100 for col in cols])   # all columns must be 100 rows long
        @test length(cols) == 12                        # 12 columns
        @test sum(cols.bool) == 58                      # 58 rows in `bool=true` partition

        partitions = []
        for partition in Tables.partitions(dataset)
            push!(partitions, partition)
        end
        @test length(partitions) == 2

        iob = IOBuffer()
        show(iob, dataset)
        @test startswith(String(take!(iob)), "Parquet.Dataset(")
        close(dataset)

        # load dataset with a filter
        dataset = read_parquet(dataset_path; filter=(path)->occursin("bool=false", lowercase(path)))
        @test Tables.istable(dataset)
        @test Tables.columnaccess(dataset)
        @test Tables.schema(dataset).names == (:int32, :int64, :float32, :float64, :bool, :string, :int32m, :int64m, :float32m, :float64m, :boolm, :stringm)
        cols = Tables.columns(dataset)
        @test all([length(col)==42 for col in cols])    # all bool=false columns must be 42 rows long
        @test length(cols) == 12                        # 12 columns

        partitions = []
        for partition in Tables.partitions(dataset)
            push!(partitions, partition)
        end
        @test length(partitions) == 1
        close(dataset)

        # load dataset partitioned by string column
        dataset_path = joinpath(@__DIR__, "datasets", "string_partition")
        dataset = read_parquet(dataset_path)
        @test Tables.istable(dataset)
        @test Tables.columnaccess(dataset)
        @test Tables.schema(dataset).names == (:col2, :col9)
        cols = Tables.columns(dataset)
        @test all([length(col)==4 for col in cols]) # all columns must be 4 rows long
        @test length(cols) == 2                     # 2 columns
        @test cols.col2 == ["2002-02-01", "2002-02-01", "2002-02-02", "2002-02-02"]
        @test all(cols.col9 .== "02/2030")

        # load dataset partitioned by date column
        @test Parquet.parse_date("2002-02-01") == Date("2002-02-01")
        @test Parquet.parse_datetime("2002-02-01") == DateTime("2002-02-01")
        @test_throws Exception Parquet.parse_date("not date")
        @test_throws Exception Parquet.parse_datetime("not date")
        dataset_path = joinpath(@__DIR__, "datasets", "date_partition")
        dataset = read_parquet(dataset_path)
        @test Tables.istable(dataset)
        @test Tables.columnaccess(dataset)
        @test Tables.schema(dataset).names == (:col2, :col9)
        cols = Tables.columns(dataset)
        @test all([length(col)==4 for col in cols]) # all columns must be 4 rows long
        @test length(cols) == 2                     # 2 columns
        @test cols.col2 == [DateTime("2002-02-01"), DateTime("2002-02-01"), DateTime("2002-02-02"), DateTime("2002-02-02")]
        @test all(cols.col9 .== "02/2030")

        # load dataset without metadata file
        mktempdir() do path
            new_dataset = joinpath(path, "datasets")
            cp(dataset_path, new_dataset)
            metafile = joinpath(new_dataset, "_common_metadata")
            rm(metafile)
            dataset = read_parquet(new_dataset)
            @test Tables.istable(dataset)
            @test Tables.columnaccess(dataset)
        end
    end
end

@testset "load files" begin
    test_load_all_pages()
    test_decode_all_pages()
    test_load_boolean_and_ts()
    test_load_nested()
    test_load_multiple_rowgroups()
    test_load_file()
    test_load_at_offset()
    test_mmap_mode()
    test_zero_rows()
    test_dataset()
end
