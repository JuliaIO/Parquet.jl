using Test
import Tables
using PooledArrays

const BLMD = Parquet.Metadata

blcorpus(parts...) = joinpath(get(ENV, "PARQUET_TESTING_DIR",
    joinpath(@__DIR__, "parquet-testing")), parts...)

function blmetadata(bytes::Vector{UInt8})
    file = Parquet.File(bytes)
    try
        return Parquet.Thrift.decode(copy(file.footer.bytes), BLMD.FileMetaData)
    finally
        close(file)
    end
end

# The filter of a column chunk, read from its offset (the header gives its size).
blwords(bytes::Vector{UInt8}, meta) =
    Parquet._bloomwords(view(bytes, (meta.bloom_filter_offset + 1):lastindex(bytes)))

blhas(words, value) = Parquet._bloomcontains(words, Parquet._bloomhash(value))

@testset "XXH64 and split-block filters match reference implementations" begin
    # Python xxhash 3 (XXH64, seed 0).
    for (input, expected) in ((UInt8[], 0xef46db3751d8e999), (b"a", 0xd24ec4f1a98c6e5b),
            (b"abc", 0x44bc2cf5ad770999),
            (b"Nobody inspects the spammish repetition", 0xfbcea83c8a378bf1),
            (UInt8.(0:63), 0xf7c67301db6713f0), (UInt8.(0:36), 0xd93fa2dfee5c24c9))
        @test Parquet._xxhash64(Vector{UInt8}(input)) == expected
    end
    for value in (0x00000000, 0x89abcdef, typemax(UInt32), 0x0123456789abcdef,
            typemax(UInt64))
        @test Parquet._xxhash64(value) == Parquet._xxhash64(collect(reinterpret(UInt8, [value])))
    end
    # parquet-mr's 1024-byte filter of "hello", "parquet", "bloom", and "filter".
    reference = read(blcorpus("data", "bloom_filter.xxhash.bin"))
    words = zeros(UInt32, 256)
    inserted = ("hello", "parquet", "bloom", "filter")
    foreach(value -> Parquet._bloominsert!(words, Parquet._bloomhash(value)), inserted)
    @test Parquet._bloomfilterbytes(words) == reference
    @test Parquet._bloomwords(reference) == words
    @test all(value -> blhas(words, value), inserted)
    @test count(index -> blhas(words, "absent$index"), 1:1000) < 20
    @test Parquet._bloombytes(4000, 0.01) == 8192
    @test Parquet._bloombytes(0, 0.01) == 32
    @test Parquet._bloombytes(10^12, 0.01) == 128 * 1024 * 1024
    @test_throws Parquet.FormatError Parquet._bloomwords(reference[1:100])
end

@testset "a parquet-mr file's bloom filter holds its column's values" begin
    bytes = read(blcorpus("data", "data_index_bloom_encoding_stats.parquet"))
    meta = blmetadata(bytes).row_groups[1].columns[1].meta_data
    words = blwords(bytes, meta)
    table = Parquet.Table(bytes)
    values = try
        [String(value) for value in Tables.getcolumn(table, 1) if !ismissing(value)]
    finally
        close(table)
    end
    @test !isempty(values)
    @test all(value -> blhas(words, value), values)
end

@testset "bloomfilter writes a filter after each row group's column chunks" begin
    n = 3000
    table = (id=collect(1:n), name=["n$(i % 100)" for i in 1:n], flag=isodd.(1:n),
        score=[iseven(i) ? missing : Float32(i) for i in 1:n],
        pooled=PooledArray(["p$(i % 7)" for i in 1:n]), lists=[[i, -i] for i in 1:n])
    bytes = take!(Parquet.write(IOBuffer(), table; bloomfilter=true, rowgroupsize=1000))
    @test bytes == take!(Parquet.write(IOBuffer(), table; bloomfilter=true,
        rowgroupsize=1000, ntasks=1))
    groups = blmetadata(bytes).row_groups
    for (index, group) in enumerate(groups)
        rows = ((index - 1) * 1000 + 1):(index * 1000)
        expected = Dict(["id"] => collect(rows), ["name"] => ["n$(i % 100)" for i in rows],
            ["score"] => [Float32(i) for i in rows if isodd(i)],
            ["pooled"] => ["p$(i % 7)" for i in rows],
            ["lists", "list", "element"] => vcat(collect(rows), -collect(rows)))
        absent = Dict(["id"] => n .+ (1:1000), ["name"] => ["m$i" for i in 1:1000],
            ["score"] => Float32.(n .+ (1:1000)), ["pooled"] => ["q$i" for i in 1:1000],
            ["lists", "list", "element"] => n .+ (1:1000))
        stop = group.file_offset + group.total_compressed_size
        limit = index < length(groups) ? groups[index + 1].file_offset : length(bytes)
        for chunk in group.columns
            meta = chunk.meta_data
            if meta.path_in_schema == ["flag"]
                @test meta.bloom_filter_offset === nothing
                continue
            end
            @test stop <= meta.bloom_filter_offset
            @test meta.bloom_filter_offset + meta.bloom_filter_length <= limit
            words = blwords(bytes, meta)
            @test all(value -> blhas(words, value), expected[meta.path_in_schema])
            @test count(value -> blhas(words, value), absent[meta.path_in_schema]) < 50
        end
    end
    readback = Parquet.Table(bytes)
    try
        @test Tables.getcolumn(readback, :id) == table.id
    finally
        close(readback)
    end
end

@testset "bloomfilter selects columns" begin
    table = (id=[1, 2, 3], name=["a", "b", "c"], flag=[true, false, true],
        lists=[[1], [2, 3], Int[]])
    filtered(bytes) = [chunk.meta_data.path_in_schema for chunk in
        only(blmetadata(bytes).row_groups).columns if chunk.meta_data.bloom_filter_offset !== nothing]
    write(; kwargs...) = take!(Parquet.write(IOBuffer(), table; kwargs...))
    @test isempty(filtered(write()))
    @test filtered(write(bloomfilter=:name)) == [["name"]]
    @test filtered(write(bloomfilter=["id", ("lists", "list", "element")])) ==
        [["id"], ["lists", "list", "element"]]
    @test_throws "BOOLEAN" write(bloomfilter=[:flag])
    @test_throws "in bloomfilter" write(bloomfilter=[:nope])
    @test_throws "bloomfilter must be" write(bloomfilter=0.5)
end
