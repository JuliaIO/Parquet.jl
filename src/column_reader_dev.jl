using Parquet
using Parquet:TYPES, read_thrift, PAR2, BitPackedIterator, decompress_with_codec
using Thrift: isfilled
using Snappy, CodecZlib, CodecZstd

path = "c:/git/parquet-data-collection/dsd50p.parquet"
path = "c:/data/Performance_2003Q3.txt.parquet"

meta = Parquet.metadata(path);
par = ParFile(path);

nrows(par)

colnames(par)

@time tbl = Parquet.read_column.(Ref(path), 1:length(colnames(par)));

using Random: randstring
tbl = (
    int32 = rand(Int32, 1000),
    int64 = rand(Int64, 1000),
    float32 = rand(Float32, 1000),
    float64 = rand(Float64, 1000),
    bool = rand(Bool, 1000),
    string = [randstring(8) for i in 1:1000],
    int32m = rand([missing, rand(Int32, 10)...], 1000),
    int64m = rand([missing, rand(Int64, 10)...], 1000),
    float32m = rand([missing, rand(Float32, 10)...], 1000),
    float64m = rand([missing, rand(Float64, 10)...], 1000),
    boolm = rand([missing, true, false], 1000),
    stringm = rand([missing, "abc", "def", "ghi"], 1000)
);

tmpfile = tempname()*".parquet"

write_parquet(tmpfile, tbl);

path = tmpfile

col_num = 3
@time col1 = Parquet.read_column(path, col_num);
col1
correct = getproperty(tbl, keys(tbl)[col_num])
all(ismissing.(col1) .== ismissing.(correct))
all(skipmissing(col1) .== skipmissing(correct))

using Test
checkcol(col_num) = begin
    println(col_num)
    @time col1 = Parquet.read_column(path, col_num);
    # correct = getproperty(tbl, keys(tbl)[col_num])
    # @test all(ismissing.(col1) .== ismissing.(correct))
    # @test all(skipmissing(col1) .== skipmissing(correct))
end

@time checkcol.(1:31)



using Base.Threads: @spawn
read1(path, n) = begin
    result = Vector{Any}(undef, length(n))
    for i in n
        result[i] = @spawn Parquet.read_column(path, i)
    end
    fetch.(result)
end

@time a = read1(path, 1:5)

using DataFrames

@time ba=DataFrame(a, copycols=false)
@time ba=DataFrame(a)

b1


import Base: add_int
@edit Base.add_int(100, 1)

add_int
