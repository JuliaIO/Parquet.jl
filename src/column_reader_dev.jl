using Parquet

path = "c:/data/Performance_2003Q3.txt.parquet"
@time Parquet.read_column(path, 1);


@time read_parquet(path);

path = "c:/git/parquet-data-collection/dsd50p.parquet"
@time adf = read_parquet(path);

@time adf = read_parquet(path, multithreaded=false);



using JDF: type_compress!

@time adf = type_compress!(DataFrame(read_parquet(path, multithreaded=false), copycols=false));

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

@time write_parquet(tmpfile, tbl);

path = tmpfile
@time adf = read_parquet(path);

all([all(c1 .=== c2) for (c1, c2) in zip(tbl, adf)])



using BenchmarkTools
@benchmark adf = read_parquet(path)

col_num = 1
@time col1 = Parquet.read_column(path, col_num);
col1

meta = Parquet.metadata(path);
par = ParFile(path);

nrows(par)

colnames(par)
close(par)

#@time tbl = Parquet.read_column.(Ref(path), 1:length(colnames(par)));


col1

col1[19:20]

last(col1)

uncompressed_data_io = col1[1]

encoded_data_header = Parquet._read_varint(uncompressed_data_io, UInt32)

using Debugger

filemetadata = Parquet.metadata(path);
Debugger.@enter Parquet.read_column(path, filemetadata, col_num);

col1
correct = getproperty(tbl, keys(tbl)[col_num])
all(ismissing.(col1) .== ismissing.(correct))
all(skipmissing(col1) .== skipmissing(correct))

using Test
using Base.Threads: @spawn

checkcol(path, n; multithreaded=true) = begin
    res = Vector{Any}(undef, n)
    if multithreaded
        for col_num in 1:n
            res[col_num] = @spawn Parquet.read_column(path, col_num);
        end
        return fetch.(res)
    else
        for col_num in 1:n
            println(col_num)
            res[col_num] = Parquet.read_column(path, col_num);
        end
        return res
    end
end

@time checkcol(path, 31, multithreaded=true);
@time checkcol(path, 31, multithreaded=false);

@time checkcol(path, 12, multithreaded=false);



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
