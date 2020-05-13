using Parquet
using Test
using Random:randstring

tbl = (
    int32 = Int32.(1:1000),
    int64 = Int32.(1:1000),
    float32 = Float32.(1:1000),
    float64 = Float64.(1:1000),
    bool = rand(Bool, 1000),
    string = [randstring(8) for i in 1:1000],
    int32m = rand([missing, 1:100...], 1000),
    int64m = rand([missing, 1:100...], 1000),
    float32m = rand([missing, Float32.(1:100)...], 1000),
    float64m = rand([missing, Float64.(1:100)...], 1000),
    boolm = rand([missing, true, false], 1000),
)

write_parquet("tmp.parquet", tbl)

ParFile("tmp.parquet")

rm("tmp.parquet")
