using Test, Parquet, DataDecimals, DataStrings, Durations, Tables
@testset "Registered shared values" begin
    for D in (DataDecimals.Decimal32{2}, DataDecimals.Decimal64{2}, DataDecimals.Decimal128{2}, DataDecimals.Decimal256{2})
        x = Union{Missing,D}[D("12.34"), missing, D("-0.01")]
        io = IOBuffer()
        Parquet.write(io, (; x))
        t = Parquet.Table(take!(io))
        actual = Tables.getcolumn(Tables.columns(t), :x)
        @test isequal(actual, x)
        @test eltype(actual) == eltype(x)
        io = IOBuffer()
        Parquet.write(io, t)
        @test isequal(Tables.getcolumn(Tables.columns(Parquet.Table(take!(io))), :x), x)
    end
    io = IOBuffer()
    Parquet.write(io, (; s=[DataStrings.DataString("text"), DataStrings.DataString("a much longer string")]))
    @test eltype(Tables.getcolumn(Tables.columns(Parquet.Table(take!(io))), :s)) == DataStrings.DataString
    @test Parquet.Interval(Durations.Duration(2, 3, 4_000_000)) == Parquet.Interval(2,3,4)
    @test_throws ArgumentError Parquet.Interval(Durations.Duration(0,0,1))
    @test_throws ArgumentError Parquet.Interval(Durations.Duration(-1,0,0))
    @test_throws InexactError Durations.Duration(Parquet.Interval(typemax(UInt32),0,0))
end
