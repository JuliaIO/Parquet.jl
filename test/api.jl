@testset "public surface" begin
    @test !Base.isexported(Parquet, :File)
    @test !Base.isexported(Parquet, :Limits)
    @test !Base.isexported(Parquet, :Table)
    @test !Base.isexported(Parquet, :close!)
    @test !Base.isexported(Parquet, :write)
    for name in (:BSONValue, :Decimal, :Interval, :JSONValue, :LogicalColumn,
        :Timestamp)
        @test !Base.isexported(Parquet, name)
    end
    if VERSION >= v"1.11"
        @test Base.ispublic(Parquet, :File)
        @test Base.ispublic(Parquet, :Limits)
        @test Base.ispublic(Parquet, :Table)
        @test Base.ispublic(Parquet, :close!)
        @test Base.ispublic(Parquet, :write)
        for name in (:BSONValue, :Decimal, :Interval, :JSONValue, :LogicalColumn,
            :Timestamp)
            @test Base.ispublic(Parquet, name)
        end
    end
end
