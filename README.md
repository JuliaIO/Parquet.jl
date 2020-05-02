# Parquet

[![Build Status](https://travis-ci.org/JuliaIO/Parquet.jl.svg?branch=master)](https://travis-ci.org/JuliaIO/Parquet.jl)
[![Build status](https://ci.appveyor.com/api/projects/status/vrqg01w2sj3mfk3d/branch/master?svg=true)](https://ci.appveyor.com/project/tanmaykm/parquet-jl/branch/master)

Load a [parquet file](https://en.wikipedia.org/wiki/Apache_Parquet). Only metadata is read initially, data is loaded in chunks on demand. (Note: [ParquetFiles.jl](https://github.com/queryverse/ParquetFiles.jl) also provides load support for Parquet files under the FileIO.jl package.)

```julia
julia> using Parquet

julia> parfile = "customer.impala.parquet"

julia> p = ParFile(parfile)
Parquet file: /home/tan/Work/julia/packages/Parquet/test/parquet-compatibility/parquet-testdata/impala/1.1.1-SNAPPY/customer.impala.parquet
    version: 1
    nrows: 150000
    created by: impala version 1.2-INTERNAL (build a462ec42e550c75fccbff98c720f37f3ee9d55a3)
    cached: 0 column chunks
```

Examine the schema.

```julia
julia> nrows(p)
150000

julia> ncols(p)
8

julia> colnames(p)
8-element Array{AbstractString,1}:
 "c_acctbal"   
 "c_mktsegment"
 "c_nationkey" 
 "c_name"      
 "c_address"   
 "c_custkey"   
 "c_phone"     
 "c_comment"   

julia> schema(p)
Schema:
    schema {
      optional INT64 c_custkey
      optional BYTE_ARRAY c_name
      optional BYTE_ARRAY c_address
      optional INT32 c_nationkey
      optional BYTE_ARRAY c_phone
      optional DOUBLE c_acctbal
      optional BYTE_ARRAY c_mktsegment
      optional BYTE_ARRAY c_comment
    }
```

Can convert the parquet schema to different forms:

```julia
julia> schema(JuliaConverter(stdout), p, :Customer)
type Customer
    Customer() = new()
    c_custkey::Int64
    c_name::Vector{UInt8}
    c_address::Vector{UInt8}
    c_nationkey::Int32
    c_phone::Vector{UInt8}
    c_acctbal::Float64
    c_mktsegment::Vector{UInt8}
    c_comment::Vector{UInt8}
end

julia> schema(ThriftConverter(stdout), p, :Customer)
struct Customer {
     optional i64 c_custkey
     optional binary c_name
     optional binary c_address
     optional i32 c_nationkey
     optional binary c_phone
     optional double c_acctbal
     optional binary c_mktsegment
     optional binary c_comment
}

julia> schema(ProtoConverter(stdout), p, :Customer)
message Customer {
    optional sint64 c_custkey;
    optional bytes c_name;
    optional bytes c_address;
    optional sint32 c_nationkey;
    optional bytes c_phone;
    optional double c_acctbal;
    optional bytes c_mktsegment;
    optional bytes c_comment;
}
```

Can inject the type dynamically to a module to have further methods working directly on the Julia type.

```julia
julia> schema(JuliaConverter(Main), p, :Customer)

julia> Base.show(io::IO, cust::Customer) = println(io, String(copy(cust.c_name)), " Phone#:", String(copy(cust.c_phone)))
```

Create cursor to iterate over records. In parallel mode, multiple remote cursors can be created and iterated on in parallel.

```julia
julia> rc = RecCursor(p, 1:5, colnames(p), JuliaBuilder(p, Customer))
Record Cursor on /home/tan/Work/julia/packages/Parquet/test/parquet-compatibility/parquet-testdata/impala/1.1.1-SNAPPY/customer.impala.parquet
    rows: 1:5
    cols: c_acctbal.c_mktsegment.c_nationkey.c_name.c_address.c_custkey.c_phone.c_comment


julia> record_state = iterate(rc);

julia> while record_state != nothing
        global record_state
        record = record_state[1]
        state = record_state[2]
        println(record)
        record_state = iterate(rc, state)
    end
Customer#000000033 Phone#:27-375-391-1280
Customer#000000065 Phone#:33-733-623-5267
Customer#000000001 Phone#:25-989-741-2988
Customer#000000642 Phone#:32-925-597-9911
Customer#000000161 Phone#:17-805-718-2449

```

The reader does not interpret any logical types by default. For example, timestamps that are `INT96` values will be represented by default as Julia Int128 types. There will be additional methods provided to interpret such fields which can be applied on the values after they are read. As of now methods are available for:

- timestamp: `logical_timestamp`
- string: `logical_string`

```
julia> for v in values
        println(logical_string(v.date_string_col), ", ", logical_timestamp(v.timestamp_col))
       end
04/01/09, 2009-04-01T12:00:00
04/01/09, 2009-04-01T12:01:00
```

