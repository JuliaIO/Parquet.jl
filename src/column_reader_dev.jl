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
)

tmpfile = tempname()*".parquet"

write_parquet(tmpfile, tbl)

path = tmpfile

for i in 1:12
    @time col1 = Parquet.read_column(path, i);
end

@time col1 = Parquet.read_column(path, 1)
col1 == tbl.int32

col_num = 5

filemetadata = Parquet.metadata(path)
par = ParFile(path)
fileio = open(path)

T = TYPES[filemetadata.schema[col_num+1]._type+1]

# TODO detect if missing is necessary
res = Vector{Union{Missing, T}}(missing, nrows(par))

length(filemetadata.row_groups)

from = 1
last_from = from

row_group = filemetadata.row_groups[1]

colchunk_meta = row_group.columns[col_num].meta_data

if isfilled(colchunk_meta, :dictionary_page_offset)
    seek(fileio, colchunk_meta.dictionary_page_offset)
    dict_page_header = read_thrift(fileio, PAR2.PageHeader)
    compressed_data = read(fileio, dict_page_header.compressed_page_size)
    uncompressed_data = decompress_with_codec(compressed_data, colchunk_meta.codec)
    @assert length(uncompressed_data) == dict_page_header.uncompressed_page_size

    if dict_page_header.dictionary_page_header.encoding == PAR2.Encoding.PLAIN_DICTIONARY
        # see https://github.com/apache/parquet-format/blob/master/Encodings.md#dictionary-encoding-plain_dictionary--2-and-rle_dictionary--8
        # which is in effect the plain encoding see https://github.com/apache/parquet-format/blob/master/Encodings.md#plain-plain--0
        dict = reinterpret(T, uncompressed_data)
    else
        error("Only Plain Dictionary encoding is supported")
    end
else
    dict = nothing
end

# seek to the first data page
seek(fileio, colchunk_meta.data_page_offset)

pg = read_thrift(fileio, PAR2.PageHeader)

Parquet.read_data_page_vals!(res, fileio, dict, colchunk_meta.codec, T, from)

# repeated read data page
while from - last_from  < row_group.num_rows
    from = read_data_page_vals!(res, fileio, dict, colchunk_meta.codec, T, from) + 1
end
last_from = from


res
