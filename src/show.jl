function print_indent(io, n)
    for d in 1:n
        print(io, "  ")
    end
end

function show(io::IO, cursor::RecordCursor)
    par = cursor.par
    rows = cursor.colcursors[1].rows
    println(io, "Record Cursor on $(par.path)")
    println(io, "    rows: $rows")

    colpaths = [join(colname, '.') for colname in cursor.colnames]
    println(io, "    cols: $(join(colpaths, ", "))")
end

function show(io::IO, cursor::BatchedColumnsCursor)
    par = cursor.par
    rows = cursor.colcursors[1].rows
    println(io, "Batched Columns Cursor on $(par.path)")
    println(io, "    rows: $rows")
    println(io, "    batches: $(length(cursor))")

    colpaths = [join(colname, '.') for colname in cursor.colnames]
    println(io, "    cols: $(join(colpaths, ", "))")
end

function show(io::IO, schema::SchemaElement, indent::AbstractString="", nchildren::Vector{Int}=Int[])
    print(io, indent)
    lchildren = length(nchildren)
    print_indent(io, lchildren)
    if hasproperty(schema, :repetition_type)
        r = schema.repetition_type
        print(io, (r == FieldRepetitionType.REQUIRED) ? "required" : (r == FieldRepetitionType.OPTIONAL) ? "optional" : "repeated", " ");
    end
    hasproperty(schema, :_type) && print(io, Thrift.enumstr(_Type, schema._type), " ")

    print(io, schema.name)
    hasproperty(schema, :field_id) && print(io, " (", schema.field_id, ")")

    if hasproperty(schema, :converted_type)
        print(io, " # (from ", Thrift.enumstr(ConvertedType, schema.converted_type))
        if schema.converted_type == ConvertedType.DECIMAL
            print(io, "(", schema.precision, ",", schema.scale, ")")
        end
        print(io, ") ")
    end

    if hasproperty(schema, :num_children) && (getproperty(schema, :num_children) > 0)
        push!(nchildren, schema.num_children)
        print(io, " {")
    elseif lchildren > 0
        nchildren[lchildren] -= 1
        if nchildren[lchildren] == 0
            pop!(nchildren)
            println(io, "")
            print_indent(io, length(nchildren))
            print(io, indent, "}")
        end
    end

    println(io, "")
end

function show(io::IO, schema::Vector{SchemaElement}, indent::AbstractString="")
    println(io, indent, "Schema:")
    nchildren=Int[]
    for schemaelem in schema
        show(io, schemaelem, indent * "    ", nchildren)
    end
end

show(io::IO, schema::Schema, indent::AbstractString="") = show(io, schema.schema, indent)

function show(io::IO, kvmeta::KeyValue, indent::AbstractString="")
    println(io, indent, kvmeta.key, " => ", kvmeta.value)
end

function show(io::IO, kvmetas::Vector{KeyValue}, indent::AbstractString="")
    isempty(kvmetas) && return
    println(io, indent, "Metadata:")
    for kvmeta in kvmetas
        show(io, kvmeta, indent * "  ")
    end
end

function show_encodings(io::IO, encodings::Vector{Int32}, indent::AbstractString="")
    isempty(encodings) && return
    print(io, indent, "Encodings: ")
    pfx = ""
    for encoding in encodings
        print(io, pfx, Thrift.enumstr(Encoding, encoding))
        pfx = ", "
    end
    println(io, "")
end

show(io::IO, hdr::IndexPageHeader, indent::AbstractString="") = nothing
function show(io::IO, page::DictionaryPageHeader, indent::AbstractString="")
    println(io, indent, page.num_values, " values")
end

function show(io::IO, hdr::DataPageHeader, indent::AbstractString="")
    println(io, indent, hdr.num_values, " values")
    println(io, indent, "encodings: values as ", Thrift.enumstr(Encoding, hdr.encoding), ", definitions as ", Thrift.enumstr(Encoding, hdr.definition_level_encoding), ", repetitions as ", Thrift.enumstr(Encoding, hdr.repetition_level_encoding))
    hasproperty(hdr, :statistics) && show(io, hdr.statistics, indent)
end

function show(io::IO, hdr::DataPageHeaderV2, indent::AbstractString="")
    compressed = hasproperty(hdr, :is_compressed) ? hdr.is_compressed : true
    println(io, indent, hdr.num_values, " values, ", hdr.num_nulls, " nulls, ", hdr.num_rows, " rows, compressed:", compressed)
    println(io, indent, "encoding:", Thrift.enumstr(Encoding, hdr.encoding), ", definition:", Thrift.enumstr(Encoding, hdr.definition_level_encoding), ", repetition:", Thrift.enumstr(Encoding, hdr.repetition_level_encoding))
    hasproperty(hdr, :statistics) && show(io, hdr.statistics, indent)
end

function show(io::IO, page::PageHeader, indent::AbstractString="")
    println(io, indent, Thrift.enumstr(PageType, page._type), " compressed bytes:", page.compressed_page_size, " (", page.uncompressed_page_size, " uncompressed)")
    hasproperty(page, :data_page_header) && show(io, page.data_page_header, indent * "  ")
    hasproperty(page, :data_page_header_v2) && show(io, page.data_page_header_v2, indent * "  ")
    hasproperty(page, :index_page_header) && show(io, page.index_page_header, indent * "  ")
    hasproperty(page, :dictionary_page_header) && show(io, page.dictionary_page_header, indent * "  ")
end

function show(io::IO, pages::Vector{PageHeader}, indent::AbstractString="")
    println(io, indent, "Pages:")
    for page in pages
        show(io, page, indent * "  ")
    end
end

show(io::IO, page::Page, indent::AbstractString="") = show(io, page.hdr, indent)
show(io::IO, pages::Vector{Page}, indent::AbstractString="") = show(io, [page.hdr for page in pages], indent)

function show(io::IO, stat::Statistics, indent::AbstractString="")
    println(io, indent, "Statistics:")
    if hasproperty(stat, :min) && hasproperty(stat, :max)
        println(io, indent, "  range:", stat.min, ":", stat.max)
    elseif hasproperty(stat, :min)
        println(io, indent, "  min:", stat.min)
    elseif hasproperty(stat, :max)
        println(io, indent, "  max:", stat.max)
    end
    hasproperty(stat, :null_count) && println(io, indent, "  null count:", stat.null_count)
    hasproperty(stat, :distinct_count) && println(io, indent, "  distinct count:", stat.distinct_count)
end

function show(io::IO, page_enc::PageEncodingStats, indent::AbstractString="")
    println(io, indent, page_enc.count, " ", Thrift.enumstr(Encoding, page_enc.encoding), " encoded ", Thrift.enumstr(PageType, page_enc.page_type), " pages")
end

function show(io::IO, page_encs::Vector{PageEncodingStats}, indent::AbstractString="")
    isempty(page_encs) && return
    println(io, indent, "Page encoding statistics:")
    for page_enc in page_encs
        show(io, page_enc, indent * "  ")
    end
end

function show(io::IO, colmeta::ColumnMetaData, indent::AbstractString="")
    println(io, indent, Thrift.enumstr(_Type, coltype(colmeta)), " ", join(colname(colmeta), '.'), ", num values:", colmeta.num_values)
    show_encodings(io, colmeta.encodings, indent)
    if colmeta.codec != CompressionCodec.UNCOMPRESSED
        println(io, indent, Thrift.enumstr(CompressionCodec, colmeta.codec), " compressed bytes:", colmeta.total_compressed_size, " (", colmeta.total_uncompressed_size, " uncompressed)")
    else
        println(io, indent, Thrift.enumstr(CompressionCodec, colmeta.codec), " bytes:", colmeta.total_compressed_size)
    end

    print(io, indent, "offsets: data:", colmeta.data_page_offset)
    hasproperty(colmeta, :index_page_offset) && print(io, ", index:", colmeta.index_page_offset)
    hasproperty(colmeta, :dictionary_page_offset) && print(io, ", dictionary:", colmeta.dictionary_page_offset)
    println(io, "")
    hasproperty(colmeta, :statistics) && show(io, colmeta.statistics, indent)
    hasproperty(colmeta, :encoding_stats) && show(io, colmeta.encoding_stats, indent)
    hasproperty(colmeta, :key_value_metadata) && show(io, colmeta.key_value_metadata, indent)
end

function show(io::IO, columns::Vector{ColumnChunk}, indent::AbstractString="")
    for col in columns
        path = hasproperty(col, :file_path) ? col.file_path : ""
        println(io, indent, "Column at offset: ", path, "#", col.file_offset)
        show(io, col.meta_data, indent * "  ")
    end
end

function show(io::IO, grp::RowGroup, indent::AbstractString="")
    println(io, indent, "Row Group: ", grp.num_rows, " rows in ", grp.total_byte_size, " bytes")
    show(io, grp.columns, indent * "  ")
end

function show(io::IO, row_groups::Vector{RowGroup}, indent::AbstractString="")
    println(io, indent, "Row Groups:")
    for grp in row_groups
        show(io, grp, indent * "  ")
    end
end

function show(io::IO, meta::FileMetaData, indent::AbstractString="")
    println(io, indent, "version: ", meta.version)
    println(io, indent, "nrows: ", meta.num_rows)
    println(io, indent, "created by: ", meta.created_by)

    show(io, meta.schema, indent)
    show(io, meta.row_groups, indent)
    hasproperty(meta, :key_value_metadata) && show(io, meta.key_value_metadata, indent)
end

function show(io::IO, par::Parquet.File)
    println(io, "Parquet file: $(par.path)")
    meta = par.meta
    println(io, "    version: $(meta.version)")
    println(io, "    nrows: $(meta.num_rows)")
    println(io, "    created by: $(meta.created_by)")
    println(io, "    cached: $(length(par.page_cache.refs)) column chunks")
end

function show(io::IO, table::Parquet.Table)
    print(io, "Parquet.Table(\"$(getfield(table, :path))\")")
end

function show(io::IO, dataset::Parquet.Dataset)
    print(io, "Parquet.Dataset(\"$(getfield(dataset, :path))\")")
end
