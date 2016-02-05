function print_indent(io, n)
    for d in 1:n
        print(io, "  ")
    end
end

function show(io::IO, schema::SchemaElement, indent::AbstractString="", nchildren::Vector{Int}=Int[])
    print(io, indent)
    lchildren = length(nchildren)
    print_indent(io, lchildren)
    if isfilled(schema, :repetition_type)
        r = schema.repetition_type
        print(io, (r == FieldRepetitionType.REQUIRED) ? "required" : (r == FieldRepetitionType.OPTIONAL) ? "optional" : "repeated", " ");
    end
    isfilled(schema, :_type) && print(io, Thrift.enumstr(_Type, schema._type), " ")

    print(io, schema.name)
    isfilled(schema, :field_id) && print(io, " (", schema.field_id, ")")

    if isfilled(schema, :converted_type)
        print(io, "# (from ", Thrift.enumstr(ConvertedType, schema.converted_type))
        if schema.converted_type == ConvertedType.DECIMAL
            print(io, "(", schema.scale, ".", schema.precision)
        end
        print(") ")
    end

    if isfilled(schema, :num_children)
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
    println(io, indent, hdr.num_values, " values")
end
function show(io::IO, hdr::DataPageHeader, indent::AbstractString="")
    println(io, indent, hdr.num_values, " values")
    println(io, indent, "encoding:", Thrift.enumstr(Encoding, hdr.encoding), ", definition:", Thrift.enumstr(Encoding, hdr.definition_level_encoding), ", repetition:", Thrift.enumstr(Encoding, hdr.repetition_level_encoding))
end

function show(io::IO, page::PageHeader, indent::AbstractString="")
    println(io, indent, Thrift.enumstr(PageType, page._type), " compressed bytes:", page.compressed_page_size, " (", page.uncompressed_page_size, " uncompressed)")
    Thrift.isfilled(page, :data_page_header) && show(io, page.data_page_header, indent * "  ")
    Thrift.isfilled(page, :index_page_header) && show(io, page.index_page_header, indent * "  ")
    Thrift.isfilled(page, :dictionary_page_header) && show(io, page.dictionary_page_header, indent * "  ")
end

function show(io::IO, pages::Vector{PageHeader}, indent::AbstractString="")
    println(io, indent, "Pages:")
    for page in pages
        show(io, page, indent * "  ")
    end
end

function show(io::IO, colmeta::ColumnMetaData, indent::AbstractString="")
    print(io, indent, Thrift.enumstr(_Type, colmeta._type), " ")
    pfx = ""
    for comp in colmeta.path_in_schema
        print(io, pfx, comp)
        pfx = "."
    end
    println(io, ", num values:", colmeta.num_values)
    show_encodings(io, colmeta.encodings, indent)
    if colmeta.codec != CompressionCodec.UNCOMPRESSED
        println(io, indent, Thrift.enumstr(CompressionCodec, colmeta.codec), " compressed bytes:", colmeta.total_compressed_size, " (", colmeta.total_uncompressed_size, " uncompressed)")
    else
        println(io, indent, Thrift.enumstr(CompressionCodec, colmeta.codec), " bytes:", colmeta.total_compressed_size)
    end

    print(io, indent, "offsets: data:", colmeta.data_page_offset)
    Thrift.isfilled(colmeta, :index_page_offset) && print(io, ", index:", colmeta.index_page_offset)
    Thrift.isfilled(colmeta, :dictionary_page_offset) && print(io, ", dictionary:", colmeta.dictionary_page_offset)
    println(io, "")
    Thrift.isfilled(colmeta, :key_value_metadata) && show(io, colmeta.key_value_metadata, indent)
end

function show(io::IO, columns::Vector{ColumnChunk}, indent::AbstractString="")
    for col in columns
        path = isfilled(col, :file_path) ? col.file_path : ""
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
    Thrift.isfilled(meta, :key_value_metadata) && show(io, meta.key_value_metadata, indent)
end

function show(io::IO, par::ParFile)
    println(io, "Parquet file: $(par.path)")
    meta = par.meta
    println(io, "    version: $(meta.version)")
    println(io, "    nrows: $(meta.num_rows)")
    println(io, "    created by: $(meta.created_by)")
end
