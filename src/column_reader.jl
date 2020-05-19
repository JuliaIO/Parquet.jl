
const TYPES = (Bool, Int32, Int64, Int128, Float32, Float64, String, UInt8)

read_column(path, col_num) = read_column(path, metadata(path), col_num)

function read_column(path, filemetadata, col_num)
    par = ParFile(path)

    T = TYPES[filemetadata.schema[col_num+1]._type+1]
    # TODO detect if missing is necessary
    res = Vector{Union{Missing, T}}(missing, nrows(par))
    write_cursor = 1
    for row_group in filemetadata.row_groups
        pgs = pages(par, row_group.columns[col_num])

        drop_page_count = 0
        # is the first page a dictionary page
        # this is not the case for boolean values for example
        if isfilled(pgs[1].hdr, :dictionary_page_header)
            # the first page is almost always the dictionary page
            dictionary_page = pgs[1]
            drop_page_count = 1
            dictionary_of_values = T.(values(par, dictionary_page)[1])
        end

        # TODO deal with other types of pages e.g. dataheaderv2

        # everything after the first data datapages
        for data_page in Base.Iterators.drop(pgs, drop_page_count)
            vals, definitions, decode = values(par, data_page)

            @assert all(in((0, 1)), definitions)

            l = sum(==(1), definitions)
            # if all definitions values are 1 then it's not used
            definitions_not_used = all(==(1), definitions)

            # data_page can be either
            # * dictionary-encoded in which case we should look into the dictionary
            # * plained-encoded in which case just return the values
            page_encoding = Parquet.page_encoding(data_page)

            if page_encoding == Encoding.PLAIN_DICTIONARY
                if definitions_not_used
                    res[write_cursor:write_cursor+l-1] .= dictionary_of_values[vals.+1]
                else
                    val_index = 1
                    for (offset, definition) in enumerate(definitions)
                        if definition != 0
                            value = vals[val_index]
                            res[write_cursor+offset-1] = dictionary_of_values[value + 1]
                            val_index += 1
                        end
                    end
                end
            elseif page_encoding == Encoding.PLAIN
                if definitions_not_used
                    res[write_cursor:write_cursor+l-1] .= T.(vals)
                else
                    val_index = 1
                    for (offset, definition)  in enumerate(definitions)
                        if definition != 0
                            value = vals[val_index]
                            res[write_cursor+offset-1] = T(value)
                            val_index += 1
                        end
                    end
                end
            else
                error("page encoding not supported yet")
            end

            write_cursor += length(definitions)
        end
    end
    return res
end

()
