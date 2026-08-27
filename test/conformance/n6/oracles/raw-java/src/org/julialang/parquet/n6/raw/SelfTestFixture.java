package org.julialang.parquet.n6.raw;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.ColumnOrder;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.DecimalType;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.Float16Type;
import org.apache.parquet.format.IEEE754TotalOrder;
import org.apache.parquet.format.IntType;
import org.apache.parquet.format.LogicalType;
import org.apache.parquet.format.MicroSeconds;
import org.apache.parquet.format.NanoSeconds;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Statistics;
import org.apache.parquet.format.TimeType;
import org.apache.parquet.format.TimeUnit;
import org.apache.parquet.format.TimestampType;
import org.apache.parquet.format.Type;
import org.apache.parquet.format.TypeDefinedOrder;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TIOStreamTransport;

public final class SelfTestFixture {
    private static final byte[] MAGIC = new byte[] {'P', 'A', 'R', '1'};

    private SelfTestFixture() {
    }

    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 1) {
            throw new IllegalArgumentException("usage: SelfTestFixture <output.parquet>");
        }
        write(Path.of(arguments[0]));
    }

    static void write(Path output) throws IOException, TException {
        write(output, metadata());
    }

    static void write(Path output, FileMetaData metadata) throws IOException, TException {
        ByteArrayOutputStream encoded = new ByteArrayOutputStream();
        TIOStreamTransport transport = new TIOStreamTransport(encoded);
        metadata.write(new TCompactProtocol(transport));
        transport.flush();
        writeRawFooter(output, encoded.toByteArray());
    }

    static void writeMissingRequired(
        Path output,
        boolean omitRowGroupNumRows,
        boolean omitColumnNumValues
    ) throws IOException, TException {
        FileMetaData metadata = metadata();
        ByteArrayOutputStream encoded = new ByteArrayOutputStream();
        TIOStreamTransport transport = new TIOStreamTransport(encoded);
        TCompactProtocol protocol = new TCompactProtocol(transport);
        protocol.writeStructBegin(new TStruct("FileMetaData"));
        writeField(protocol, TType.I32, 1, () -> protocol.writeI32(metadata.getVersion()));
        writeField(protocol, TType.LIST, 2, () -> {
            protocol.writeListBegin(new TList(TType.STRUCT, metadata.getSchemaSize()));
            for (SchemaElement element : metadata.getSchema()) {
                element.write(protocol);
            }
            protocol.writeListEnd();
        });
        writeField(protocol, TType.I64, 3, () -> protocol.writeI64(metadata.getNum_rows()));
        writeField(protocol, TType.LIST, 4, () -> {
            protocol.writeListBegin(new TList(TType.STRUCT, metadata.getRow_groupsSize()));
            for (RowGroup rowGroup : metadata.getRow_groups()) {
                writeRowGroup(protocol, rowGroup, omitRowGroupNumRows, omitColumnNumValues);
            }
            protocol.writeListEnd();
        });
        if (metadata.isSetCreated_by()) {
            writeField(protocol, TType.STRING, 6,
                () -> protocol.writeString(metadata.getCreated_by()));
        }
        writeField(protocol, TType.LIST, 7, () -> {
            protocol.writeListBegin(new TList(TType.STRUCT, metadata.getColumn_ordersSize()));
            for (ColumnOrder order : metadata.getColumn_orders()) {
                order.write(protocol);
            }
            protocol.writeListEnd();
        });
        protocol.writeFieldStop();
        protocol.writeStructEnd();
        transport.flush();
        writeRawFooter(output, encoded.toByteArray());
    }

    static void writeRawFooter(Path output, byte[] footer) throws IOException {
        if (footer.length > Integer.MAX_VALUE - 12) {
            throw new IOException("self-test footer is unexpectedly large");
        }
        ByteArrayOutputStream file = new ByteArrayOutputStream(footer.length + 12);
        file.write(MAGIC);
        file.write(footer);
        writeLittleEndianInt(file, footer.length);
        file.write(MAGIC);
        Path absolute = output.toAbsolutePath().normalize();
        Path parent = absolute.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Files.write(absolute, file.toByteArray());
    }

    @FunctionalInterface
    private interface ThriftAction {
        void run() throws TException;
    }

    private static void writeField(
        TCompactProtocol protocol,
        byte type,
        int id,
        ThriftAction action
    ) throws TException {
        protocol.writeFieldBegin(new TField("", type, (short) id));
        action.run();
        protocol.writeFieldEnd();
    }

    private static void writeRowGroup(
        TCompactProtocol protocol,
        RowGroup rowGroup,
        boolean omitNumRows,
        boolean omitColumnNumValues
    ) throws TException {
        protocol.writeStructBegin(new TStruct("RowGroup"));
        writeField(protocol, TType.LIST, 1, () -> {
            protocol.writeListBegin(new TList(TType.STRUCT, rowGroup.getColumnsSize()));
            for (ColumnChunk chunk : rowGroup.getColumns()) {
                writeColumnChunk(protocol, chunk, omitColumnNumValues);
            }
            protocol.writeListEnd();
        });
        writeField(protocol, TType.I64, 2, () -> protocol.writeI64(rowGroup.getTotal_byte_size()));
        if (!omitNumRows) {
            writeField(protocol, TType.I64, 3, () -> protocol.writeI64(rowGroup.getNum_rows()));
        }
        protocol.writeFieldStop();
        protocol.writeStructEnd();
    }

    private static void writeColumnChunk(
        TCompactProtocol protocol,
        ColumnChunk chunk,
        boolean omitNumValues
    ) throws TException {
        protocol.writeStructBegin(new TStruct("ColumnChunk"));
        writeField(protocol, TType.I64, 2, () -> protocol.writeI64(chunk.getFile_offset()));
        writeField(protocol, TType.STRUCT, 3,
            () -> writeColumnMetaData(protocol, chunk.getMeta_data(), omitNumValues));
        protocol.writeFieldStop();
        protocol.writeStructEnd();
    }

    private static void writeColumnMetaData(
        TCompactProtocol protocol,
        ColumnMetaData metadata,
        boolean omitNumValues
    ) throws TException {
        protocol.writeStructBegin(new TStruct("ColumnMetaData"));
        writeField(protocol, TType.I32, 1, () -> protocol.writeI32(metadata.getType().getValue()));
        writeField(protocol, TType.LIST, 2, () -> {
            protocol.writeListBegin(new TList(TType.I32, metadata.getEncodingsSize()));
            for (Encoding encoding : metadata.getEncodings()) {
                protocol.writeI32(encoding.getValue());
            }
            protocol.writeListEnd();
        });
        writeField(protocol, TType.LIST, 3, () -> {
            protocol.writeListBegin(new TList(TType.STRING, metadata.getPath_in_schemaSize()));
            for (String component : metadata.getPath_in_schema()) {
                protocol.writeString(component);
            }
            protocol.writeListEnd();
        });
        writeField(protocol, TType.I32, 4, () -> protocol.writeI32(metadata.getCodec().getValue()));
        if (!omitNumValues) {
            writeField(protocol, TType.I64, 5, () -> protocol.writeI64(metadata.getNum_values()));
        }
        writeField(protocol, TType.I64, 6,
            () -> protocol.writeI64(metadata.getTotal_uncompressed_size()));
        writeField(protocol, TType.I64, 7,
            () -> protocol.writeI64(metadata.getTotal_compressed_size()));
        writeField(protocol, TType.I64, 9, () -> protocol.writeI64(metadata.getData_page_offset()));
        if (metadata.isSetStatistics()) {
            writeField(protocol, TType.STRUCT, 12, () -> metadata.getStatistics().write(protocol));
        }
        protocol.writeFieldStop();
        protocol.writeStructEnd();
    }

    static FileMetaData metadata() {
        SchemaElement root = new SchemaElement("schema").setNum_children(8);
        SchemaElement float32 = leaf("float32", Type.FLOAT);
        SchemaElement float64 = leaf("float64", Type.DOUBLE);
        SchemaElement float16 = leaf("float16", Type.FIXED_LEN_BYTE_ARRAY)
            .setType_length(2)
            .setLogicalType(LogicalType.FLOAT16(new Float16Type()));
        SchemaElement binary = leaf("binary", Type.BYTE_ARRAY);
        SchemaElement integer = leaf("uint16", Type.INT32)
            .setType_length(16)
            .setConverted_type(ConvertedType.UINT_16)
            .setField_id(17)
            .setLogicalType(LogicalType.INTEGER(new IntType((byte) 16, false)));
        SchemaElement decimal = leaf("decimal4", Type.FIXED_LEN_BYTE_ARRAY)
            .setType_length(4)
            .setConverted_type(ConvertedType.DECIMAL)
            .setScale(2)
            .setPrecision(9)
            .setLogicalType(LogicalType.DECIMAL(new DecimalType(2, 9)));
        SchemaElement time = leaf("time64", Type.INT64)
            .setConverted_type(ConvertedType.TIME_MICROS)
            .setLogicalType(LogicalType.TIME(new TimeType(
                true, TimeUnit.MICROS(new MicroSeconds()))));
        SchemaElement timestamp = leaf("timestamp64", Type.INT64)
            .setLogicalType(LogicalType.TIMESTAMP(new TimestampType(
                false, TimeUnit.NANOS(new NanoSeconds()))));
        List<SchemaElement> schema = List.of(
            root, float32, float64, float16, binary, integer, decimal, time, timestamp);

        Statistics float32Statistics = new Statistics()
            .setMax(hex("0000803f"))
            .setMin(hex("000080bf"))
            .setNull_count(1)
            .setDistinct_count(2)
            .setMax_value(hex("00000000"))
            .setMin_value(hex("00000080"))
            .setIs_max_value_exact(true)
            .setIs_min_value_exact(false)
            .setNan_count(0);
        Statistics float64Statistics = new Statistics()
            .setNull_count(1)
            .setMax_value(hex("420000000000f87f"))
            .setMin_value(hex("010000000000f8ff"))
            .setIs_max_value_exact(true)
            .setIs_min_value_exact(true)
            .setNan_count(2);
        Statistics float16Statistics = new Statistics()
            .setNull_count(0)
            .setDistinct_count(2)
            .setMax_value(hex("007c"))
            .setMin_value(hex("0080"))
            .setIs_min_value_exact(true)
            .setNan_count(1);
        Statistics binaryStatistics = new Statistics()
            .setMax(hex("ff"))
            .setMin(hex("00"))
            .setDistinct_count(3)
            .setMax_value(hex("ff007f"))
            .setMin_value(hex("00ff"))
            .setIs_max_value_exact(false);

        List<ColumnChunk> columns = new ArrayList<>();
        columns.add(column("float32", Type.FLOAT, float32Statistics));
        columns.add(column("float64", Type.DOUBLE, float64Statistics));
        columns.add(column("float16", Type.FIXED_LEN_BYTE_ARRAY, float16Statistics));
        columns.add(column("binary", Type.BYTE_ARRAY, binaryStatistics));
        columns.add(column("uint16", Type.INT32, new Statistics().setNull_count(0)));
        columns.add(column("decimal4", Type.FIXED_LEN_BYTE_ARRAY,
            new Statistics().setNull_count(0)));
        columns.add(column("time64", Type.INT64, new Statistics().setNull_count(0)));
        columns.add(column("timestamp64", Type.INT64, new Statistics().setNull_count(0)));
        RowGroup rowGroup = new RowGroup(columns, 0, 3);
        List<ColumnOrder> orders = List.of(
            ColumnOrder.TYPE_ORDER(new TypeDefinedOrder()),
            ColumnOrder.IEEE_754_TOTAL_ORDER(new IEEE754TotalOrder()),
            ColumnOrder.IEEE_754_TOTAL_ORDER(new IEEE754TotalOrder()),
            ColumnOrder.TYPE_ORDER(new TypeDefinedOrder()),
            ColumnOrder.TYPE_ORDER(new TypeDefinedOrder()),
            ColumnOrder.TYPE_ORDER(new TypeDefinedOrder()),
            ColumnOrder.TYPE_ORDER(new TypeDefinedOrder()),
            ColumnOrder.TYPE_ORDER(new TypeDefinedOrder()));
        return new FileMetaData(1, schema, 3, List.of(rowGroup))
            .setCreated_by("raw-java self-test \u03c0")
            .setColumn_orders(orders);
    }

    private static SchemaElement leaf(String name, Type type) {
        return new SchemaElement(name)
            .setType(type)
            .setRepetition_type(FieldRepetitionType.OPTIONAL);
    }

    private static ColumnChunk column(String name, Type type, Statistics statistics) {
        ColumnMetaData metadata = new ColumnMetaData(
            type,
            List.of(Encoding.PLAIN),
            List.of(name),
            CompressionCodec.UNCOMPRESSED,
            3,
            0,
            0,
            4)
            .setStatistics(statistics);
        return new ColumnChunk(4).setMeta_data(metadata);
    }

    private static byte[] hex(String value) {
        if ((value.length() & 1) != 0) {
            throw new IllegalArgumentException("hex value has odd length");
        }
        byte[] bytes = new byte[value.length() / 2];
        for (int index = 0; index < bytes.length; index++) {
            int high = Character.digit(value.charAt(index * 2), 16);
            int low = Character.digit(value.charAt(index * 2 + 1), 16);
            if (high < 0 || low < 0) {
                throw new IllegalArgumentException("invalid hex value");
            }
            bytes[index] = (byte) ((high << 4) | low);
        }
        return bytes;
    }

    private static void writeLittleEndianInt(ByteArrayOutputStream output, int value) {
        output.write(value & 0xff);
        output.write((value >>> 8) & 0xff);
        output.write((value >>> 16) & 0xff);
        output.write((value >>> 24) & 0xff);
    }
}
