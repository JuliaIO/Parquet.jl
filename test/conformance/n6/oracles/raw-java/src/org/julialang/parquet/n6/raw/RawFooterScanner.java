package org.julialang.parquet.n6.raw;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.DecimalType;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.GeographyType;
import org.apache.parquet.format.GeometryType;
import org.apache.parquet.format.IntType;
import org.apache.parquet.format.LogicalType;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Statistics;
import org.apache.parquet.format.TimeType;
import org.apache.parquet.format.TimeUnit;
import org.apache.parquet.format.TimestampType;
import org.apache.parquet.format.Type;
import org.apache.parquet.format.VariantType;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TMemoryInputTransport;

public final class RawFooterScanner {
    private static final byte[] MAGIC = new byte[] {'P', 'A', 'R', '1'};
    private static final String EVIDENCE_VERSION = "parquet-2.13-raw-footer-v3";
    private static final String FORMAT_COMMIT = "c47e2a66e88943fc46fde1b028a9432f14fdf5c0";
    private static final String THRIFT_VERSION = "0.23.0";
    private static final long MAX_FILE_BYTES = 8L * 1024 * 1024 * 1024;
    private static final int MAX_FOOTER_BYTES = 64 * 1024 * 1024;
    private static final int MAX_INPUT_FILES = 10_000;
    private static final int MAX_SCHEMA_ELEMENTS = 1_000_000;
    private static final long MAX_THRIFT_BINARY_BYTES = 64L * 1024 * 1024;
    private static final long MAX_THRIFT_CONTAINER_ELEMENTS = 1_000_000;
    private static final int MAX_THRIFT_DEPTH = 128;
    private static final long MAX_EVIDENCE_CHARACTERS = 256L * 1024 * 1024;

    private static final class InputFile {
        private final String label;
        private final Path path;

        private InputFile(String label, Path path) {
            this.label = label;
            this.path = path;
        }
    }
    static final class Footer {
        final long fileSize;
        final int footerLength;
        final byte[] footerBytes;
        final String fileSha256;
        final FileMetaData metadata;
        final RawColumnOrders rawColumnOrders;

        Footer(
            long fileSize,
            int footerLength,
            byte[] footerBytes,
            String fileSha256,
            FileMetaData metadata,
            RawColumnOrders rawColumnOrders
        ) {
            this.fileSize = fileSize;
            this.footerLength = footerLength;
            this.footerBytes = footerBytes;
            this.fileSha256 = fileSha256;
            this.metadata = metadata;
            this.rawColumnOrders = rawColumnOrders;
        }
    }

    static final class RawColumnOrder {
        final Integer fieldId;
        final Byte wireType;
        final String headerHex;
        final String state;
        final String member;
        final int headerOffset;

        RawColumnOrder(
            Integer fieldId,
            Byte wireType,
            String headerHex,
            String state,
            String member,
            int headerOffset
        ) {
            this.fieldId = fieldId;
            this.wireType = wireType;
            this.headerHex = headerHex;
            this.state = state;
            this.member = member;
            this.headerOffset = headerOffset;
        }
    }

    static final class RawColumnOrders {
        final boolean present;
        final List<RawColumnOrder> values;

        RawColumnOrders(boolean present, List<RawColumnOrder> values) {
            this.present = present;
            this.values = values;
        }
    }

    private static final class SchemaFrame {
        private int remaining;
        private final List<String> path;

        private SchemaFrame(int remaining, List<String> path) {
            this.remaining = remaining;
            this.path = path;
        }
    }

    private static final class Leaf {
        private final int ordinal;
        private final List<String> path;
        private final SchemaElement schema;

        private Leaf(int ordinal, List<String> path, SchemaElement schema) {
            this.ordinal = ordinal;
            this.path = path;
            this.schema = schema;
        }
    }

    private RawFooterScanner() {
    }

    public static void main(String[] arguments) throws Exception {
        execute(arguments);
    }

    static void execute(String[] arguments) throws Exception {
        execute(arguments, MAX_EVIDENCE_CHARACTERS);
    }

    static void execute(String[] arguments, long maximumEvidenceCharacters) throws Exception {
        if (maximumEvidenceCharacters < 0) {
            throw new IOException("raw footer evidence exceeds the scanner memory limit");
        }
        if (arguments.length == 0 || !"scan".equals(arguments[0])) {
            throw new IllegalArgumentException(
                "usage: RawFooterScanner scan --input <file-or-directory> [--input ...] [--output file]");
        }
        List<Path> inputs = new ArrayList<>();
        Path output = null;
        for (int index = 1; index < arguments.length; index++) {
            String argument = arguments[index];
            if ("--input".equals(argument)) {
                index++;
                requireArgument(arguments, index, "--input");
                inputs.add(Path.of(arguments[index]));
            } else if ("--output".equals(argument)) {
                if (output != null) {
                    throw new IllegalArgumentException("--output may be specified only once");
                }
                index++;
                requireArgument(arguments, index, "--output");
                output = Path.of(arguments[index]);
            } else {
                throw new IllegalArgumentException("unknown argument: " + argument);
            }
        }
        if (inputs.isEmpty()) {
            throw new IllegalArgumentException("at least one --input is required");
        }
        List<InputFile> files = collectInputs(inputs);
        validateOutput(files, output);
        List<String> evidence = new ArrayList<>(files.size());
        long evidenceCharacters = 0;
        for (InputFile file : files) {
            long remaining = maximumEvidenceCharacters - evidenceCharacters;
            if (remaining <= 1) {
                throw new IOException("raw footer evidence exceeds the scanner memory limit");
            }
            String record = scan(file.path, file.label, remaining - 1);
            evidenceCharacters = Math.addExact(evidenceCharacters, record.length() + 1L);
            evidence.add(record);
        }
        validateOutput(files, output);
        writeEvidence(evidence, output);
    }

    static String scan(Path path, String label) throws IOException, TException {
        return scan(path, label, MAX_EVIDENCE_CHARACTERS - 1);
    }

    static String scan(Path path, String label, long maximumCharacters) throws IOException, TException {
        Footer footer = readFooter(path);
        List<Leaf> leaves = schemaLeaves(footer.metadata);
        Map<List<String>, Leaf> leavesByPath = new HashMap<>();
        for (Leaf leaf : leaves) {
            if (leavesByPath.put(leaf.path, leaf) != null) {
                throw new IOException("schema contains a duplicate leaf path: " + leaf.path);
            }
        }
        validateRowGroups(footer.metadata, leaves, leavesByPath);
        try {
            JsonWriter json = new JsonWriter(maximumCharacters);
            json.beginObject();
            json.name("evidence_version").value(EVIDENCE_VERSION);
            json.name("format_commit").value(FORMAT_COMMIT);
            json.name("thrift_version").value(THRIFT_VERSION);
            json.name("file").value(label);
            json.name("file_size").value(footer.fileSize);
            json.name("file_sha256").value(footer.fileSha256);
            json.name("footer_length").value(footer.footerLength);
            json.name("footer_sha256").value(sha256(footer.footerBytes));
            appendMetadata(json, footer, leaves);
            json.endObject();
            return json.finish();
        } catch (JsonWriter.LimitException exception) {
            throw new IOException(exception.getMessage(), exception);
        }
    }

    static Footer readFooter(Path path) throws IOException, TException {
        return readFooter(path, null);
    }

    static Footer readFooter(Path path, Runnable betweenSnapshots) throws IOException, TException {
        BasicFileAttributes before = Files.readAttributes(path, BasicFileAttributes.class);
        Footer result;
        try (RandomAccessFile file = new RandomAccessFile(path.toFile(), "r")) {
            long fileSize = file.length();
            if (fileSize < 12) {
                throw new IOException("Parquet file is shorter than the minimum footer envelope: " + path);
            }
            if (fileSize > MAX_FILE_BYTES) {
                throw new IOException("Parquet file exceeds the scanner size limit: " + fileSize);
            }
            byte[] marker = new byte[4];
            file.seek(0);
            file.readFully(marker);
            requireMagic(marker, "leading", path);
            file.seek(fileSize - 4);
            file.readFully(marker);
            requireMagic(marker, "trailing", path);
            byte[] lengthBytes = new byte[4];
            file.seek(fileSize - 8);
            file.readFully(lengthBytes);
            long unsignedLength = unsignedLittleEndianInt(lengthBytes);
            long footerStart;
            try {
                footerStart = Math.subtractExact(Math.subtractExact(fileSize, 8L), unsignedLength);
            } catch (ArithmeticException exception) {
                throw new IOException("Parquet footer length overflows its file envelope: " + path, exception);
            }
            if (footerStart < 4) {
                throw new IOException("Parquet footer length is outside its file envelope: " + path);
            }
            if (unsignedLength > MAX_FOOTER_BYTES) {
                throw new IOException("Parquet footer exceeds the scanner size limit: " + unsignedLength);
            }
            String firstFileHash = sha256(file, fileSize);
            byte[] footerBytes = new byte[(int) unsignedLength];
            file.seek(footerStart);
            file.readFully(footerBytes);
            CompactThriftPreflight.validate(footerBytes);
            RawColumnOrders rawColumnOrders = readRawColumnOrders(footerBytes);
            byte[] semanticBytes = withoutColumnOrders(footerBytes);
            TMemoryInputTransport transport = new TMemoryInputTransport(semanticBytes);
            FileMetaData metadata = new FileMetaData();
            metadata.read(new TCompactProtocol(
                transport, MAX_THRIFT_BINARY_BYTES, MAX_THRIFT_CONTAINER_ELEMENTS));
            int trailing = transport.getBytesRemainingInBuffer();
            if (trailing != 0) {
                throw new IOException("Compact-Thrift footer has " + trailing + " trailing byte(s): " + path);
            }
            if (betweenSnapshots != null) {
                betweenSnapshots.run();
            }
            if (file.length() != fileSize) {
                throw new IOException("Parquet input changed size during the scan: " + path);
            }
            String secondFileHash = sha256(file, fileSize);
            if (!firstFileHash.equals(secondFileHash)) {
                throw new IOException("Parquet input changed during the scan: " + path);
            }
            result = new Footer(fileSize, (int) unsignedLength, footerBytes,
                firstFileHash, metadata, rawColumnOrders);
        }
        BasicFileAttributes after = Files.readAttributes(path, BasicFileAttributes.class);
        if (!sameFileSnapshot(before, after)) {
            throw new IOException("Parquet input path changed during the scan: " + path);
        }
        return result;
    }

    private static void appendMetadata(
        JsonWriter json,
        Footer footer,
        List<Leaf> leaves
    ) {
        FileMetaData metadata = footer.metadata;
        json.name("file_metadata").beginObject();
        json.name("version").value(metadata.getVersion());
        json.name("num_rows").value(metadata.getNum_rows());
        json.name("created_by").beginObject();
        json.name("present").value(metadata.isSetCreated_by());
        json.name("value");
        if (metadata.isSetCreated_by()) {
            json.value(metadata.getCreated_by());
        } else {
            json.nullValue();
        }
        json.endObject();
        json.name("schema_leaf_count").value(leaves.size());
        appendSchemaLeaves(json, leaves);
        appendColumnOrders(json, footer.rawColumnOrders, leaves);
        List<RowGroup> rowGroups = metadata.getRow_groups();
        json.name("row_group_count").value(rowGroups == null ? 0 : rowGroups.size());
        json.name("row_groups").beginArray();
        if (rowGroups != null) {
            for (int rowGroupOrdinal = 0; rowGroupOrdinal < rowGroups.size(); rowGroupOrdinal++) {
                appendRowGroup(json, rowGroups.get(rowGroupOrdinal), rowGroupOrdinal, leaves);
            }
        }
        json.endArray();
        json.endObject();
    }

    private static void appendSchemaLeaves(JsonWriter json, List<Leaf> leaves) {
        json.name("schema_leaves").beginArray();
        for (Leaf leaf : leaves) {
            SchemaElement schema = leaf.schema;
            json.beginObject();
            json.name("ordinal").value(leaf.ordinal);
            appendPath(json, "path", leaf.path);
            json.name("physical_type").value(schema.getType().name());
            appendOptionalInteger(json, "type_length", schema.isSetType_length(), schema.getType_length());
            appendOptionalEnum(json, "converted_type", schema.isSetConverted_type(),
                schema.isSetConverted_type() ? schema.getConverted_type().name() : null);
            appendOptionalInteger(json, "scale", schema.isSetScale(), schema.getScale());
            appendOptionalInteger(json, "precision", schema.isSetPrecision(), schema.getPrecision());
            appendOptionalEnum(json, "repetition_type", schema.isSetRepetition_type(),
                schema.isSetRepetition_type() ? schema.getRepetition_type().name() : null);
            appendOptionalInteger(json, "field_id", schema.isSetField_id(), schema.getField_id());
            appendLogicalTypeDescriptor(json, schema);
            json.endObject();
        }
        json.endArray();
    }

    private static void appendLogicalTypeDescriptor(JsonWriter json, SchemaElement schema) {
        LogicalType logical = schema.isSetLogicalType() ? schema.getLogicalType() : null;
        LogicalType._Fields member = logical == null ? null : logical.getSetField();
        json.name("logical_type").beginObject();
        json.name("present").value(logical != null);
        json.name("member").value(member == null ? null : member.getFieldName());
        json.name("parameters");
        if (member == null || !hasLogicalParameters(member)) {
            json.nullValue();
        } else {
            json.beginObject();
            appendLogicalParameters(json, logical, member);
            json.endObject();
        }
        json.endObject();
    }

    private static boolean hasLogicalParameters(LogicalType._Fields member) {
        return member == LogicalType._Fields.INTEGER
            || member == LogicalType._Fields.DECIMAL
            || member == LogicalType._Fields.TIME
            || member == LogicalType._Fields.TIMESTAMP
            || member == LogicalType._Fields.VARIANT
            || member == LogicalType._Fields.GEOMETRY
            || member == LogicalType._Fields.GEOGRAPHY;
    }

    private static void appendLogicalParameters(
        JsonWriter json,
        LogicalType logical,
        LogicalType._Fields member
    ) {
        if (member == LogicalType._Fields.INTEGER) {
            IntType integer = logical.getINTEGER();
            json.name("bit_width").value(integer.getBitWidth());
            json.name("is_signed").value(integer.isIsSigned());
        } else if (member == LogicalType._Fields.DECIMAL) {
            DecimalType decimal = logical.getDECIMAL();
            json.name("scale").value(decimal.getScale());
            json.name("precision").value(decimal.getPrecision());
        } else if (member == LogicalType._Fields.TIME) {
            TimeType time = logical.getTIME();
            json.name("unit").value(timeUnitName(time.getUnit()));
            json.name("is_adjusted_to_utc").value(time.isIsAdjustedToUTC());
        } else if (member == LogicalType._Fields.TIMESTAMP) {
            TimestampType timestamp = logical.getTIMESTAMP();
            json.name("unit").value(timeUnitName(timestamp.getUnit()));
            json.name("is_adjusted_to_utc").value(timestamp.isIsAdjustedToUTC());
        } else if (member == LogicalType._Fields.VARIANT) {
            VariantType variant = logical.getVARIANT();
            appendOptionalInteger(json, "specification_version",
                variant.isSetSpecification_version(), variant.getSpecification_version());
        } else if (member == LogicalType._Fields.GEOMETRY) {
            GeometryType geometry = logical.getGEOMETRY();
            appendOptionalString(json, "crs", geometry.isSetCrs(), geometry.getCrs());
        } else {
            GeographyType geography = logical.getGEOGRAPHY();
            appendOptionalString(json, "crs", geography.isSetCrs(), geography.getCrs());
            appendOptionalEnum(json, "algorithm", geography.isSetAlgorithm(),
                geography.isSetAlgorithm() ? geography.getAlgorithm().name() : null);
        }
    }

    private static String timeUnitName(TimeUnit unit) {
        return unit == null || unit.getSetField() == null ? null : unit.getSetField().getFieldName();
    }

    private static void appendOptionalInteger(
        JsonWriter json,
        String name,
        boolean present,
        long value
    ) {
        json.name(name).beginObject();
        json.name("present").value(present);
        json.name("value");
        if (present) {
            json.value(value);
        } else {
            json.nullValue();
        }
        json.endObject();
    }

    private static void appendOptionalString(
        JsonWriter json,
        String name,
        boolean present,
        String value
    ) {
        json.name(name).beginObject();
        json.name("present").value(present);
        json.name("value");
        if (present) {
            json.value(value);
        } else {
            json.nullValue();
        }
        json.endObject();
    }

    private static void appendOptionalEnum(
        JsonWriter json,
        String name,
        boolean present,
        String value
    ) {
        appendOptionalString(json, name, present, value);
    }

    private static void appendColumnOrders(
        JsonWriter json,
        RawColumnOrders orders,
        List<Leaf> leaves
    ) {
        json.name("column_orders").beginObject();
        json.name("present").value(orders.present);
        json.name("count");
        if (!orders.present) {
            json.nullValue();
        } else {
            json.value(orders.values.size());
        }
        json.name("values").beginArray();
        if (orders.present) {
            for (int ordinal = 0; ordinal < orders.values.size(); ordinal++) {
                RawColumnOrder order = orders.values.get(ordinal);
                Leaf leaf = ordinal < leaves.size() ? leaves.get(ordinal) : null;
                json.beginObject();
                json.name("ordinal").value(ordinal);
                json.name("schema_leaf_ordinal");
                if (leaf == null) {
                    json.nullValue();
                } else {
                    json.value(leaf.ordinal);
                }
                json.name("state").value(order.state);
                json.name("field_id");
                if (order.fieldId == null) {
                    json.nullValue();
                } else {
                    json.value(order.fieldId);
                }
                json.name("wire_type");
                if (order.wireType == null) {
                    json.nullValue();
                } else {
                    json.value(order.wireType);
                }
                json.name("header_hex").value(order.headerHex);
                json.name("member").value(order.member);
                appendPath(json, "path", leaf == null ? null : leaf.path);
                json.name("physical_type").value(
                    leaf == null || !leaf.schema.isSetType() ? null : leaf.schema.getType().name());
                json.name("logical_type").value(logicalTypeName(leaf == null ? null : leaf.schema));
                json.endObject();
            }
        }
        json.endArray();
        json.endObject();
    }

    private static void appendRowGroup(
        JsonWriter json,
        RowGroup rowGroup,
        int ordinal,
        List<Leaf> leaves
    ) {
        json.beginObject();
        json.name("ordinal").value(ordinal);
        json.name("num_rows").value(rowGroup.getNum_rows());
        List<ColumnChunk> columns = rowGroup.getColumns();
        json.name("column_count").value(columns == null ? 0 : columns.size());
        json.name("columns").beginArray();
        if (columns != null) {
            for (int columnOrdinal = 0; columnOrdinal < columns.size(); columnOrdinal++) {
                appendColumn(json, columns.get(columnOrdinal), columnOrdinal, leaves.get(columnOrdinal));
            }
        }
        json.endArray();
        json.endObject();
    }

    private static void appendColumn(
        JsonWriter json,
        ColumnChunk chunk,
        int ordinal,
        Leaf leaf
    ) {
        ColumnMetaData metadata = chunk == null ? null : chunk.getMeta_data();
        List<String> path = metadata == null ? null : metadata.getPath_in_schema();
        Type physicalType = metadata == null ? null : metadata.getType();
        boolean float16 = leaf != null && "FLOAT16".equals(logicalTypeName(leaf.schema));
        json.beginObject();
        json.name("ordinal").value(ordinal);
        json.name("schema_leaf_ordinal");
        if (leaf == null) {
            json.nullValue();
        } else {
            json.value(leaf.ordinal);
        }
        json.name("metadata_present").value(metadata != null);
        appendPath(json, "path", path);
        json.name("physical_type").value(physicalType == null ? null : physicalType.name());
        json.name("logical_type").value(logicalTypeName(leaf == null ? null : leaf.schema));
        json.name("num_values");
        if (metadata == null) {
            json.nullValue();
        } else {
            json.value(metadata.getNum_values());
        }
        appendStatistics(json, metadata == null ? null : metadata.getStatistics(), physicalType, float16);
        json.endObject();
    }

    private static void appendStatistics(JsonWriter json, Statistics statistics, Type physicalType, boolean float16) {
        json.name("statistics").beginObject();
        json.name("present").value(statistics != null);
        json.name("fields").beginArray();
        for (int fieldId = 1; fieldId <= 9; fieldId++) {
            appendStatisticsField(json, statistics, fieldId, physicalType, float16);
        }
        json.endArray();
        json.endObject();
    }

    private static void appendStatisticsField(
        JsonWriter json,
        Statistics statistics,
        int fieldId,
        Type physicalType,
        boolean float16
    ) {
        Statistics._Fields field = Statistics._Fields.findByThriftId(fieldId);
        boolean present = statistics != null && statistics.isSet(field);
        json.beginObject();
        json.name("field_id").value(fieldId);
        json.name("name").value(field.getFieldName());
        json.name("present").value(present);
        json.name("value");
        if (!present) {
            json.nullValue();
        } else if (fieldId == 1 || fieldId == 2 || fieldId == 5 || fieldId == 6) {
            appendBinary(json, binaryField(statistics, fieldId), physicalType, float16);
        } else if (fieldId == 7) {
            json.value(statistics.isIs_max_value_exact());
        } else if (fieldId == 8) {
            json.value(statistics.isIs_min_value_exact());
        } else if (fieldId == 3) {
            json.value(statistics.getNull_count());
        } else if (fieldId == 4) {
            json.value(statistics.getDistinct_count());
        } else {
            json.value(statistics.getNan_count());
        }
        json.endObject();
    }

    private static ByteBuffer binaryField(Statistics statistics, int fieldId) {
        ByteBuffer buffer;
        if (fieldId == 1) {
            buffer = statistics.bufferForMax();
        } else if (fieldId == 2) {
            buffer = statistics.bufferForMin();
        } else if (fieldId == 5) {
            buffer = statistics.bufferForMax_value();
        } else {
            buffer = statistics.bufferForMin_value();
        }
        return buffer.duplicate();
    }

    private static void appendBinary(JsonWriter json, ByteBuffer bytes, Type physicalType, boolean float16) {
        json.beginObject();
        json.name("hex").hexValue(bytes, false, false);
        json.name("byte_length").value(bytes.remaining());
        int width = floatingWidth(physicalType, float16);
        json.name("float_bits");
        if (width == 0) {
            json.nullValue();
        } else {
            json.beginObject();
            json.name("width").value(width);
            boolean valid = bytes.remaining() * 8 == width;
            json.name("valid_width").value(valid);
            json.name("hex");
            if (valid) {
                json.hexValue(bytes, true, true);
            } else {
                json.nullValue();
            }
            json.endObject();
        }
        json.endObject();
    }

    private static int floatingWidth(Type physicalType, boolean float16) {
        if (float16) {
            return 16;
        }
        if (physicalType == Type.FLOAT) {
            return 32;
        }
        if (physicalType == Type.DOUBLE) {
            return 64;
        }
        return 0;
    }

    private static void appendPath(JsonWriter json, String name, List<String> path) {
        json.name(name);
        if (path == null) {
            json.nullValue();
            return;
        }
        json.beginArray();
        for (String component : path) {
            json.value(component);
        }
        json.endArray();
    }

    private static String logicalTypeName(SchemaElement schema) {
        if (schema == null || !schema.isSetLogicalType() || schema.getLogicalType().getSetField() == null) {
            return null;
        }
        return schema.getLogicalType().getSetField().getFieldName();
    }

    private static RawColumnOrders readRawColumnOrders(byte[] footerBytes)
        throws IOException, TException {
        TMemoryInputTransport transport = new TMemoryInputTransport(footerBytes);
        TCompactProtocol protocol = new TCompactProtocol(
            transport, MAX_THRIFT_BINARY_BYTES, MAX_THRIFT_CONTAINER_ELEMENTS);
        boolean present = false;
        List<RawColumnOrder> values = List.of();
        protocol.readStructBegin();
        while (true) {
            TField field = protocol.readFieldBegin();
            if (field.type == TType.STOP) {
                break;
            }
            if (field.id == 7) {
                if (present) {
                    throw new IOException("FileMetaData contains duplicate column_orders fields");
                }
                present = true;
                if (field.type != TType.LIST) {
                    throw new IOException("FileMetaData column_orders has the wrong wire type");
                }
                TList list = protocol.readListBegin();
                if (list.elemType != TType.STRUCT || list.size < 0 || list.size > MAX_SCHEMA_ELEMENTS) {
                    throw new IOException("FileMetaData column_orders has an invalid list header");
                }
                List<RawColumnOrder> captured = new ArrayList<>(list.size);
                for (int ordinal = 0; ordinal < list.size; ordinal++) {
                    protocol.readStructBegin();
                    int headerOffset = transport.getBufferPosition();
                    TField member = protocol.readFieldBegin();
                    String headerHex = hex(Arrays.copyOfRange(
                        footerBytes, headerOffset, transport.getBufferPosition()));
                    if (member.type == TType.STOP) {
                        captured.add(new RawColumnOrder(
                            null, null, headerHex, "empty", null, headerOffset));
                    } else {
                        int fieldId = member.id;
                        byte wireType = member.type;
                        String state = rawColumnOrderState(fieldId, wireType);
                        String name = "known".equals(state) ?
                            (fieldId == 1 ? "TYPE_ORDER" : "IEEE_754_TOTAL_ORDER") : null;
                TProtocolUtil.skip(protocol, wireType, MAX_THRIFT_DEPTH);
                        protocol.readFieldEnd();
                        TField extra = protocol.readFieldBegin();
                        if (extra.type != TType.STOP) {
                            throw new IOException("ColumnOrder union has more than one raw member");
                        }
                        captured.add(new RawColumnOrder(
                            fieldId, wireType, headerHex, state, name, headerOffset));
                    }
                    protocol.readStructEnd();
                }
                protocol.readListEnd();
                values = Collections.unmodifiableList(captured);
            } else {
                TProtocolUtil.skip(protocol, field.type, MAX_THRIFT_DEPTH);
            }
            protocol.readFieldEnd();
        }
        protocol.readStructEnd();
        if (transport.getBytesRemainingInBuffer() != 0) {
            throw new IOException("raw Compact-Thrift footer scan left trailing bytes");
        }
        return new RawColumnOrders(present, values);
    }

    private static byte[] withoutColumnOrders(byte[] footerBytes) throws IOException, TException {
        TMemoryInputTransport inputTransport = new TMemoryInputTransport(footerBytes);
        TCompactProtocol input = new TCompactProtocol(
            inputTransport, MAX_THRIFT_BINARY_BYTES, MAX_THRIFT_CONTAINER_ELEMENTS);
        ByteArrayOutputStream encoded = new ByteArrayOutputStream(footerBytes.length);
        TIOStreamTransport outputTransport = new TIOStreamTransport(encoded);
        TCompactProtocol output = new TCompactProtocol(outputTransport);
        input.readStructBegin();
        output.writeStructBegin(new TStruct("FileMetaData"));
        while (true) {
            TField field = input.readFieldBegin();
            if (field.type == TType.STOP) {
                break;
            }
            if (field.id == 7) {
                TProtocolUtil.skip(input, field.type, MAX_THRIFT_DEPTH);
            } else {
                output.writeFieldBegin(new TField(field.name, field.type, field.id));
                copyValue(input, output, field.type, 0);
                output.writeFieldEnd();
            }
            input.readFieldEnd();
        }
        input.readStructEnd();
        output.writeFieldStop();
        output.writeStructEnd();
        outputTransport.flush();
        if (inputTransport.getBytesRemainingInBuffer() != 0) {
            throw new IOException("Compact-Thrift footer transcode left trailing bytes");
        }
        return encoded.toByteArray();
    }

    private static void copyValue(
        TCompactProtocol input,
        TCompactProtocol output,
        byte type,
        int depth
    ) throws TException {
        if (depth > MAX_THRIFT_DEPTH) {
            throw new TException("raw Compact-Thrift nesting exceeds the scanner limit");
        }
        switch (type) {
            case TType.BOOL:
                output.writeBool(input.readBool());
                return;
            case TType.BYTE:
                output.writeByte(input.readByte());
                return;
            case TType.I16:
                output.writeI16(input.readI16());
                return;
            case TType.I32:
            case TType.ENUM:
                output.writeI32(input.readI32());
                return;
            case TType.I64:
                output.writeI64(input.readI64());
                return;
            case TType.DOUBLE:
                output.writeDouble(input.readDouble());
                return;
            case TType.STRING:
                output.writeBinary(input.readBinary());
                return;
            case TType.UUID:
                output.writeUuid(input.readUuid());
                return;
            case TType.STRUCT:
                copyStruct(input, output, depth + 1);
                return;
            case TType.MAP:
                TMap map = input.readMapBegin();
                output.writeMapBegin(new TMap(map.keyType, map.valueType, map.size));
                for (int index = 0; index < map.size; index++) {
                    copyValue(input, output, map.keyType, depth + 1);
                    copyValue(input, output, map.valueType, depth + 1);
                }
                input.readMapEnd();
                output.writeMapEnd();
                return;
            case TType.LIST:
                TList list = input.readListBegin();
                output.writeListBegin(new TList(list.elemType, list.size));
                for (int index = 0; index < list.size; index++) {
                    copyValue(input, output, list.elemType, depth + 1);
                }
                input.readListEnd();
                output.writeListEnd();
                return;
            case TType.SET:
                TSet set = input.readSetBegin();
                output.writeSetBegin(new TSet(set.elemType, set.size));
                for (int index = 0; index < set.size; index++) {
                    copyValue(input, output, set.elemType, depth + 1);
                }
                input.readSetEnd();
                output.writeSetEnd();
                return;
            default:
                throw new TException("unsupported raw Compact-Thrift type: " + type);
        }
    }

    private static void copyStruct(TCompactProtocol input, TCompactProtocol output, int depth)
        throws TException {
        input.readStructBegin();
        output.writeStructBegin(new TStruct());
        while (true) {
            TField field = input.readFieldBegin();
            if (field.type == TType.STOP) {
                break;
            }
            output.writeFieldBegin(new TField(field.name, field.type, field.id));
            copyValue(input, output, field.type, depth);
            input.readFieldEnd();
            output.writeFieldEnd();
        }
        input.readStructEnd();
        output.writeFieldStop();
        output.writeStructEnd();
    }

    private static String rawColumnOrderState(int fieldId, byte wireType) {
        if (fieldId == 1 || fieldId == 2) {
            return wireType == TType.STRUCT ? "known" : "wrong_type";
        }
        return "unknown";
    }

    private static List<Leaf> schemaLeaves(FileMetaData metadata) throws IOException {
        if (!metadata.isSetVersion() || metadata.getVersion() <= 0) {
            throw new IOException("FileMetaData version must be present and positive");
        }
        if (!metadata.isSetNum_rows() || metadata.getNum_rows() < 0) {
            throw new IOException("FileMetaData num_rows must be present and nonnegative");
        }
        if (!metadata.isSetRow_groups() || metadata.getRow_groups() == null) {
            throw new IOException("FileMetaData row_groups must be present");
        }
        List<SchemaElement> schema = metadata.getSchema();
        List<Leaf> leaves = new ArrayList<>();
        if (!metadata.isSetSchema() || schema == null || schema.isEmpty()) {
            throw new IOException("FileMetaData schema must be present and nonempty");
        }
        if (schema.size() > MAX_SCHEMA_ELEMENTS) {
            throw new IOException("FileMetaData schema exceeds the scanner element limit");
        }
        SchemaElement root = schema.get(0);
        if (root == null || root.getName() == null || root.getName().isEmpty() || root.isSetType()
            || !root.isSetNum_children() || root.getNum_children() < 0) {
            throw new IOException("FileMetaData schema root is invalid");
        }
        int rootChildren = root.getNum_children();
        Deque<SchemaFrame> stack = new ArrayDeque<>();
        stack.push(new SchemaFrame(rootChildren, List.of()));
        Set<List<String>> paths = new HashSet<>();
        for (int index = 1; index < schema.size(); index++) {
            while (!stack.isEmpty() && stack.peek().remaining == 0) {
                stack.pop();
            }
            if (stack.isEmpty()) {
                throw new IOException("FileMetaData schema has an orphan element");
            }
            SchemaElement element = schema.get(index);
            if (element == null || element.getName() == null || element.getName().isEmpty()) {
                throw new IOException("FileMetaData schema has an unnamed element");
            }
            List<String> parent = stack.peek().path;
            stack.peek().remaining--;
            List<String> path = new ArrayList<>(parent.size() + 1);
            path.addAll(parent);
            path.add(element.getName());
            List<String> stablePath = Collections.unmodifiableList(new ArrayList<>(path));
            if (!paths.add(stablePath)) {
                throw new IOException("FileMetaData schema has a duplicate path: " + stablePath);
            }
            if (element.isSetType()) {
                if (element.isSetNum_children() && element.getNum_children() != 0) {
                    throw new IOException("FileMetaData leaf has child elements: " + stablePath);
                }
                leaves.add(new Leaf(leaves.size(), stablePath, element));
            } else {
                if (!element.isSetNum_children() || element.getNum_children() < 0) {
                    throw new IOException("FileMetaData group has an invalid child count: " + stablePath);
                }
                stack.push(new SchemaFrame(element.getNum_children(), stablePath));
            }
        }
        while (!stack.isEmpty() && stack.peek().remaining == 0) {
            stack.pop();
        }
        if (!stack.isEmpty()) {
            throw new IOException("FileMetaData schema ends before all declared children");
        }
        return leaves;
    }

    static void validateMetadata(FileMetaData metadata) throws IOException {
        List<Leaf> leaves = schemaLeaves(metadata);
        Map<List<String>, Leaf> leavesByPath = new HashMap<>();
        for (Leaf leaf : leaves) {
            leavesByPath.put(leaf.path, leaf);
        }
        validateRowGroups(metadata, leaves, leavesByPath);
    }

    private static void validateRowGroups(
        FileMetaData metadata,
        List<Leaf> leaves,
        Map<List<String>, Leaf> leavesByPath
    ) throws IOException {
        List<RowGroup> rowGroups = metadata.getRow_groups();
        for (int rowGroupOrdinal = 0; rowGroupOrdinal < rowGroups.size(); rowGroupOrdinal++) {
            RowGroup rowGroup = rowGroups.get(rowGroupOrdinal);
            if (rowGroup == null || !rowGroup.isSetColumns() || rowGroup.getColumns() == null) {
                throw new IOException("RowGroup columns must be present: " + rowGroupOrdinal);
            }
            if (!rowGroup.isSetTotal_byte_size() || rowGroup.getTotal_byte_size() < 0) {
                throw new IOException(
                    "RowGroup total_byte_size must be present and nonnegative: " + rowGroupOrdinal);
            }
            if (!rowGroup.isSetNum_rows() || rowGroup.getNum_rows() < 0) {
                throw new IOException("RowGroup num_rows must be present and nonnegative: " + rowGroupOrdinal);
            }
            if (rowGroup.isSetFile_offset() && rowGroup.getFile_offset() < 0) {
                throw new IOException("RowGroup file_offset must be nonnegative: " + rowGroupOrdinal);
            }
            if (rowGroup.isSetTotal_compressed_size() && rowGroup.getTotal_compressed_size() < 0) {
                throw new IOException(
                    "RowGroup total_compressed_size must be nonnegative: " + rowGroupOrdinal);
            }
            List<ColumnChunk> columns = rowGroup.getColumns();
            if (columns.size() != leaves.size()) {
                throw new IOException(
                    "RowGroup column count differs from the schema leaf count: " + rowGroupOrdinal);
            }
            Set<List<String>> columnPaths = new HashSet<>();
            for (int columnOrdinal = 0; columnOrdinal < columns.size(); columnOrdinal++) {
                validateColumnChunk(
                    columns.get(columnOrdinal), rowGroupOrdinal, columnOrdinal, leaves.get(columnOrdinal),
                    leavesByPath, columnPaths);
            }
        }
    }

    private static void validateColumnChunk(
        ColumnChunk chunk,
        int rowGroupOrdinal,
        int columnOrdinal,
        Leaf expectedLeaf,
        Map<List<String>, Leaf> leavesByPath,
        Set<List<String>> columnPaths
    ) throws IOException {
        String location = rowGroupOrdinal + "/" + columnOrdinal;
        if (chunk == null || !chunk.isSetFile_offset() || chunk.getFile_offset() < 0) {
            throw new IOException("ColumnChunk file_offset must be present and nonnegative: " + location);
        }
        validateOptionalOffset(chunk.isSetOffset_index_offset(), chunk.getOffset_index_offset(),
            "offset_index_offset", location);
        validateOptionalLength(chunk.isSetOffset_index_length(), chunk.getOffset_index_length(),
            "offset_index_length", location);
        validateOptionalOffset(chunk.isSetColumn_index_offset(), chunk.getColumn_index_offset(),
            "column_index_offset", location);
        validateOptionalLength(chunk.isSetColumn_index_length(), chunk.getColumn_index_length(),
            "column_index_length", location);
        ColumnMetaData metadata = chunk.getMeta_data();
        if (metadata == null) {
            return;
        }
        if (!metadata.isSetType() || !metadata.isSetEncodings() || metadata.getEncodings() == null
            || !metadata.isSetPath_in_schema() || metadata.getPath_in_schema() == null
            || metadata.getPath_in_schema().isEmpty() || !metadata.isSetCodec()) {
            throw new IOException("ColumnMetaData required descriptors are missing: " + location);
        }
        List<String> path = metadata.getPath_in_schema();
        for (String component : path) {
            if (component == null || component.isEmpty()) {
                throw new IOException("ColumnMetaData path contains an empty component: " + location);
            }
        }
        Leaf leaf = leavesByPath.get(path);
        if (leaf == null) {
            throw new IOException("ColumnMetaData path is not a schema leaf: " + path);
        }
        if (metadata.getType() != leaf.schema.getType()) {
            throw new IOException("ColumnMetaData physical type differs from schema leaf: " + path);
        }
        if (leaf != expectedLeaf) {
            throw new IOException("ColumnMetaData order differs from the schema leaf order: " + path);
        }
        if (!columnPaths.add(path)) {
            throw new IOException("RowGroup contains a duplicate column path: " + path);
        }
        if (!metadata.isSetNum_values() || metadata.getNum_values() < 0) {
            throw new IOException("ColumnMetaData num_values must be present and nonnegative: " + location);
        }
        if (!metadata.isSetTotal_uncompressed_size() || metadata.getTotal_uncompressed_size() < 0) {
            throw new IOException(
                "ColumnMetaData total_uncompressed_size must be present and nonnegative: " + location);
        }
        if (!metadata.isSetTotal_compressed_size() || metadata.getTotal_compressed_size() < 0) {
            throw new IOException(
                "ColumnMetaData total_compressed_size must be present and nonnegative: " + location);
        }
        if (!metadata.isSetData_page_offset() || metadata.getData_page_offset() < 0) {
            throw new IOException(
                "ColumnMetaData data_page_offset must be present and nonnegative: " + location);
        }
        validateOptionalOffset(metadata.isSetIndex_page_offset(), metadata.getIndex_page_offset(),
            "index_page_offset", location);
        validateOptionalOffset(metadata.isSetDictionary_page_offset(), metadata.getDictionary_page_offset(),
            "dictionary_page_offset", location);
        validateOptionalOffset(metadata.isSetBloom_filter_offset(), metadata.getBloom_filter_offset(),
            "bloom_filter_offset", location);
        validateOptionalLength(metadata.isSetBloom_filter_length(), metadata.getBloom_filter_length(),
            "bloom_filter_length", location);
    }

    private static void validateOptionalOffset(
        boolean present,
        long value,
        String name,
        String location
    ) throws IOException {
        if (present && value < 0) {
            throw new IOException(name + " must be nonnegative: " + location);
        }
    }

    private static void validateOptionalLength(
        boolean present,
        int value,
        String name,
        String location
    ) throws IOException {
        if (present && value < 0) {
            throw new IOException(name + " must be nonnegative: " + location);
        }
    }

    private static List<InputFile> collectInputs(List<Path> inputs) throws IOException {
        List<InputFile> files = new ArrayList<>();
        for (Path input : inputs) {
            Path normalized = input.toAbsolutePath().normalize();
            if (Files.isRegularFile(normalized)) {
                files.add(new InputFile(normalized.getFileName().toString(), normalized));
            } else if (Files.isDirectory(normalized)) {
                try (Stream<Path> stream = Files.walk(normalized)) {
                    java.util.Iterator<Path> paths = stream.filter(Files::isRegularFile)
                        .filter(path -> path.getFileName().toString().endsWith(".parquet"))
                        .iterator();
                    while (paths.hasNext()) {
                        Path path = paths.next();
                        files.add(new InputFile(
                            normalized.relativize(path).toString().replace(path.getFileSystem().getSeparator(), "/"),
                            path));
                        requireInputCount(files.size());
                    }
                }
            } else {
                throw new IOException("input does not exist or is not a regular file or directory: " + input);
            }
            requireInputCount(files.size());
        }
        files.sort(Comparator.comparing(file -> file.label));
        Set<String> labels = new HashSet<>();
        for (InputFile file : files) {
            if (!labels.add(file.label)) {
                throw new IOException("duplicate deterministic input label: " + file.label);
            }
        }
        if (files.isEmpty()) {
            throw new IOException("no Parquet input files found");
        }
        return files;
    }

    private static void requireInputCount(int count) throws IOException {
        if (count > MAX_INPUT_FILES) {
            throw new IOException("Parquet input count exceeds the scanner limit: " + count);
        }
    }

    private static void validateOutput(List<InputFile> inputs, Path output) throws IOException {
        if (output == null) {
            return;
        }
        Path absolute = output.toAbsolutePath().normalize();
        for (InputFile input : inputs) {
            if (absolute.equals(input.path)) {
                throw new IOException("output path aliases an input path: " + output);
            }
            if (Files.exists(absolute) && Files.isSameFile(absolute, input.path)) {
                throw new IOException("output file aliases an input file: " + output);
            }
        }
    }

    private static void writeEvidence(List<String> evidence, Path output) throws IOException {
        if (output == null) {
            for (String record : evidence) {
                System.out.print(record);
                System.out.print('\n');
            }
            return;
        }
        Path absolute = output.toAbsolutePath().normalize();
        Path parent = absolute.getParent();
        if (parent == null || !Files.isDirectory(parent)) {
            throw new IOException("output parent directory does not exist: " + output);
        }
        Path temporary = Files.createTempFile(parent, ".raw-footer-", ".tmp");
        boolean moved = false;
        try {
            try (BufferedWriter writer = Files.newBufferedWriter(temporary, StandardCharsets.UTF_8)) {
                for (String record : evidence) {
                    writer.write(record);
                    writer.write('\n');
                }
            }
            Files.move(temporary, absolute,
                StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            moved = true;
        } finally {
            if (!moved) {
                Files.deleteIfExists(temporary);
            }
        }
    }

    private static void requireArgument(String[] arguments, int index, String option) {
        if (index >= arguments.length) {
            throw new IllegalArgumentException(option + " requires a value");
        }
    }

    private static void requireMagic(byte[] actual, String position, Path path) throws IOException {
        if (!Arrays.equals(actual, MAGIC)) {
            throw new IOException("invalid " + position + " Parquet magic: " + path);
        }
    }

    private static long unsignedLittleEndianInt(byte[] bytes) {
        return (bytes[0] & 0xffL)
            | ((bytes[1] & 0xffL) << 8)
            | ((bytes[2] & 0xffL) << 16)
            | ((bytes[3] & 0xffL) << 24);
    }

    private static String sha256(RandomAccessFile file, long expectedSize) throws IOException {
        MessageDigest digest = sha256Digest();
        byte[] buffer = new byte[16 * 1024];
        long remaining = expectedSize;
        file.seek(0);
        while (remaining > 0) {
            int count = file.read(buffer, 0, (int) Math.min(buffer.length, remaining));
            if (count < 0) {
                throw new IOException("Parquet input became shorter during hashing");
            }
            digest.update(buffer, 0, count);
            remaining -= count;
        }
        return hex(digest.digest());
    }

    private static boolean sameFileSnapshot(BasicFileAttributes before, BasicFileAttributes after) {
        if (!before.isRegularFile() || !after.isRegularFile() || before.size() != after.size()
            || !before.lastModifiedTime().equals(after.lastModifiedTime())) {
            return false;
        }
        Object beforeKey = before.fileKey();
        Object afterKey = after.fileKey();
        return beforeKey == null || afterKey == null || beforeKey.equals(afterKey);
    }

    private static String sha256(byte[] bytes) {
        MessageDigest digest = sha256Digest();
        return hex(digest.digest(bytes));
    }

    private static MessageDigest sha256Digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("Java runtime does not provide SHA-256", exception);
        }
    }

    private static String hex(byte[] bytes) {
        StringBuilder result = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            int unsigned = value & 0xff;
            result.append("0123456789abcdef".charAt(unsigned >>> 4));
            result.append("0123456789abcdef".charAt(unsigned & 0x0f));
        }
        return result.toString();
    }

}
