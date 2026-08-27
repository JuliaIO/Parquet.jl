package org.julialang.parquet.n6.raw;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.Statistics;

public final class SelfTestMain {
    @FunctionalInterface
    private interface ThrowingAction {
        void run() throws Exception;
    }

    private SelfTestMain() {
    }

    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 1) {
            throw new IllegalArgumentException("usage: SelfTestMain <self-test.parquet>");
        }
        Path fixture = Path.of(arguments[0]).toAbsolutePath().normalize();
        checkGeneratedFieldIds();
        checkDecodedFixture(fixture);
        checkEvidence(fixture);
        checkRawColumnOrderMutations(fixture);
        checkSchemaFailures();
        checkStructuralPreflight();
        checkFailures(fixture);
    }

    private static void checkGeneratedFieldIds() {
        require(org.apache.parquet.format.ColumnOrder._Fields.TYPE__ORDER.getThriftFieldId() == 1,
            "TYPE_ORDER field ID");
        require(org.apache.parquet.format.ColumnOrder._Fields.IEEE_754__TOTAL__ORDER
            .getThriftFieldId() == 2,
            "IEEE_754_TOTAL_ORDER field ID");
        for (int fieldId = 1; fieldId <= 9; fieldId++) {
            Statistics._Fields field = Statistics._Fields.findByThriftId(fieldId);
            require(field != null, "Statistics field " + fieldId + " exists");
            require(field.getThriftFieldId() == fieldId, "Statistics field " + fieldId + " identity");
        }
        require("max_value".equals(Statistics._Fields.findByThriftId(5).getFieldName()),
            "Statistics field 5 name");
        require("min_value".equals(Statistics._Fields.findByThriftId(6).getFieldName()),
            "Statistics field 6 name");
        require("is_max_value_exact".equals(Statistics._Fields.findByThriftId(7).getFieldName()),
            "Statistics field 7 name");
        require("is_min_value_exact".equals(Statistics._Fields.findByThriftId(8).getFieldName()),
            "Statistics field 8 name");
        require("nan_count".equals(Statistics._Fields.findByThriftId(9).getFieldName()),
            "Statistics field 9 name");
    }

    private static void checkDecodedFixture(Path fixture) throws Exception {
        RawFooterScanner.Footer footer = RawFooterScanner.readFooter(fixture);
        FileMetaData metadata = footer.metadata;
        require(!metadata.isSetColumn_orders(), "semantic decode excludes raw column orders");
        require(footer.rawColumnOrders.present && footer.rawColumnOrders.values.size() == 8,
            "raw column order count");
        require("TYPE_ORDER".equals(footer.rawColumnOrders.values.get(0).member),
            "raw TYPE_ORDER union member");
        require("IEEE_754_TOTAL_ORDER".equals(footer.rawColumnOrders.values.get(1).member),
            "raw IEEE union member");
        Statistics float32 = metadata.getRow_groups().get(0).getColumns().get(0)
            .getMeta_data().getStatistics();
        require(float32.isSetMax_value() && float32.isSetMin_value(), "modern float32 bounds");
        require(Arrays.equals(float32.getMin_value(), hex("00000080")), "float32 negative-zero bytes");
        require(Arrays.equals(float32.getMax_value(), hex("00000000")), "float32 positive-zero bytes");
        require(float32.isSetNan_count() && float32.getNan_count() == 0, "present zero nan_count");
        require(float32.isSetIs_max_value_exact() && float32.isIs_max_value_exact(),
            "present true exactness");
        require(float32.isSetIs_min_value_exact() && !float32.isIs_min_value_exact(),
            "present false exactness");
        Statistics float16 = metadata.getRow_groups().get(0).getColumns().get(2)
            .getMeta_data().getStatistics();
        require(!float16.isSetIs_max_value_exact(), "absent exactness");
        require(Arrays.equals(float16.getMin_value(), hex("0080")), "float16 negative-zero bytes");
    }

    private static void checkEvidence(Path fixture) throws Exception {
        String evidence = RawFooterScanner.scan(fixture, "self-test.parquet");
        require(evidence.indexOf('\n') < 0, "one JSON object per evidence line");
        require(evidence.contains("\"evidence_version\":\"parquet-2.13-raw-footer-v3\""),
            "raw evidence schema version");
        require(evidence.contains("\"schema_leaf_count\":8,\"schema_leaves\":["),
            "raw schema leaf table");
        require(evidence.contains("\"path\":[\"uint16\"],\"physical_type\":\"INT32\","
            + "\"type_length\":{\"present\":true,\"value\":16},"
            + "\"converted_type\":{\"present\":true,\"value\":\"UINT_16\"}"),
            "raw integer legacy descriptor");
        require(evidence.contains("\"field_id\":{\"present\":true,\"value\":17},"
            + "\"logical_type\":{\"present\":true,\"member\":\"INTEGER\","
            + "\"parameters\":{\"bit_width\":16,\"is_signed\":false}}"),
            "raw integer logical descriptor");
        require(evidence.contains("\"path\":[\"decimal4\"],\"physical_type\":"
            + "\"FIXED_LEN_BYTE_ARRAY\",\"type_length\":{\"present\":true,\"value\":4},"
            + "\"converted_type\":{\"present\":true,\"value\":\"DECIMAL\"},"
            + "\"scale\":{\"present\":true,\"value\":2},"
            + "\"precision\":{\"present\":true,\"value\":9}"),
            "raw decimal legacy descriptor");
        require(evidence.contains("\"member\":\"DECIMAL\","
            + "\"parameters\":{\"scale\":2,\"precision\":9}"),
            "raw decimal logical descriptor");
        require(evidence.contains("\"path\":[\"time64\"]")
            && evidence.contains("\"member\":\"TIME\","
                + "\"parameters\":{\"unit\":\"MICROS\",\"is_adjusted_to_utc\":true}")
            && evidence.contains("\"member\":\"TIMESTAMP\","
                + "\"parameters\":{\"unit\":\"NANOS\",\"is_adjusted_to_utc\":false}"),
            "raw time logical descriptors");
        require(evidence.contains("\"state\":\"known\",\"field_id\":2,\"wire_type\":12,"
            + "\"header_hex\":\"2c\","
            + "\"member\":\"IEEE_754_TOTAL_ORDER\""),
            "raw IEEE union field");
        require(evidence.contains("\"name\":\"nan_count\",\"present\":true,\"value\":0"),
            "raw present-zero nan count");
        require(evidence.contains("\"hex\":\"00000080\",\"byte_length\":4,"
            + "\"float_bits\":{\"width\":32,\"valid_width\":true,\"hex\":\"0x80000000\"}"),
            "raw float32 signed-zero bit pattern");
        require(evidence.contains("\"hex\":\"010000000000f8ff\",\"byte_length\":8,"
            + "\"float_bits\":{\"width\":64,\"valid_width\":true,"
            + "\"hex\":\"0xfff8000000000001\"}"), "raw float64 NaN payload bits");
        require(evidence.contains("\"hex\":\"0080\",\"byte_length\":2,"
            + "\"float_bits\":{\"width\":16,\"valid_width\":true,\"hex\":\"0x8000\"}"),
            "raw float16 signed-zero bit pattern");
        require(evidence.contains("\"name\":\"is_max_value_exact\",\"present\":false,\"value\":null"),
            "raw absent exactness");
        require(evidence.contains("raw-java self-test \\u03c0"), "stable non-ASCII JSON escaping");
    }

    private static void checkRawColumnOrderMutations(Path fixture) throws Exception {
        Path directory = Files.createTempDirectory("raw-java-orders-");
        try {
            Path id1 = mutateOrderHeader(fixture, directory.resolve("id1.parquet"), 1, (byte) 0x1c);
            require(RawFooterScanner.scan(id1, "id1.parquet").contains(
                "\"ordinal\":1,\"schema_leaf_ordinal\":1,\"state\":\"known\","
                    + "\"field_id\":1,\"wire_type\":12,"
                    + "\"header_hex\":\"1c\","
                    + "\"member\":\"TYPE_ORDER\""), "mutated raw TYPE_ORDER field ID");

            Path id2 = mutateOrderHeader(fixture, directory.resolve("id2.parquet"), 0, (byte) 0x2c);
            require(RawFooterScanner.scan(id2, "id2.parquet").contains(
                "\"ordinal\":0,\"schema_leaf_ordinal\":0,\"state\":\"known\","
                    + "\"field_id\":2,\"wire_type\":12,"
                    + "\"header_hex\":\"2c\","
                    + "\"member\":\"IEEE_754_TOTAL_ORDER\""), "mutated raw IEEE field ID");

            Path unknown = mutateOrderHeader(
                fixture, directory.resolve("unknown.parquet"), 0, (byte) 0x3c);
            require(RawFooterScanner.scan(unknown, "unknown.parquet").contains(
                "\"ordinal\":0,\"schema_leaf_ordinal\":0,\"state\":\"unknown\","
                    + "\"field_id\":3,"
                    + "\"wire_type\":12,\"header_hex\":\"3c\",\"member\":null"),
                "unknown raw union field ID");

            Path minimumId = mutateOrderExplicitId(
                fixture, directory.resolve("minimum-id.parquet"), 0, Short.MIN_VALUE);
            require(RawFooterScanner.scan(minimumId, "minimum-id.parquet").contains(
                "\"ordinal\":0,\"schema_leaf_ordinal\":0,\"state\":\"unknown\","
                    + "\"field_id\":-32768,"
                    + "\"wire_type\":12,\"header_hex\":\"0cffff03\",\"member\":null"),
                "minimum raw i16 field ID");

            Path maximumId = mutateOrderExplicitId(
                fixture, directory.resolve("maximum-id.parquet"), 0, Short.MAX_VALUE);
            require(RawFooterScanner.scan(maximumId, "maximum-id.parquet").contains(
                "\"ordinal\":0,\"schema_leaf_ordinal\":0,\"state\":\"unknown\","
                    + "\"field_id\":32767,"
                    + "\"wire_type\":12,\"header_hex\":\"0cfeff03\",\"member\":null"),
                "maximum raw i16 field ID");

            Path wrongType = mutateOrderHeader(
                fixture, directory.resolve("wrong-type.parquet"), 0, (byte) 0x15);
            require(RawFooterScanner.scan(wrongType, "wrong-type.parquet").contains(
                "\"ordinal\":0,\"schema_leaf_ordinal\":0,\"state\":\"wrong_type\","
                    + "\"field_id\":1,"
                    + "\"wire_type\":8,\"header_hex\":\"15\",\"member\":null"),
                "wrong raw union wire type");

            Path empty = mutateOrderEmpty(fixture, directory.resolve("empty.parquet"), 0);
            require(RawFooterScanner.scan(empty, "empty.parquet").contains(
                "\"ordinal\":0,\"schema_leaf_ordinal\":0,\"state\":\"empty\","
                    + "\"field_id\":null,"
                    + "\"wire_type\":null,\"header_hex\":\"00\",\"member\":null"),
                "empty raw union member");
        } finally {
            deleteTree(directory);
        }
    }

    private static void checkSchemaFailures() throws Exception {
        FileMetaData absent = SelfTestFixture.metadata().deepCopy();
        absent.unsetSchema();
        expect(IOException.class, () -> RawFooterScanner.validateMetadata(absent), "absent schema");

        FileMetaData empty = SelfTestFixture.metadata().deepCopy();
        empty.setSchema(List.of());
        expect(IOException.class, () -> RawFooterScanner.validateMetadata(empty), "empty schema");

        FileMetaData truncated = SelfTestFixture.metadata().deepCopy();
        truncated.getSchema().get(0).setNum_children(5);
        expect(IOException.class, () -> RawFooterScanner.validateMetadata(truncated),
            "truncated schema topology");

        FileMetaData orphan = SelfTestFixture.metadata().deepCopy();
        orphan.getSchema().get(0).setNum_children(3);
        expect(IOException.class, () -> RawFooterScanner.validateMetadata(orphan),
            "orphan schema element");

        FileMetaData duplicate = SelfTestFixture.metadata().deepCopy();
        duplicate.getSchema().get(4).setName("float32");
        expect(IOException.class, () -> RawFooterScanner.validateMetadata(duplicate),
            "duplicate schema path");

        FileMetaData invalidRoot = SelfTestFixture.metadata().deepCopy();
        invalidRoot.getSchema().get(0).setNum_children(-1);
        expect(IOException.class, () -> RawFooterScanner.validateMetadata(invalidRoot),
            "negative schema child count");

        FileMetaData invalidLeaf = SelfTestFixture.metadata().deepCopy();
        invalidLeaf.getSchema().get(1).setNum_children(1);
        expect(IOException.class, () -> RawFooterScanner.validateMetadata(invalidLeaf),
            "leaf with child count");

        FileMetaData absentRows = SelfTestFixture.metadata().deepCopy();
        absentRows.unsetRow_groups();
        expect(IOException.class, () -> RawFooterScanner.validateMetadata(absentRows),
            "absent row groups");

        FileMetaData negativeRows = SelfTestFixture.metadata().deepCopy();
        negativeRows.setNum_rows(-1);
        expect(IOException.class, () -> RawFooterScanner.validateMetadata(negativeRows),
            "negative file row count");

        FileMetaData absentRowGroupRows = SelfTestFixture.metadata().deepCopy();
        absentRowGroupRows.getRow_groups().get(0).unsetNum_rows();
        expect(IOException.class, () -> RawFooterScanner.validateMetadata(absentRowGroupRows),
            "absent row-group row count");

        FileMetaData negativeRowGroupRows = SelfTestFixture.metadata().deepCopy();
        negativeRowGroupRows.getRow_groups().get(0).setNum_rows(-1);
        expect(IOException.class, () -> RawFooterScanner.validateMetadata(negativeRowGroupRows),
            "negative row-group row count");

        FileMetaData absentByteSize = SelfTestFixture.metadata().deepCopy();
        absentByteSize.getRow_groups().get(0).unsetTotal_byte_size();
        expect(IOException.class, () -> RawFooterScanner.validateMetadata(absentByteSize),
            "absent row-group total byte size");

        FileMetaData absentValues = SelfTestFixture.metadata().deepCopy();
        absentValues.getRow_groups().get(0).getColumns().get(0).getMeta_data().unsetNum_values();
        expect(IOException.class, () -> RawFooterScanner.validateMetadata(absentValues),
            "absent column value count");

        FileMetaData negativeValues = SelfTestFixture.metadata().deepCopy();
        negativeValues.getRow_groups().get(0).getColumns().get(0).getMeta_data().setNum_values(-1);
        expect(IOException.class, () -> RawFooterScanner.validateMetadata(negativeValues),
            "negative column value count");

        FileMetaData wrongPhysicalType = SelfTestFixture.metadata().deepCopy();
        wrongPhysicalType.getRow_groups().get(0).getColumns().get(0).getMeta_data()
            .setType(org.apache.parquet.format.Type.INT32);
        expect(IOException.class, () -> RawFooterScanner.validateMetadata(wrongPhysicalType),
            "column and leaf physical type mismatch");

        FileMetaData unknownPath = SelfTestFixture.metadata().deepCopy();
        unknownPath.getRow_groups().get(0).getColumns().get(0).getMeta_data()
            .setPath_in_schema(List.of("unknown"));
        expect(IOException.class, () -> RawFooterScanner.validateMetadata(unknownPath),
            "column path absent from leaf table");

        FileMetaData missingColumn = SelfTestFixture.metadata().deepCopy();
        missingColumn.getRow_groups().get(0).getColumns().remove(0);
        expect(IOException.class, () -> RawFooterScanner.validateMetadata(missingColumn),
            "row-group column and leaf count mismatch");

        FileMetaData wrongColumnOrder = SelfTestFixture.metadata().deepCopy();
        org.apache.parquet.format.ColumnChunk first =
            wrongColumnOrder.getRow_groups().get(0).getColumns().get(0);
        wrongColumnOrder.getRow_groups().get(0).getColumns().set(0,
            wrongColumnOrder.getRow_groups().get(0).getColumns().get(1));
        wrongColumnOrder.getRow_groups().get(0).getColumns().set(1, first);
        expect(IOException.class, () -> RawFooterScanner.validateMetadata(wrongColumnOrder),
            "row-group column and leaf order mismatch");
    }

    private static void checkStructuralPreflight() throws Exception {
        CompactThriftPreflight.validate(new byte[] {0});
        expect(IOException.class, () -> CompactThriftPreflight.validate(
            compactFieldWithLength((byte) 0x18, 64L * 1024 * 1024 + 1)),
            "oversized Compact-Thrift binary declaration");
        expect(IOException.class, () -> CompactThriftPreflight.validate(
            compactListWithLength(1_000_001)),
            "oversized Compact-Thrift container declaration");
        expect(IOException.class, () -> CompactThriftPreflight.validate(
            new byte[] {0x18, (byte) 0x80}), "unterminated Compact-Thrift length");
        byte[] aggregateElements = new byte[] {
            0x19, 0x23, 0, 0, 0x19, 0x23, 0, 0, 0
        };
        CompactThriftPreflight.validateForTest(aggregateElements, 128, 10, 4, 10, 10);
        expect(IOException.class, () -> CompactThriftPreflight.validateForTest(
            aggregateElements, 128, 10, 3, 10, 10),
            "oversized Compact-Thrift aggregate element count");
        byte[] aggregateBytes = new byte[] {
            0x18, 2, 0, 0, 0x18, 2, 0, 0, 0
        };
        CompactThriftPreflight.validateForTest(aggregateBytes, 128, 10, 10, 2, 4);
        expect(IOException.class, () -> CompactThriftPreflight.validateForTest(
            aggregateBytes, 128, 10, 10, 2, 3),
            "oversized Compact-Thrift aggregate binary bytes");
        expect(JsonWriter.LimitException.class, () -> new JsonWriter(5)
            .hexValue(ByteBuffer.wrap(new byte[] {0, 1}), false, false),
            "hex evidence checks its exact size before growth");
        byte[] nested = new byte[261];
        Arrays.fill(nested, 0, 130, (byte) 0x1c);
        Arrays.fill(nested, 130, nested.length, (byte) 0);
        expect(IOException.class, () -> CompactThriftPreflight.validate(nested),
            "oversized Compact-Thrift depth");
    }

    private static void checkFailures(Path fixture) throws Exception {
        Path directory = Files.createTempDirectory("raw-java-negative-");
        try {
            byte[] valid = Files.readAllBytes(fixture);
            Path leading = directory.resolve("leading.parquet");
            byte[] badLeading = valid.clone();
            badLeading[0] = 'X';
            Files.write(leading, badLeading);
            expect(IOException.class, () -> RawFooterScanner.readFooter(leading), "leading magic");

            Path trailing = directory.resolve("trailing.parquet");
            byte[] badTrailing = valid.clone();
            badTrailing[badTrailing.length - 1] = 'X';
            Files.write(trailing, badTrailing);
            expect(IOException.class, () -> RawFooterScanner.readFooter(trailing), "trailing magic");

            Path envelope = directory.resolve("envelope.parquet");
            byte[] badEnvelope = valid.clone();
            int lengthOffset = badEnvelope.length - 8;
            Arrays.fill(badEnvelope, lengthOffset, lengthOffset + 4, (byte) 0xff);
            Files.write(envelope, badEnvelope);
            expect(IOException.class, () -> RawFooterScanner.readFooter(envelope), "footer containment");

            Path trailingThrift = directory.resolve("trailing-thrift.parquet");
            Files.write(trailingThrift, addTrailingFooterByte(valid));
            expect(IOException.class, () -> RawFooterScanner.readFooter(trailingThrift),
                "trailing Compact-Thrift byte");

            Path shortFile = directory.resolve("short.parquet");
            Files.write(shortFile, new byte[] {'P', 'A', 'R', '1'});
            expect(IOException.class, () -> RawFooterScanner.readFooter(shortFile), "short file");

            Path output = directory.resolve("evidence.jsonl");
            byte[] sentinel = "unchanged\n".getBytes(StandardCharsets.UTF_8);
            Files.write(output, sentinel);
            expect(IOException.class, () -> RawFooterScanner.execute(new String[] {
                "scan", "--input", envelope.toString(), "--output", output.toString()
            }), "failure before output replacement");
            require(Arrays.equals(Files.readAllBytes(output), sentinel), "failed scan leaves output unchanged");

            expect(IOException.class, () -> RawFooterScanner.execute(new String[] {
                "scan", "--input", fixture.toString(), "--output", output.toString()
            }, 128), "incremental evidence output limit");
            require(Arrays.equals(Files.readAllBytes(output), sentinel),
                "evidence limit leaves output unchanged");

            Path missingRowCount = directory.resolve("missing-row-count.parquet");
            SelfTestFixture.writeMissingRequired(missingRowCount, true, false);
            expect(Exception.class, () -> RawFooterScanner.execute(new String[] {
                "scan", "--input", missingRowCount.toString(), "--output", output.toString()
            }), "missing row-group count before output");
            require(Arrays.equals(Files.readAllBytes(output), sentinel),
                "missing row-group count leaves output unchanged");

            Path missingValueCount = directory.resolve("missing-value-count.parquet");
            SelfTestFixture.writeMissingRequired(missingValueCount, false, true);
            expect(Exception.class, () -> RawFooterScanner.execute(new String[] {
                "scan", "--input", missingValueCount.toString(), "--output", output.toString()
            }), "missing column value count before output");
            require(Arrays.equals(Files.readAllBytes(output), sentinel),
                "missing column value count leaves output unchanged");

            FileMetaData negativeRowMetadata = SelfTestFixture.metadata().deepCopy();
            negativeRowMetadata.getRow_groups().get(0).setNum_rows(-1);
            Path negativeRowCount = directory.resolve("negative-row-count.parquet");
            SelfTestFixture.write(negativeRowCount, negativeRowMetadata);
            expect(IOException.class, () -> RawFooterScanner.execute(new String[] {
                "scan", "--input", negativeRowCount.toString(), "--output", output.toString()
            }), "negative row-group count before output");
            require(Arrays.equals(Files.readAllBytes(output), sentinel),
                "negative row-group count leaves output unchanged");

            FileMetaData negativeValueMetadata = SelfTestFixture.metadata().deepCopy();
            negativeValueMetadata.getRow_groups().get(0).getColumns().get(0).getMeta_data()
                .setNum_values(-1);
            Path negativeValueCount = directory.resolve("negative-value-count.parquet");
            SelfTestFixture.write(negativeValueCount, negativeValueMetadata);
            expect(IOException.class, () -> RawFooterScanner.execute(new String[] {
                "scan", "--input", negativeValueCount.toString(), "--output", output.toString()
            }), "negative column value count before output");
            require(Arrays.equals(Files.readAllBytes(output), sentinel),
                "negative column value count leaves output unchanged");

            String fixtureHash = sha256(Files.readAllBytes(fixture));
            expect(IOException.class, () -> RawFooterScanner.execute(new String[] {
                "scan", "--input", fixture.toString(), "--output", fixture.toString()
            }), "direct output alias");
            require(fixtureHash.equals(sha256(Files.readAllBytes(fixture))),
                "direct output alias leaves input unchanged");

            Path hardlink = directory.resolve("hardlink.parquet");
            Files.createLink(hardlink, fixture);
            expect(IOException.class, () -> RawFooterScanner.execute(new String[] {
                "scan", "--input", fixture.toString(), "--output", hardlink.toString()
            }), "hard-link output alias");
            require(fixtureHash.equals(sha256(Files.readAllBytes(fixture)))
                && fixtureHash.equals(sha256(Files.readAllBytes(hardlink))),
                "hard-link output alias leaves input unchanged");

            Path symlink = directory.resolve("symlink.parquet");
            Files.createSymbolicLink(symlink, fixture);
            expect(IOException.class, () -> RawFooterScanner.execute(new String[] {
                "scan", "--input", fixture.toString(), "--output", symlink.toString()
            }), "symbolic-link output alias");
            require(fixtureHash.equals(sha256(Files.readAllBytes(fixture)))
                && fixtureHash.equals(sha256(Files.readAllBytes(symlink))),
                "symbolic-link output alias leaves input unchanged");

            Path changing = directory.resolve("changing.parquet");
            Files.copy(fixture, changing);
            expect(IOException.class, () -> RawFooterScanner.readFooter(changing, () -> {
                try (RandomAccessFile changed = new RandomAccessFile(changing.toFile(), "rw")) {
                    changed.seek(0);
                    changed.write('X');
                } catch (IOException exception) {
                    throw new IllegalStateException("cannot mutate snapshot fixture", exception);
                }
            }), "input snapshot race");

            Path oversized = directory.resolve("oversized-footer.parquet");
            writeOversizedFooterEnvelope(oversized);
            expect(IOException.class, () -> RawFooterScanner.readFooter(oversized),
                "footer resource limit");

            Path oversizedDeclaration = directory.resolve("oversized-declaration.parquet");
            SelfTestFixture.writeRawFooter(oversizedDeclaration,
                compactFieldWithLength((byte) 0x18, 64L * 1024 * 1024 + 1));
            expect(IOException.class, () -> RawFooterScanner.readFooter(oversizedDeclaration),
                "pre-decode binary declaration limit");
        } finally {
            deleteTree(directory);
        }
    }

    private static Path mutateOrderHeader(Path fixture, Path output, int ordinal, byte header)
        throws Exception {
        RawFooterScanner.Footer footer = RawFooterScanner.readFooter(fixture);
        RawFooterScanner.RawColumnOrder order = footer.rawColumnOrders.values.get(ordinal);
        byte[] bytes = Files.readAllBytes(fixture);
        int footerStart = bytes.length - 8 - footer.footerLength;
        int offset = footerStart + order.headerOffset;
        require((bytes[offset] & 0x0f) == 0x0c, "source order uses a raw struct header");
        bytes[offset] = header;
        Files.write(output, bytes);
        return output;
    }

    private static byte[] compactFieldWithLength(byte fieldHeader, long length) {
        byte[] encoded = unsignedVarint(length);
        byte[] bytes = new byte[encoded.length + 2];
        bytes[0] = fieldHeader;
        System.arraycopy(encoded, 0, bytes, 1, encoded.length);
        bytes[bytes.length - 1] = 0;
        return bytes;
    }

    private static byte[] compactListWithLength(long length) {
        byte[] encoded = unsignedVarint(length);
        byte[] bytes = new byte[encoded.length + 3];
        bytes[0] = 0x19;
        bytes[1] = (byte) 0xf3;
        System.arraycopy(encoded, 0, bytes, 2, encoded.length);
        bytes[bytes.length - 1] = 0;
        return bytes;
    }

    private static byte[] unsignedVarint(long value) {
        byte[] bytes = new byte[10];
        int length = 0;
        do {
            int next = (int) (value & 0x7f);
            value >>>= 7;
            bytes[length++] = (byte) (value == 0 ? next : next | 0x80);
        } while (value != 0);
        return Arrays.copyOf(bytes, length);
    }

    private static Path mutateOrderEmpty(Path fixture, Path output, int ordinal) throws Exception {
        RawFooterScanner.Footer footer = RawFooterScanner.readFooter(fixture);
        RawFooterScanner.RawColumnOrder order = footer.rawColumnOrders.values.get(ordinal);
        byte[] bytes = Files.readAllBytes(fixture);
        int footerStart = bytes.length - 8 - footer.footerLength;
        int offset = footerStart + order.headerOffset;
        require((bytes[offset] & 0x0f) == 0x0c && bytes[offset + 1] == 0,
            "source order uses an empty struct payload");
        byte[] changed = new byte[bytes.length - 2];
        System.arraycopy(bytes, 0, changed, 0, offset);
        System.arraycopy(bytes, offset + 2, changed, offset, bytes.length - offset - 2);
        writeLittleEndianInt(changed, changed.length - 8, footer.footerLength - 2);
        Files.write(output, changed);
        return output;
    }

    private static Path mutateOrderExplicitId(
        Path fixture,
        Path output,
        int ordinal,
        short fieldId
    ) throws Exception {
        RawFooterScanner.Footer footer = RawFooterScanner.readFooter(fixture);
        RawFooterScanner.RawColumnOrder order = footer.rawColumnOrders.values.get(ordinal);
        byte[] bytes = Files.readAllBytes(fixture);
        int footerStart = bytes.length - 8 - footer.footerLength;
        int offset = footerStart + order.headerOffset;
        require((bytes[offset] & 0x0f) == 0x0c, "source order uses a raw struct header");
        int encodedId = ((fieldId << 1) ^ (fieldId >> 15)) & 0xffff;
        byte[] header = new byte[4];
        int headerLength = 1;
        header[0] = 0x0c;
        do {
            int next = encodedId & 0x7f;
            encodedId >>>= 7;
            header[headerLength++] = (byte) (encodedId == 0 ? next : next | 0x80);
        } while (encodedId != 0);
        byte[] changed = new byte[bytes.length + headerLength - 1];
        System.arraycopy(bytes, 0, changed, 0, offset);
        System.arraycopy(header, 0, changed, offset, headerLength);
        System.arraycopy(bytes, offset + 1, changed, offset + headerLength,
            bytes.length - offset - 1);
        writeLittleEndianInt(changed, changed.length - 8,
            footer.footerLength + headerLength - 1);
        Files.write(output, changed);
        return output;
    }

    private static void writeOversizedFooterEnvelope(Path output) throws IOException {
        int footerLength = 64 * 1024 * 1024 + 1;
        try (RandomAccessFile file = new RandomAccessFile(output.toFile(), "rw")) {
            file.setLength((long) footerLength + 12);
            file.seek(0);
            file.write(new byte[] {'P', 'A', 'R', '1'});
            file.seek((long) footerLength + 4);
            file.write(footerLength & 0xff);
            file.write((footerLength >>> 8) & 0xff);
            file.write((footerLength >>> 16) & 0xff);
            file.write((footerLength >>> 24) & 0xff);
            file.write(new byte[] {'P', 'A', 'R', '1'});
        }
    }

    private static String sha256(byte[] bytes) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
            StringBuilder hex = new StringBuilder(digest.length * 2);
            for (byte value : digest) {
                hex.append(String.format("%02x", value & 0xff));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is unavailable", exception);
        }
    }

    private static void deleteTree(Path directory) throws IOException {
        try (java.util.stream.Stream<Path> paths = Files.walk(directory)) {
            paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.delete(path);
                } catch (IOException exception) {
                    throw new IllegalStateException("cannot clean self-test path " + path, exception);
                }
            });
        }
    }

    private static byte[] addTrailingFooterByte(byte[] valid) {
        int lengthOffset = valid.length - 8;
        int footerLength = littleEndianInt(valid, lengthOffset);
        byte[] changed = new byte[valid.length + 1];
        System.arraycopy(valid, 0, changed, 0, lengthOffset);
        changed[lengthOffset] = 0;
        writeLittleEndianInt(changed, lengthOffset + 1, footerLength + 1);
        System.arraycopy(valid, valid.length - 4, changed, changed.length - 4, 4);
        return changed;
    }

    private static int littleEndianInt(byte[] bytes, int offset) {
        return (bytes[offset] & 0xff)
            | ((bytes[offset + 1] & 0xff) << 8)
            | ((bytes[offset + 2] & 0xff) << 16)
            | ((bytes[offset + 3] & 0xff) << 24);
    }

    private static void writeLittleEndianInt(byte[] bytes, int offset, int value) {
        bytes[offset] = (byte) value;
        bytes[offset + 1] = (byte) (value >>> 8);
        bytes[offset + 2] = (byte) (value >>> 16);
        bytes[offset + 3] = (byte) (value >>> 24);
    }

    private static byte[] hex(String value) {
        byte[] bytes = new byte[value.length() / 2];
        for (int index = 0; index < bytes.length; index++) {
            int high = Character.digit(value.charAt(index * 2), 16);
            int low = Character.digit(value.charAt(index * 2 + 1), 16);
            bytes[index] = (byte) ((high << 4) | low);
        }
        return bytes;
    }

    private static <T extends Throwable> void expect(
        Class<T> expected,
        ThrowingAction action,
        String description
    ) throws Exception {
        try {
            action.run();
        } catch (Throwable exception) {
            if (expected.isInstance(exception)) {
                return;
            }
            throw new AssertionError(description + " raised " + exception.getClass().getName(), exception);
        }
        throw new AssertionError(description + " did not fail");
    }

    private static void require(boolean condition, String description) {
        if (!condition) {
            throw new AssertionError(description);
        }
    }
}
