package org.julialang.parquet.n6.raw;

import java.io.IOException;

final class CompactThriftPreflight {
    private static final int BOOLEAN_TRUE = 1;
    private static final int BOOLEAN_FALSE = 2;
    private static final int BYTE = 3;
    private static final int I16 = 4;
    private static final int I32 = 5;
    private static final int I64 = 6;
    private static final int DOUBLE = 7;
    private static final int BINARY = 8;
    private static final int LIST = 9;
    private static final int SET = 10;
    private static final int MAP = 11;
    private static final int STRUCT = 12;
    private static final int UUID = 13;
    private static final int MAXIMUM_DEPTH = 128;
    private static final long MAXIMUM_CONTAINER_ELEMENTS = 1_000_000;
    private static final long MAXIMUM_AGGREGATE_ELEMENTS = 4_000_000;
    private static final long MAXIMUM_BINARY_BYTES = 64L * 1024 * 1024;
    private static final long MAXIMUM_AGGREGATE_BYTES = 64L * 1024 * 1024;

    private final byte[] bytes;
    private final int maximumDepth;
    private final long maximumContainerElements;
    private final long maximumAggregateElements;
    private final long maximumBinaryBytes;
    private final long maximumAggregateBytes;
    private int position;
    private long aggregateElements;
    private long aggregateBytes;

    private CompactThriftPreflight(byte[] bytes) {
        this(bytes, MAXIMUM_DEPTH, MAXIMUM_CONTAINER_ELEMENTS, MAXIMUM_AGGREGATE_ELEMENTS,
            MAXIMUM_BINARY_BYTES, MAXIMUM_AGGREGATE_BYTES);
    }

    private CompactThriftPreflight(
        byte[] bytes,
        int maximumDepth,
        long maximumContainerElements,
        long maximumAggregateElements,
        long maximumBinaryBytes,
        long maximumAggregateBytes
    ) {
        if (maximumDepth < 0 || maximumContainerElements < 0 || maximumAggregateElements < 0
            || maximumBinaryBytes < 0 || maximumAggregateBytes < 0) {
            throw new IllegalArgumentException("Compact-Thrift limits must be nonnegative");
        }
        this.bytes = bytes;
        this.maximumDepth = maximumDepth;
        this.maximumContainerElements = maximumContainerElements;
        this.maximumAggregateElements = maximumAggregateElements;
        this.maximumBinaryBytes = maximumBinaryBytes;
        this.maximumAggregateBytes = maximumAggregateBytes;
    }

    static void validate(byte[] bytes) throws IOException {
        CompactThriftPreflight preflight = new CompactThriftPreflight(bytes);
        preflight.validate();
    }

    static void validateForTest(
        byte[] bytes,
        int maximumDepth,
        long maximumContainerElements,
        long maximumAggregateElements,
        long maximumBinaryBytes,
        long maximumAggregateBytes
    ) throws IOException {
        CompactThriftPreflight preflight = new CompactThriftPreflight(
            bytes, maximumDepth, maximumContainerElements, maximumAggregateElements,
            maximumBinaryBytes, maximumAggregateBytes);
        preflight.validate();
    }

    private void validate() throws IOException {
        readStruct(0);
        if (position != bytes.length) {
            throw new IOException("Compact-Thrift footer has trailing bytes after structural preflight");
        }
    }

    private void readStruct(int depth) throws IOException {
        requireDepth(depth);
        int lastFieldId = 0;
        while (true) {
            int header = readUnsignedByte();
            if (header == 0) {
                return;
            }
            int type = header & 0x0f;
            requireType(type, false);
            int delta = header >>> 4;
            int fieldId;
            if (delta == 0) {
                long encoded = readUnsignedVarint32();
                if (encoded > 0xffffL) {
                    throw new IOException("Compact-Thrift field ID is outside i16 range");
                }
                int value = (int) encoded;
                fieldId = (value >>> 1) ^ -(value & 1);
            } else {
                fieldId = lastFieldId + delta;
                if (fieldId > Short.MAX_VALUE) {
                    throw new IOException("Compact-Thrift delta field ID is outside i16 range");
                }
            }
            lastFieldId = fieldId;
            readFieldValue(type, depth);
        }
    }

    private void readFieldValue(int type, int depth) throws IOException {
        if (type == BOOLEAN_TRUE || type == BOOLEAN_FALSE) {
            return;
        }
        readValue(type, depth);
    }

    private void readValue(int type, int depth) throws IOException {
        switch (type) {
            case BOOLEAN_TRUE:
            case BOOLEAN_FALSE:
                int booleanValue = readUnsignedByte();
                if (booleanValue != BOOLEAN_TRUE && booleanValue != BOOLEAN_FALSE) {
                    throw new IOException("Compact-Thrift collection contains an invalid boolean");
                }
                return;
            case BYTE:
                skip(1);
                return;
            case I16:
            case I32:
                readUnsignedVarint32();
                return;
            case I64:
                readUnsignedVarint64();
                return;
            case DOUBLE:
                skip(8);
                return;
            case BINARY:
                readBinary();
                return;
            case LIST:
            case SET:
                readList(depth + 1);
                return;
            case MAP:
                readMap(depth + 1);
                return;
            case STRUCT:
                readStruct(depth + 1);
                return;
            case UUID:
                skip(16);
                return;
            default:
                throw new IOException("unsupported Compact-Thrift type: " + type);
        }
    }

    private void readBinary() throws IOException {
        long length = readUnsignedVarint32();
        if (length > Integer.MAX_VALUE || length > maximumBinaryBytes) {
            throw new IOException("Compact-Thrift binary length exceeds the scanner limit: " + length);
        }
        addAggregateBytes(length);
        skip((int) length);
    }

    private void readList(int depth) throws IOException {
        requireDepth(depth);
        int header = readUnsignedByte();
        long size = header >>> 4;
        int type = header & 0x0f;
        requireType(type, true);
        if (size == 15) {
            size = readUnsignedVarint32();
        }
        requireContainer(size, 1);
        for (long index = 0; index < size; index++) {
            readValue(type, depth);
        }
    }

    private void readMap(int depth) throws IOException {
        requireDepth(depth);
        long size = readUnsignedVarint32();
        requireContainer(size, 2);
        if (size == 0) {
            return;
        }
        int types = readUnsignedByte();
        int keyType = types >>> 4;
        int valueType = types & 0x0f;
        requireType(keyType, true);
        requireType(valueType, true);
        for (long index = 0; index < size; index++) {
            readValue(keyType, depth);
            readValue(valueType, depth);
        }
    }

    private void requireContainer(long size, int valuesPerElement) throws IOException {
        if (size > Integer.MAX_VALUE || size > maximumContainerElements) {
            throw new IOException("Compact-Thrift container length exceeds the scanner limit: " + size);
        }
        long values;
        try {
            values = Math.multiplyExact(size, valuesPerElement);
            aggregateElements = Math.addExact(aggregateElements, values);
        } catch (ArithmeticException exception) {
            throw new IOException("Compact-Thrift aggregate element count overflows", exception);
        }
        if (aggregateElements > maximumAggregateElements) {
            throw new IOException(
                "Compact-Thrift aggregate element count exceeds the scanner limit: " + aggregateElements);
        }
    }

    private void addAggregateBytes(long length) throws IOException {
        try {
            aggregateBytes = Math.addExact(aggregateBytes, length);
        } catch (ArithmeticException exception) {
            throw new IOException("Compact-Thrift aggregate binary byte count overflows", exception);
        }
        if (aggregateBytes > maximumAggregateBytes) {
            throw new IOException(
                "Compact-Thrift aggregate binary bytes exceed the scanner limit: " + aggregateBytes);
        }
    }

    private void requireDepth(int depth) throws IOException {
        if (depth > maximumDepth) {
            throw new IOException("Compact-Thrift nesting exceeds the scanner limit: " + depth);
        }
    }

    private static void requireType(int type, boolean collection) throws IOException {
        boolean valid = type >= BOOLEAN_TRUE && type <= UUID;
        if (!valid || (!collection && type == 0)) {
            throw new IOException("Compact-Thrift value has an invalid type: " + type);
        }
    }

    private long readUnsignedVarint32() throws IOException {
        long value = 0;
        for (int index = 0; index < 5; index++) {
            int next = readUnsignedByte();
            if (index == 4 && (next & 0xf0) != 0) {
                throw new IOException("Compact-Thrift varint32 overflows");
            }
            value |= (long) (next & 0x7f) << (index * 7);
            if ((next & 0x80) == 0) {
                return value;
            }
        }
        throw new IOException("Compact-Thrift varint32 is unterminated");
    }

    private void readUnsignedVarint64() throws IOException {
        for (int index = 0; index < 10; index++) {
            int next = readUnsignedByte();
            if (index == 9 && (next & 0xfe) != 0) {
                throw new IOException("Compact-Thrift varint64 overflows");
            }
            if ((next & 0x80) == 0) {
                return;
            }
        }
        throw new IOException("Compact-Thrift varint64 is unterminated");
    }

    private int readUnsignedByte() throws IOException {
        if (position >= bytes.length) {
            throw new IOException("Compact-Thrift footer ends inside a value");
        }
        return bytes[position++] & 0xff;
    }

    private void skip(int count) throws IOException {
        if (count < 0 || count > bytes.length - position) {
            throw new IOException("Compact-Thrift declared value exceeds the footer envelope");
        }
        position += count;
    }
}
