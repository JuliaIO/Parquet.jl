package org.julialang.parquet.n6.raw;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;

final class JsonWriter {
    static final class LimitException extends IllegalStateException {
        private LimitException() {
            super("raw footer evidence exceeds the scanner memory limit");
        }
    }

    private static final class Context {
        private final boolean object;
        private boolean first = true;
        private boolean awaitingValue;

        private Context(boolean object) {
            this.object = object;
        }
    }

    private final StringBuilder output = new StringBuilder();
    private final Deque<Context> contexts = new ArrayDeque<>();
    private final long maximumCharacters;
    private boolean rootWritten;

    JsonWriter(long maximumCharacters) {
        if (maximumCharacters < 0) {
            throw new LimitException();
        }
        this.maximumCharacters = maximumCharacters;
    }

    JsonWriter beginObject() {
        beforeValue();
        append('{');
        contexts.push(new Context(true));
        return this;
    }

    JsonWriter endObject() {
        Context context = requireContext(true);
        if (context.awaitingValue) {
            throw new IllegalStateException("JSON object name has no value");
        }
        contexts.pop();
        append('}');
        return this;
    }

    JsonWriter beginArray() {
        beforeValue();
        append('[');
        contexts.push(new Context(false));
        return this;
    }

    JsonWriter endArray() {
        requireContext(false);
        contexts.pop();
        append(']');
        return this;
    }

    JsonWriter name(String name) {
        Context context = requireContext(true);
        if (context.awaitingValue) {
            throw new IllegalStateException("JSON object name has no value");
        }
        if (!context.first) {
            append(',');
        }
        context.first = false;
        appendString(name);
        append(':');
        context.awaitingValue = true;
        return this;
    }

    JsonWriter value(String value) {
        if (value == null) {
            return nullValue();
        }
        beforeValue();
        appendString(value);
        return this;
    }

    JsonWriter value(long value) {
        beforeValue();
        append(Long.toString(value));
        return this;
    }

    JsonWriter value(boolean value) {
        beforeValue();
        append(value ? "true" : "false");
        return this;
    }

    JsonWriter nullValue() {
        beforeValue();
        append("null");
        return this;
    }

    JsonWriter hexValue(ByteBuffer bytes, boolean reverse, boolean prefix) {
        beforeValue();
        int first = bytes.position();
        int length = bytes.remaining();
        long characters = 2L + length * 2L + (prefix ? 2L : 0L);
        requireCharacters(characters);
        output.append('"');
        if (prefix) {
            output.append("0x");
        }
        if (reverse) {
            for (int index = first + length - 1; index >= first; index--) {
                appendHexByteUnchecked(bytes.get(index));
            }
        } else {
            for (int index = first; index < first + length; index++) {
                appendHexByteUnchecked(bytes.get(index));
            }
        }
        output.append('"');
        return this;
    }

    String finish() {
        if (!rootWritten || !contexts.isEmpty()) {
            throw new IllegalStateException("JSON document is incomplete");
        }
        return output.toString();
    }

    private Context requireContext(boolean object) {
        Context context = contexts.peek();
        if (context == null || context.object != object) {
            throw new IllegalStateException("JSON container mismatch");
        }
        return context;
    }

    private void beforeValue() {
        Context context = contexts.peek();
        if (context == null) {
            if (rootWritten) {
                throw new IllegalStateException("JSON document has multiple roots");
            }
            rootWritten = true;
            return;
        }
        if (context.object) {
            if (!context.awaitingValue) {
                throw new IllegalStateException("JSON object value has no name");
            }
            context.awaitingValue = false;
            return;
        }
        if (!context.first) {
            append(',');
        }
        context.first = false;
    }

    private void appendString(String value) {
        append('"');
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            switch (character) {
                case '"':
                    append("\\\"");
                    break;
                case '\\':
                    append("\\\\");
                    break;
                case '\b':
                    append("\\b");
                    break;
                case '\f':
                    append("\\f");
                    break;
                case '\n':
                    append("\\n");
                    break;
                case '\r':
                    append("\\r");
                    break;
                case '\t':
                    append("\\t");
                    break;
                default:
                    if (character < 0x20 || character > 0x7e) {
                        append("\\u");
                        appendHexDigit(character >>> 12);
                        appendHexDigit(character >>> 8);
                        appendHexDigit(character >>> 4);
                        appendHexDigit(character);
                    } else {
                        append(character);
                    }
                    break;
            }
        }
        append('"');
    }

    private void appendHexDigit(int value) {
        append("0123456789abcdef".charAt(value & 0x0f));
    }

    private void appendHexByteUnchecked(byte value) {
        int unsigned = value & 0xff;
        output.append("0123456789abcdef".charAt(unsigned >>> 4));
        output.append("0123456789abcdef".charAt(unsigned & 0x0f));
    }

    private void append(char value) {
        requireCharacters(1);
        output.append(value);
    }

    private void append(String value) {
        requireCharacters(value.length());
        output.append(value);
    }

    private void requireCharacters(long additional) {
        if (additional > maximumCharacters - output.length()) {
            throw new LimitException();
        }
    }
}
