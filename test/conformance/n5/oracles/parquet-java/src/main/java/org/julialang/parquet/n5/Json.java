package org.julialang.parquet.n5;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class Json {
  private Json() {}

  static Map<String, Object> object(Object... pairs) {
    if ((pairs.length & 1) != 0) {
      throw new IllegalArgumentException("JSON object needs key/value pairs");
    }
    Map<String, Object> result = new LinkedHashMap<>();
    for (int index = 0; index < pairs.length; index += 2) {
      result.put((String) pairs[index], pairs[index + 1]);
    }
    return result;
  }

  static String encode(Object value) {
    StringBuilder output = new StringBuilder();
    append(output, value);
    return output.toString();
  }

  static void writeLines(Path output, List<Map<String, Object>> records) throws IOException {
    Path target = output.toAbsolutePath().normalize();
    Path parent = target.getParent();
    if (parent == null) {
      throw new IllegalArgumentException("evidence path has no parent: " + output);
    }
    Files.createDirectories(parent);
    Path temporary = Files.createTempFile(parent, ".n5-java-evidence-", ".tmp");
    boolean committed = false;
    try {
      StringBuilder lines = new StringBuilder();
      for (Map<String, Object> record : records) {
        lines.append(encode(record)).append('\n');
      }
      Files.writeString(temporary, lines, StandardCharsets.UTF_8);
      moveReplace(temporary, target);
      committed = true;
    } finally {
      if (!committed) {
        Files.deleteIfExists(temporary);
      }
    }
  }

  static void moveReplace(Path source, Path target) throws IOException {
    try {
      Files.move(source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    } catch (AtomicMoveNotSupportedException error) {
      Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
    }
  }

  private static void append(StringBuilder output, Object value) {
    if (value == null) {
      output.append("null");
    } else if (value instanceof String) {
      appendString(output, (String) value);
    } else if (value instanceof Boolean || value instanceof Byte || value instanceof Short
        || value instanceof Integer || value instanceof Long) {
      output.append(value);
    } else if (value instanceof Float) {
      appendFinite(output, ((Float) value).doubleValue());
    } else if (value instanceof Double) {
      appendFinite(output, (Double) value);
    } else if (value instanceof Map<?, ?>) {
      appendMap(output, (Map<?, ?>) value);
    } else if (value instanceof Collection<?>) {
      appendCollection(output, (Collection<?>) value);
    } else if (value.getClass().isArray()) {
      throw new IllegalArgumentException("convert arrays to ordered collections before JSON encoding");
    } else {
      throw new IllegalArgumentException("unsupported JSON value: " + value.getClass().getName());
    }
  }

  private static void appendFinite(StringBuilder output, double value) {
    if (!Double.isFinite(value)) {
      appendString(output, Double.toString(value));
      return;
    }
    output.append(Double.toString(value));
  }

  private static void appendMap(StringBuilder output, Map<?, ?> value) {
    output.append('{');
    boolean first = true;
    for (Map.Entry<?, ?> entry : value.entrySet()) {
      if (!(entry.getKey() instanceof String)) {
        throw new IllegalArgumentException("JSON object key is not a string");
      }
      if (!first) {
        output.append(',');
      }
      first = false;
      appendString(output, (String) entry.getKey());
      output.append(':');
      append(output, entry.getValue());
    }
    output.append('}');
  }

  private static void appendCollection(StringBuilder output, Collection<?> value) {
    output.append('[');
    boolean first = true;
    for (Object element : value) {
      if (!first) {
        output.append(',');
      }
      first = false;
      append(output, element);
    }
    output.append(']');
  }

  private static void appendString(StringBuilder output, String value) {
    output.append('"');
    for (int index = 0; index < value.length(); index++) {
      char character = value.charAt(index);
      switch (character) {
        case '"':
          output.append("\\\"");
          break;
        case '\\':
          output.append("\\\\");
          break;
        case '\b':
          output.append("\\b");
          break;
        case '\f':
          output.append("\\f");
          break;
        case '\n':
          output.append("\\n");
          break;
        case '\r':
          output.append("\\r");
          break;
        case '\t':
          output.append("\\t");
          break;
        default:
          if (character < 0x20 || Character.isSurrogate(character)) {
            output.append(String.format("\\u%04x", (int) character));
          } else {
            output.append(character);
          }
      }
    }
    output.append('"');
  }
}
