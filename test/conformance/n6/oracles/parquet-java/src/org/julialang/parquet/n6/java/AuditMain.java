package org.julialang.parquet.n6.java;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.parquet.CorruptStatistics;
import org.apache.parquet.VersionParser;
import org.apache.parquet.VersionParser.ParsedVersion;
import org.apache.parquet.VersionParser.VersionParseException;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

public final class AuditMain {
  private static final String PRODUCER = "parquet-java";
  private static final String PRODUCER_VERSION = "1.17.1";
  private static final String SOURCE_REVISION = "78a8d3230eb4769db93de5f2f2e18363c04cae81";
  private static final int MAX_COLUMNS = 1024;
  private static final int MAX_ROWS = 1_000_000;
  private static final int MAX_BINARY_BYTES = 16 * 1024 * 1024;
  private static final ObjectMapper JSON = new ObjectMapper();

  private AuditMain() {}

  public static void main(String[] arguments) throws Exception {
    if (arguments.length == 2 && "audit".equals(arguments[0])) {
      JSON.writeValue(System.out, audit(checkedInput(arguments[1])));
      System.out.write('\n');
      return;
    }
    if (arguments.length == 1 && "policy".equals(arguments[0])) {
      JSON.writeValue(System.out, policy());
      System.out.write('\n');
      return;
    }
    throw new IllegalArgumentException("usage: AuditMain audit FILE | policy");
  }

  private static Path checkedInput(String value) throws IOException {
    Path path = Paths.get(value).toAbsolutePath().normalize();
    if (Files.isSymbolicLink(path)
        || !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
      throw new IOException("audit input is not a regular non-link file");
    }
    return path;
  }

  private static Map<String, Object> oracle() {
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("name", PRODUCER);
    result.put("version", PRODUCER_VERSION);
    result.put("commit", SOURCE_REVISION);
    return result;
  }

  private static Map<String, Object> audit(Path path) throws IOException {
    InputFile input = new LocalInputFile(path);
    ParquetMetadata footer;
    try (ParquetFileReader reader = ParquetFileReader.open(input)) {
      footer = reader.getFooter();
    }
    MessageType schema = footer.getFileMetaData().getSchema();
    List<ColumnDescriptor> columns = schema.getColumns();
    if (columns.size() > MAX_COLUMNS) {
      throw new IOException("column count exceeds the audit limit");
    }
    requireFlat(columns);
    List<List<Map<String, Object>>> values = emptyColumns(columns.size());
    long rowCount = readRows(path, columns, values);
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("oracle", oracle());
    result.put("action", "audit");
    result.put("created_by", footer.getFileMetaData().getCreatedBy());
    result.put("schema", schemaRecords(columns));
    result.put("row_count", rowCount);
    result.put("columns", values);
    result.put("row_groups", statisticsRecords(footer));
    return result;
  }

  private static void requireFlat(List<ColumnDescriptor> columns) throws IOException {
    for (ColumnDescriptor column : columns) {
      if (column.getPath().length != 1 || column.getMaxRepetitionLevel() != 0) {
        throw new IOException("the reviewed logical-value fixture is not flat");
      }
    }
  }

  private static List<List<Map<String, Object>>> emptyColumns(int count) {
    List<List<Map<String, Object>>> result = new ArrayList<>(count);
    for (int index = 0; index < count; index += 1) {
      result.add(new ArrayList<>());
    }
    return result;
  }

  private static long readRows(
      Path path,
      List<ColumnDescriptor> columns,
      List<List<Map<String, Object>>> values) throws IOException {
    long rowCount = 0;
    try (ParquetReader<Group> reader =
        new LocalGroupReaderBuilder(new LocalInputFile(path)).build()) {
      Group row;
      while ((row = reader.read()) != null) {
        if (rowCount >= MAX_ROWS) {
          throw new IOException("row count exceeds the audit limit");
        }
        for (int index = 0; index < columns.size(); index += 1) {
          int count = row.getFieldRepetitionCount(index);
          if (count > 1) {
            throw new IOException("the reviewed logical-value fixture has repeated values");
          }
          values.get(index).add(count == 0 ? null : encodedValue(row, index, columns.get(index)));
        }
        rowCount += 1;
      }
    }
    return rowCount;
  }

  private static Map<String, Object> encodedValue(
      Group row, int index, ColumnDescriptor column) throws IOException {
    PrimitiveTypeName type = column.getPrimitiveType().getPrimitiveTypeName();
    switch (type) {
      case BOOLEAN:
        return tagged("boolean", row.getBoolean(index, 0));
      case INT32:
        return tagged("int32", Integer.toString(row.getInteger(index, 0)));
      case INT64:
        return tagged("int64", Long.toString(row.getLong(index, 0)));
      case FLOAT:
        return tagged(
            "float32_bits",
            String.format("%08x", Float.floatToRawIntBits(row.getFloat(index, 0))));
      case DOUBLE:
        return tagged(
            "float64_bits",
            String.format("%016x", Double.doubleToRawLongBits(row.getDouble(index, 0))));
      case BINARY:
        return taggedBinary("binary", row.getBinary(index, 0));
      case FIXED_LEN_BYTE_ARRAY:
        return taggedBinary("fixed", row.getBinary(index, 0));
      case INT96:
        return taggedBinary("int96", row.getInt96(index, 0));
      default:
        throw new IOException("unsupported physical type: " + type);
    }
  }

  private static Map<String, Object> tagged(String kind, Object value) {
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("kind", kind);
    result.put("value", value);
    return result;
  }

  private static Map<String, Object> taggedBinary(String kind, Binary value) throws IOException {
    byte[] bytes = value.getBytes();
    if (bytes.length > MAX_BINARY_BYTES) {
      throw new IOException("binary value exceeds the audit limit");
    }
    return tagged(kind, hex(bytes));
  }

  private static List<Map<String, Object>> schemaRecords(List<ColumnDescriptor> columns) {
    List<Map<String, Object>> result = new ArrayList<>(columns.size());
    for (ColumnDescriptor column : columns) {
      Map<String, Object> record = new LinkedHashMap<>();
      record.put("name", column.getPath()[0]);
      record.put("physical_type", column.getPrimitiveType().getPrimitiveTypeName().name());
      record.put("maximum_definition_level", column.getMaxDefinitionLevel());
      record.put("maximum_repetition_level", column.getMaxRepetitionLevel());
      record.put(
          "column_order",
          column.getPrimitiveType().columnOrder().getColumnOrderName().name());
      result.add(record);
    }
    return result;
  }

  private static List<Map<String, Object>> statisticsRecords(ParquetMetadata footer) {
    List<Map<String, Object>> groups = new ArrayList<>();
    int ordinal = 0;
    for (BlockMetaData block : footer.getBlocks()) {
      Map<String, Object> group = new LinkedHashMap<>();
      group.put("row_group", ordinal);
      group.put("row_count", block.getRowCount());
      List<Map<String, Object>> columns = new ArrayList<>();
      for (ColumnChunkMetaData column : block.getColumns()) {
        Statistics<?> statistics = column.getStatistics();
        Map<String, Object> record = new LinkedHashMap<>();
        record.put("path", Arrays.asList(column.getPath().toArray()));
        record.put("physical_type", column.getType().name());
        record.put("statistics_class", statistics.getClass().getName());
        record.put("empty", statistics.isEmpty());
        record.put("has_non_null_value", statistics.hasNonNullValue());
        record.put("num_nulls_set", statistics.isNumNullsSet());
        record.put("num_nulls", statistics.isNumNullsSet() ? statistics.getNumNulls() : null);
        record.put("min_hex", statistics.hasNonNullValue() ? hex(statistics.getMinBytes()) : null);
        record.put("max_hex", statistics.hasNonNullValue() ? hex(statistics.getMaxBytes()) : null);
        record.put(
            "should_ignore_statistics",
            CorruptStatistics.shouldIgnoreStatistics(
                footer.getFileMetaData().getCreatedBy(), column.getType()));
        columns.add(record);
      }
      group.put("columns", columns);
      groups.add(group);
      ordinal += 1;
    }
    return groups;
  }

  private static Map<String, Object> policy() {
    List<Map<String, Object>> observations = new ArrayList<>();
    observations.add(policyCase("null-binary", null, PrimitiveTypeName.BINARY, "not_applicable"));
    observations.add(policyCase("empty-binary", "", PrimitiveTypeName.BINARY, "not_applicable"));
    observations.add(policyCase(
        "empty-application-binary", " version 1.0.0", PrimitiveTypeName.BINARY, "not_applicable"));
    observations.add(policyCase(
        "unparsable-binary", "garbage!", PrimitiveTypeName.BINARY, "not_applicable"));
    observations.add(policyCase(
        "unrelated-binary", "impala version 1.0.0", PrimitiveTypeName.BINARY, "not_applicable"));
    observations.add(policyCase(
        "missing-semver-binary", "parquet-mr version", PrimitiveTypeName.BINARY, "not_applicable"));
    observations.add(policyCase(
        "before-fix-distinct-binary", "parquet-mr version 1.7.9", PrimitiveTypeName.BINARY, "distinct"));
    observations.add(policyCase(
        "before-fix-equal-binary", "parquet-mr version 1.7.9", PrimitiveTypeName.BINARY, "equal"));
    observations.add(policyCase(
        "release-candidate-binary", "parquet-mr version 1.8.0-rc1", PrimitiveTypeName.BINARY, "not_applicable"));
    observations.add(policyCase(
        "fixed-binary", "parquet-mr version 1.8.0", PrimitiveTypeName.BINARY, "not_applicable"));
    observations.add(policyCase(
        "cdh-before-binary", "parquet-mr version 1.5.0-cdh5.4.9", PrimitiveTypeName.BINARY, "not_applicable"));
    observations.add(policyCase(
        "cdh-start-binary", "parquet-mr version 1.5.0-cdh5.5.0", PrimitiveTypeName.BINARY, "not_applicable"));
    observations.add(policyCase(
        "cdh-end-binary", "parquet-mr version 1.5.0", PrimitiveTypeName.BINARY, "not_applicable"));
    observations.add(policyCase(
        "before-fix-fixed", "parquet-mr version 1.7.9", PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
        "not_applicable"));
    observations.add(policyCase(
        "fixed-fixed", "parquet-mr version 1.8.0", PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
        "not_applicable"));
    observations.add(policyCase(
        "before-fix-int32", "parquet-mr version 1.7.9", PrimitiveTypeName.INT32, "not_applicable"));
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("oracle", oracle());
    result.put("action", "policy");
    result.put("observations", observations);
    return result;
  }

  private static Map<String, Object> policyCase(
      String scenarioId,
      String createdBy,
      PrimitiveTypeName physicalType,
      String boundRelation) {
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("scenario_id", scenarioId);
    result.put("created_by", createdBy);
    result.put("physical_type", physicalType.name());
    result.put("bound_relation", boundRelation);
    result.put("version_parse", versionParse(createdBy));
    result.put("should_ignore_statistics", CorruptStatistics.shouldIgnoreStatistics(createdBy, physicalType));
    return result;
  }

  private static Map<String, Object> versionParse(String createdBy) {
    Map<String, Object> result = new LinkedHashMap<>();
    try {
      ParsedVersion parsed = VersionParser.parse(createdBy);
      result.put("status", "parsed");
      result.put("application", parsed.application);
      result.put("version", parsed.version);
      result.put("build", parsed.appBuildHash);
    } catch (RuntimeException | VersionParseException error) {
      result.put("status", "error");
    }
    return result;
  }

  private static String hex(byte[] bytes) {
    char[] digits = "0123456789abcdef".toCharArray();
    char[] result = new char[bytes.length * 2];
    for (int index = 0; index < bytes.length; index += 1) {
      int value = bytes[index] & 0xff;
      result[index * 2] = digits[value >>> 4];
      result[index * 2 + 1] = digits[value & 0x0f];
    }
    return new String(result);
  }

  private static final class LocalGroupReaderBuilder extends ParquetReader.Builder<Group> {
    private LocalGroupReaderBuilder(InputFile input) {
      super(input);
    }

    @Override
    protected ReadSupport<Group> getReadSupport() {
      return new GroupReadSupport();
    }
  }
}
