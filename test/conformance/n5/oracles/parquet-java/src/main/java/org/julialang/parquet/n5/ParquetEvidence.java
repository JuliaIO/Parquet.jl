package org.julialang.parquet.n5;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReadStore;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.DummyRecordConverter;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

final class ParquetEvidence {
  private static final String AVRO_READ_SCHEMA_PROPERTY = "parquet.avro.read.schema";

  private static final class HarnessFailure extends RuntimeException {
    HarnessFailure(String message, Throwable cause) {
      super(message, cause);
    }
  }

  static final class ColumnEvidence {
    final ColumnDescriptor descriptor;
    final List<Integer> repetition = new ArrayList<>();
    final List<Integer> definition = new ArrayList<>();
    final List<String> dense = new ArrayList<>();
    final List<Map<String, Object>> pages = new ArrayList<>();
    final List<Map<String, Object>> dictionaries = new ArrayList<>();
    final List<ColumnRowGroupEvidence> rowGroups = new ArrayList<>();

    ColumnEvidence(ColumnDescriptor descriptor) {
      this.descriptor = descriptor;
    }

    String path() {
      return String.join(".", descriptor.getPath());
    }

    Map<String, Object> toMap() {
      PrimitiveType primitive = descriptor.getPrimitiveType();
      return Json.object(
          "path", path(),
          "physical_type", primitive.getPrimitiveTypeName().name(),
          "logical_type", primitive.getLogicalTypeAnnotation() == null
              ? null : primitive.getLogicalTypeAnnotation().toString(),
          "type_length", primitive.getTypeLength(),
          "max_repetition_level", descriptor.getMaxRepetitionLevel(),
          "max_definition_level", descriptor.getMaxDefinitionLevel(),
          "repetition", repetition,
          "definition", definition,
          "dense", dense,
          "dictionaries", dictionaries,
          "pages", pages,
          "row_groups", rowGroups.stream()
              .map(ColumnRowGroupEvidence::toMap)
              .collect(java.util.stream.Collectors.toList()));
    }
  }

  static final class ColumnRowGroupEvidence {
    final int rowGroup;
    final long rows;
    final List<Integer> repetition = new ArrayList<>();
    final List<Integer> definition = new ArrayList<>();
    final List<String> dense = new ArrayList<>();
    final List<Map<String, Object>> pages = new ArrayList<>();
    final List<Map<String, Object>> dictionaries = new ArrayList<>();

    ColumnRowGroupEvidence(int rowGroup, long rows) {
      this.rowGroup = rowGroup;
      this.rows = rows;
    }

    Map<String, Object> toMap() {
      return Json.object(
          "row_group", rowGroup,
          "rows", rows,
          "repetition", repetition,
          "definition", definition,
          "dense", dense,
          "dictionaries", dictionaries,
          "pages", pages);
    }
  }

  static final class AvroEvidence {
    final String status;
    final String readSchema;
    final String materializedSchema;
    final List<String> rows;
    final List<String> normalizedRows;
    final String errorClass;
    final String errorMessage;
    final List<String> errorStack;
    final List<Map<String, Object>> exceptionChain;

    AvroEvidence(
        String status,
        String readSchema,
        String materializedSchema,
        List<String> rows,
        List<String> normalizedRows,
        String errorClass,
        String errorMessage,
        List<String> errorStack,
        List<Map<String, Object>> exceptionChain) {
      this.status = status;
      this.readSchema = readSchema;
      this.materializedSchema = materializedSchema;
      this.rows = rows;
      this.normalizedRows = normalizedRows;
      this.errorClass = errorClass;
      this.errorMessage = errorMessage;
      this.errorStack = errorStack;
      this.exceptionChain = exceptionChain;
    }

    Map<String, Object> toMap() {
      return Json.object(
          "status", status,
          "read_schema", readSchema,
          "materialized_schema", materializedSchema,
          "rows", rows,
          "normalized_rows", normalizedRows,
          "error_class", errorClass,
          "error_message", errorMessage,
          "error_stack", errorStack,
          "exception_chain", exceptionChain);
    }
  }

  static final class FileEvidence {
    final Path source;
    final String file;
    final String sha256;
    final MessageType schema;
    final String createdBy;
    final Map<String, String> metadata;
    final long rowCount;
    final List<Map<String, Object>> rowGroups;
    final List<String> rawRows;
    final List<ColumnEvidence> columns;
    final AvroEvidence avro;
    final AvroEvidence explicitAvro;

    FileEvidence(
        Path source,
        String file,
        String sha256,
        MessageType schema,
        String createdBy,
        Map<String, String> metadata,
        long rowCount,
        List<Map<String, Object>> rowGroups,
        List<String> rawRows,
        List<ColumnEvidence> columns,
        AvroEvidence avro,
        AvroEvidence explicitAvro) {
      this.source = source;
      this.file = file;
      this.sha256 = sha256;
      this.schema = schema;
      this.createdBy = createdBy;
      this.metadata = metadata;
      this.rowCount = rowCount;
      this.rowGroups = rowGroups;
      this.rawRows = rawRows;
      this.columns = columns;
      this.avro = avro;
      this.explicitAvro = explicitAvro;
    }

    String caseId() {
      return metadata.get(OracleMain.CASE_METADATA_KEY);
    }

    String pageVersion() {
      return metadata.get(OracleMain.PAGE_VERSION_METADATA_KEY);
    }

    ColumnEvidence column(String path) {
      for (ColumnEvidence column : columns) {
        if (column.path().equals(path)) {
          return column;
        }
      }
      return null;
    }

    Map<String, Object> toMap() {
      List<Map<String, Object>> flattened = new ArrayList<>();
      for (ColumnEvidence column : columns) {
        flattened.add(column.toMap());
      }
      return Json.object(
          "record", "file",
          "file", file,
          "sha256", sha256,
          "case_id", caseId(),
          "page_version", pageVersion(),
          "created_by", createdBy,
          "row_count", rowCount,
          "metadata", metadata,
          "physical_schema", schema.toString(),
          "row_groups", rowGroups,
          "raw_group_rows", rawRows,
          "columns", flattened,
          "avro", Json.object(
              "add_list_element_records", false,
              "inferred", avro.toMap(),
              "explicit", explicitAvro == null ? null : explicitAvro.toMap()));
    }
  }

  private ParquetEvidence() {}

  static FileEvidence inspect(
      Path source,
      String file,
      FixtureCases.AvroNormalizer normalizer,
      String explicitAvroSchema) throws IOException {
    Path absolute = source.toAbsolutePath().normalize();
    ParquetMetadata footer;
    try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(absolute))) {
      footer = reader.getFooter();
    }
    MessageType schema = footer.getFileMetaData().getSchema();
    String createdBy = footer.getFileMetaData().getCreatedBy();
    Map<String, String> metadata = new TreeMap<>(footer.getFileMetaData().getKeyValueMetaData());
    List<Map<String, Object>> rowGroups = rowGroupEvidence(footer.getBlocks());
    long rowCount = 0;
    for (BlockMetaData block : footer.getBlocks()) {
      rowCount = Math.addExact(rowCount, block.getRowCount());
    }
    List<String> rawRows = readRawRows(absolute, schema);
    if (rowCount != rawRows.size()) {
      throw new IllegalStateException(
          "raw Group row count " + rawRows.size() + " differs from footer " + rowCount);
    }
    List<ColumnEvidence> columns = readColumns(absolute, footer, schema, createdBy);
    readPages(absolute, schema, columns);
    AvroEvidence avro = readAvro(absolute, normalizer, null);
    AvroEvidence explicitAvro = explicitAvroSchema == null
        ? null : readAvro(absolute, row -> row, explicitAvroSchema);
    return new FileEvidence(
        absolute,
        file,
        sha256(absolute),
        schema,
        createdBy,
        metadata,
        rowCount,
        rowGroups,
        rawRows,
        columns,
        avro,
        explicitAvro);
  }

  private static List<Map<String, Object>> rowGroupEvidence(List<BlockMetaData> blocks) {
    List<Map<String, Object>> result = new ArrayList<>();
    for (int blockIndex = 0; blockIndex < blocks.size(); blockIndex++) {
      BlockMetaData block = blocks.get(blockIndex);
      List<Map<String, Object>> chunks = new ArrayList<>();
      for (ColumnChunkMetaData chunk : block.getColumns()) {
        chunks.add(Json.object(
            "path", chunk.getPath().toDotString(),
            "value_count", chunk.getValueCount(),
            "codec", chunk.getCodec().name(),
            "encodings", sortedStrings(chunk.getEncodings()),
            "total_compressed_size", chunk.getTotalSize(),
            "total_uncompressed_size", chunk.getTotalUncompressedSize()));
      }
      result.add(Json.object(
          "ordinal", blockIndex,
          "row_count", block.getRowCount(),
          "total_byte_size", block.getTotalByteSize(),
          "columns", chunks));
    }
    return result;
  }

  private static List<String> sortedStrings(Collection<?> values) {
    List<String> result = new ArrayList<>();
    for (Object value : values) {
      result.add(value.toString());
    }
    CollectionsSupport.sort(result);
    return result;
  }

  private static List<String> readRawRows(Path source, MessageType schema) throws IOException {
    List<String> result = new ArrayList<>();
    MessageColumnIO columnIo = new ColumnIOFactory().getColumnIO(schema);
    try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(source))) {
      PageReadStore pages;
      while ((pages = reader.readNextRowGroup()) != null) {
        RecordReader<Group> records =
            columnIo.getRecordReader(pages, new GroupRecordConverter(schema));
        for (long index = 0; index < pages.getRowCount(); index++) {
          result.add(canonicalGroup(records.read(), schema));
        }
      }
    }
    return result;
  }

  private static List<ColumnEvidence> readColumns(
      Path source, ParquetMetadata footer, MessageType schema, String createdBy) throws IOException {
    List<ColumnEvidence> result = new ArrayList<>();
    for (ColumnDescriptor descriptor : schema.getColumns()) {
      result.add(new ColumnEvidence(descriptor));
    }
    try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(source))) {
      PageReadStore pages;
      int rowGroup = 0;
      while ((pages = reader.readNextRowGroup()) != null) {
        ColumnReadStore store = new ColumnReadStoreImpl(
            pages, new DummyRecordConverter(schema).getRootConverter(), schema, createdBy);
        BlockMetaData block = footer.getBlocks().get(rowGroup);
        if (block.getColumns().size() != result.size()) {
          throw new IllegalStateException("row-group physical column count changed");
        }
        for (int columnIndex = 0; columnIndex < result.size(); columnIndex++) {
          ColumnEvidence evidence = result.get(columnIndex);
          ColumnRowGroupEvidence group =
              new ColumnRowGroupEvidence(rowGroup, block.getRowCount());
          ColumnReader column = store.getColumnReader(evidence.descriptor);
          long valueCount = block.getColumns().get(columnIndex).getValueCount();
          for (long valueIndex = 0; valueIndex < valueCount; valueIndex++) {
            int repetition = column.getCurrentRepetitionLevel();
            int definition = column.getCurrentDefinitionLevel();
            evidence.repetition.add(repetition);
            evidence.definition.add(definition);
            group.repetition.add(repetition);
            group.definition.add(definition);
            if (definition == evidence.descriptor.getMaxDefinitionLevel()) {
              String dense = canonicalColumnValue(column, evidence.descriptor.getPrimitiveType());
              evidence.dense.add(dense);
              group.dense.add(dense);
            }
            column.consume();
          }
          evidence.rowGroups.add(group);
        }
        rowGroup++;
      }
      if (rowGroup != footer.getBlocks().size()) {
        throw new IllegalStateException("row-group count changed while reading columns");
      }
    }
    return result;
  }

  private static void readPages(
      Path source, MessageType schema, List<ColumnEvidence> columns) throws IOException {
    try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(source))) {
      PageReadStore pages;
      int rowGroup = 0;
      while ((pages = reader.readNextRowGroup()) != null) {
        for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
          ColumnEvidence evidence = columns.get(columnIndex);
          if (rowGroup >= evidence.rowGroups.size()) {
            throw new IllegalStateException("page reader has more row groups than column evidence");
          }
          ColumnRowGroupEvidence group = evidence.rowGroups.get(rowGroup);
          PageReader pageReader = pages.getPageReader(evidence.descriptor);
          DictionaryPage dictionary = pageReader.readDictionaryPage();
          if (dictionary != null) {
            Map<String, Object> dictionaryEvidence = Json.object(
                "row_group", rowGroup,
                "value_count", dictionary.getDictionarySize(),
                "encoding", dictionary.getEncoding().name(),
                "compressed_size", dictionary.getCompressedSize(),
                "uncompressed_size", dictionary.getUncompressedSize());
            evidence.dictionaries.add(dictionaryEvidence);
            group.dictionaries.add(dictionaryEvidence);
          }
          int ordinal = 0;
          int valueOffset = 0;
          DataPage page;
          while ((page = pageReader.readPage()) != null) {
            int valueEnd = Math.addExact(valueOffset, page.getValueCount());
            if (valueEnd > group.repetition.size()) {
              throw new IllegalStateException("page value count exceeds column stream");
            }
            int derivedRowCount = 0;
            for (int valueIndex = valueOffset; valueIndex < valueEnd; valueIndex++) {
              if (group.repetition.get(valueIndex) == 0) {
                derivedRowCount++;
              }
            }
            Map<String, Object> pageEvidence =
                pageEvidence(page, rowGroup, ordinal, derivedRowCount);
            evidence.pages.add(pageEvidence);
            group.pages.add(pageEvidence);
            valueOffset = valueEnd;
            ordinal++;
          }
          if (valueOffset != group.repetition.size()) {
            throw new IllegalStateException("page value count differs from row-group column stream");
          }
        }
        rowGroup++;
      }
    }
    for (ColumnEvidence column : columns) {
      int repetitionCount = 0;
      for (ColumnRowGroupEvidence group : column.rowGroups) {
        repetitionCount = Math.addExact(repetitionCount, group.repetition.size());
      }
      if (repetitionCount != column.repetition.size()) {
        throw new IllegalStateException("row-group streams do not span the column stream");
      }
    }
  }

  private static Map<String, Object> pageEvidence(
      DataPage page, int rowGroup, int ordinal, int derivedRowCount) {
    if (page instanceof DataPageV1) {
      DataPageV1 v1 = (DataPageV1) page;
      Integer indexRowCount = v1.getIndexRowCount().orElse(null);
      if (indexRowCount != null && indexRowCount != derivedRowCount) {
        throw new IllegalStateException("V1 page row count differs from repetition stream");
      }
      return Json.object(
          "row_group", rowGroup,
          "ordinal", ordinal,
          "type", "DATA_PAGE_V1",
          "value_count", v1.getValueCount(),
          "row_count", derivedRowCount,
          "index_row_count", indexRowCount,
          "null_count", null,
          "encoding", v1.getValueEncoding().name(),
          "compressed_size", v1.getCompressedSize(),
          "uncompressed_size", v1.getUncompressedSize());
    }
    DataPageV2 v2 = (DataPageV2) page;
    if (v2.getRowCount() != derivedRowCount) {
      throw new IllegalStateException("V2 page row count differs from repetition stream");
    }
    return Json.object(
        "row_group", rowGroup,
        "ordinal", ordinal,
        "type", "DATA_PAGE_V2",
        "value_count", v2.getValueCount(),
        "row_count", v2.getRowCount(),
        "index_row_count", null,
        "null_count", v2.getNullCount(),
        "encoding", v2.getDataEncoding().name(),
        "compressed_size", v2.getCompressedSize(),
        "uncompressed_size", v2.getUncompressedSize());
  }

  private static AvroEvidence readAvro(
      Path source, FixtureCases.AvroNormalizer normalizer, String readSchema) {
    PlainParquetConfiguration configuration = new PlainParquetConfiguration();
    configuration.setBoolean(AvroSchemaConverter.ADD_LIST_ELEMENT_RECORDS, false);
    if (readSchema != null) {
      configuration.set(AVRO_READ_SCHEMA_PROPERTY, readSchema);
    }
    List<String> rows = new ArrayList<>();
    List<String> normalized = new ArrayList<>();
    String materializedSchema = null;
    try (ParquetReader<GenericRecord> reader =
        AvroParquetReader.genericRecordReader(new LocalInputFile(source), configuration)) {
      GenericRecord row;
      while ((row = reader.read()) != null) {
        if (materializedSchema == null) {
          materializedSchema = row.getSchema().toString(false);
        }
        try {
          rows.add(Json.encode(canonicalAvro(row)));
          normalized.add(Json.encode(canonicalAvro(normalizer.normalize(row))));
        } catch (RuntimeException error) {
          throw new HarnessFailure("Avro evidence normalization failed", error);
        }
      }
      return new AvroEvidence(
          "success",
          readSchema,
          materializedSchema,
          rows,
          normalized,
          null,
          null,
          List.of(),
          List.of());
    } catch (HarnessFailure error) {
      throw error;
    } catch (IOException | RuntimeException error) {
      Throwable cause = rootCause(error);
      return new AvroEvidence(
          "rejected",
          readSchema,
          materializedSchema,
          rows,
          normalized,
          cause.getClass().getName(),
          sanitize(cause.getMessage(), source),
          stackEvidence(cause, source),
          exceptionChain(error, source));
    }
  }

  private static List<String> stackEvidence(Throwable error, Path source) {
    List<String> result = new ArrayList<>();
    for (StackTraceElement frame : error.getStackTrace()) {
      result.add(sanitize(frame.toString(), source));
    }
    return result;
  }

  private static List<Map<String, Object>> exceptionChain(Throwable error, Path source) {
    List<Map<String, Object>> result = new ArrayList<>();
    Throwable current = error;
    while (current != null) {
      result.add(Json.object(
          "class", current.getClass().getName(),
          "message", sanitize(current.getMessage(), source)));
      Throwable next = current.getCause();
      current = next == current ? null : next;
    }
    return result;
  }

  private static String sanitize(String value, Path source) {
    if (value == null) {
      return null;
    }
    return value.replace(source.toAbsolutePath().toString(), "<file>")
        .replace('\n', ' ').replace('\r', ' ');
  }

  static String canonicalGroup(Group group, GroupType type) {
    StringBuilder result = new StringBuilder("G{");
    for (int fieldIndex = 0; fieldIndex < type.getFieldCount(); fieldIndex++) {
      if (fieldIndex > 0) {
        result.append(',');
      }
      Type field = type.getType(fieldIndex);
      result.append(field.getName()).append('=');
      int count = group.getFieldRepetitionCount(fieldIndex);
      if (field.isRepetition(Type.Repetition.REPEATED)) {
        result.append('[');
        for (int valueIndex = 0; valueIndex < count; valueIndex++) {
          if (valueIndex > 0) {
            result.append(',');
          }
          appendGroupValue(result, group, field, fieldIndex, valueIndex);
        }
        result.append(']');
      } else if (count == 0) {
        result.append("null");
      } else {
        appendGroupValue(result, group, field, fieldIndex, 0);
      }
    }
    return result.append('}').toString();
  }

  static Object canonicalAvro(Object value) {
    if (value == null || value instanceof Boolean || value instanceof Number || value instanceof String) {
      return value;
    }
    if (value instanceof Utf8 || value instanceof GenericEnumSymbol<?>) {
      return value.toString();
    }
    if (value instanceof GenericRecord) {
      GenericRecord record = (GenericRecord) value;
      Map<String, Object> result = new LinkedHashMap<>();
      for (org.apache.avro.Schema.Field field : record.getSchema().getFields()) {
        result.put(field.name(), canonicalAvro(record.get(field.name())));
      }
      return result;
    }
    if (value instanceof Map<?, ?>) {
      Map<String, Object> result = new TreeMap<>();
      for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
        result.put(entry.getKey().toString(), canonicalAvro(entry.getValue()));
      }
      return result;
    }
    if (value instanceof Iterable<?>) {
      List<Object> result = new ArrayList<>();
      for (Object element : (Iterable<?>) value) {
        result.add(canonicalAvro(element));
      }
      return result;
    }
    if (value instanceof ByteBuffer) {
      ByteBuffer bytes = ((ByteBuffer) value).duplicate();
      byte[] output = new byte[bytes.remaining()];
      bytes.get(output);
      return "base64:" + Base64.getEncoder().encodeToString(output);
    }
    if (value instanceof GenericFixed) {
      return "base64:" + Base64.getEncoder().encodeToString(((GenericFixed) value).bytes());
    }
    return value.toString();
  }

  private static void appendGroupValue(
      StringBuilder result, Group group, Type field, int fieldIndex, int valueIndex) {
    if (!field.isPrimitive()) {
      result.append(canonicalGroup(group.getGroup(fieldIndex, valueIndex), field.asGroupType()));
      return;
    }
    PrimitiveType primitive = field.asPrimitiveType();
    switch (primitive.getPrimitiveTypeName()) {
      case BOOLEAN:
        result.append("bool:").append(group.getBoolean(fieldIndex, valueIndex));
        return;
      case INT32:
        result.append("i32:").append(group.getInteger(fieldIndex, valueIndex));
        return;
      case INT64:
        result.append("i64:").append(group.getLong(fieldIndex, valueIndex));
        return;
      case FLOAT:
        result.append("f32:").append(Float.toHexString(group.getFloat(fieldIndex, valueIndex)));
        return;
      case DOUBLE:
        result.append("f64:").append(Double.toHexString(group.getDouble(fieldIndex, valueIndex)));
        return;
      case BINARY:
      case FIXED_LEN_BYTE_ARRAY:
      case INT96:
        Binary binary = group.getBinary(fieldIndex, valueIndex);
        if (primitive.getLogicalTypeAnnotation()
            instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
          result.append("utf8:").append(binary.toStringUsingUTF8());
        } else {
          result.append("hex:").append(hex(binary.getBytes()));
        }
        return;
      default:
        throw new IllegalStateException("unsupported primitive: " + primitive);
    }
  }

  private static String canonicalColumnValue(ColumnReader column, PrimitiveType primitive) {
    switch (primitive.getPrimitiveTypeName()) {
      case BOOLEAN:
        return Boolean.toString(column.getBoolean());
      case INT32:
        return Integer.toString(column.getInteger());
      case INT64:
        return Long.toString(column.getLong());
      case FLOAT:
        return Float.toHexString(column.getFloat());
      case DOUBLE:
        return Double.toHexString(column.getDouble());
      case BINARY:
      case FIXED_LEN_BYTE_ARRAY:
      case INT96:
        Binary binary = column.getBinary();
        if (primitive.getLogicalTypeAnnotation()
            instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
          return "utf8:" + binary.toStringUsingUTF8();
        }
        return "hex:" + hex(binary.getBytes());
      default:
        throw new IllegalStateException("unsupported primitive: " + primitive);
    }
  }

  private static String sha256(Path source) throws IOException {
    MessageDigest digest;
    try {
      digest = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException error) {
      throw new IllegalStateException("SHA-256 is unavailable", error);
    }
    byte[] buffer = new byte[64 * 1024];
    try (InputStream input = Files.newInputStream(source)) {
      int count;
      while ((count = input.read(buffer)) >= 0) {
        if (count > 0) {
          digest.update(buffer, 0, count);
        }
      }
    }
    return hex(digest.digest());
  }

  private static String hex(byte[] bytes) {
    char[] digits = "0123456789abcdef".toCharArray();
    char[] result = new char[bytes.length * 2];
    for (int index = 0; index < bytes.length; index++) {
      int value = bytes[index] & 0xff;
      result[index * 2] = digits[value >>> 4];
      result[index * 2 + 1] = digits[value & 0x0f];
    }
    return new String(result);
  }

  private static Throwable rootCause(Throwable error) {
    Throwable current = error;
    while (current.getCause() != null && current.getCause() != current) {
      current = current.getCause();
    }
    return current;
  }

  private static final class CollectionsSupport {
    private CollectionsSupport() {}

    static void sort(List<String> values) {
      values.sort(String::compareTo);
    }
  }
}
