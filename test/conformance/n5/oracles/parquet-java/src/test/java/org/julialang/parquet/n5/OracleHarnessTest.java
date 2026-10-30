package org.julialang.parquet.n5;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class OracleHarnessTest {
  @TempDir Path temporary;

  @Test
  void canonicalEncodingOrderUsesNumericValuesAndPreservesDuplicates() {
    assertEquals(
        List.of(Encoding.PLAIN, Encoding.RLE, Encoding.RLE),
        CanonicalParquetFooter.sortedEncodings(
            List.of(Encoding.RLE, Encoding.PLAIN, Encoding.RLE)));
  }

  @Test
  void generatesDeterministicSelfVerifiedV1AndV2Fixtures() throws Exception {
    List<ParquetEvidence.FileEvidence> evidence = OracleMain.selfTest(temporary);
    assertEquals(FixtureCases.all().size() * 2, evidence.size());
    for (ParquetEvidence.FileEvidence file : evidence) {
      assertTrue(Files.isRegularFile(temporary.resolve("first").resolve(file.file)));
      assertEquals(64, file.sha256.length());
      assertEquals("false", System.getProperty("parquet.avro.add-list-element-records"));
    }
    for (String version : List.of("v1", "v2")) {
      ParquetEvidence.FileEvidence rule3 = evidence.stream()
          .filter(file -> file.file.equals("list_rule3_nested." + version + ".parquet"))
          .findFirst()
          .orElseThrow();
      ParquetEvidence.ColumnEvidence column = rule3.column("items.array.array");
      assertEquals(List.of(0, 0, 0, 0, 2, 1, 1), column.repetition);
      assertEquals(List.of(0, 1, 2, 3, 3, 2, 3), column.definition);
      assertEquals(List.of("1", "2", "3"), column.dense);
      assertEquals(1, column.rowGroups.size());
      assertEquals(column.repetition, column.rowGroups.get(0).repetition);
      assertEquals(column.definition, column.rowGroups.get(0).definition);
      assertEquals(column.dense, column.rowGroups.get(0).dense);
      assertEquals("success", rule3.avro.status);
      assertEquals(
          List.of(
              "{\"items\":null}",
              "{\"items\":[]}",
              "{\"items\":[[]]}",
              "{\"items\":[[1,2],[],[3]]}"),
          rule3.avro.normalizedRows);
      assertNull(rule3.explicitAvro);

      ParquetEvidence.FileEvidence diagnostic = evidence.stream()
          .filter(file -> file.file.equals(
              "list_rule3_unannotated_diagnostic." + version + ".parquet"))
          .findFirst()
          .orElseThrow();
      assertEquals("rejected", diagnostic.avro.status);
      assertEquals("java.lang.ClassCastException", diagnostic.avro.errorClass);
      assertEquals("repeated int32 element is not a group", diagnostic.avro.errorMessage);
      assertTrue(!diagnostic.avro.errorStack.isEmpty());
      assertNull(diagnostic.explicitAvro);

      ParquetEvidence.FileEvidence listMap = evidence.stream()
          .filter(file -> file.file.equals("list_direct_map_utf8." + version + ".parquet"))
          .findFirst()
          .orElseThrow();
      assertEquals(List.of("utf8:a", "utf8:a", "utf8:b"),
          listMap.column("items.map.key_value.key").dense);
      assertEquals("success", listMap.avro.status);
      assertEquals(
          "{\"type\":\"record\",\"name\":\"list_direct_map_utf8\",\"fields\":["
              + "{\"name\":\"items\",\"type\":[\"null\",{\"type\":\"array\","
              + "\"items\":{\"type\":\"map\",\"values\":\"int\"}}],"
              + "\"default\":null}]}",
          listMap.avro.materializedSchema);
      assertEquals(
          List.of(
              "{\"items\":null}",
              "{\"items\":[]}",
              "{\"items\":[{}]}",
              "{\"items\":[{\"a\":20},{},{\"b\":30}]}"),
          listMap.avro.normalizedRows);
    }
  }

  @Test
  void strictVerificationBindsUnownedFilesAndPreservesRowGroups() throws Exception {
    FixtureCases.CaseSpec spec = FixtureCases.find("list_rule1_primitive");
    Path source = temporary.resolve("unbound.parquet");
    writeUnbound(source, spec);

    ParquetEvidence.FileEvidence inspected =
        ParquetEvidence.inspect(source, source.getFileName().toString(), spec.avroNormalizer, null);
    ParquetEvidence.ColumnEvidence column = inspected.column("items.element");
    assertEquals(2, column.rowGroups.size());
    assertEquals(List.of(0, 0), column.rowGroups.get(0).repetition);
    assertEquals(List.of(0, 1), column.rowGroups.get(0).definition);
    assertEquals(List.of(), column.rowGroups.get(0).dense);
    assertEquals(List.of(0, 0, 1), column.rowGroups.get(1).repetition);
    assertEquals(List.of(2, 2, 2), column.rowGroups.get(1).definition);
    assertEquals(List.of("10", "20", "30"), column.rowGroups.get(1).dense);

    assertThrows(IllegalStateException.class,
        () -> OracleMain.verifyFiles(source, null, null));
    List<ParquetEvidence.FileEvidence> verified =
        OracleMain.verifyFiles(source, spec.id, "v1");
    assertEquals(1, verified.size());
    assertEquals(2, verified.get(0).column("items.element").rowGroups.size());
    assertThrows(IllegalStateException.class,
        () -> OracleMain.verifyFiles(source, spec.id, "v2"));
    assertThrows(IllegalArgumentException.class,
        () -> OracleMain.verifyFiles(source, "missing-case", "v1"));
  }

  @Test
  void auditRecordsEverySupportedAndUnsupportedFile() throws Exception {
    FixtureCases.CaseSpec spec = FixtureCases.find("list_rule1_primitive");
    Path valid = temporary.resolve("valid.parquet");
    Path invalid = temporary.resolve("invalid.parquet");
    writeUnbound(valid, spec);
    Files.write(invalid, new byte[] {0, 1, 2, 3});

    List<Map<String, Object>> records = OracleMain.auditRecords(temporary);
    assertEquals(3, records.size());
    assertEquals("run", records.get(0).get("record"));
    assertEquals(2, records.get(0).get("file_count"));
    assertEquals(1, records.get(0).get("supported_count"));
    assertEquals(1, records.get(0).get("unsupported_count"));
    assertEquals("unsupported", records.get(1).get("record"));
    assertEquals("invalid.parquet", records.get(1).get("file"));
    assertEquals("file", records.get(2).get("record"));
    assertEquals("valid.parquet", records.get(2).get("file"));
  }

  private static void writeUnbound(Path output, FixtureCases.CaseSpec spec) throws Exception {
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(output))
        .withType(spec.schema)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .withRowGroupSize(1024 * 1024)
        .withRowGroupRowCountLimit(2)
        .withPageSize(1024 * 1024)
        .withDictionaryEncoding(false)
        .withValidation(true)
        .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
        .withPageWriteChecksumEnabled(true)
        .build()) {
      for (Group row : spec.createRows()) {
        writer.write(row);
      }
    }
  }
}
