package org.julialang.parquet.n5;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;

public final class OracleMain {
  static final String PARQUET_JAVA_VERSION = "1.17.1";
  static final String PARQUET_JAVA_COMMIT = "78a8d3230eb4769db93de5f2f2e18363c04cae81";
  static final String CASE_METADATA_KEY = "parquet.jl.n5.case_id";
  static final String PAGE_VERSION_METADATA_KEY = "parquet.jl.n5.page_version";
  private static final String PRODUCER_METADATA_KEY = "parquet.jl.n5.producer";
  private static final String COMMIT_METADATA_KEY = "parquet.jl.n5.parquet_java_commit";
  private static final String AVRO_PROPERTY = "parquet.avro.add-list-element-records";
  private static final int PAGE_SIZE = 1024 * 1024;
  private static final long ROW_GROUP_SIZE = 128L * 1024L * 1024L;

  private enum PageVersion {
    V1("v1", ParquetProperties.WriterVersion.PARQUET_1_0, "DATA_PAGE_V1"),
    V2("v2", ParquetProperties.WriterVersion.PARQUET_2_0, "DATA_PAGE_V2");

    final String id;
    final ParquetProperties.WriterVersion writerVersion;
    final String pageType;

    PageVersion(String id, ParquetProperties.WriterVersion writerVersion, String pageType) {
      this.id = id;
      this.writerVersion = writerVersion;
      this.pageType = pageType;
    }

    static PageVersion parse(String value) {
      for (PageVersion version : values()) {
        if (version.id.equals(value)) {
          return version;
        }
      }
      return null;
    }
  }

  private static final class GeneratedFile {
    final Path staged;
    final Path target;
    final ParquetEvidence.FileEvidence evidence;

    GeneratedFile(Path staged, Path target, ParquetEvidence.FileEvidence evidence) {
      this.staged = staged;
      this.target = target;
      this.evidence = evidence;
    }
  }

  private OracleMain() {}

  public static void main(String[] arguments) {
    try {
      requireAvroConfiguration();
      run(arguments);
    } catch (Exception error) {
      System.err.println(Json.encode(Json.object(
          "record", "error",
          "error_class", error.getClass().getName(),
          "message", error.getMessage())));
      error.printStackTrace(System.err);
      System.exit(1);
    }
  }

  private static void run(String[] arguments) throws IOException {
    if (arguments.length == 0) {
      throw new IllegalArgumentException(
          "usage: <generate|verify|inspect|audit|self-test> [options]; "
              + "verify accepts --case-id ID --page-version v1|v2 for one file");
    }
    String command = arguments[0];
    Options options = Options.parse(arguments, 1);
    switch (command) {
      case "generate":
        runGenerate(options);
        return;
      case "verify":
        runInspect(options, true);
        return;
      case "inspect":
        runInspect(options, false);
        return;
      case "audit":
        runAudit(options);
        return;
      case "self-test":
        runSelfTest(options);
        return;
      default:
        throw new IllegalArgumentException("unknown command: " + command);
    }
  }

  private static void runGenerate(Options options) throws IOException {
    Path output = options.requiredPath("--output");
    Path evidencePath = options.requiredPath("--evidence");
    options.requireNoUnused();
    List<ParquetEvidence.FileEvidence> files = generateFixtures(output);
    Json.writeLines(evidencePath, evidenceRecords("generate", files));
    printSummary("generate", files.size());
  }

  private static void runInspect(Options options, boolean validateKnown) throws IOException {
    Path input = options.requiredPath("--input");
    Path evidencePath = options.requiredPath("--evidence");
    boolean requireOwned = options.flag("--require-owned");
    String explicitCaseId = options.optional("--case-id");
    String explicitPageVersion = options.optional("--page-version");
    options.requireNoUnused();
    if (!validateKnown && (requireOwned || explicitCaseId != null || explicitPageVersion != null)) {
      throw new IllegalArgumentException(
          "--require-owned, --case-id, and --page-version are valid only for verify");
    }
    List<ParquetEvidence.FileEvidence> evidence =
        inspectFiles(input, validateKnown, explicitCaseId, explicitPageVersion);
    Json.writeLines(evidencePath, evidenceRecords(validateKnown ? "verify" : "inspect", evidence));
    printSummary(validateKnown ? "verify" : "inspect", evidence.size());
  }

  private static void runAudit(Options options) throws IOException {
    Path input = options.requiredPath("--input");
    Path evidencePath = options.requiredPath("--evidence");
    options.requireNoUnused();
    List<Map<String, Object>> records = auditRecords(input);
    Json.writeLines(evidencePath, records);
    int unsupported = 0;
    for (Map<String, Object> record : records) {
      if ("unsupported".equals(record.get("record"))) {
        unsupported++;
      }
    }
    printAuditSummary(records.size() - 1, unsupported);
  }

  static List<Map<String, Object>> auditRecords(Path input) throws IOException {
    List<Path> sources = parquetFiles(input);
    List<Map<String, Object>> result = runRecord("audit", sources.size());
    int supported = 0;
    int unsupported = 0;
    for (Path source : sources) {
      String relative = relativeName(input, source);
      try {
        ParquetEvidence.FileEvidence file =
            ParquetEvidence.inspect(source, relative, row -> row, null);
        result.add(file.toMap());
        supported++;
      } catch (IOException | RuntimeException error) {
        Throwable cause = rootCause(error);
        result.add(Json.object(
            "record", "unsupported",
            "file", relative,
            "error_class", cause.getClass().getName(),
            "error_message", sanitizeError(cause.getMessage(), source)));
        unsupported++;
      }
    }
    Map<String, Object> run = result.get(0);
    run.put("supported_count", supported);
    run.put("unsupported_count", unsupported);
    return result;
  }

  static List<ParquetEvidence.FileEvidence> verifyFiles(
      Path input, String explicitCaseId, String explicitPageVersion) throws IOException {
    return inspectFiles(input, true, explicitCaseId, explicitPageVersion);
  }

  private static List<ParquetEvidence.FileEvidence> inspectFiles(
      Path input, boolean validateKnown, String explicitCaseId, String explicitPageVersion)
      throws IOException {
    List<Path> sources = parquetFiles(input);
    if ((explicitCaseId == null) != (explicitPageVersion == null)) {
      throw new IllegalArgumentException("--case-id and --page-version must be used together");
    }
    FixtureCases.CaseSpec explicitSpec = null;
    PageVersion explicitVersion = null;
    if (explicitCaseId != null) {
      if (sources.size() != 1) {
        throw new IllegalArgumentException("explicit case binding requires one input file");
      }
      explicitSpec = FixtureCases.find(explicitCaseId);
      if (explicitSpec == null) {
        throw new IllegalArgumentException("unknown Java case ID: " + explicitCaseId);
      }
      explicitVersion = PageVersion.parse(explicitPageVersion);
      if (explicitVersion == null) {
        throw new IllegalArgumentException("unknown page version: " + explicitPageVersion);
      }
    }
    List<ParquetEvidence.FileEvidence> evidence = new ArrayList<>();
    for (Path source : sources) {
      String relative = relativeName(input, source);
      FixtureCases.CaseSpec spec = explicitSpec == null ? resolveCase(source, relative) : explicitSpec;
      if (validateKnown && spec == null) {
        throw new IllegalStateException("no Java-owned case binding for " + relative);
      }
      FixtureCases.AvroNormalizer normalizer = spec == null ? row -> row : spec.avroNormalizer;
      String explicitAvroSchema = spec == null ? null : spec.explicitAvroSchema;
      ParquetEvidence.FileEvidence file =
          ParquetEvidence.inspect(source, relative, normalizer, explicitAvroSchema);
      if (validateKnown) {
        PageVersion pageVersion = explicitVersion == null
            ? resolvePageVersion(file, relative) : explicitVersion;
        if (pageVersion == null) {
          throw new IllegalStateException("no V1/V2 binding for " + relative);
        }
        validate(spec, pageVersion, file, false);
      }
      evidence.add(file);
    }
    return evidence;
  }

  private static void runSelfTest(Options options) throws IOException {
    Path work = options.requiredPath("--work");
    Path evidencePath = options.requiredPath("--evidence");
    options.requireNoUnused();
    List<ParquetEvidence.FileEvidence> files = selfTest(work);
    Json.writeLines(evidencePath, evidenceRecords("self-test", files));
    printSummary("self-test", files.size());
  }

  static List<ParquetEvidence.FileEvidence> generateFixtures(Path output) throws IOException {
    Files.createDirectories(output);
    Path staging = Files.createTempDirectory(output, ".parquet-java-stage-");
    List<GeneratedFile> generated = new ArrayList<>();
    boolean committed = false;
    try {
      for (FixtureCases.CaseSpec spec : FixtureCases.all()) {
        for (PageVersion pageVersion : PageVersion.values()) {
          String name = spec.id + "." + pageVersion.id + ".parquet";
          Path staged = staging.resolve(name);
          writeFixture(staged, spec, pageVersion);
          ParquetEvidence.FileEvidence evidence =
              ParquetEvidence.inspect(
                  staged, name, spec.avroNormalizer, spec.explicitAvroSchema);
          validateGeneratedIdentity(evidence);
          validate(spec, pageVersion, evidence, true);
          generated.add(new GeneratedFile(staged, output.resolve(name), evidence));
        }
      }
      for (GeneratedFile file : generated) {
        try {
          Files.move(
              file.staged,
              file.target,
              StandardCopyOption.ATOMIC_MOVE,
              StandardCopyOption.REPLACE_EXISTING);
        } catch (java.nio.file.AtomicMoveNotSupportedException error) {
          Files.move(file.staged, file.target, StandardCopyOption.REPLACE_EXISTING);
        }
      }
      committed = true;
      return generated.stream().map(file -> file.evidence).collect(Collectors.toList());
    } finally {
      deleteTree(staging);
      if (!committed) {
        for (GeneratedFile file : generated) {
          Files.deleteIfExists(file.staged);
        }
      }
    }
  }

  static List<ParquetEvidence.FileEvidence> selfTest(Path work) throws IOException {
    Files.createDirectories(work);
    Path first = work.resolve("first");
    Path second = work.resolve("second");
    List<ParquetEvidence.FileEvidence> firstEvidence = generateFixtures(first);
    List<ParquetEvidence.FileEvidence> secondEvidence = generateFixtures(second);
    Map<String, String> firstHashes = hashes(firstEvidence);
    Map<String, String> secondHashes = hashes(secondEvidence);
    if (!firstHashes.equals(secondHashes)) {
      throw new IllegalStateException("repeated Java fixture generation changed file hashes");
    }
    return firstEvidence;
  }

  private static Map<String, String> hashes(List<ParquetEvidence.FileEvidence> evidence) {
    Map<String, String> result = new LinkedHashMap<>();
    for (ParquetEvidence.FileEvidence file : evidence) {
      result.put(file.file, file.sha256);
    }
    return result;
  }

  private static void writeFixture(
      Path output, FixtureCases.CaseSpec spec, PageVersion pageVersion) throws IOException {
    Map<String, String> metadata = new LinkedHashMap<>();
    metadata.put(CASE_METADATA_KEY, spec.id);
    metadata.put(PAGE_VERSION_METADATA_KEY, pageVersion.id);
    metadata.put(PRODUCER_METADATA_KEY, "parquet-java");
    metadata.put(COMMIT_METADATA_KEY, PARQUET_JAVA_COMMIT);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(output))
        .withType(spec.schema)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .withRowGroupSize(ROW_GROUP_SIZE)
        .withPageSize(PAGE_SIZE)
        .withDictionaryEncoding(false)
        .withValidation(true)
        .withWriterVersion(pageVersion.writerVersion)
        .withPageWriteChecksumEnabled(true)
        .withExtraMetaData(metadata)
        .build()) {
      for (Group row : spec.createRows()) {
        writer.write(row);
      }
    }
    CanonicalParquetFooter.canonicalize(output);
    CanonicalParquetFooter.canonicalize(output);
  }

  private static void validate(
      FixtureCases.CaseSpec spec,
      PageVersion pageVersion,
      ParquetEvidence.FileEvidence evidence,
      boolean requireMetadataIdentity) {
    if (requireMetadataIdentity || evidence.caseId() != null) {
      requireEqual(spec.id, evidence.caseId(), "case metadata");
    }
    if (pageVersion != null) {
      if (requireMetadataIdentity || evidence.pageVersion() != null) {
        requireEqual(pageVersion.id, evidence.pageVersion(), "page-version metadata");
      }
    }
    requireEqual(spec.schema, evidence.schema, "physical schema");
    requireEqual((long) spec.rawRows.size(), evidence.rowCount, "row count");
    requireEqual(spec.rawRows, evidence.rawRows, "raw Group rows");
    requireEqual(spec.columns.size(), evidence.columns.size(), "physical column count");
    for (FixtureCases.ExpectedColumn expected : spec.columns) {
      ParquetEvidence.ColumnEvidence column = evidence.column(expected.path);
      if (column == null) {
        throw new IllegalStateException("missing column " + expected.path + " for " + spec.id);
      }
      requireEqual(evidence.rowGroups.size(), column.rowGroups.size(),
          expected.path + " row-group count");
      if (requireMetadataIdentity) {
        requireEqual(1, column.rowGroups.size(), expected.path + " generated row-group count");
      }
      requireEqual(expected.repetition, column.repetition, expected.path + " repetition");
      requireEqual(expected.definition, column.definition, expected.path + " definition");
      requireEqual(expected.dense, column.dense, expected.path + " dense values");
      if (!column.dictionaries.isEmpty()) {
        throw new IllegalStateException("unexpected dictionary page for " + expected.path);
      }
      if (column.pages.isEmpty()) {
        throw new IllegalStateException("no data page for " + expected.path);
      }
      long groupRows = 0;
      for (int groupIndex = 0; groupIndex < column.rowGroups.size(); groupIndex++) {
        ParquetEvidence.ColumnRowGroupEvidence group = column.rowGroups.get(groupIndex);
        requireEqual(groupIndex, group.rowGroup, expected.path + " row-group ordinal");
        groupRows = Math.addExact(groupRows, group.rows);
        if (!group.dictionaries.isEmpty()) {
          throw new IllegalStateException(
              "unexpected row-group dictionary page for " + expected.path);
        }
        if (group.pages.isEmpty()) {
          throw new IllegalStateException("no row-group data page for " + expected.path);
        }
        for (Map<String, Object> page : group.pages) {
          requireEqual(pageVersion.pageType, page.get("type"), expected.path + " page type");
        }
      }
      requireEqual((long) spec.rawRows.size(), groupRows, expected.path + " row-group rows");
      if (column.rowGroups.size() == 1) {
        ParquetEvidence.ColumnRowGroupEvidence group = column.rowGroups.get(0);
        requireEqual(expected.repetition, group.repetition, expected.path + " row-group repetition");
        requireEqual(expected.definition, group.definition, expected.path + " row-group definition");
        requireEqual(expected.dense, group.dense, expected.path + " row-group dense values");
      }
    }
    validateAvro(
        spec.id + " inferred Avro",
        spec.avroMode,
        spec.avroRows,
        spec.avroMaterializedSchema,
        evidence.avro);
    if (spec.explicitAvroSchema == null) {
      requireEqual(null, evidence.explicitAvro, "unexpected explicit Avro attempt");
    } else {
      if (evidence.explicitAvro == null) {
        throw new IllegalStateException("missing explicit Avro attempt for " + spec.id);
      }
      validateAvro(
          spec.id + " explicit Avro",
          spec.explicitAvroMode,
          spec.explicitAvroRows,
          null,
          evidence.explicitAvro);
    }
  }

  private static void validateGeneratedIdentity(ParquetEvidence.FileEvidence evidence) {
    requireEqual(
        "parquet-mr version " + PARQUET_JAVA_VERSION + " (build " + PARQUET_JAVA_COMMIT + ")",
        evidence.createdBy,
        "generated created-by identity");
    requireEqual(
        "parquet-java", evidence.metadata.get(PRODUCER_METADATA_KEY), "producer metadata");
    requireEqual(
        PARQUET_JAVA_COMMIT,
        evidence.metadata.get(COMMIT_METADATA_KEY),
        "Parquet Java commit metadata");
  }

  private static void validateAvro(
      String label,
      FixtureCases.AvroMode mode,
      List<String> expectedRows,
      String expectedSchema,
      ParquetEvidence.AvroEvidence evidence) {
    if (mode == FixtureCases.AvroMode.SUCCESS) {
      if (!"success".equals(evidence.status)) {
        throw new IllegalStateException(
            label + " rejected with " + evidence.errorClass + ": " + evidence.errorMessage);
      }
      if (expectedSchema != null) {
        requireEqual(expectedSchema, evidence.materializedSchema, label + " materialized schema");
      }
      requireEqual(expectedRows, evidence.normalizedRows, label + " normalized rows");
      return;
    }
    requireEqual("rejected", evidence.status, label + " diagnostic rejection");
    if (evidence.errorClass == null || evidence.errorStack.isEmpty()) {
      throw new IllegalStateException(label + " rejection has incomplete exception evidence");
    }
  }

  private static void requireEqual(Object expected, Object actual, String label) {
    if (!Objects.equals(expected, actual)) {
      throw new IllegalStateException(
          label + " mismatch: expected " + expected + ", got " + actual);
    }
  }

  private static FixtureCases.CaseSpec resolveCase(Path source, String relative) throws IOException {
    try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(source))) {
      String id = reader.getFooter().getFileMetaData().getKeyValueMetaData().get(CASE_METADATA_KEY);
      if (id != null) {
        return FixtureCases.find(id);
      }
    }
    FixtureCases.CaseSpec result = null;
    for (FixtureCases.CaseSpec spec : FixtureCases.all()) {
      if (relative.contains(spec.id)
          && (result == null || spec.id.length() > result.id.length())) {
        result = spec;
      }
    }
    return result;
  }

  private static PageVersion resolvePageVersion(
      ParquetEvidence.FileEvidence evidence, String relative) {
    PageVersion version = PageVersion.parse(evidence.pageVersion());
    if (version != null) {
      return version;
    }
    if (relative.contains(".v1.")) {
      return PageVersion.V1;
    }
    if (relative.contains(".v2.")) {
      return PageVersion.V2;
    }
    return null;
  }

  private static List<Path> parquetFiles(Path input) throws IOException {
    Path absolute = input.toAbsolutePath().normalize();
    List<Path> result = new ArrayList<>();
    if (Files.isRegularFile(absolute)) {
      if (!absolute.getFileName().toString().endsWith(".parquet")) {
        throw new IllegalArgumentException("input file is not .parquet: " + input);
      }
      result.add(absolute);
    } else if (Files.isDirectory(absolute)) {
      try (Stream<Path> paths = Files.walk(absolute)) {
        paths.filter(Files::isRegularFile)
            .filter(path -> path.getFileName().toString().endsWith(".parquet"))
            .sorted(Comparator.comparing(path -> relativeName(absolute, path)))
            .forEach(result::add);
      }
    } else {
      throw new IllegalArgumentException("input does not exist: " + input);
    }
    if (result.isEmpty()) {
      throw new IllegalStateException("input contains no .parquet files: " + input);
    }
    return result;
  }

  private static String relativeName(Path input, Path source) {
    Path absolute = input.toAbsolutePath().normalize();
    if (Files.isRegularFile(absolute)) {
      return source.getFileName().toString();
    }
    return absolute.relativize(source.toAbsolutePath().normalize()).toString()
        .replace(File.separatorChar, '/');
  }

  private static List<Map<String, Object>> evidenceRecords(
      String command, List<ParquetEvidence.FileEvidence> files) {
    List<Map<String, Object>> result = runRecord(command, files.size());
    for (ParquetEvidence.FileEvidence file : files) {
      result.add(file.toMap());
    }
    return result;
  }

  private static List<Map<String, Object>> runRecord(String command, int fileCount) {
    List<Map<String, Object>> result = new ArrayList<>();
    result.add(Json.object(
        "record", "run",
        "schema_version", 1,
        "oracle", "parquet-java",
        "command", command,
        "parquet_java_version", PARQUET_JAVA_VERSION,
        "parquet_java_commit", PARQUET_JAVA_COMMIT,
        "avro_add_list_element_records", false,
        "fixture_writer", Json.object(
            "compression", CompressionCodecName.UNCOMPRESSED.name(),
            "dictionary", false,
            "page_size", PAGE_SIZE,
            "row_group_size", ROW_GROUP_SIZE,
            "page_checksums", true,
            "validation", true,
            "page_versions", List.of("v1", "v2")),
        "java_version", System.getProperty("java.version"),
        "java_vendor", System.getProperty("java.vendor"),
        "file_count", fileCount));
    return result;
  }

  private static Throwable rootCause(Throwable error) {
    Throwable current = error;
    while (current.getCause() != null && current.getCause() != current) {
      current = current.getCause();
    }
    return current;
  }

  private static String sanitizeError(String value, Path source) {
    if (value == null) {
      return null;
    }
    return value.replace(source.toAbsolutePath().normalize().toString(), "<file>")
        .replace('\n', ' ').replace('\r', ' ');
  }

  private static void printSummary(String command, int fileCount) {
    System.out.println(Json.encode(Json.object(
        "record", "summary",
        "status", "ok",
        "oracle", "parquet-java",
        "command", command,
        "file_count", fileCount)));
  }

  private static void printAuditSummary(int fileCount, int unsupportedCount) {
    System.out.println(Json.encode(Json.object(
        "record", "summary",
        "status", "ok",
        "oracle", "parquet-java",
        "command", "audit",
        "file_count", fileCount,
        "supported_count", fileCount - unsupportedCount,
        "unsupported_count", unsupportedCount)));
  }

  private static void requireAvroConfiguration() {
    String value = System.getProperty(AVRO_PROPERTY);
    if (!"false".equals(value)) {
      throw new IllegalStateException(
          AVRO_PROPERTY + " must be the explicit JVM system property false");
    }
  }

  private static void deleteTree(Path root) throws IOException {
    if (!Files.exists(root)) {
      return;
    }
    try (Stream<Path> paths = Files.walk(root)) {
      List<Path> ordered = paths.sorted(Comparator.reverseOrder()).collect(Collectors.toList());
      for (Path path : ordered) {
        Files.deleteIfExists(path);
      }
    }
  }

  private static final class Options {
    private final Map<String, String> values = new LinkedHashMap<>();
    private final List<String> flags = new ArrayList<>();

    static Options parse(String[] arguments, int start) {
      Options result = new Options();
      for (int index = start; index < arguments.length; index++) {
        String option = arguments[index];
        if ("--require-owned".equals(option)) {
          result.flags.add(option);
          continue;
        }
        if (!option.startsWith("--") || index + 1 >= arguments.length) {
          throw new IllegalArgumentException("invalid option: " + option);
        }
        String value = arguments[++index];
        if (result.values.put(option, value) != null) {
          throw new IllegalArgumentException("duplicate option: " + option);
        }
      }
      return result;
    }

    Path requiredPath(String name) {
      String value = values.remove(name);
      if (value == null) {
        throw new IllegalArgumentException("missing required option " + name);
      }
      return Path.of(value);
    }

    String optional(String name) {
      return values.remove(name);
    }

    boolean flag(String name) {
      return flags.remove(name);
    }

    void requireNoUnused() {
      if (!values.isEmpty() || !flags.isEmpty()) {
        throw new IllegalArgumentException(
            "unknown options: " + values.keySet() + flags);
      }
    }
  }
}
