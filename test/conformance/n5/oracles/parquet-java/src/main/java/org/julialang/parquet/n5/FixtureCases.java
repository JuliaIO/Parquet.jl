package org.julialang.parquet.n5;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

final class FixtureCases {
  enum AvroMode {
    SUCCESS,
    REJECTED
  }

  @FunctionalInterface
  interface RowFactory {
    List<Group> create(SimpleGroupFactory factory);
  }

  @FunctionalInterface
  interface AvroNormalizer {
    Object normalize(GenericRecord row);
  }

  static final class ExpectedColumn {
    final String path;
    final List<Integer> repetition;
    final List<Integer> definition;
    final List<String> dense;

    ExpectedColumn(String path, List<Integer> repetition, List<Integer> definition, List<String> dense) {
      this.path = path;
      this.repetition = repetition;
      this.definition = definition;
      this.dense = dense;
    }
  }

  static final class CaseSpec {
    final String id;
    final MessageType schema;
    final RowFactory rows;
    final List<String> rawRows;
    final List<ExpectedColumn> columns;
    final AvroMode avroMode;
    final List<String> avroRows;
    final AvroNormalizer avroNormalizer;
    final String avroMaterializedSchema;
    final String explicitAvroSchema;
    final AvroMode explicitAvroMode;
    final List<String> explicitAvroRows;

    CaseSpec(
        String id,
        String schema,
        RowFactory rows,
        List<String> rawRows,
        List<ExpectedColumn> columns,
        AvroMode avroMode,
        List<String> avroRows,
        AvroNormalizer avroNormalizer) {
      this(
          id,
          schema,
          rows,
          rawRows,
          columns,
          avroMode,
          avroRows,
          avroNormalizer,
          null,
          null,
          null,
          List.of());
    }

    CaseSpec(
        String id,
        String schema,
        RowFactory rows,
        List<String> rawRows,
        List<ExpectedColumn> columns,
        AvroMode avroMode,
        List<String> avroRows,
        AvroNormalizer avroNormalizer,
        String avroMaterializedSchema) {
      this(
          id,
          schema,
          rows,
          rawRows,
          columns,
          avroMode,
          avroRows,
          avroNormalizer,
          avroMaterializedSchema,
          null,
          null,
          List.of());
    }

    CaseSpec(
        String id,
        String schema,
        RowFactory rows,
        List<String> rawRows,
        List<ExpectedColumn> columns,
        AvroMode avroMode,
        List<String> avroRows,
        AvroNormalizer avroNormalizer,
        String avroMaterializedSchema,
        String explicitAvroSchema,
        AvroMode explicitAvroMode,
        List<String> explicitAvroRows) {
      this.id = id;
      this.schema = MessageTypeParser.parseMessageType(schema);
      this.rows = rows;
      this.rawRows = rawRows;
      this.columns = columns;
      this.avroMode = avroMode;
      this.avroRows = avroRows;
      this.avroNormalizer = avroNormalizer;
      this.avroMaterializedSchema = avroMaterializedSchema;
      this.explicitAvroSchema = explicitAvroSchema;
      this.explicitAvroMode = explicitAvroMode;
      this.explicitAvroRows = explicitAvroRows;
    }

    List<Group> createRows() {
      return rows.create(new SimpleGroupFactory(schema));
    }
  }

  private static final AvroNormalizer IDENTITY = row -> row;
  private static final List<CaseSpec> CASES = buildCases();
  private static final Map<String, CaseSpec> BY_ID = indexCases();

  private FixtureCases() {}

  static List<CaseSpec> all() {
    return CASES;
  }

  static CaseSpec find(String id) {
    return BY_ID.get(id);
  }

  private static Map<String, CaseSpec> indexCases() {
    Map<String, CaseSpec> result = new LinkedHashMap<>();
    for (CaseSpec spec : CASES) {
      if (result.put(spec.id, spec) != null) {
        throw new IllegalStateException("duplicate case ID: " + spec.id);
      }
    }
    return Collections.unmodifiableMap(result);
  }

  private static List<CaseSpec> buildCases() {
    List<CaseSpec> cases = new ArrayList<>();
    cases.add(rule1());
    cases.add(rule2());
    cases.add(rule3());
    cases.add(rule3UnannotatedDiagnostic());
    cases.add(rule4Array());
    cases.add(rule4Tuple());
    cases.add(rule5Required());
    cases.add(rule5OptionalPaired());
    cases.add(rule5OptionalExtended());
    cases.add(directListMap());
    cases.add(directListMapUtf8());
    cases.add(standardMap());
    cases.add(arbitraryMapNames());
    cases.add(standaloneMapKeyValue());
    cases.add(keyOnlyMap());
    return Collections.unmodifiableList(cases);
  }

  private static CaseSpec rule1() {
    String schema = "message list_rule1_primitive {\n"
        + "  optional group items (LIST) {\n"
        + "    repeated int32 element;\n"
        + "  }\n"
        + "}";
    return new CaseSpec(
        "list_rule1_primitive",
        schema,
        FixtureCases::rule1Rows,
        strings(
            "G{items=null}",
            "G{items=G{element=[]}}",
            "G{items=G{element=[i32:10]}}",
            "G{items=G{element=[i32:20,i32:30]}}"),
        columns(column("items.element", ints(0, 0, 0, 0, 1), ints(0, 1, 2, 2, 2),
            strings("10", "20", "30"))),
        AvroMode.SUCCESS,
        strings(
            "{\"items\":null}",
            "{\"items\":[]}",
            "{\"items\":[10]}",
            "{\"items\":[20,30]}"),
        IDENTITY);
  }

  private static CaseSpec rule2() {
    String schema = "message list_rule2_struct {\n"
        + "  optional group items (LIST) {\n"
        + "    repeated group element {\n"
        + "      required int32 x;\n"
        + "      optional int32 y;\n"
        + "    }\n"
        + "  }\n"
        + "}";
    return new CaseSpec(
        "list_rule2_struct",
        schema,
        FixtureCases::rule2Rows,
        strings(
            "G{items=null}",
            "G{items=G{element=[]}}",
            "G{items=G{element=[G{x=i32:1,y=null}]}}",
            "G{items=G{element=[G{x=i32:2,y=i32:20},G{x=i32:3,y=i32:30}]}}"),
        columns(
            column("items.element.x", ints(0, 0, 0, 0, 1), ints(0, 1, 2, 2, 2),
                strings("1", "2", "3")),
            column("items.element.y", ints(0, 0, 0, 0, 1), ints(0, 1, 2, 3, 3),
                strings("20", "30"))),
        AvroMode.SUCCESS,
        strings(
            "{\"items\":null}",
            "{\"items\":[]}",
            "{\"items\":[{\"x\":1,\"y\":null}]}",
            "{\"items\":[{\"x\":2,\"y\":20},{\"x\":3,\"y\":30}]}"),
        IDENTITY);
  }

  private static CaseSpec rule3() {
    String schema = "message list_rule3_nested {\n"
        + "  optional group items (LIST) {\n"
        + "    repeated group array (LIST) {\n"
        + "      repeated int32 array;\n"
        + "    }\n"
        + "  }\n"
        + "}";
    return new CaseSpec(
        "list_rule3_nested",
        schema,
        FixtureCases::rule3Rows,
        strings(
            "G{items=null}",
            "G{items=G{array=[]}}",
            "G{items=G{array=[G{array=[]}]}}",
            "G{items=G{array=[G{array=[i32:1,i32:2]},G{array=[]},G{array=[i32:3]}]}}"),
        columns(column("items.array.array", ints(0, 0, 0, 0, 2, 1, 1),
            ints(0, 1, 2, 3, 3, 2, 3), strings("1", "2", "3"))),
        AvroMode.SUCCESS,
        strings(
            "{\"items\":null}",
            "{\"items\":[]}",
            "{\"items\":[[]]}",
            "{\"items\":[[1,2],[],[3]]}"),
        IDENTITY,
        "{\"type\":\"record\",\"name\":\"list_rule3_nested\",\"fields\":["
            + "{\"name\":\"items\",\"type\":[\"null\",{\"type\":\"array\","
            + "\"items\":{\"type\":\"array\",\"items\":\"int\"}}],\"default\":null}]}");
  }

  private static CaseSpec rule3UnannotatedDiagnostic() {
    String schema = "message list_rule3_unannotated_diagnostic {\n"
        + "  optional group items (LIST) {\n"
        + "    repeated group list {\n"
        + "      repeated int32 element;\n"
        + "    }\n"
        + "  }\n"
        + "}";
    return new CaseSpec(
        "list_rule3_unannotated_diagnostic",
        schema,
        FixtureCases::rule3UnannotatedRows,
        strings(
            "G{items=null}",
            "G{items=G{list=[]}}",
            "G{items=G{list=[G{element=[]}]}}",
            "G{items=G{list=[G{element=[i32:1,i32:2]},G{element=[]},G{element=[i32:3]}]}}"),
        columns(column("items.list.element", ints(0, 0, 0, 0, 2, 1, 1),
            ints(0, 1, 2, 3, 3, 2, 3), strings("1", "2", "3"))),
        AvroMode.REJECTED,
        strings(),
        IDENTITY);
  }

  private static CaseSpec rule4Array() {
    String schema = "message list_rule4_array {\n"
        + "  optional group items (LIST) {\n"
        + "    repeated group array {\n"
        + "      optional int32 value;\n"
        + "    }\n"
        + "  }\n"
        + "}";
    return new CaseSpec(
        "list_rule4_array",
        schema,
        factory -> rule4Rows(factory, "array", 4, true),
        strings(
            "G{items=null}",
            "G{items=G{array=[]}}",
            "G{items=G{array=[G{value=null}]}}",
            "G{items=G{array=[G{value=i32:4},G{value=null}]}}"),
        columns(column("items.array.value", ints(0, 0, 0, 0, 1), ints(0, 1, 2, 3, 2),
            strings("4"))),
        AvroMode.SUCCESS,
        strings(
            "{\"items\":null}",
            "{\"items\":[]}",
            "{\"items\":[{\"value\":null}]}",
            "{\"items\":[{\"value\":4},{\"value\":null}]}"),
        IDENTITY);
  }

  private static CaseSpec rule4Tuple() {
    String schema = "message list_rule4_tuple {\n"
        + "  optional group items (LIST) {\n"
        + "    repeated group items_tuple {\n"
        + "      optional int32 value;\n"
        + "    }\n"
        + "  }\n"
        + "}";
    return new CaseSpec(
        "list_rule4_tuple",
        schema,
        factory -> rule4Rows(factory, "items_tuple", 7, false),
        strings(
            "G{items=null}",
            "G{items=G{items_tuple=[]}}",
            "G{items=G{items_tuple=[G{value=i32:7}]}}",
            "G{items=G{items_tuple=[G{value=null},G{value=i32:8}]}}"),
        columns(column("items.items_tuple.value", ints(0, 0, 0, 0, 1),
            ints(0, 1, 3, 2, 3), strings("7", "8"))),
        AvroMode.SUCCESS,
        strings(
            "{\"items\":null}",
            "{\"items\":[]}",
            "{\"items\":[{\"value\":7}]}",
            "{\"items\":[{\"value\":null},{\"value\":8}]}"),
        IDENTITY);
  }

  private static CaseSpec rule5Required() {
    String schema = "message list_rule5_required {\n"
        + "  optional group items (LIST) {\n"
        + "    repeated group list {\n"
        + "      required int32 element;\n"
        + "    }\n"
        + "  }\n"
        + "}";
    return new CaseSpec(
        "list_rule5_required",
        schema,
        FixtureCases::rule5RequiredRows,
        strings(
            "G{items=null}",
            "G{items=G{list=[]}}",
            "G{items=G{list=[G{element=i32:10}]}}",
            "G{items=G{list=[G{element=i32:20},G{element=i32:30}]}}"),
        columns(column("items.list.element", ints(0, 0, 0, 0, 1), ints(0, 1, 2, 2, 2),
            strings("10", "20", "30"))),
        AvroMode.SUCCESS,
        strings(
            "{\"items\":null}",
            "{\"items\":[]}",
            "{\"items\":[10]}",
            "{\"items\":[20,30]}"),
        IDENTITY);
  }

  private static CaseSpec rule5OptionalPaired() {
    String schema = optionalRule5Schema("list_rule5_optional_paired");
    return new CaseSpec(
        "list_rule5_optional_paired",
        schema,
        factory -> rule5OptionalRows(factory, 4, false),
        strings(
            "G{items=null}",
            "G{items=G{list=[]}}",
            "G{items=G{list=[G{element=null}]}}",
            "G{items=G{list=[G{element=i32:4},G{element=null}]}}"),
        columns(column("items.list.element", ints(0, 0, 0, 0, 1), ints(0, 1, 2, 3, 2),
            strings("4"))),
        AvroMode.SUCCESS,
        strings(
            "{\"items\":null}",
            "{\"items\":[]}",
            "{\"items\":[null]}",
            "{\"items\":[4,null]}"),
        IDENTITY);
  }

  private static CaseSpec rule5OptionalExtended() {
    String schema = optionalRule5Schema("list_rule5_optional_extended");
    return new CaseSpec(
        "list_rule5_optional_extended",
        schema,
        factory -> rule5OptionalRows(factory, 5, true),
        strings(
            "G{items=null}",
            "G{items=G{list=[]}}",
            "G{items=G{list=[G{element=null}]}}",
            "G{items=G{list=[G{element=i32:5},G{element=null},G{element=i32:6}]}}"),
        columns(column("items.list.element", ints(0, 0, 0, 0, 1, 1),
            ints(0, 1, 2, 3, 2, 3), strings("5", "6"))),
        AvroMode.SUCCESS,
        strings(
            "{\"items\":null}",
            "{\"items\":[]}",
            "{\"items\":[null]}",
            "{\"items\":[5,null,6]}"),
        IDENTITY);
  }

  private static CaseSpec directListMap() {
    String schema = "message list_direct_map {\n"
        + "  optional group items (LIST) {\n"
        + "    repeated group map (MAP) {\n"
        + "      repeated group key_value {\n"
        + "        required int32 key;\n"
        + "        required int32 value;\n"
        + "      }\n"
        + "    }\n"
        + "  }\n"
        + "}";
    return new CaseSpec(
        "list_direct_map",
        schema,
        FixtureCases::directListMapRows,
        strings(
            "G{items=null}",
            "G{items=G{map=[]}}",
            "G{items=G{map=[G{key_value=[]}]}}",
            "G{items=G{map=[G{key_value=[G{key=i32:1,value=i32:10},G{key=i32:1,value=i32:20}]},G{key_value=[]},G{key_value=[G{key=i32:2,value=i32:30}]}]}}"),
        columns(
            column("items.map.key_value.key", ints(0, 0, 0, 0, 2, 1, 1),
                ints(0, 1, 2, 3, 3, 2, 3), strings("1", "1", "2")),
            column("items.map.key_value.value", ints(0, 0, 0, 0, 2, 1, 1),
                ints(0, 1, 2, 3, 3, 2, 3), strings("10", "20", "30"))),
        AvroMode.REJECTED,
        strings(),
        IDENTITY);
  }

  private static CaseSpec directListMapUtf8() {
    String schema = "message list_direct_map_utf8 {\n"
        + "  optional group items (LIST) {\n"
        + "    repeated group map (MAP) {\n"
        + "      repeated group key_value {\n"
        + "        required binary key (UTF8);\n"
        + "        required int32 value;\n"
        + "      }\n"
        + "    }\n"
        + "  }\n"
        + "}";
    return new CaseSpec(
        "list_direct_map_utf8",
        schema,
        FixtureCases::directListMapUtf8Rows,
        strings(
            "G{items=null}",
            "G{items=G{map=[]}}",
            "G{items=G{map=[G{key_value=[]}]}}",
            "G{items=G{map=[G{key_value=[G{key=utf8:a,value=i32:10},"
                + "G{key=utf8:a,value=i32:20}]},G{key_value=[]},"
                + "G{key_value=[G{key=utf8:b,value=i32:30}]}]}}"),
        columns(
            column("items.map.key_value.key", ints(0, 0, 0, 0, 2, 1, 1),
                ints(0, 1, 2, 3, 3, 2, 3), strings("utf8:a", "utf8:a", "utf8:b")),
            column("items.map.key_value.value", ints(0, 0, 0, 0, 2, 1, 1),
                ints(0, 1, 2, 3, 3, 2, 3), strings("10", "20", "30"))),
        AvroMode.SUCCESS,
        strings(
            "{\"items\":null}",
            "{\"items\":[]}",
            "{\"items\":[{}]}",
            "{\"items\":[{\"a\":20},{},{\"b\":30}]}"),
        IDENTITY,
        "{\"type\":\"record\",\"name\":\"list_direct_map_utf8\",\"fields\":["
            + "{\"name\":\"items\",\"type\":[\"null\",{\"type\":\"array\","
            + "\"items\":{\"type\":\"map\",\"values\":\"int\"}}],\"default\":null}]}");
  }

  private static CaseSpec standardMap() {
    String schema = "message map_standard {\n"
        + "  optional group map (MAP) {\n"
        + "    repeated group key_value {\n"
        + "      required binary key (UTF8);\n"
        + "      optional int32 value;\n"
        + "    }\n"
        + "  }\n"
        + "}";
    return mapCase("map_standard", schema, "map", "key_value", "key", "value");
  }

  private static CaseSpec arbitraryMapNames() {
    String schema = "message map_arbitrary_names {\n"
        + "  optional group bag (MAP) {\n"
        + "    repeated group pairs {\n"
        + "      required binary left (UTF8);\n"
        + "      optional int32 right;\n"
        + "    }\n"
        + "  }\n"
        + "}";
    return mapCase("map_arbitrary_names", schema, "bag", "pairs", "left", "right");
  }

  private static CaseSpec standaloneMapKeyValue() {
    String schema = "message map_standalone_mkv {\n"
        + "  optional group map (MAP_KEY_VALUE) {\n"
        + "    repeated group entries (MAP_KEY_VALUE) {\n"
        + "      required binary key (UTF8);\n"
        + "      optional int32 value;\n"
        + "    }\n"
        + "  }\n"
        + "}";
    return mapCase("map_standalone_mkv", schema, "map", "entries", "key", "value");
  }

  private static CaseSpec keyOnlyMap() {
    String schema = "message map_key_only {\n"
        + "  required group map (MAP) {\n"
        + "    repeated group key_value {\n"
        + "      required binary key (UTF8);\n"
        + "    }\n"
        + "  }\n"
        + "}";
    return new CaseSpec(
        "map_key_only",
        schema,
        FixtureCases::keyOnlyMapRows,
        strings(
            "G{map=G{key_value=[]}}",
            "G{map=G{key_value=[G{key=utf8:k1}]}}",
            "G{map=G{key_value=[G{key=utf8:k2},G{key=utf8:k2}]}}"),
        columns(column("map.key_value.key", ints(0, 0, 0, 1), ints(0, 1, 1, 1),
            strings("utf8:k1", "utf8:k2", "utf8:k2"))),
        AvroMode.REJECTED,
        strings(),
        IDENTITY);
  }

  private static CaseSpec mapCase(
      String id, String schema, String outer, String entry, String key, String value) {
    String prefix = outer + "." + entry + ".";
    String rawOuter = outer;
    String rawEntry = entry;
    String rawKey = key;
    String rawValue = value;
    List<String> rawRows = strings(
        "G{" + rawOuter + "=null}",
        "G{" + rawOuter + "=G{" + rawEntry + "=[]}}",
        "G{" + rawOuter + "=G{" + rawEntry + "=[G{" + rawKey + "=utf8:a," + rawValue + "=null}]}}",
        "G{" + rawOuter + "=G{" + rawEntry + "=[G{" + rawKey + "=utf8:a," + rawValue
            + "=i32:1},G{" + rawKey + "=utf8:a," + rawValue + "=i32:2},G{" + rawKey
            + "=utf8:b," + rawValue + "=i32:3}]}}",
        "G{" + rawOuter + "=G{" + rawEntry + "=[G{" + rawKey + "=utf8:c," + rawValue + "=i32:4}]}}");
    List<String> avroRows = strings(
        "{\"" + outer + "\":null}",
        "{\"" + outer + "\":{}}",
        "{\"" + outer + "\":{\"a\":null}}",
        "{\"" + outer + "\":{\"a\":2,\"b\":3}}",
        "{\"" + outer + "\":{\"c\":4}}");
    return new CaseSpec(
        id,
        schema,
        factory -> mapRows(factory, outer, entry, key, value),
        rawRows,
        columns(
            column(prefix + key, ints(0, 0, 0, 0, 1, 1, 0), ints(0, 1, 2, 2, 2, 2, 2),
                strings("utf8:a", "utf8:a", "utf8:a", "utf8:b", "utf8:c")),
            column(prefix + value, ints(0, 0, 0, 0, 1, 1, 0), ints(0, 1, 2, 3, 3, 3, 3),
                strings("1", "2", "3", "4"))),
        AvroMode.SUCCESS,
        avroRows,
        IDENTITY);
  }

  private static String optionalRule5Schema(String messageName) {
    return "message " + messageName + " {\n"
        + "  optional group items (LIST) {\n"
        + "    repeated group list {\n"
        + "      optional int32 element;\n"
        + "    }\n"
        + "  }\n"
        + "}";
  }

  private static List<Group> rule1Rows(SimpleGroupFactory factory) {
    List<Group> rows = new ArrayList<>();
    rows.add(factory.newGroup());
    rows.add(presentEmpty(factory, "items"));
    Group one = factory.newGroup();
    one.addGroup("items").append("element", 10);
    rows.add(one);
    Group two = factory.newGroup();
    two.addGroup("items").append("element", 20).append("element", 30);
    rows.add(two);
    return rows;
  }

  private static List<Group> rule2Rows(SimpleGroupFactory factory) {
    List<Group> rows = new ArrayList<>();
    rows.add(factory.newGroup());
    rows.add(presentEmpty(factory, "items"));
    Group one = factory.newGroup();
    one.addGroup("items").addGroup("element").append("x", 1);
    rows.add(one);
    Group two = factory.newGroup();
    Group items = two.addGroup("items");
    items.addGroup("element").append("x", 2).append("y", 20);
    items.addGroup("element").append("x", 3).append("y", 30);
    rows.add(two);
    return rows;
  }

  private static List<Group> rule3Rows(SimpleGroupFactory factory) {
    List<Group> rows = new ArrayList<>();
    rows.add(factory.newGroup());
    rows.add(presentEmpty(factory, "items"));
    Group one = factory.newGroup();
    one.addGroup("items").addGroup("array");
    rows.add(one);
    Group nested = factory.newGroup();
    Group items = nested.addGroup("items");
    items.addGroup("array").append("array", 1).append("array", 2);
    items.addGroup("array");
    items.addGroup("array").append("array", 3);
    rows.add(nested);
    return rows;
  }

  private static List<Group> rule3UnannotatedRows(SimpleGroupFactory factory) {
    List<Group> rows = new ArrayList<>();
    rows.add(factory.newGroup());
    rows.add(presentEmpty(factory, "items"));
    Group one = factory.newGroup();
    one.addGroup("items").addGroup("list");
    rows.add(one);
    Group nested = factory.newGroup();
    Group items = nested.addGroup("items");
    items.addGroup("list").append("element", 1).append("element", 2);
    items.addGroup("list");
    items.addGroup("list").append("element", 3);
    rows.add(nested);
    return rows;
  }

  private static List<Group> rule4Rows(
      SimpleGroupFactory factory, String wrapper, int firstValue, boolean firstNull) {
    List<Group> rows = new ArrayList<>();
    rows.add(factory.newGroup());
    rows.add(presentEmpty(factory, "items"));
    Group one = factory.newGroup();
    Group first = one.addGroup("items").addGroup(wrapper);
    if (!firstNull) {
      first.append("value", firstValue);
    }
    rows.add(one);
    Group two = factory.newGroup();
    Group items = two.addGroup("items");
    if (firstNull) {
      items.addGroup(wrapper).append("value", firstValue);
      items.addGroup(wrapper);
    } else {
      items.addGroup(wrapper);
      items.addGroup(wrapper).append("value", 8);
    }
    rows.add(two);
    return rows;
  }

  private static List<Group> rule5RequiredRows(SimpleGroupFactory factory) {
    List<Group> rows = new ArrayList<>();
    rows.add(factory.newGroup());
    rows.add(presentEmpty(factory, "items"));
    Group one = factory.newGroup();
    one.addGroup("items").addGroup("list").append("element", 10);
    rows.add(one);
    Group two = factory.newGroup();
    Group items = two.addGroup("items");
    items.addGroup("list").append("element", 20);
    items.addGroup("list").append("element", 30);
    rows.add(two);
    return rows;
  }

  private static List<Group> rule5OptionalRows(
      SimpleGroupFactory factory, int firstValue, boolean extended) {
    List<Group> rows = new ArrayList<>();
    rows.add(factory.newGroup());
    rows.add(presentEmpty(factory, "items"));
    Group one = factory.newGroup();
    one.addGroup("items").addGroup("list");
    rows.add(one);
    Group two = factory.newGroup();
    Group items = two.addGroup("items");
    items.addGroup("list").append("element", firstValue);
    items.addGroup("list");
    if (extended) {
      items.addGroup("list").append("element", 6);
    }
    rows.add(two);
    return rows;
  }

  private static List<Group> directListMapRows(SimpleGroupFactory factory) {
    List<Group> rows = new ArrayList<>();
    rows.add(factory.newGroup());
    rows.add(presentEmpty(factory, "items"));
    Group one = factory.newGroup();
    one.addGroup("items").addGroup("map");
    rows.add(one);
    Group nested = factory.newGroup();
    Group items = nested.addGroup("items");
    Group first = items.addGroup("map");
    first.addGroup("key_value").append("key", 1).append("value", 10);
    first.addGroup("key_value").append("key", 1).append("value", 20);
    items.addGroup("map");
    items.addGroup("map").addGroup("key_value").append("key", 2).append("value", 30);
    rows.add(nested);
    return rows;
  }

  private static List<Group> directListMapUtf8Rows(SimpleGroupFactory factory) {
    List<Group> rows = new ArrayList<>();
    rows.add(factory.newGroup());
    rows.add(presentEmpty(factory, "items"));
    Group one = factory.newGroup();
    one.addGroup("items").addGroup("map");
    rows.add(one);
    Group nested = factory.newGroup();
    Group items = nested.addGroup("items");
    Group first = items.addGroup("map");
    first.addGroup("key_value").append("key", "a").append("value", 10);
    first.addGroup("key_value").append("key", "a").append("value", 20);
    items.addGroup("map");
    items.addGroup("map").addGroup("key_value").append("key", "b").append("value", 30);
    rows.add(nested);
    return rows;
  }

  private static List<Group> mapRows(
      SimpleGroupFactory factory, String outer, String entry, String key, String value) {
    List<Group> rows = new ArrayList<>();
    rows.add(factory.newGroup());
    rows.add(presentEmpty(factory, outer));
    Group nullValue = factory.newGroup();
    nullValue.addGroup(outer).addGroup(entry).append(key, "a");
    rows.add(nullValue);
    Group duplicate = factory.newGroup();
    Group duplicateMap = duplicate.addGroup(outer);
    duplicateMap.addGroup(entry).append(key, "a").append(value, 1);
    duplicateMap.addGroup(entry).append(key, "a").append(value, 2);
    duplicateMap.addGroup(entry).append(key, "b").append(value, 3);
    rows.add(duplicate);
    Group last = factory.newGroup();
    last.addGroup(outer).addGroup(entry).append(key, "c").append(value, 4);
    rows.add(last);
    return rows;
  }

  private static List<Group> keyOnlyMapRows(SimpleGroupFactory factory) {
    List<Group> rows = new ArrayList<>();
    rows.add(presentEmpty(factory, "map"));
    Group one = factory.newGroup();
    one.addGroup("map").addGroup("key_value").append("key", "k1");
    rows.add(one);
    Group duplicate = factory.newGroup();
    Group map = duplicate.addGroup("map");
    map.addGroup("key_value").append("key", "k2");
    map.addGroup("key_value").append("key", "k2");
    rows.add(duplicate);
    return rows;
  }

  private static Group presentEmpty(SimpleGroupFactory factory, String field) {
    Group root = factory.newGroup();
    root.addGroup(field);
    return root;
  }

  private static ExpectedColumn column(
      String path, List<Integer> repetition, List<Integer> definition, List<String> dense) {
    return new ExpectedColumn(path, repetition, definition, dense);
  }

  @SafeVarargs
  private static List<ExpectedColumn> columns(ExpectedColumn... columns) {
    return List.of(columns);
  }

  private static List<Integer> ints(Integer... values) {
    return List.of(values);
  }

  private static List<String> strings(String... values) {
    return List.of(values);
  }
}
