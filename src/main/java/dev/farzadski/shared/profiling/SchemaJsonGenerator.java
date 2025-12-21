package dev.farzadski.shared.profiling;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.util.List;

public class SchemaJsonGenerator {
  public static void generate(List<String> columns, String outputPath) {
    try {
      ObjectMapper mapper = new ObjectMapper();
      ObjectNode root = mapper.createObjectNode();
      root.putPOJO("_COLUMNS", columns);

      mapper.writerWithDefaultPrettyPrinter().writeValue(new File(outputPath), root);

    } catch (Exception e) {
      throw new RuntimeException("FAILED_TO_GENERATE_SCHEMA_JSON", e);
    }
  }
}
