package io.queryaudit.core.reporter;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.AuditOutcome;
import io.queryaudit.core.model.IncompleteReasonCode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class AuditRunSchemaContractTest {

  private static final Path REPORT_SCHEMA =
      Path.of("..", "docs", "schema", "report-1.2.schema.json");

  @Test
  void schemaRequiresTheSuiteResultFields() throws IOException {
    Map<?, ?> schema = readSchema();

    assertThat(((List<?>) schema.get("required")).stream().map(String.class::cast).toList())
        .contains("schemaVersion", "outcome", "incompleteReasons", "reports");
  }

  @Test
  void schemaOutcomeValuesMatchTheCoreModel() throws IOException {
    Map<?, ?> properties = object(readSchema(), "properties");
    Map<?, ?> outcome = object(properties, "outcome");

    assertThat(strings(outcome, "enum"))
        .containsExactlyInAnyOrderElementsOf(
            Arrays.stream(AuditOutcome.values()).map(Enum::name).collect(Collectors.toSet()));
  }

  @Test
  void schemaIncompleteReasonCodesMatchTheCoreModel() throws IOException {
    Map<?, ?> definitions = object(readSchema(), "definitions");
    Map<?, ?> reason = object(definitions, "incompleteReason");
    Map<?, ?> reasonProperties = object(reason, "properties");
    Map<?, ?> code = object(reasonProperties, "code");

    assertThat(strings(code, "enum"))
        .containsExactlyInAnyOrderElementsOf(
            Arrays.stream(IncompleteReasonCode.values())
                .map(Enum::name)
                .collect(Collectors.toSet()));
  }

  private static Map<?, ?> readSchema() throws IOException {
    return (Map<?, ?>) MiniJsonParser.parse(Files.readString(REPORT_SCHEMA));
  }

  private static Map<?, ?> object(Map<?, ?> parent, String field) {
    return (Map<?, ?>) parent.get(field);
  }

  private static Set<String> strings(Map<?, ?> parent, String field) {
    return ((List<?>) parent.get(field))
        .stream().map(String.class::cast).collect(Collectors.toSet());
  }
}
