package io.queryaudit.core.reporter;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.TestSelector;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TestIdentitySchemaContractTest {

  private static final Path REPORT_SCHEMA =
      Path.of("..", "docs", "schema", "report-1.5.schema.json");

  @Test
  void reporterIdentityFieldsMatchThePublishedSchema() throws IOException {
    Map<?, ?> schema = object(MiniJsonParser.parse(Files.readString(REPORT_SCHEMA)));
    Map<?, ?> definitions = object(schema.get("definitions"));
    Map<?, ?> testReport = object(definitions.get("testReport"));
    List<?> required = list(testReport.get("required"));
    Map<?, ?> properties = object(testReport.get("properties"));

    assertThat(required.contains("testId")).isTrue();
    assertThat(required.contains("testName")).isTrue();
    assertThat(required.contains("testSelector")).isTrue();
    assertThat(properties.containsKey("testId")).isTrue();
    assertThat(properties.containsKey("testName")).isTrue();
    assertThat(properties.containsKey("testSelector")).isTrue();

    String uniqueId = "[engine:junit-jupiter]/[class:example.OrderTest]/[method:loadsOrders()]";
    QueryAuditReport report =
        emptyReport().withTestIdentity(uniqueId, new TestSelector("junit-unique-id", uniqueId));
    Map<?, ?> json = object(MiniJsonParser.parse(JsonReporter.toJson(report)));
    Map<?, ?> selector = object(json.get("testSelector"));

    assertThat(json.get("testId")).isEqualTo(uniqueId);
    assertThat(json.get("testName")).isEqualTo("loads orders");
    assertThat(selector.get("type")).isEqualTo("junit-unique-id");
    assertThat(selector.get("value")).isEqualTo(uniqueId);
  }

  @Test
  void coreFallbackMatchesTheNullableSelectorContract() {
    Map<?, ?> json = object(MiniJsonParser.parse(JsonReporter.toJson(emptyReport())));

    assertThat(json.get("testId")).isInstanceOf(String.class).asString().isNotBlank();
    assertThat(json.containsKey("testSelector")).isTrue();
    assertThat(json.get("testSelector")).isNull();
  }

  private static QueryAuditReport emptyReport() {
    return new QueryAuditReport(
        "example.OrderTest", "loads orders", List.of(), List.of(), List.of(), 0, 0, 0L);
  }

  private static Map<?, ?> object(Object value) {
    assertThat(value).isInstanceOf(Map.class);
    return (Map<?, ?>) value;
  }

  private static List<?> list(Object value) {
    assertThat(value).isInstanceOf(List.class);
    return (List<?>) value;
  }
}
