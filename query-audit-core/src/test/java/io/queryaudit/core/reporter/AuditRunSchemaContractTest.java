package io.queryaudit.core.reporter;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import io.queryaudit.core.model.AuditCoverage;
import io.queryaudit.core.model.AuditOutcome;
import io.queryaudit.core.model.AuditRunResult;
import io.queryaudit.core.model.IncompleteReasonCode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class AuditRunSchemaContractTest {

  private static final Path REPORT_SCHEMA = Path.of("..", "docs", "schema", "report.schema.json");

  @Test
  void schemaRequiresTheSuiteResultFields() throws IOException {
    Map<?, ?> schema = readSchema();

    assertThat(((List<?>) schema.get("required")).stream().map(String.class::cast).toList())
        .contains("schemaVersion", "outcome", "incompleteReasons", "redaction", "reports", "coverage");
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

  @Test
  void schemaCoverageGapsMatchTheCoreModel() throws IOException {
    Map<?, ?> definitions = object(readSchema(), "definitions");
    Map<?, ?> properties = object(object(definitions, "coverageTest"), "properties");
    List<?> accepted = (List<?>) object(properties, "gap").get("enum");

    assertThat(accepted.contains(null)).isTrue();
    assertThat(accepted.stream().filter(Objects::nonNull).map(String.class::cast).toList())
        .containsExactlyInAnyOrderElementsOf(
            Arrays.stream(AuditCoverage.Gap.values()).map(Enum::name).toList());
  }

  @Test
  void currentSchemaAcceptsUnverifiedCoverageAndEveryCoverageGap() throws IOException {
    JsonSchema schema = validator();
    ObjectMapper mapper = new ObjectMapper();
    String unverified = JsonReporter.toRunEnvelopeJson(AuditRunResult.pass(List.of()));
    assertThat(schema.validate(mapper.readTree(unverified))).isEmpty();

    for (AuditCoverage.Gap gap : AuditCoverage.Gap.values()) {
      boolean executed =
          gap == AuditCoverage.Gap.ABORTED
              || gap == AuditCoverage.Gap.TEST_FAILED
              || gap == AuditCoverage.Gap.AUDIT_MISSING;
      AuditCoverage coverage =
          new AuditCoverage(
              List.of(new AuditCoverage.Test("expected-test", true, executed, false, gap)));
      String json =
          JsonReporter.toRunEnvelopeJson(AuditRunResult.pass(List.of()).withCoverage(coverage));

      assertThat(schema.validate(mapper.readTree(json))).as("coverage gap %s", gap).isEmpty();
    }
  }

  @Test
  void currentSchemaRejectsMissingCoverageAndContradictoryAuditStates() throws IOException {
    JsonSchema schema = validator();
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode unverified =
        (ObjectNode)
            mapper.readTree(JsonReporter.toRunEnvelopeJson(AuditRunResult.pass(List.of())));
    unverified.remove("coverage");
    assertThat(schema.validate(unverified)).isNotEmpty();

    AuditCoverage coverage =
        new AuditCoverage(
            List.of(
                new AuditCoverage.Test(
                    "expected-test", true, false, false, AuditCoverage.Gap.SKIPPED)));
    ObjectNode skipped =
        (ObjectNode)
            mapper.readTree(
                JsonReporter.toRunEnvelopeJson(
                    AuditRunResult.pass(List.of()).withCoverage(coverage)));

    ObjectNode passing = skipped.deepCopy();
    passing.put("outcome", "PASS");
    passing.putArray("incompleteReasons");
    assertThat(schema.validate(passing)).isNotEmpty();

    ObjectNode audited = skipped.deepCopy();
    ((ObjectNode) audited.path("coverage").path("tests").get(0)).put("audited", true);
    assertThat(schema.validate(audited)).isNotEmpty();

    ObjectNode missingGap = skipped.deepCopy();
    ((ObjectNode) missingGap.path("coverage").path("tests").get(0)).putNull("gap");
    assertThat(schema.validate(missingGap)).isNotEmpty();
  }

  private static JsonSchema validator() throws IOException {
    Map<?, ?> alias = (Map<?, ?>) MiniJsonParser.parse(Files.readString(REPORT_SCHEMA));
    Path versioned = REPORT_SCHEMA.resolveSibling((String) alias.get("$ref"));
    try (var input = Files.newInputStream(versioned)) {
      return JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7).getSchema(input);
    }
  }

  private static Map<?, ?> readSchema() throws IOException {
    Map<?, ?> alias = (Map<?, ?>) MiniJsonParser.parse(Files.readString(REPORT_SCHEMA));
    Path versioned = REPORT_SCHEMA.resolveSibling((String) alias.get("$ref"));
    return (Map<?, ?>) MiniJsonParser.parse(Files.readString(versioned));
  }

  private static Map<?, ?> object(Map<?, ?> parent, String field) {
    return (Map<?, ?>) parent.get(field);
  }

  private static Set<String> strings(Map<?, ?> parent, String field) {
    return ((List<?>) parent.get(field))
        .stream().map(String.class::cast).collect(Collectors.toSet());
  }
}
