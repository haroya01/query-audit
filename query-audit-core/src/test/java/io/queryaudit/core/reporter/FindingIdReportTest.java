package io.queryaudit.core.reporter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import io.queryaudit.core.config.ReportRedaction;
import io.queryaudit.core.detector.FindByIdForAssociationDetector;
import io.queryaudit.core.interceptor.LazyLoadTracker.ExplicitLoadRecord;
import io.queryaudit.core.model.AuditRunResult;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FindingIdReportTest {

  private static final ObjectMapper JSON = new ObjectMapper();

  @Test
  void everyCategoryHasAnIdIndependentOfArtifactRedaction() throws Exception {
    Issue confirmed = issue("account_id", "fixture-secret", Severity.ERROR);
    Issue info = issue("owner_id", "fixture-secret", Severity.INFO);
    Issue acknowledged = issue("tenant_id", "fixture-secret", Severity.WARNING);
    QueryAuditReport report = report(List.of(confirmed), List.of(info), List.of(acknowledged));
    String full = JsonReporter.toJson(report, ReportRedaction.FULL);
    String redacted = JsonReporter.toJson(report);

    assertThat(full).contains("fixture-secret", "private-project");
    assertThat(redacted).doesNotContain("fixture-secret", "private-project");
    for (String category : List.of("confirmedIssues", "infoIssues", "acknowledgedIssues")) {
      String id = JSON.readTree(full).get(category).get(0).get("findingId").asText();
      assertThat(id).matches("qa-finding-v1:[0-9a-f]{64}");
      assertThat(JSON.readTree(redacted).get(category).get(0).get("findingId").asText())
          .isEqualTo(id);
    }
    assertCurrentSchema(report);
  }

  @Test
  void groupsRepeatedQueriesWithoutLosingDiagnosticsOrLoweringSeverity() throws Exception {
    Issue warning = issue("account_id", "first-secret", Severity.WARNING);
    Issue error = issue("account_id", "second-secret", Severity.ERROR);
    QueryAuditReport report = report(List.of(warning, error), List.of(), List.of());
    JsonNode full = JSON.readTree(JsonReporter.toJson(report, ReportRedaction.FULL));
    JsonNode findings = full.get("confirmedIssues");

    assertThat(findings.size()).isEqualTo(1);
    assertThat(full.get("summary").get("confirmedIssues").asInt()).isEqualTo(2);
    assertThat(findings.get(0).get("severity").asText()).isEqualTo("ERROR");
    JsonNode occurrences = findings.get(0).get("occurrences");
    assertThat(occurrences.size()).isEqualTo(2);
    assertThat(occurrences.get(0).get("detail").asText()).contains("first-secret");
    assertThat(occurrences.get(1).get("detail").asText()).contains("second-secret");
    assertThat(occurrences.get(0).has("findingId")).isFalse();
    assertThat(occurrences.get(0).has("occurrences")).isFalse();
    assertThat(report.getConfirmedIssues()).containsExactly(warning, error);
    assertThat(JsonReporter.toJson(report))
        .doesNotContain("first-secret", "second-secret", "private-project");
    String envelope = ComparisonInputFixtures.json(AuditRunResult.fail(List.of(report)));
    assertThat(ReportComparator.compare(envelope, envelope).persisting()).hasSize(1);
    assertCurrentSchema(report);
  }

  @Test
  void repeatedAssociationLoadsProduceOneFindingWithBothObservations() throws Exception {
    String frame = "com.example.OrderService.create:42";
    List<ExplicitLoadRecord> loads =
        List.of(
            new ExplicitLoadRecord("com.example.Account", "first-secret", 1, frame),
            new ExplicitLoadRecord("com.example.Account", "second-secret", 2, frame));
    List<QueryRecord> queries =
        List.of(new QueryRecord("INSERT INTO orders(account_id) VALUES (?)", 1, 3, frame));
    List<Issue> issues = new FindByIdForAssociationDetector().evaluate(loads, List.of(), queries);
    assertThat(issues).hasSize(2);
    QueryAuditReport report = report(List.of(), issues, List.of());
    JsonNode findings = JSON.readTree(JsonReporter.toJson(report)).get("infoIssues");
    assertThat(findings.size()).isEqualTo(1);
    assertThat(findings.get(0).get("occurrences").size()).isEqualTo(2);
    assertThat(findings.toString()).doesNotContain("first-secret", "second-secret");
    assertCurrentSchema(report);
  }

  @Test
  void rejectsConflictingCategoriesForTheSameFinding() {
    Issue issue = issue("account_id", "fixture-secret", Severity.WARNING);
    QueryAuditReport report = report(List.of(issue), List.of(), List.of(issue));
    assertThatThrownBy(() -> JsonReporter.toJson(report))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("more than one issue category");
  }

  @ParameterizedTest
  @ValueSource(strings = {"confirmedIssues", "infoIssues", "acknowledgedIssues"})
  void schemaRequiresARecognizedFindingIdInEveryCategory(String category) throws Exception {
    QueryAuditReport report =
        report(
            List.of(issue("account_id", "value", Severity.ERROR)),
            List.of(issue("owner_id", "value", Severity.INFO)),
            List.of(issue("tenant_id", "value", Severity.WARNING)));
    JsonNode envelope =
        JSON.readTree(JsonReporter.toRunEnvelopeJson(AuditRunResult.fail(List.of(report))));
    ObjectNode finding = (ObjectNode) envelope.get("reports").get(0).get(category).get(0);
    var schema = currentSchema();
    finding.remove("findingId");
    assertThat(schema.validate(envelope)).isNotEmpty();
    finding.put("findingId", "qa-finding-v2:" + "a".repeat(64));
    assertThat(schema.validate(envelope)).isNotEmpty();
    finding.put("findingId", "qa-finding-v1:" + "a".repeat(64));
    assertThat(schema.validate(envelope)).isEmpty();
  }

  @Test
  void occurrenceEvidenceCannotContainNestedFindingIds() throws Exception {
    Issue first = issue("account_id", "first", Severity.WARNING);
    Issue second = issue("account_id", "second", Severity.WARNING);
    QueryAuditReport report = report(List.of(first, second), List.of(), List.of());
    JsonNode envelope =
        JSON.readTree(JsonReporter.toRunEnvelopeJson(AuditRunResult.fail(List.of(report))));
    JsonNode finding = envelope.get("reports").get(0).get("confirmedIssues").get(0);
    ObjectNode occurrence = (ObjectNode) finding.get("occurrences").get(0);
    occurrence.set("findingId", finding.get("findingId"));
    assertThat(currentSchema().validate(envelope)).isNotEmpty();
  }

  @Test
  @SuppressWarnings("deprecation")
  void legacyWriterKeepsIndividualIssuesWithoutClaimingStableIds() throws Exception {
    QueryAuditReport report =
        report(
            List.of(
                issue("account_id", "first-secret", Severity.WARNING),
                issue("account_id", "second-secret", Severity.WARNING)),
            List.of(),
            List.of());
    String legacy = JsonReporter.toEnvelopeJson(List.of(report));
    assertThat(legacy).doesNotContain("findingId", "occurrences");
    assertThat(JSON.readTree(legacy).get("reports").get(0).get("confirmedIssues").size())
        .isEqualTo(2);
  }

  private static void assertCurrentSchema(QueryAuditReport report) throws Exception {
    var schema = currentSchema();
    for (ReportRedaction mode : ReportRedaction.values()) {
      String json = JsonReporter.toRunEnvelopeJson(AuditRunResult.fail(List.of(report)), mode);
      assertThat(schema.validate(JSON.readTree(json))).isEmpty();
    }
  }

  private static JsonSchema currentSchema() {
    return JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7)
        .getSchema(FindingIdReportTest.class.getResourceAsStream("/report-1.7.schema.json"));
  }

  private static QueryAuditReport report(
      List<Issue> confirmed, List<Issue> info, List<Issue> acknowledged) {
    return new QueryAuditReport(
            "AccountTest", "loadsAccount", confirmed, info, acknowledged, List.of(), 1, 0, 0)
        .withTestIdentity("account-test:loads", null);
  }

  private static Issue issue(String column, String value, Severity severity) {
    return new Issue(
        IssueType.MISSING_WHERE_INDEX,
        severity,
        "SELECT * FROM accounts WHERE " + column + " = '" + value + "'",
        "accounts",
        column,
        "Value: " + value,
        "Review " + value,
        "at com.example.AccountRepository.load(/private-project/AccountRepository.java:42)");
  }
}
