package io.queryaudit.core.reporter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.queryaudit.core.config.ReportRedaction;
import io.queryaudit.core.model.AuditOutcome;
import io.queryaudit.core.model.AuditRunResult;
import io.queryaudit.core.model.IncompleteReasonCode;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.provenance.ComparisonInputs;
import io.queryaudit.core.reporter.ComparisonEnvelope.ReportedFinding;
import io.queryaudit.core.reporter.ComparisonEnvelope.SchemaVersion;
import io.queryaudit.core.reporter.ComparisonEnvelope.TestReport;
import io.queryaudit.core.reporter.ReportComparator.TestRef;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ComparisonEnvelopeReaderTest {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final String TEST_ID = "test:load";
  private static final String FINDING_ID = FindingId.PREFIX + "a".repeat(64);

  @Test
  void projectsValidatedFieldsWithoutRequiringPresentationOnlyFields() throws Exception {
    ObjectNode document = document();
    ObjectNode report = (ObjectNode) document.path("reports").get(0);
    ObjectNode finding = (ObjectNode) report.path("confirmedIssues").get(0);
    finding.put("type", "acme:query/budget");
    finding.remove(List.of("severity", "suggestion", "remediation", "occurrenceCount"));
    report.remove(List.of("queries", "detectionContext"));

    ComparisonEnvelope input = ComparisonEnvelopeReader.read(document.toString());

    assertThat(input.schemaVersion().text()).isEqualTo(JsonReporter.SCHEMA_VERSION);
    assertThat(input.hasFindingIds()).isTrue();
    assertThat(input.redaction()).isEqualTo(ReportRedaction.FULL);
    assertThat(input.comparisonInputs()).containsKey(input.reports().get(0).testId());
    TestReport typed = input.reports().get(0);
    assertThat(typed.ref().testClass()).isEqualTo("OrdersTest");
    assertThat(typed.totalQueries()).isEqualTo(12);
    assertThat(typed.executionTimeMs()).isEqualTo(7);
    assertThat(typed.confirmed())
        .singleElement()
        .satisfies(
            value -> {
              assertThat(value.findingId()).startsWith(FindingId.PREFIX);
              assertThat(value.type()).isEqualTo("acme:query/budget");
              assertThat(value.query()).isEqualTo("SELECT * FROM orders");
              assertThat(value.table()).isEqualTo("orders");
              assertThat(value.column()).isNull();
              assertThat(value.detail()).isEqualTo("repeated query");
            });
  }

  @ParameterizedTest
  @ValueSource(strings = {"confirmedIssues", "infoIssues", "acknowledgedIssues"})
  void rejectsMalformedEvidenceAtTheReaderBoundaryInEveryBucket(String bucket) throws Exception {
    ObjectNode document = document();
    ObjectNode report = (ObjectNode) document.path("reports").get(0);
    ObjectNode finding = ((ObjectNode) report.path("confirmedIssues").get(0)).deepCopy();
    ((ArrayNode) report.path("confirmedIssues")).removeAll();
    finding.put("query", 42);
    ((ArrayNode) report.path(bucket)).add(finding);

    assertThatThrownBy(() -> ComparisonEnvelopeReader.read(document.toString()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("reports[0]." + bucket + "[0].query must be a string or null");
  }

  @Test
  void rejectsDuplicateRecordedIdsAcrossBucketsBeforeComparison() throws Exception {
    ObjectNode document = document();
    ObjectNode report = (ObjectNode) document.path("reports").get(0);
    ((ArrayNode) report.path("infoIssues")).add(report.path("confirmedIssues").get(0).deepCopy());

    assertThatThrownBy(() -> ComparisonEnvelopeReader.read(document.toString()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("infoIssues[0].findingId duplicates another finding in this test");
  }

  @Test
  void preservesLegacyAbsentBucketsAndMissingOutcomeAsInconclusive() {
    ComparisonEnvelope legacy =
        ComparisonEnvelopeReader.read(
            """
        {"schemaVersion":"1.0.0","reports":[
          {"testClass":null,"testName":"load","summary":{"totalQueries":3,"executionTimeMs":4},
           "confirmedIssues":[{"type":"N_PLUS_ONE","query":null,"sourceLocation":null,
                               "table":null,"detail":null}]}]}
        """);

    assertThat(legacy.outcome()).isEqualTo(AuditOutcome.INCONCLUSIVE);
    assertThat(legacy.incompleteReasons())
        .extracting(reason -> reason.code())
        .containsExactly(IncompleteReasonCode.UNSUPPORTED_SCHEMA);
    assertThat(legacy.redaction()).isEqualTo(ReportRedaction.FULL);
    assertThat(legacy.hasFindingIds()).isFalse();
    assertThat(legacy.reports().get(0).confirmed().get(0).findingId()).isNull();
    assertThat(legacy.reports().get(0).confirmed().get(0).column()).isNull();
    assertThat(legacy.reports().get(0).info()).isEmpty();
    assertThat(legacy.reports().get(0).acknowledged()).isEmpty();
  }

  @Test
  void rejectsMalformedSummaryBeforeItCanReachTypedComparison() throws Exception {
    ObjectNode document = document();
    ((ObjectNode) document.path("reports").get(0).path("summary")).put("totalQueries", "12");

    assertThatThrownBy(() -> ComparisonEnvelopeReader.read(document.toString()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("reports[0].summary.totalQueries must be an integer");
  }

  @Test
  void typedComparisonInputTakesImmutableSnapshots() {
    List<ReportedFinding> findings = new ArrayList<>(List.of(finding()));
    TestReport report = testReport(2, 7, findings, List.of());
    List<TestReport> reports = new ArrayList<>(List.of(report));
    Map<String, ComparisonInputs> inputs = new LinkedHashMap<>();
    inputs.put(TEST_ID, ComparisonInputFixtures.defaults());
    ComparisonEnvelope envelope =
        new ComparisonEnvelope(
            AuditOutcome.PASS,
            List.of(),
            reports,
            ReportRedaction.REDACTED,
            null,
            inputs,
            new SchemaVersion(1, 7, "1.7.0"));
    findings.clear();
    reports.clear();
    inputs.clear();

    assertThat(envelope.reports()).containsExactly(report);
    assertThat(report.confirmed()).containsExactly(finding());
    assertThat(envelope.comparisonInputs()).containsKey(TEST_ID);
    assertThatThrownBy(() -> report.confirmed().clear())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> envelope.reports().clear())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> envelope.comparisonInputs().clear())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void engineUsesTypedCountsAndAllBucketsWithoutJson() {
    ComparisonEnvelope before = input(testReport(12, 7, List.of(finding()), List.of()));
    ComparisonEnvelope after = input(testReport(3, 2, List.of(), List.of(finding())));

    ReportComparator.Verdict verdict =
        ReportComparisonEngine.compare(before, after, List.of(FINDING_ID));

    assertThat(verdict.queriesBefore()).isEqualTo(12);
    assertThat(verdict.queriesAfter()).isEqualTo(3);
    assertThat(verdict.executionTimeMsBefore()).isEqualTo(7);
    assertThat(verdict.executionTimeMsAfter()).isEqualTo(2);
    assertThat(verdict.resolved()).hasSize(1);
    assertThat(verdict.newFindings()).isEmpty();
    assertThat(verdict.outcome()).isEqualTo(AuditOutcome.FAIL);
    assertThat(verdict.targetResolutions())
        .containsExactly(
            new TargetResolution(FINDING_ID, TEST_ID, TargetResolution.Status.PERSISTING));
  }

  private static ObjectNode document() throws Exception {
    Issue issue =
        new Issue(
            IssueType.N_PLUS_ONE,
            Severity.ERROR,
            "SELECT * FROM orders",
            "orders",
            null,
            "repeated query",
            "batch it",
            "at OrdersTest.load:7");
    QueryAuditReport report =
        new QueryAuditReport(
            "OrdersTest",
            "load",
            List.of(issue),
            List.of(),
            List.of(),
            List.of(),
            1,
            12,
            7_000_000L);
    return (ObjectNode)
        JSON.readTree(
            ComparisonInputFixtures.json(
                AuditRunResult.pass(List.of(report)), ReportRedaction.FULL));
  }

  private static ReportedFinding finding() {
    return new ReportedFinding(FINDING_ID, "acme:query/budget", null, null, null, null, null);
  }

  private static TestReport testReport(
      long queries, long duration, List<ReportedFinding> confirmed, List<ReportedFinding> info) {
    return new TestReport(
        TEST_ID,
        new TestRef(TEST_ID, "OrdersTest", "load"),
        queries,
        duration,
        confirmed,
        info,
        List.of());
  }

  private static ComparisonEnvelope input(TestReport report) {
    return new ComparisonEnvelope(
        AuditOutcome.PASS,
        List.of(),
        List.of(report),
        ReportRedaction.REDACTED,
        null,
        Map.of(TEST_ID, ComparisonInputFixtures.defaults()),
        new SchemaVersion(1, 7, "1.7.0"));
  }
}
