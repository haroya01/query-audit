package io.queryaudit.core.reporter.delivery;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.queryaudit.core.extension.FindingKindId;
import io.queryaudit.core.model.AuditIncompleteReason;
import io.queryaudit.core.model.AuditOutcome;
import io.queryaudit.core.model.AuditRunResult;
import io.queryaudit.core.model.Finding;
import io.queryaudit.core.model.IncompleteReasonCode;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.model.TestSelector;
import io.queryaudit.core.reporter.FindingId;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class PublishedAuditRunTest {
  private static final String SECRET = "customer-secret-42";
  private static final String SQL = "SELECT * FROM users WHERE password = '" + SECRET + "'";
  private static final Issue ISSUE =
      new Issue(
          IssueType.SELECT_ALL,
          Severity.ERROR,
          SQL,
          SECRET,
          SECRET,
          SECRET,
          SECRET,
          "java.sql.SQLException: " + SQL);

  @Test
  void emitsOnlyAllowlistedSummaryFieldsAndHostGeneratedIdentities() throws Exception {
    QueryAuditReport report =
        new QueryAuditReport(
                SECRET,
                SECRET,
                List.of(ISSUE),
                List.of(ISSUE),
                List.of(ISSUE),
                List.of(new QueryRecord(SQL, 1, 0, SECRET)),
                1,
                3,
                1)
            .withTestIdentity(SQL, new TestSelector(SECRET, SECRET));
    PublishedAuditRun published = PublishedAuditRun.from(AuditRunResult.fail(List.of(report)));
    JsonNode root = new ObjectMapper().readTree(published.json());
    JsonNode test = root.path("tests").get(0);
    JsonNode finding = test.path("confirmedFindings").get(0);

    assertThat(published.outcome()).isEqualTo(AuditOutcome.FAIL);
    assertThat(published.json())
        .doesNotContain(
            SECRET,
            "SELECT",
            "password",
            "SQLException",
            "java.sql",
            "testName",
            "testClass",
            "sourceLocation",
            "detail",
            "suggestion",
            "stackTrace");
    assertThat(root.path("reportedTests").asInt()).isEqualTo(1);
    assertThat(root.path("totalQueries").asInt()).isEqualTo(3);
    assertThat(test.path("testId").asText()).matches("qa-published-test-v1:[0-9a-f]{64}");
    assertThat(test.path("retainedQueries").asInt()).isEqualTo(1);
    assertThat(test.path("omittedQueries").asInt()).isEqualTo(2);
    assertThat(finding.path("findingId").asText())
        .isEqualTo(FindingId.of(report.getTestId(), ISSUE));
    assertThat(finding.path("severity").asText()).isEqualTo("ERROR");
    assertThat(fieldNames(finding)).containsExactlyInAnyOrder("findingId", "kindId", "severity");
    assertThat(test.path("informationalFindings")).hasSize(1);
    assertThat(test.path("acknowledgedFindings")).hasSize(1);
  }

  @Test
  void copiesTheViewOnceAndKeepsIncompleteCodesWithoutDiagnosticDetails() throws Exception {
    List<Issue> issues = new ArrayList<>(List.of(ISSUE));
    QueryAuditReport report =
        new QueryAuditReport("Test", "method", issues, List.of(), List.of(), 0, 0, 0);
    AuditRunResult run =
        AuditRunResult.inconclusive(
            List.of(report),
            new AuditIncompleteReason(IncompleteReasonCode.CAPABILITY_EXECUTION_FAILED, SECRET));
    PublishedAuditRun published = PublishedAuditRun.from(run);
    String before = published.json();
    issues.clear();

    assertThat(published.outcome()).isEqualTo(AuditOutcome.INCONCLUSIVE);
    assertThat(published.json()).isEqualTo(before).doesNotContain(SECRET);
    assertThat(new ObjectMapper().readTree(before).path("incompleteReasonCodes").get(0).asText())
        .isEqualTo("CAPABILITY_EXECUTION_FAILED");
    assertThat(PublishedAuditRun.class.getConstructors()).isEmpty();
  }

  @Test
  void acceptsNamespacedCustomKindsWithSlashesWithoutExportingTheirFreeText() throws Exception {
    Finding finding =
        new Finding(
            FindingKindId.of("acme:query/budget"),
            Severity.WARNING,
            SQL,
            SECRET,
            SECRET,
            SECRET,
            SECRET,
            SECRET);
    QueryAuditReport report =
        new QueryAuditReport("Test", "method", List.of(), List.of(), List.of(), 0, 0, 0)
            .withCustomFindings(List.of(finding), List.of(), List.of());

    String json = PublishedAuditRun.from(AuditRunResult.fail(List.of(report))).json();
    JsonNode publishedFinding =
        new ObjectMapper().readTree(json).path("tests").get(0).path("confirmedFindings").get(0);

    assertThat(publishedFinding.path("kindId").asText()).isEqualTo("acme:query/budget");
    assertThat(publishedFinding.path("findingId").asText())
        .isEqualTo(FindingId.of(report.getTestId(), finding));
    assertThat(json).doesNotContain(SECRET, "SELECT", "password");
  }

  private static Set<String> fieldNames(JsonNode node) {
    List<String> names = new ArrayList<>();
    node.fieldNames().forEachRemaining(names::add);
    return names.stream().collect(Collectors.toSet());
  }
}
