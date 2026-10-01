package io.queryaudit.core.reporter;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import io.queryaudit.core.config.ReportRedaction;
import io.queryaudit.core.extension.FindingKindId;
import io.queryaudit.core.model.AuditOutcome;
import io.queryaudit.core.model.AuditRunResult;
import io.queryaudit.core.model.Finding;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.Severity;
import java.util.List;
import org.junit.jupiter.api.Test;

class CustomFindingReportTest {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final FindingKindId KIND = FindingKindId.of("shop:unbounded-read");

  @Test
  void customFindingsShareTheVersionedEnvelopeAndStableIdentity() throws Exception {
    Finding first =
        finding(
            Severity.WARNING,
            "select * from orders where email = 'secret@example.com'",
            "Service.load:10");
    Finding second =
        finding(
            Severity.ERROR,
            "select * from orders where email = 'another@example.com'",
            "Service.load:20");
    String text =
        JsonReporter.toRunEnvelopeJson(
            AuditRunResult.fail(List.of(report(List.of(first, second), List.of(), List.of()))));
    JsonNode result = JSON.readTree(text).path("reports").get(0);
    assertThat(result.path("summary").path("confirmedIssues").asInt()).isEqualTo(2);
    assertThat(result.path("confirmedIssues").size()).isEqualTo(1);
    JsonNode group = result.path("confirmedIssues").get(0);
    assertThat(group.path("type").asText()).isEqualTo(KIND.value());
    assertThat(group.path("severity").asText()).isEqualTo("ERROR");
    assertThat(group.path("findingId").asText()).isEqualTo(FindingId.of("test:custom", first));
    assertThat(group.path("occurrences").size()).isEqualTo(2);
    assertThat(text)
        .doesNotContain(
            "secret@example.com",
            "another@example.com",
            "sensitive diagnostic",
            "secret suggestion");
    String full =
        JsonReporter.toRunEnvelopeJson(
            AuditRunResult.fail(List.of(report(List.of(first), List.of(), List.of()))),
            ReportRedaction.FULL);
    assertThat(full).contains("secret@example.com", FindingId.of("test:custom", first));
    var schema =
        JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7)
            .getSchema(
                CustomFindingReportTest.class.getResourceAsStream("/report-1.7.schema.json"));
    assertThat(schema.validate(JSON.readTree(text))).isEmpty();
    assertThat(schema.validate(JSON.readTree(full))).isEmpty();
  }

  @Test
  void explicitTargetsPersistThroughInfoAndBaselineBuckets() {
    Finding warning = finding(Severity.WARNING, "select * from orders", "Service.load:10");
    String before = known(report(List.of(warning), List.of(), List.of()));
    String id = FindingId.of("test:custom", warning);
    for (QueryAuditReport after :
        List.of(
            report(List.of(), List.of(warning.withSeverity(Severity.INFO)), List.of()),
            report(List.of(), List.of(), List.of(warning)))) {
      var verdict = ReportComparator.compare(before, known(after), List.of(id));
      assertThat(verdict.outcome()).isEqualTo(AuditOutcome.FAIL);
      assertThat(verdict.targetResolutions())
          .extracting(TargetResolution::status)
          .containsExactly(TargetResolution.Status.PERSISTING);
    }
    var resolved =
        ReportComparator.compare(
            before, known(report(List.of(), List.of(), List.of())), List.of(id));
    assertThat(resolved.outcome()).isEqualTo(AuditOutcome.PASS);
    assertThat(resolved.allTargetsResolved()).isTrue();
  }

  @Test
  void unverifiedInputsNeverDeclareCustomTargetsResolved() {
    Finding finding = finding(Severity.ERROR, "select * from orders", null);
    String before =
        JsonReporter.toRunEnvelopeJson(
            AuditRunResult.pass(List.of(report(List.of(finding), List.of(), List.of()))));
    String after =
        JsonReporter.toRunEnvelopeJson(
            AuditRunResult.pass(List.of(report(List.of(), List.of(), List.of()))));
    var verdict =
        ReportComparator.compare(before, after, List.of(FindingId.of("test:custom", finding)));
    assertThat(verdict.outcome()).isEqualTo(AuditOutcome.INCONCLUSIVE);
    assertThat(verdict.allTargetsResolved()).isFalse();
  }

  @Test
  void legacyIdentityIsUnchangedByTheAdditiveModel() {
    Issue issue =
        new Issue(
            IssueType.SELECT_ALL,
            Severity.WARNING,
            "select * from orders",
            "orders",
            null,
            "detail",
            "suggestion");
    assertThat(FindingId.of("test:custom", Finding.fromIssue(issue)))
        .isEqualTo(FindingId.of("test:custom", issue));
    assertThat(Finding.fromIssue(issue).toIssue()).contains(issue);
    assertThat(finding(Severity.INFO, null, null).toIssue()).isEmpty();
  }

  private static Finding finding(Severity severity, String sql, String source) {
    return new Finding(
        KIND,
        severity,
        sql,
        "orders",
        "email",
        "sensitive diagnostic",
        "secret suggestion",
        source);
  }

  private static QueryAuditReport report(
      List<Finding> confirmed, List<Finding> info, List<Finding> acknowledged) {
    return new QueryAuditReport(
            "CustomTest", "custom", List.of(), List.of(), List.of(), List.of(), 1, 2, 0)
        .withTestIdentity("test:custom", null)
        .withCustomFindings(confirmed, info, acknowledged);
  }

  private static String known(QueryAuditReport report) {
    // A host with independently verified inputs; the plugin cannot set this flag itself.
    return ComparisonInputFixtures.json(AuditRunResult.pass(List.of(report)));
  }
}
