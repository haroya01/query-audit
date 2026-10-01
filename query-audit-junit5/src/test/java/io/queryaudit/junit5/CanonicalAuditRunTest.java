package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.config.ReportFormat;
import io.queryaudit.core.detector.QueryAuditAnalyzer;
import io.queryaudit.core.extension.FindingKindId;
import io.queryaudit.core.model.AuditOutcome;
import io.queryaudit.core.model.AuditRunResult;
import io.queryaudit.core.model.Finding;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.provenance.AuditCapability;
import io.queryaudit.core.provenance.AuditPolicyInputs;
import io.queryaudit.core.provenance.ComparisonInputs;
import io.queryaudit.core.reporter.FindingId;
import io.queryaudit.core.reporter.HtmlReportAggregator;
import io.queryaudit.core.reporter.JsonReporter;
import io.queryaudit.core.reporter.ReportComparator;
import io.queryaudit.core.reporter.TargetResolution;
import io.queryaudit.core.reporter.delivery.PublishedAuditRun;
import io.queryaudit.core.reporter.delivery.ReportSinkRegistration;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CanonicalAuditRunTest {
  private static final String TEST_ID = "[engine:junit-jupiter]/[class:Canonical]/[method:query()]";

  @AfterEach
  void cleanup() {
    HtmlReportAggregator.getInstance().reset();
  }

  @Test
  void suiteJsonAndSinksRetainInfoHiddenFromTheDisplay(@TempDir Path output) throws Exception {
    HtmlReportAggregator.getInstance().reset();
    QueryAuditReport analysis =
        report(Severity.INFO)
            .withCustomFindings(
                List.of(),
                List.of(
                    new Finding(
                        FindingKindId.of("company:info"),
                        Severity.INFO,
                        "SELECT 1",
                        null,
                        null,
                        "detail",
                        null)),
                List.of());
    var state = new QueryAuditExtension.AuditRunState();
    state.recordReport(analysis);
    QueryAuditReport display = QueryAuditExtension.applyInfoVisibility(analysis, false);
    HtmlReportAggregator.getInstance().addReport(display);
    state.retainReport(display, HtmlReportAggregator.DEFAULT_MAX_IN_MEMORY_REPORTS);
    List<PublishedAuditRun> delivered = new ArrayList<>();
    var finalizer =
        new QueryAuditExtension.ReportFinalizer(
            new QueryAuditExtension(), output, ReportFormat.JSON, state);
    finalizer.requireReportSinks(
        List.of(new ReportSinkRegistration("company:sink", true, delivered::add)));

    finalizer.close();

    assertThat(display.getInfoFindings()).isEmpty();
    assertThat(state.result(List.of(display)).reports().get(0).getInfoFindings()).hasSize(2);
    assertThat(Files.readString(output.resolve("report.json")))
        .contains("slow-query", "company:info");
    assertThat(delivered).hasSize(1);
    assertThat(delivered.get(0).json()).contains("slow-query", "company:info");
  }

  @Test
  void canonicalRestorationDoesNotReintroduceEvictedQueryEvidence() {
    QueryAuditReport analysis = report(Severity.INFO);
    var state = new QueryAuditExtension.AuditRunState();
    state.recordReport(analysis);
    QueryAuditReport retained =
        QueryAuditExtension.applyInfoVisibility(analysis, false).withoutQueryEvidence();

    QueryAuditReport canonical = state.result(List.of(retained)).reports().get(0);

    assertThat(canonical.getInfoIssues()).hasSize(1);
    assertThat(canonical.getAllQueries()).isEmpty();
    assertThat(canonical.getTotalQueryCount()).isEqualTo(1);
    assertThat(canonical.getOmittedQueryCount()).isEqualTo(1);
    assertThat(canonical.getTestId()).isEqualTo(TEST_ID);
  }

  @Test
  void eachRootBoundsItsOwnEvidenceAndRetainsCanonicalInfo() {
    QueryAuditReport analysis = report(Severity.INFO);
    QueryAuditReport display = QueryAuditExtension.applyInfoVisibility(analysis, false);
    var first = new QueryAuditExtension.AuditRunState();
    first.recordReport(analysis);
    first.retainReport(display, 1);
    first.retainReport(display, 1);
    var second = new QueryAuditExtension.AuditRunState();
    second.recordReport(analysis);
    second.retainReport(display, 1);

    assertThat(first.retainedReports()).hasSize(2);
    assertThat(first.retainedReports().get(0).getAllQueries()).hasSize(1);
    assertThat(first.retainedReports().get(1).getAllQueries()).isEmpty();
    assertThat(first.result(first.retainedReports()).reports())
        .allSatisfy(report -> assertThat(report.getInfoFindings()).hasSize(1));
    assertThat(first.result(first.retainedReports()).reports().get(1).getOmittedQueryCount())
        .isEqualTo(1);
    assertThat(second.retainedReports())
        .singleElement()
        .satisfies(report -> assertThat(report.getAllQueries()).hasSize(1));
  }

  @Test
  void warningChangedToHiddenInfoStillPersistsAsAnExplicitComparisonTarget() {
    QueryAuditReport before = report(Severity.WARNING);
    QueryAuditReport after = report(Severity.INFO);
    QueryAuditConfig config = QueryAuditConfig.builder().showInfo(false).build();
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(config, List.of());
    AuditCapability absent = AuditCapability.absent();
    ComparisonInputs inputs =
        new AuditInputContext("h2", absent, absent, absent, null)
            .describe(analyzer, AuditPolicyInputs.empty(), absent);
    var beforeState = new QueryAuditExtension.AuditRunState();
    beforeState.recordReport(before);
    beforeState.recordInputs(TEST_ID, inputs);
    var afterState = new QueryAuditExtension.AuditRunState();
    afterState.recordReport(after);
    afterState.recordInputs(TEST_ID, inputs);
    AuditRunResult beforeRun =
        beforeState.result(List.of(QueryAuditExtension.applyInfoVisibility(before, false)));
    AuditRunResult afterRun =
        afterState.result(List.of(QueryAuditExtension.applyInfoVisibility(after, false)));
    String target = FindingId.of(TEST_ID, before.getConfirmedIssues().get(0));

    var result =
        ReportComparator.compare(
            JsonReporter.toRunEnvelopeJson(beforeRun),
            JsonReporter.toRunEnvelopeJson(afterRun),
            List.of(target));

    assertThat(result.comparisonComplete()).isTrue();
    assertThat(result.outcome()).isEqualTo(AuditOutcome.FAIL);
    assertThat(result.targetResolutions())
        .extracting(TargetResolution::status)
        .containsExactly(TargetResolution.Status.PERSISTING);
  }

  private static QueryAuditReport report(Severity severity) {
    Issue issue =
        new Issue(IssueType.SLOW_QUERY, severity, "SELECT 1", null, null, "detail", "suggestion");
    return new QueryAuditReport(
            "Canonical",
            "query",
            severity == Severity.INFO ? List.of() : List.of(issue),
            severity == Severity.INFO ? List.of(issue) : List.of(),
            List.of(),
            List.of(new QueryRecord("SELECT 1", 100, 0, null)),
            1,
            1,
            100)
        .withTestIdentity(TEST_ID, null);
  }
}
