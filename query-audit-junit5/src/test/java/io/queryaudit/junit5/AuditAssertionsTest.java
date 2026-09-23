package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.extension.FindingKindId;
import io.queryaudit.core.model.Finding;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.util.List;
import org.junit.jupiter.api.Test;

class AuditAssertionsTest {
  @Test
  void aZeroBudgetIsAnAssertionEvenWithoutAnyDetectedFinding() throws Exception {
    ExpectMaxQueryCount budget =
        Fixtures.class.getDeclaredMethod("noQueries").getAnnotation(ExpectMaxQueryCount.class);

    assertThat(AuditAssertions.maxQueryCountFailure(budget, List.of(), "noQueries")).isNull();
    assertThat(
            AuditAssertions.maxQueryCountFailure(
                budget, List.of(new QueryRecord("SELECT 1", 0, 0, null)), "noQueries"))
        .contains("executed 1 queries, expected at most 0");
  }

  @Test
  void failureTypeSelectionDoesNotMutateTheAnalysisReport() throws Exception {
    QueryAudit annotation =
        Fixtures.class.getDeclaredMethod("onlyNPlusOne").getAnnotation(QueryAudit.class);
    Issue nPlusOne = issue(IssueType.N_PLUS_ONE);
    Issue slow = issue(IssueType.SLOW_QUERY);
    QueryAuditReport report =
        new QueryAuditReport(
            "fixture", "method", List.of(nPlusOne, slow), List.of(), List.of(), List.of(), 0, 0, 0);

    assertThat(AuditAssertions.failableIssues(report, annotation)).containsExactly(nPlusOne);
    assertThat(report.getConfirmedIssues()).containsExactly(nPlusOne, slow);
    assertThat(AuditAssertions.failableIssues(report, null)).containsExactly(nPlusOne, slow);
  }

  @Test
  void focusedNPlusOneAssertionIgnoresUnrelatedConfirmedIssues() throws Exception {
    DetectNPlusOne annotation =
        Fixtures.class.getDeclaredMethod("onlyNPlusOne").getAnnotation(DetectNPlusOne.class);
    QueryAuditReport report =
        new QueryAuditReport(
            "fixture",
            "method",
            List.of(issue(IssueType.SLOW_QUERY)),
            List.of(),
            List.of(),
            List.of(),
            0,
            0,
            0);

    assertThat(AuditAssertions.nPlusOneFailure(annotation, report, "method")).isNull();
  }

  @Test
  void openConfirmedFindingsParticipateInTheDefaultFailurePolicy() {
    Finding custom =
        new Finding(
            FindingKindId.of("company:custom"),
            Severity.WARNING,
            "SELECT 1",
            null,
            null,
            "detail",
            "suggestion");
    QueryAuditReport report =
        new QueryAuditReport(
                "fixture", "method", List.of(), List.of(), List.of(), List.of(), 0, 0, 0)
            .withCustomFindings(List.of(custom), List.of(), List.of());

    assertThat(AuditAssertions.failableFindings(report, null)).containsExactly(custom);
    assertThat(AuditAssertions.findingsFailureMessage("method", List.of(custom)))
        .contains("company:custom", "WARNING", "detail");
  }

  @Test
  void failureMessagesShowTheTopOfTheCapturedCallStack() {
    Finding nPlusOne =
        new Finding(
            FindingKindId.of("n-plus-one"),
            Severity.ERROR,
            "SELECT * FROM customers WHERE id = ?",
            "customers",
            null,
            "The same SELECT ran 5 times from one call site",
            "Load the rows once before the loop",
            String.join(
                "\n",
                "shop.CustomerRepository.findById:12",
                "shop.OrderService.describe:40",
                "shop.OrderService.list:31",
                "shop.OrderController.list:18",
                "shop.OrderControllerTest.lists:22",
                "shop.Unshown.frame:1"));

    assertThat(AuditAssertions.findingsFailureMessage("lists", List.of(nPlusOne)))
        .contains(
            "Call stack:", "at shop.OrderService.list:31", "at shop.OrderControllerTest.lists:22")
        .doesNotContain("shop.Unshown.frame");
  }

  @Test
  void hidingOnlyCustomInfoRetainsConfirmedAndAcknowledgedFindings() {
    Finding custom =
        new Finding(
            FindingKindId.of("company:custom"),
            Severity.WARNING,
            "SELECT 1",
            null,
            null,
            "detail",
            "suggestion");
    Finding info = custom.withSeverity(Severity.INFO);
    QueryAuditReport report =
        new QueryAuditReport(
                "fixture", "method", List.of(), List.of(), List.of(), List.of(), 0, 0, 0)
            .withCustomFindings(List.of(custom), List.of(info), List.of(custom));

    QueryAuditReport visible = QueryAuditExtension.applyInfoVisibility(report, false);

    assertThat(visible.getCustomInfoFindings()).isEmpty();
    assertThat(visible.getCustomConfirmedFindings()).containsExactly(custom);
    assertThat(visible.getCustomAcknowledgedFindings()).containsExactly(custom);
    assertThat(report.getCustomInfoFindings()).containsExactly(info);
  }

  private static Issue issue(IssueType type) {
    return new Issue(type, Severity.WARNING, "SELECT 1", null, null, "detail", "suggestion");
  }

  static class Fixtures {
    @ExpectMaxQueryCount(0)
    void noQueries() {}

    @QueryAudit(failOn = IssueType.N_PLUS_ONE)
    @DetectNPlusOne
    void onlyNPlusOne() {}
  }
}
