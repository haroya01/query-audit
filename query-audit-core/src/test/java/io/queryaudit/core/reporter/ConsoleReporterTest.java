package io.queryaudit.core.reporter;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

class ConsoleReporterTest {

  private String captureReport(QueryAuditReport report) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos, true, StandardCharsets.UTF_8);
    ConsoleReporter reporter = new ConsoleReporter(ps, false);
    reporter.report(report);
    return baos.toString(StandardCharsets.UTF_8);
  }

  @Test
  void reportWithConfirmedIssuesOutputsConfirmedSection() {
    Issue error =
        new Issue(
            IssueType.N_PLUS_ONE,
            Severity.ERROR,
            "select * from users where id = ?",
            "users",
            null,
            "Executed 5 times",
            "Use JOIN FETCH or @EntityGraph");

    QueryAuditReport report =
        new QueryAuditReport(
            "testWithIssues", List.of(error), List.of(), List.of(), 1, 5, 1_000_000L);

    String output = captureReport(report);

    assertThat(output).contains("CONFIRMED");
    assertThat(output).contains("[ERROR]");
    assertThat(output).contains("N+1 Query detected");
    assertThat(output).contains("Executed 5 times");
    assertThat(output).contains("Use JOIN FETCH");
  }

  @Test
  void reportWithWarningOutputsConfirmedSection() {
    Issue warning =
        new Issue(
            IssueType.SELECT_ALL,
            Severity.WARNING,
            "select * from users",
            "users",
            null,
            "SELECT * usage detected",
            "Specify only needed columns");

    QueryAuditReport report =
        new QueryAuditReport(
            "testWithWarning", List.of(warning), List.of(), List.of(), 1, 1, 500_000L);

    String output = captureReport(report);

    assertThat(output).contains("CONFIRMED");
    assertThat(output).contains("[WARNING]");
    assertThat(output).contains("SELECT * usage");
  }

  @Test
  void reportWithNoIssuesOutputsCleanReport() {
    QueryAuditReport report =
        new QueryAuditReport(
            "cleanTest",
            List.of(),
            List.of(),
            List.of(new QueryRecord("SELECT id FROM users", 0L, 0L, "")),
            1,
            1,
            100_000L);

    String output = captureReport(report);

    assertThat(output).doesNotContain("CONFIRMED");
    assertThat(output).contains("[OK]");
    assertThat(output).contains("passed");
  }

  @Test
  void reportContainsTestName() {
    QueryAuditReport report =
        new QueryAuditReport("mySpecificTestName", List.of(), List.of(), List.of(), 0, 0, 0L);

    String output = captureReport(report);

    assertThat(output).contains("mySpecificTestName");
  }

  @Test
  void reportContainsInfoSection() {
    Issue infoIssue =
        new Issue(
            IssueType.FULL_TABLE_SCAN,
            Severity.INFO,
            "select * from users",
            "users",
            null,
            "Full table scan detected",
            "Add appropriate index");

    QueryAuditReport report =
        new QueryAuditReport("infoTest", List.of(), List.of(infoIssue), List.of(), 1, 1, 200_000L);

    String output = captureReport(report);

    assertThat(output).contains("INFO");
    assertThat(output).contains("Full table scan");
  }

  @Test
  void reportContainsSummaryStats() {
    QueryAuditReport report =
        new QueryAuditReport("summaryTest", List.of(), List.of(), List.of(), 3, 10, 5_000_000L);

    String output = captureReport(report);

    assertThat(output).contains("3 unique patterns");
    assertThat(output).contains("10 total queries");
    assertThat(output).contains("5 ms total");
  }

  @Test
  void reportShowsTableAndColumnTarget() {
    Issue issue =
        new Issue(
            IssueType.MISSING_WHERE_INDEX,
            Severity.ERROR,
            "select * from users where email = ?",
            "users",
            "email",
            "Missing index",
            "Add index on email");

    QueryAuditReport report =
        new QueryAuditReport("targetTest", List.of(issue), List.of(), List.of(), 1, 1, 0L);

    String output = captureReport(report);

    assertThat(output).contains("users.email");
  }

  @Test
  void reportShowsTopIssuesByImpactSection() {
    Issue nPlusOne =
        new Issue(
            IssueType.N_PLUS_ONE,
            Severity.ERROR,
            "select * from orders where user_id = ?",
            "orders",
            "user_id",
            "Executed 10 times",
            "Use JOIN FETCH");

    Issue selectAll =
        new Issue(
            IssueType.SELECT_ALL,
            Severity.WARNING,
            "select * from users",
            "users",
            null,
            "SELECT * usage",
            "Specify columns");

    QueryAuditReport report =
        new QueryAuditReport(
            "topIssuesTest", List.of(nPlusOne, selectAll), List.of(), List.of(), 2, 11, 1_000_000L);

    String output = captureReport(report);

    assertThat(output).contains("TOP ISSUES BY IMPACT");
    assertThat(output).contains("#1");
    assertThat(output).contains("pts");
    assertThat(output).contains("N+1 Query detected");
  }

  @Test
  void reportWithNoConfirmedIssuesSkipsTopIssues() {
    QueryAuditReport report =
        new QueryAuditReport("noIssuesTest", List.of(), List.of(), List.of(), 0, 0, 0L);

    String output = captureReport(report);

    assertThat(output).doesNotContain("TOP ISSUES BY IMPACT");
  }
}
