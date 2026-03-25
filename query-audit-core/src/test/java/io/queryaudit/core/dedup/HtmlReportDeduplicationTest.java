package io.queryaudit.core.dedup;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.reporter.HtmlReporter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies that the HTML report includes the deduplicated "Unique Issues Summary" section and the
 * "Unique Issues" stat card in the dashboard.
 */
class HtmlReportDeduplicationTest {

  @TempDir Path tempDir;

  @Test
  void htmlReportContainsUniqueIssuesSummarySection() throws IOException {
    Issue sameIssue =
        new Issue(
            IssueType.MISSING_JOIN_INDEX,
            Severity.ERROR,
            "SELECT * FROM rooms JOIN users ON rooms.owner_id = users.id",
            "rooms",
            "owner_id",
            "Missing index on JOIN column",
            "CREATE INDEX idx_rooms_owner_id ON rooms(owner_id)");

    List<QueryAuditReport> reports = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      reports.add(
          new QueryAuditReport(
              "RoomApiTest",
              "testMethod" + i,
              List.of(sameIssue),
              List.of(),
              List.of(),
              List.of(),
              1,
              1,
              100_000L));
    }

    List<DeduplicatedIssue> dedup = IssueFingerprintDeduplicator.deduplicate(reports);

    HtmlReporter reporter = new HtmlReporter();
    reporter.writeToFile(tempDir, reports, List.of(), dedup);

    String html = Files.readString(tempDir.resolve("index.html"), StandardCharsets.UTF_8);

    // Verify "Unique Issues Summary" section exists
    assertThat(html).contains("Unique Issues Summary");

    // Verify occurrence count badge
    assertThat(html).contains("&times;5");

    // Verify the fix suggestion is present
    assertThat(html).contains("CREATE INDEX idx_rooms_owner_id ON rooms(owner_id)");

    // Verify affected test names are shown
    assertThat(html).contains("testMethod0");

    // Verify it only appears once as a deduplicated row (not 5 times)
    // Count occurrences of the target in the dedup table
    assertThat(dedup).hasSize(1);
    assertThat(dedup.get(0).occurrenceCount()).isEqualTo(5);
  }

  @Test
  void htmlReportDashboardShowsUniqueIssuesStatCard() throws IOException {
    Issue issue =
        new Issue(
            IssueType.SELECT_ALL,
            Severity.WARNING,
            "SELECT * FROM users",
            "users",
            null,
            "SELECT * usage",
            "Specify columns explicitly");

    QueryAuditReport report =
        new QueryAuditReport(
            "UserApiTest",
            "test1",
            List.of(issue),
            List.of(),
            List.of(),
            List.of(),
            1,
            1,
            100_000L);

    List<DeduplicatedIssue> dedup = IssueFingerprintDeduplicator.deduplicate(List.of(report));

    HtmlReporter reporter = new HtmlReporter();
    reporter.writeToFile(tempDir, List.of(report), List.of(), dedup);

    String html = Files.readString(tempDir.resolve("index.html"), StandardCharsets.UTF_8);

    // Verify "Unique Issues" stat card is in the dashboard
    assertThat(html).contains("Unique Issues");
    assertThat(html).contains("stat-unique");
  }

  @Test
  void htmlReportWithEmptyDedupDoesNotShowSection() throws IOException {
    QueryAuditReport report =
        new QueryAuditReport(
            "CleanTest", "testClean", List.of(), List.of(), List.of(), List.of(), 0, 0, 100_000L);

    HtmlReporter reporter = new HtmlReporter();
    reporter.writeToFile(tempDir, List.of(report), List.of(), List.of());

    String html = Files.readString(tempDir.resolve("index.html"), StandardCharsets.UTF_8);

    // Section should not appear when there are no deduplicated issues
    assertThat(html).doesNotContain("Unique Issues Summary");
  }

  @Test
  void htmlReportShowsOccurrenceBadgeColorsCorrectly() throws IOException {
    Issue errorIssue =
        new Issue(
            IssueType.MISSING_JOIN_INDEX,
            Severity.ERROR,
            "SELECT * FROM rooms WHERE owner_id = 1",
            "rooms",
            "owner_id",
            "detail",
            "suggestion");
    Issue warningIssue =
        new Issue(
            IssueType.SELECT_ALL,
            Severity.WARNING,
            "SELECT * FROM users",
            "users",
            null,
            "detail",
            "suggestion");

    List<QueryAuditReport> reports = new ArrayList<>();
    // 25 occurrences -> red badge (>20)
    for (int i = 0; i < 25; i++) {
      reports.add(
          new QueryAuditReport(
              "Test",
              "t" + i,
              List.of(errorIssue),
              List.of(),
              List.of(),
              List.of(),
              1,
              1,
              100_000L));
    }
    // 3 occurrences -> gray badge (<=5)
    for (int i = 0; i < 3; i++) {
      reports.add(
          new QueryAuditReport(
              "Test",
              "w" + i,
              List.of(warningIssue),
              List.of(),
              List.of(),
              List.of(),
              1,
              1,
              100_000L));
    }

    List<DeduplicatedIssue> dedup = IssueFingerprintDeduplicator.deduplicate(reports);

    HtmlReporter reporter = new HtmlReporter();
    reporter.writeToFile(tempDir, reports, List.of(), dedup);

    String html = Files.readString(tempDir.resolve("index.html"), StandardCharsets.UTF_8);

    // Red badge for >20 occurrences
    assertThat(html).contains("badge-error\">&times;25");
    // Gray badge for <=5 occurrences
    assertThat(html).contains("badge-neutral\">&times;3");
  }

  @Test
  void htmlReportCollapsesAffectedTestsAfterThree() throws IOException {
    Issue issue =
        new Issue(
            IssueType.MISSING_JOIN_INDEX,
            Severity.ERROR,
            "SELECT * FROM rooms WHERE owner_id = 1",
            "rooms",
            "owner_id",
            "detail",
            "suggestion");

    List<QueryAuditReport> reports = new ArrayList<>();
    for (int i = 0; i < 7; i++) {
      reports.add(
          new QueryAuditReport(
              "Test",
              "testMethod" + i,
              List.of(issue),
              List.of(),
              List.of(),
              List.of(),
              1,
              1,
              100_000L));
    }

    List<DeduplicatedIssue> dedup = IssueFingerprintDeduplicator.deduplicate(reports);

    HtmlReporter reporter = new HtmlReporter();
    reporter.writeToFile(tempDir, reports, List.of(), dedup);

    String html = Files.readString(tempDir.resolve("index.html"), StandardCharsets.UTF_8);

    // First 3 tests should be visible
    assertThat(html).contains("testMethod0");
    assertThat(html).contains("testMethod1");
    assertThat(html).contains("testMethod2");

    // The rest should be in a collapsible "and N more..." section
    assertThat(html).contains("and 4 more...");
  }
}
