package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.model.AuditRunResult;
import io.queryaudit.core.model.IndexInfo;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.model.TestSelector;
import io.queryaudit.core.reporter.ConsoleReporter;
import io.queryaudit.core.reporter.HtmlReportAggregator;
import io.queryaudit.core.reporter.JsonReporter;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ReportInfoVisibilityTest {

  private static final String INFO_MARKER = "info-output-marker";
  private static final String CONFIRMED_MARKER = "confirmed-output-marker";
  private static final String TEST_ID =
      "[engine:junit-jupiter]/[class:VisibilityTest]/[method:showsConfiguredFindings()]";

  @TempDir Path tempDir;

  @BeforeEach
  void resetAggregator() {
    HtmlReportAggregator.getInstance().reset();
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("visibilitySettings")
  void showInfoControlsEveryGeneratedFormat(
      String settingName, QueryAuditConfig config, boolean expectInfo) throws Exception {
    QueryAuditReport analysisReport = reportWithInfo();

    QueryAuditReport outputReport =
        QueryAuditExtension.applyInfoVisibility(analysisReport, config.isShowInfo());

    assertPreservedReportData(outputReport, analysisReport);
    assertThat(analysisReport.getInfoIssues()).hasSize(1);
    assertThat(outputReport.getInfoIssues()).hasSize(expectInfo ? 1 : 0);

    String console = renderConsole(outputReport);

    HtmlReportAggregator aggregator = HtmlReportAggregator.getInstance();
    aggregator.addReport(outputReport);
    Path htmlDir = tempDir.resolve(settingName.replace(' ', '-'));
    aggregator.writeReport(htmlDir);
    String html = Files.readString(htmlDir.resolve("VisibilityTest.html"));
    String json = JsonReporter.toRunEnvelopeJson(AuditRunResult.pass(aggregator.getReports()));

    assertVisibleFindingCounts(console, html, json, expectInfo);
    assertMarkerVisibility(console, html, json, INFO_MARKER, expectInfo);
    assertThat(console).contains(CONFIRMED_MARKER).contains("3 unique patterns");
    assertThat(html).contains(CONFIRMED_MARKER).contains("3 unique");
    assertThat(json)
        .contains(CONFIRMED_MARKER)
        .contains("\"uniquePatterns\": 3")
        .contains("\"totalQueries\": 4")
        .contains("\"executionTimeMs\": 5")
        .contains("\"orders\":");
  }

  private static Stream<Arguments> visibilitySettings() {
    return Stream.of(
        arguments("explicit false", QueryAuditConfig.builder().showInfo(false).build(), false),
        arguments("explicit true", QueryAuditConfig.builder().showInfo(true).build(), true),
        arguments("default", QueryAuditConfig.defaults(), true));
  }

  private static QueryAuditReport reportWithInfo() {
    Issue confirmed =
        new Issue(
            IssueType.N_PLUS_ONE,
            Severity.ERROR,
            "select * from orders where user_id = ?",
            "orders",
            "user_id",
            "Repeated association lookup",
            CONFIRMED_MARKER);
    Issue info =
        new Issue(
            IssueType.FULL_TABLE_SCAN,
            Severity.INFO,
            "select * from catalog",
            "catalog",
            null,
            "Plan uses a full table scan",
            INFO_MARKER);
    Issue acknowledged =
        new Issue(
            IssueType.OR_ABUSE,
            Severity.WARNING,
            "select id from accounts where active = ? or locked = ?",
            "accounts",
            null,
            "accepted finding",
            "No change required");
    QueryRecord query =
        new QueryRecord("select id from orders where user_id = 1", 5_000_000L, 1_000L, "");
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "orders",
                List.of(new IndexInfo("orders", "idx_user_id", "user_id", 1, true, 100L))));

    return new QueryAuditReport(
            "VisibilityTest",
            "showsConfiguredFindings()",
            List.of(confirmed),
            List.of(info),
            List.of(acknowledged),
            List.of(query),
            3,
            4,
            5_000_000L)
        .withTestIdentity(TEST_ID, new TestSelector("junit-unique-id", TEST_ID))
        .withIndexMetadata(metadata);
  }

  private static void assertPreservedReportData(
      QueryAuditReport outputReport, QueryAuditReport analysisReport) {
    assertThat(outputReport.getConfirmedIssues()).isEqualTo(analysisReport.getConfirmedIssues());
    assertThat(outputReport.getAcknowledgedIssues())
        .isEqualTo(analysisReport.getAcknowledgedIssues());
    assertThat(outputReport.getAllQueries()).isEqualTo(analysisReport.getAllQueries());
    assertThat(outputReport.getIndexMetadata()).isSameAs(analysisReport.getIndexMetadata());
    assertThat(outputReport.getTestId()).isEqualTo(analysisReport.getTestId());
    assertThat(outputReport.getTestSelector()).isEqualTo(analysisReport.getTestSelector());
    assertThat(outputReport.getUniquePatternCount()).isEqualTo(3);
    assertThat(outputReport.getTotalQueryCount()).isEqualTo(4);
    assertThat(outputReport.getTotalExecutionTimeNanos()).isEqualTo(5_000_000L);
  }

  private static String renderConsole(QueryAuditReport report) {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    try (PrintStream printStream = new PrintStream(output, true, StandardCharsets.UTF_8)) {
      new ConsoleReporter(printStream, false).report(report);
    }
    return output.toString(StandardCharsets.UTF_8);
  }

  private static void assertVisibleFindingCounts(
      String console, String html, String json, boolean expectInfo) {
    int expectedInfoCount = expectInfo ? 1 : 0;
    int expectedPassedCount = expectInfo ? 2 : 3;

    assertThat(json).contains("\"infoIssues\": " + expectedInfoCount);
    assertThat(console).contains(expectedPassedCount + " passed");
    assertThat(html).contains(expectInfo ? "2 issues" : "1 issue");
    if (expectInfo) {
      assertThat(console).contains("1 info");
      assertThat(html).contains("1 info");
    } else {
      assertThat(console).doesNotContain("1 info");
      assertThat(html).doesNotContain("1 info");
    }
  }

  private static void assertMarkerVisibility(
      String console, String html, String json, String marker, boolean visible) {
    if (visible) {
      assertThat(console).contains(marker);
      assertThat(html).contains(marker);
      assertThat(json).contains(marker);
    } else {
      assertThat(console).doesNotContain(marker);
      assertThat(html).doesNotContain(marker);
      assertThat(json).doesNotContain(marker);
    }
  }
}
