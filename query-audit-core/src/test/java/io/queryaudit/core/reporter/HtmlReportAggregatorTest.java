package io.queryaudit.core.reporter;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.Severity;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HtmlReportAggregatorTest {

  @TempDir Path tempDir;

  @BeforeEach
  void setUp() {
    HtmlReportAggregator.getInstance().reset();
  }

  // ── Helpers ──────────────────────────────────────────────────────

  private static QueryAuditReport dummyReport(String testClass, String testName) {
    Issue issue =
        new Issue(
            IssueType.N_PLUS_ONE,
            Severity.ERROR,
            "SELECT * FROM orders WHERE user_id = ?",
            "orders",
            null,
            "Repeated query detected 5 times",
            "Use JOIN FETCH or batch loading");
    return new QueryAuditReport(
        testClass, testName, List.of(issue), List.of(), List.of(), List.of(), 1, 5, 100_000L);
  }

  // ── Tests ────────────────────────────────────────────────────────

  @Nested
  @DisplayName("Issue #41: singleton accumulation behavior")
  class SingletonAccumulation {

    /**
     * The aggregator is a singleton that accumulates reports without resetting.
     * Calling writeReport() at intermediate points produces incomplete HTML files
     * because later test classes haven't added their reports yet.
     *
     * <p>This is why writeReport should only be called once, after ALL test classes finish.
     * The fix is in QueryAuditExtension (ReportFinalizer), not in the aggregator itself.
     */
    @Test
    @DisplayName("intermediate writes produce incomplete reports — only final write has all data")
    void intermediateWritesAreIncomplete() throws IOException {
      HtmlReportAggregator aggregator = HtmlReportAggregator.getInstance();

      // Class 1: 2 tests
      aggregator.addReport(dummyReport("ClassA", "test1"));
      aggregator.addReport(dummyReport("ClassA", "test2"));

      Path dir1 = tempDir.resolve("intermediate1");
      aggregator.writeReport(dir1);

      // Class 2: 1 test
      aggregator.addReport(dummyReport("ClassB", "test3"));

      Path dir2 = tempDir.resolve("intermediate2");
      aggregator.writeReport(dir2);

      // Class 3: 1 test
      aggregator.addReport(dummyReport("ClassC", "test4"));

      Path dirFinal = tempDir.resolve("final");
      aggregator.writeReport(dirFinal);

      // Intermediate report 1 is missing ClassB and ClassC data
      String html1 = Files.readString(dir1.resolve("index.html"));
      assertThat(html1).doesNotContain("ClassB");
      assertThat(html1).doesNotContain("ClassC");

      // Intermediate report 2 is missing ClassC data
      String html2 = Files.readString(dir2.resolve("index.html"));
      assertThat(html2).doesNotContain("ClassC");

      // Only the final report has everything
      String htmlFinal = Files.readString(dirFinal.resolve("index.html"));
      assertThat(htmlFinal).contains("ClassA").contains("ClassB").contains("ClassC");
    }

    @Test
    @DisplayName("single writeReport after all addReport calls produces complete report")
    void singleWriteProducesCompleteReport() throws IOException {
      HtmlReportAggregator aggregator = HtmlReportAggregator.getInstance();

      // Simulate multiple test classes adding reports
      aggregator.addReport(dummyReport("OrderServiceTest", "testCreateOrder"));
      aggregator.addReport(dummyReport("OrderServiceTest", "testDeleteOrder"));
      aggregator.addReport(dummyReport("UserServiceTest", "testFindUser"));
      aggregator.addReport(dummyReport("PaymentServiceTest", "testCharge"));

      // One single write at the very end — the correct behavior after fix
      Path outputDir = tempDir.resolve("output");
      aggregator.writeReport(outputDir);

      String html = Files.readString(outputDir.resolve("index.html"));
      assertThat(html)
          .contains("OrderServiceTest")
          .contains("UserServiceTest")
          .contains("PaymentServiceTest");

      assertThat(aggregator.getReports())
          .as("All 4 reports present in singleton")
          .hasSize(4);
    }
  }
}
