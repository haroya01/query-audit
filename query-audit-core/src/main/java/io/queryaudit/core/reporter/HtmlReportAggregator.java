package io.queryaudit.core.reporter;

import io.queryaudit.core.dedup.DeduplicatedIssue;
import io.queryaudit.core.dedup.IssueFingerprintDeduplicator;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.ranking.ImpactScorer;
import io.queryaudit.core.ranking.RankedIssue;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Thread-safe singleton that accumulates {@link QueryAuditReport} instances from multiple test
 * methods and delegates to {@link HtmlReporter} to produce a single consolidated HTML report.
 *
 * <h3>Memory management</h3>
 *
 * <p>To prevent unbounded memory growth in large test suites, this aggregator stores only
 * <em>lightweight summaries</em> of each report once the number of accumulated reports exceeds
 * {@value #DEFAULT_MAX_IN_MEMORY_REPORTS}. Lightweight summaries retain all issues but drop the
 * per-query {@link QueryRecord} list (which is the largest contributor to memory usage). The full
 * query list is only needed for the per-test console report, which has already been printed by the
 * time the report reaches the aggregator.
 *
 * @author haroya
 * @since 0.2.0
 */
public final class HtmlReportAggregator {

  /**
   * Default maximum number of reports kept with full query data in memory. Beyond this threshold,
   * newly added reports are stored as lightweight summaries (issues only, no raw query list).
   */
  public static final int DEFAULT_MAX_IN_MEMORY_REPORTS = 200;

  private static final HtmlReportAggregator INSTANCE = new HtmlReportAggregator();

  private final List<QueryAuditReport> reports = new CopyOnWriteArrayList<>();
  private volatile int maxInMemoryReports = DEFAULT_MAX_IN_MEMORY_REPORTS;

  private HtmlReportAggregator() {}

  /** Returns the singleton instance. */
  public static HtmlReportAggregator getInstance() {
    return INSTANCE;
  }

  /**
   * Adds a report to the aggregation list. This method is thread-safe.
   *
   * <p>When the number of accumulated reports exceeds {@link #getMaxInMemoryReports()}, the report
   * is stored as a lightweight summary that retains issues but drops the per-query list to conserve
   * memory.
   *
   * @param report the report to accumulate
   */
  public void addReport(QueryAuditReport report) {
    if (report != null) {
      if (reports.size() >= maxInMemoryReports) {
        // Store a lightweight summary: keep issues but drop the query list
        reports.add(toLightweight(report));
      } else {
        reports.add(report);
      }
    }
  }

  /**
   * Generates a consolidated HTML report containing all accumulated reports and writes it as {@code
   * index.html} inside the given output directory.
   *
   * @param outputDir directory where the report file will be created
   * @throws IOException if the file cannot be written
   */
  public void writeReport(Path outputDir) throws IOException {
    List<QueryAuditReport> snapshot = Collections.unmodifiableList(new ArrayList<>(reports));

    // Compute global impact ranking across all accumulated reports
    List<Issue> allIssues = new ArrayList<>();
    for (QueryAuditReport r : snapshot) {
      if (r.getConfirmedIssues() != null) {
        allIssues.addAll(r.getConfirmedIssues());
      }
    }
    List<RankedIssue> rankedIssues = ImpactScorer.rank(allIssues);

    // Compute deduplicated issues across all reports
    List<DeduplicatedIssue> deduplicatedIssues = IssueFingerprintDeduplicator.deduplicate(snapshot);

    HtmlReporter reporter = new HtmlReporter();
    reporter.writeToFile(outputDir, snapshot, rankedIssues, deduplicatedIssues);
  }

  /**
   * Runs {@link IssueFingerprintDeduplicator} on all accumulated reports.
   *
   * @return deduplicated issues sorted by severity DESC, then occurrence count DESC
   */
  public List<DeduplicatedIssue> getDeduplicatedIssues() {
    return IssueFingerprintDeduplicator.deduplicate(new ArrayList<>(reports));
  }

  /** Returns an unmodifiable snapshot of all accumulated reports. */
  public List<QueryAuditReport> getReports() {
    return Collections.unmodifiableList(new ArrayList<>(reports));
  }

  /** Clears all accumulated reports. This method is thread-safe. */
  public void reset() {
    reports.clear();
  }

  /**
   * Returns the maximum number of reports kept with full query data in memory.
   *
   * @return current limit
   */
  public int getMaxInMemoryReports() {
    return maxInMemoryReports;
  }

  /**
   * Sets the maximum number of reports kept with full query data in memory. Reports added beyond
   * this threshold are stored as lightweight summaries (no query list).
   *
   * @param max the maximum (must be positive)
   * @throws IllegalArgumentException if max is not positive
   */
  public void setMaxInMemoryReports(int max) {
    if (max <= 0) {
      throw new IllegalArgumentException("maxInMemoryReports must be positive, got: " + max);
    }
    this.maxInMemoryReports = max;
  }

  // ── Internal helpers ──────────────────────────────────────────────────

  /**
   * Creates a lightweight copy of a report that retains all issues and metadata but replaces the
   * query list with an empty list. This dramatically reduces memory usage for large test suites.
   */
  private static QueryAuditReport toLightweight(QueryAuditReport report) {
    return new QueryAuditReport(
        report.getTestClass(),
        report.getTestName(),
        report.getConfirmedIssues(),
        report.getInfoIssues(),
        report.getAcknowledgedIssues(),
        List.of(), // drop query list to save memory
        report.getUniquePatternCount(),
        report.getTotalQueryCount(),
        report.getTotalExecutionTimeNanos());
  }
}
