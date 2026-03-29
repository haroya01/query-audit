package io.queryaudit.core.reporter;

import io.queryaudit.core.baseline.Baseline;
import io.queryaudit.core.baseline.BaselineEntry;
import io.queryaudit.core.dedup.DeduplicatedIssue;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.parser.SqlParser;
import io.queryaudit.core.ranking.RankedIssue;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Generates a multi-page HTML report (like JaCoCo) for query-audit analysis results.
 *
 * <p>The report can be generated from a single report via {@link #report(QueryAuditReport)} or from
 * an aggregated list of reports via {@link #writeToFile(Path, List)}.
 *
 * <p>Output structure:
 *
 * <ul>
 *   <li>{@code index.html} — Global overview dashboard with class-level cards
 *   <li>{@code {ClassName}.html} — One detail page per test class
 * </ul>
 *
 * @author haroya
 * @since 0.2.0
 */
public class HtmlReporter implements Reporter {

  private static final DateTimeFormatter TIMESTAMP_FMT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

  private final List<BaselineEntry> baseline;

  public HtmlReporter() {
    this(List.of());
  }

  public HtmlReporter(List<BaselineEntry> baseline) {
    this.baseline = baseline != null ? baseline : List.of();
  }

  @Override
  public void report(QueryAuditReport report) {
    HtmlReportAggregator.getInstance().addReport(report);
  }

  /**
   * Generates the multi-page HTML report (without impact ranking).
   *
   * @param outputDir directory where HTML files will be written
   * @param reports list of reports to include
   * @throws IOException if files cannot be written
   */
  public void writeToFile(Path outputDir, List<QueryAuditReport> reports) throws IOException {
    writeToFile(outputDir, reports, List.of());
  }

  /**
   * Generates the multi-page HTML report with impact-ranked issues.
   *
   * @param outputDir directory where HTML files will be written
   * @param reports list of reports to include
   * @param rankedIssues globally ranked issues (may be empty)
   * @throws IOException if files cannot be written
   */
  public void writeToFile(
      Path outputDir, List<QueryAuditReport> reports, List<RankedIssue> rankedIssues)
      throws IOException {
    writeToFile(outputDir, reports, rankedIssues, List.of());
  }

  /**
   * Generates the multi-page HTML report with impact-ranked and deduplicated issues.
   *
   * @param outputDir directory where HTML files will be written
   * @param reports list of reports to include
   * @param rankedIssues globally ranked issues (may be empty)
   * @param deduplicatedIssues cross-test deduplicated issues (may be empty)
   * @throws IOException if files cannot be written
   */
  public void writeToFile(
      Path outputDir,
      List<QueryAuditReport> reports,
      List<RankedIssue> rankedIssues,
      List<DeduplicatedIssue> deduplicatedIssues)
      throws IOException {
    Files.createDirectories(outputDir);

    List<DeduplicatedIssue> dedup = deduplicatedIssues != null ? deduplicatedIssues : List.of();

    // Group reports by test class
    Map<String, List<QueryAuditReport>> byClass = new LinkedHashMap<>();
    for (QueryAuditReport r : reports) {
      String cls = r.getTestClass() != null ? r.getTestClass() : "Unknown";
      byClass.computeIfAbsent(cls, k -> new ArrayList<>()).add(r);
    }

    // Write index.html using buffered streaming to avoid OOM from one giant string
    try (BufferedWriter writer =
        Files.newBufferedWriter(outputDir.resolve("index.html"), StandardCharsets.UTF_8)) {
      writeIndexHtml(writer, reports, byClass, rankedIssues, dedup);
    }

    // Write one {ClassName}.html per class using buffered streaming
    for (Map.Entry<String, List<QueryAuditReport>> entry : byClass.entrySet()) {
      String className = entry.getKey();
      List<QueryAuditReport> classReports = entry.getValue();
      try (BufferedWriter writer =
          Files.newBufferedWriter(
              outputDir.resolve(classFileName(className)), StandardCharsets.UTF_8)) {
        writeClassHtml(writer, className, classReports);
      }
    }
  }

  /** Returns a safe file name for a class page. */
  private static String classFileName(String className) {
    return className.replace('/', '.').replace('\\', '.') + ".html";
  }

  // =========================================================================
  // index.html — Global overview
  // =========================================================================

  private void writeIndexHtml(
      BufferedWriter writer,
      List<QueryAuditReport> reports,
      Map<String, List<QueryAuditReport>> byClass,
      List<RankedIssue> rankedIssues,
      List<DeduplicatedIssue> deduplicatedIssues)
      throws IOException {
    // Aggregate global stats
    int totalTests = reports.size();
    int totalQueries = reports.stream().mapToInt(QueryAuditReport::getTotalQueryCount).sum();
    long totalErrors = reports.stream().flatMap(r -> r.getErrors().stream()).count();
    long totalWarnings = reports.stream().flatMap(r -> r.getWarnings().stream()).count();
    long totalInfos =
        reports.stream()
            .filter(r -> r.getInfoIssues() != null)
            .flatMap(r -> r.getInfoIssues().stream())
            .count();
    long totalAcknowledged =
        reports.stream().mapToLong(QueryAuditReport::getAcknowledgedCount).sum();
    int totalClasses = byClass.size();
    int totalMethods = totalTests;

    String timestamp = LocalDateTime.now().format(TIMESTAMP_FMT);

    // Build and flush each section incrementally to avoid holding the entire
    // HTML document in memory at once (prevents OOM with large test suites).
    StringBuilder sb = new StringBuilder(16_384);

    sb.append("<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n");
    sb.append("<meta charset=\"UTF-8\">\n");
    sb.append("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n");
    sb.append("<title>Query Guard Report</title>\n");
    appendStyles(sb);
    sb.append("</head>\n<body>\n");

    // Header
    sb.append("<header class=\"header\">\n");
    sb.append("  <div class=\"header-content\">\n");
    sb.append("    <div class=\"header-title\">\n");
    sb.append(
        "      <svg class=\"logo\" viewBox=\"0 0 24 24\" width=\"32\" height=\"32\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\">");
    sb.append("<path d=\"M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z\"/></svg>\n");
    sb.append("      <h1>Query Guard Report</h1>\n");
    sb.append("    </div>\n");
    sb.append("    <span class=\"timestamp\">Generated: ")
        .append(esc(timestamp))
        .append("</span>\n");
    sb.append("  </div>\n");
    sb.append("</header>\n");

    sb.append("<main class=\"container\">\n");

    // Index page: classes table only. No summary bar (info is in the table).
    // Issues are visible inside each class > method detail page.
    appendClassesTable(sb, byClass);
    flushSection(sb, writer);

    // Unique Issues Summary (deduplicated cross-test view)
    if (deduplicatedIssues != null && !deduplicatedIssues.isEmpty()) {
      appendUniqueIssuesSummary(sb, deduplicatedIssues);
      flushSection(sb, writer);
    }

    // Top Issues by Impact
    if (rankedIssues != null && !rankedIssues.isEmpty()) {
      appendTopIssuesByImpact(sb, rankedIssues);
      flushSection(sb, writer);
    }

    sb.append("</main>\n");

    // Footer
    sb.append("<footer class=\"footer\">\n");
    sb.append("  <p>Query Guard &mdash; Static &amp; Runtime SQL Analysis</p>\n");
    sb.append("</footer>\n");

    appendScript(sb);
    flushSection(sb, writer);
  }

  // =========================================================================
  // {ClassName}.html — Class detail page
  // =========================================================================

  private void writeClassHtml(
      BufferedWriter writer, String className, List<QueryAuditReport> classReports)
      throws IOException {
    // Class-level stats
    int totalTests = classReports.size();
    int totalQueries = classReports.stream().mapToInt(QueryAuditReport::getTotalQueryCount).sum();
    long totalErrors = classReports.stream().flatMap(r -> r.getErrors().stream()).count();
    long totalWarnings = classReports.stream().flatMap(r -> r.getWarnings().stream()).count();
    long totalInfos =
        classReports.stream()
            .filter(r -> r.getInfoIssues() != null)
            .flatMap(r -> r.getInfoIssues().stream())
            .count();
    long totalIssues = totalErrors + totalWarnings + totalInfos;

    String timestamp = LocalDateTime.now().format(TIMESTAMP_FMT);

    // Build and flush each section incrementally to avoid holding the entire
    // HTML document in memory at once (prevents OOM with large test suites).
    StringBuilder sb = new StringBuilder(16_384);

    String reportHash = computeReportHash(classReports);

    sb.append("<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n");
    sb.append("<meta charset=\"UTF-8\">\n");
    sb.append("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n");
    sb.append("<meta name=\"qg-report-hash\" content=\"").append(reportHash).append("\">\n");
    sb.append("<title>Query Guard Report — ").append(esc(className)).append("</title>\n");
    appendStyles(sb);
    sb.append("</head>\n<body data-class=\"").append(esc(className)).append("\">\n");

    // Header
    sb.append("<header class=\"header\">\n");
    sb.append("  <div class=\"header-content\">\n");
    sb.append("    <div class=\"header-title\">\n");
    sb.append(
        "      <svg class=\"logo\" viewBox=\"0 0 24 24\" width=\"32\" height=\"32\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\">");
    sb.append("<path d=\"M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z\"/></svg>\n");
    sb.append("      <h1>").append(esc(className)).append("</h1>\n");
    sb.append("    </div>\n");
    sb.append("    <span class=\"timestamp\">Generated: ")
        .append(esc(timestamp))
        .append("</span>\n");
    sb.append("  </div>\n");
    sb.append("</header>\n");

    sb.append("<main class=\"container\">\n");

    // Breadcrumb
    sb.append("<div class=\"breadcrumb\">\n");
    sb.append("  <a href=\"index.html\">&larr; Back to Overview</a>\n");
    sb.append("</div>\n");

    // Class summary bar
    sb.append("<div class=\"summary-bar\">\n");
    sb.append("  <div class=\"stat\">").append(totalTests).append(" methods</div>\n");
    sb.append("  <div class=\"stat\">").append(totalQueries).append(" queries</div>\n");
    if (totalErrors > 0) {
      sb.append("  <div class=\"stat error\">").append(totalErrors).append(" errors</div>\n");
    }
    if (totalWarnings > 0) {
      sb.append("  <div class=\"stat warning\">")
          .append(totalWarnings)
          .append(" warnings</div>\n");
    }
    if (totalInfos > 0) {
      sb.append("  <div class=\"stat info\">").append(totalInfos).append(" info</div>\n");
    }
    if (totalErrors == 0 && totalWarnings == 0) {
      sb.append("  <div class=\"stat ok\">all clean</div>\n");
    }
    sb.append("  <div class=\"stat ok class-reviewed-status\" style=\"display:none\">all reviewed</div>\n");
    sb.append("</div>\n");
    flushSection(sb, writer);

    // Methods as collapsible cards
    appendMethodCards(sb, writer, classReports);

    sb.append("</main>\n");

    // Footer
    sb.append("<footer class=\"footer\">\n");
    sb.append("  <p>Query Guard &mdash; Static &amp; Runtime SQL Analysis</p>\n");
    sb.append("</footer>\n");

    appendScript(sb);
    flushSection(sb, writer);
  }

  // =========================================================================
  // Top Issues by Impact (for index.html)
  // =========================================================================

  private void appendUniqueIssuesSummary(
      StringBuilder sb, List<DeduplicatedIssue> deduplicatedIssues) {
    sb.append("<section class=\"section\">\n");
    sb.append("  <h2>Unique Issues Summary</h2>\n");
    sb.append(
        "  <p class=\"section-desc\">Each unique issue is shown once, with the number of tests affected.</p>\n");
    sb.append("  <div class=\"table-wrapper\">\n");
    sb.append("  <table class=\"breakdown-table dedup-table\">\n");
    sb.append("    <thead><tr>");
    sb.append(
        "<th>Issue Type</th><th>Target</th><th>Occurrences</th><th>SQL (truncated)</th><th>Fix</th><th>Affected Tests</th>");
    sb.append("</tr></thead>\n");
    sb.append("    <tbody>\n");

    int rowIndex = 0;
    for (DeduplicatedIssue di : deduplicatedIssues) {
      rowIndex++;
      Issue issue = di.issue();
      String sevClass = severityCssClass(di.highestSeverity());
      int count = di.occurrenceCount();

      // Occurrence badge color: red if >20, yellow if >5, gray if <=5
      String countBadgeClass;
      if (count > 20) {
        countBadgeClass = "badge-error";
      } else if (count > 5) {
        countBadgeClass = "badge-warning";
      } else {
        countBadgeClass = "badge-neutral";
      }

      // Truncated SQL (80 chars)
      String sql = issue.query() != null ? issue.query() : "";
      String truncatedSql = sql.length() > 80 ? sql.substring(0, 80) + "..." : sql;

      // Target: table.column
      String target = "";
      if (issue.table() != null && !issue.table().isBlank()) {
        target = issue.table();
        if (issue.column() != null && !issue.column().isBlank()) {
          target += "." + issue.column();
        }
      }

      // Fix suggestion
      String fix = issue.suggestion() != null ? issue.suggestion() : "";

      sb.append("    <tr>\n");

      // Issue type badge
      sb.append("      <td><span class=\"badge ")
          .append(sevClass)
          .append("\">")
          .append(di.highestSeverity())
          .append("</span> ")
          .append(esc(issue.type().getDescription()))
          .append("</td>\n");

      // Target
      sb.append("      <td><code>").append(esc(target)).append("</code></td>\n");

      // Occurrence count badge
      sb.append("      <td class=\"count-cell\"><span class=\"badge ")
          .append(countBadgeClass)
          .append("\">")
          .append("&times;")
          .append(count)
          .append("</span></td>\n");

      // Truncated SQL
      sb.append("      <td><code class=\"dedup-sql\">")
          .append(esc(truncatedSql))
          .append("</code></td>\n");

      // Fix suggestion
      sb.append("      <td class=\"fix-cell\">").append(esc(fix)).append("</td>\n");

      // Affected tests (collapsible)
      List<String> tests = di.affectedTests();
      sb.append("      <td>\n");
      if (!tests.isEmpty()) {
        int showCount = Math.min(3, tests.size());
        for (int i = 0; i < showCount; i++) {
          appendTestLink(sb, tests.get(i), "        ");
          if (i < showCount - 1) sb.append("        <br>\n");
        }
        int remaining = tests.size() - showCount;
        if (remaining > 0) {
          String detailId = "dedup-tests-" + rowIndex;
          sb.append("        <details class=\"dedup-more\">\n");
          sb.append("          <summary>and ").append(remaining).append(" more...</summary>\n");
          sb.append("          <div id=\"").append(detailId).append("\">\n");
          for (int i = showCount; i < tests.size(); i++) {
            appendTestLink(sb, tests.get(i), "            ");
            sb.append("<br>\n");
          }
          sb.append("          </div>\n");
          sb.append("        </details>\n");
        }
        // If total affected tests were capped at 10 and count > tests.size()
        if (di.occurrenceCount() > tests.size()) {
          sb.append("        <span class=\"affected-test-overflow\">... and ")
              .append(di.occurrenceCount() - tests.size())
              .append(" more occurrences</span>\n");
        }
      }
      sb.append("      </td>\n");

      sb.append("    </tr>\n");
    }

    sb.append("    </tbody>\n");
    sb.append("  </table>\n");
    sb.append("  </div>\n");
    sb.append("</section>\n");
  }

  private void appendTopIssuesByImpact(StringBuilder sb, List<RankedIssue> rankedIssues) {
    int limit = Math.min(rankedIssues.size(), 20);
    int maxScore = rankedIssues.isEmpty() ? 1 : rankedIssues.get(0).impactScore();
    if (maxScore == 0) maxScore = 1;

    sb.append("<section class=\"section\">\n");
    sb.append("  <h2>Top Issues by Impact</h2>\n");
    sb.append(
        "  <p class=\"section-desc\">Ranked by impact score (frequency &times; severity &times; pattern weight). ");
    sb.append("Higher score = higher priority to fix.</p>\n");
    sb.append("  <div class=\"ranked-issues\">\n");

    for (int i = 0; i < limit; i++) {
      RankedIssue ri = rankedIssues.get(i);
      Issue issue = ri.issue();
      boolean isTop3 = ri.rank() <= 3;
      String sevClass = severityCssClass(issue.severity());
      double barPct = (double) ri.impactScore() / maxScore * 100.0;

      sb.append("    <div class=\"ranked-card");
      if (isTop3) sb.append(" ranked-highlight");
      sb.append("\">\n");

      // Rank number
      sb.append("      <div class=\"ranked-rank\">#").append(ri.rank()).append("</div>\n");

      // Main content
      sb.append("      <div class=\"ranked-body\">\n");

      // Header row: badge + type + target
      sb.append("        <div class=\"ranked-header\">\n");
      sb.append("          <span class=\"badge ")
          .append(sevClass)
          .append("\">")
          .append(issue.severity())
          .append("</span>\n");
      sb.append("          <span class=\"ranked-type\">")
          .append(esc(issue.type().getDescription()))
          .append("</span>\n");

      // Target (table.column)
      if (issue.table() != null && !issue.table().isBlank()) {
        sb.append("          <span class=\"ranked-target\">");
        sb.append(esc(issue.table()));
        if (issue.column() != null && !issue.column().isBlank()) {
          sb.append(".").append(esc(issue.column()));
        }
        sb.append("</span>\n");
      }

      sb.append("          <span class=\"ranked-freq\">")
          .append(ri.frequency())
          .append("x across tests</span>\n");
      sb.append("        </div>\n");

      // Impact score bar
      sb.append("        <div class=\"ranked-score-row\">\n");
      sb.append("          <span class=\"ranked-score-value\">")
          .append(ri.impactScore())
          .append(" pts</span>\n");
      sb.append("          <div class=\"ranked-score-track\">\n");
      sb.append("            <div class=\"ranked-score-bar ")
          .append(sevClass)
          .append("\" style=\"width:")
          .append(String.format("%.1f", barPct))
          .append("%\"></div>\n");
      sb.append("          </div>\n");
      sb.append("        </div>\n");

      // Truncated SQL
      if (issue.query() != null && !issue.query().isBlank()) {
        String truncated = issue.query().replaceAll("\\s+", " ").trim();
        if (truncated.length() > 120) {
          truncated = truncated.substring(0, 117) + "...";
        }
        sb.append("        <div class=\"ranked-sql\"><code>")
            .append(esc(truncated))
            .append("</code></div>\n");
      }

      // Fix suggestion
      if (issue.suggestion() != null && !issue.suggestion().isBlank()) {
        sb.append("        <div class=\"ranked-fix\">Fix: ")
            .append(esc(issue.suggestion()))
            .append("</div>\n");
      }

      sb.append("      </div>\n"); // ranked-body
      sb.append("    </div>\n"); // ranked-card
    }

    sb.append("  </div>\n");
    sb.append("</section>\n");
  }

  // =========================================================================
  // Class-level cards (for index.html)
  // =========================================================================

  private void appendClassesTable(
      StringBuilder sb, Map<String, List<QueryAuditReport>> byClass) {
    sb.append("<section class=\"section\">\n");
    sb.append("  <h2>Classes</h2>\n");
    sb.append("  <div class=\"table-wrapper\">\n");
    sb.append("  <table class=\"classes-table\">\n");
    sb.append("    <thead><tr>");
    sb.append("<th>Class</th><th>Tests</th><th>Issues</th><th>Queries</th><th>Duration</th>");
    sb.append("<th>Status</th>");
    sb.append("</tr></thead>\n");
    sb.append("    <tbody>\n");

    // Sort: failing classes first, then passing
    List<Map.Entry<String, List<QueryAuditReport>>> sorted = new ArrayList<>(byClass.entrySet());
    sorted.sort(
        Comparator.<Map.Entry<String, List<QueryAuditReport>>, Boolean>comparing(
                e -> {
                  long errors = e.getValue().stream().mapToInt(r -> r.getErrors().size()).sum();
                  long warnings = e.getValue().stream().mapToInt(r -> r.getWarnings().size()).sum();
                  return errors == 0 && warnings == 0;
                })
            .thenComparing(Map.Entry::getKey));

    for (Map.Entry<String, List<QueryAuditReport>> entry : sorted) {
      String className = entry.getKey();
      List<QueryAuditReport> classReports = entry.getValue();
      int testCount = classReports.size();
      int queryCount =
          classReports.stream().mapToInt(QueryAuditReport::getTotalQueryCount).sum();
      long errorCount = classReports.stream().mapToInt(r -> r.getErrors().size()).sum();
      long warningCount = classReports.stream().mapToInt(r -> r.getWarnings().size()).sum();
      long infoCount =
          classReports.stream()
              .filter(r -> r.getInfoIssues() != null)
              .flatMap(r -> r.getInfoIssues().stream())
              .count();
      long issueCount = errorCount + warningCount + infoCount;
      boolean hasIssues = errorCount > 0 || warningCount > 0;
      long durationMs =
          classReports.stream()
              .mapToLong(QueryAuditReport::getTotalExecutionTimeNanos)
              .sum()
              / 1_000_000L;
      String durationStr =
          durationMs >= 1000
              ? String.format("%.1fs", durationMs / 1000.0)
              : durationMs + "ms";

      String classHash = computeReportHash(classReports);
      sb.append("    <tr class=\"").append(hasIssues ? "row-fail" : "row-pass")
          .append("\" data-class=\"").append(esc(className))
          .append("\" data-hash=\"").append(classHash).append("\">\n");
      sb.append("      <td><a href=\"")
          .append(esc(classFileName(className)))
          .append("\">")
          .append(esc(className))
          .append("</a></td>\n");
      sb.append("      <td class=\"num-cell\">").append(testCount).append("</td>\n");
      sb.append("      <td class=\"num-cell\">");
      if (issueCount > 0) {
        sb.append("<span class=\"badge ").append(hasIssues ? "badge-error" : "badge-info");
        sb.append("\">").append(issueCount).append("</span>");
      } else {
        sb.append("0");
      }
      sb.append("</td>\n");
      sb.append("      <td class=\"num-cell\">").append(queryCount).append("</td>\n");
      sb.append("      <td class=\"num-cell\">").append(esc(durationStr)).append("</td>\n");
      sb.append("      <td>");
      if (hasIssues) {
        sb.append("<span class=\"status-dot error-dot\"></span>");
      } else {
        sb.append("<span class=\"status-dot ok-dot\"></span>");
      }
      sb.append("</td>\n");
      sb.append("    </tr>\n");
    }

    sb.append("    </tbody>\n");
    sb.append("  </table>\n");
    sb.append("  </div>\n");
    sb.append("</section>\n");
  }

  // =========================================================================
  // Flat per-test results (for class detail pages)
  // =========================================================================

  private void appendFlatTestResults(StringBuilder sb, List<QueryAuditReport> reports) {
    sb.append("<section class=\"section\">\n");
    sb.append("  <h2>Per-Test Results</h2>\n");

    if (reports.isEmpty()) {
      sb.append("  <p class=\"empty-message\">No test reports collected.</p>\n");
      sb.append("</section>\n");
      return;
    }

    int testIndex = 0;
    for (QueryAuditReport report : reports) {
      testIndex++;
      boolean hasIssues = report.hasConfirmedIssues();
      String statusClass = hasIssues ? "test-fail" : "test-pass";
      String statusIcon = hasIssues ? "&#x2716;" : "&#x2714;";
      int errorCount = report.getErrors().size();
      int warningCount = report.getWarnings().size();
      int infoCount = report.getInfoIssues() != null ? report.getInfoIssues().size() : 0;
      int ackCount = report.getAcknowledgedCount();
      int passedCount =
          Math.max(0, report.getTotalQueryCount() - errorCount - warningCount - infoCount);

      String testAnchor = sanitizeAnchor(report.getTestName());
      sb.append("  <details class=\"test-card ").append(statusClass).append("\"");
      sb.append(" id=\"test-").append(testAnchor).append("\"");
      // All methods start collapsed — user expands what they need
      sb.append(">\n");
      sb.append("    <summary class=\"test-summary\">\n");
      sb.append("      <span class=\"test-number\">#").append(testIndex).append("</span>\n");
      sb.append("      <span class=\"test-status\">").append(statusIcon).append("</span>\n");
      sb.append("      <span class=\"test-name\">")
          .append(esc(report.getTestName()))
          .append("</span>\n");
      sb.append("      <span class=\"test-meta\">\n");
      sb.append("        <span class=\"meta-item meta-queries\">")
          .append(report.getTotalQueryCount())
          .append(" queries</span>\n");
      if (errorCount > 0) {
        sb.append("        <span class=\"meta-item badge badge-error\">")
            .append(errorCount)
            .append(" error")
            .append(errorCount > 1 ? "s" : "")
            .append("</span>\n");
      }
      if (warningCount > 0) {
        sb.append("        <span class=\"meta-item badge badge-warning\">")
            .append(warningCount)
            .append(" warning")
            .append(warningCount > 1 ? "s" : "")
            .append("</span>\n");
      }
      if (ackCount > 0) {
        sb.append("        <span class=\"meta-item badge badge-acknowledged\">")
            .append(ackCount)
            .append(" acknowledged</span>\n");
      }
      if (passedCount > 0 && !hasIssues) {
        sb.append("        <span class=\"meta-item badge badge-ok\">all passed</span>\n");
      }
      sb.append("      </span>\n");
      sb.append("    </summary>\n");

      // Stats bar inside detail
      sb.append("    <div class=\"test-detail\">\n");
      sb.append("      <div class=\"test-stats-bar\">\n");
      int total = report.getTotalQueryCount();
      if (total > 0) {
        double errPct = (double) errorCount / total * 100;
        double warnPct = (double) warningCount / total * 100;
        double passPct = 100.0 - errPct - warnPct;
        sb.append("        <div class=\"stats-track\">\n");
        if (errPct > 0)
          sb.append("          <div class=\"stats-fill stats-error\" style=\"width:")
              .append(String.format("%.1f", errPct))
              .append("%\"></div>\n");
        if (warnPct > 0)
          sb.append("          <div class=\"stats-fill stats-warning\" style=\"width:")
              .append(String.format("%.1f", warnPct))
              .append("%\"></div>\n");
        if (passPct > 0)
          sb.append("          <div class=\"stats-fill stats-pass\" style=\"width:")
              .append(String.format("%.1f", passPct))
              .append("%\"></div>\n");
        sb.append("        </div>\n");
        sb.append("        <div class=\"stats-labels\">\n");
        long ms = report.getTotalExecutionTimeNanos() / 1_000_000L;
        sb.append("          <span>")
            .append(errorCount)
            .append(" errors &middot; ")
            .append(warningCount)
            .append(" warnings &middot; ")
            .append(passedCount)
            .append(" passed</span>\n");
        sb.append("          <span>")
            .append(report.getUniquePatternCount())
            .append(" patterns &middot; ")
            .append(ms)
            .append("ms</span>\n");
        sb.append("        </div>\n");
      }
      sb.append("      </div>\n");

      // Query Timeline
      appendQueryTimeline(sb, report);

      // Query Patterns
      appendQueryPatterns(sb, report);

      // Confirmed issues
      if (report.getConfirmedIssues() != null && !report.getConfirmedIssues().isEmpty()) {
        sb.append("      <h4>Confirmed Issues</h4>\n");
        for (Issue issue : report.getConfirmedIssues()) {
          appendIssueCard(sb, issue);
        }
      }

      // Acknowledged issues
      List<Issue> ackIssues = report.getAcknowledgedIssues();
      if (!ackIssues.isEmpty()) {
        sb.append("      <h4>Acknowledged</h4>\n");
        for (Issue issue : ackIssues) {
          appendAcknowledgedIssueCard(sb, issue);
        }
      }

      // Info issues
      if (report.getInfoIssues() != null && !report.getInfoIssues().isEmpty()) {
        sb.append("      <h4>Informational</h4>\n");
        for (Issue issue : report.getInfoIssues()) {
          appendIssueCard(sb, issue);
        }
      }

      // No issues
      boolean hasAck = !ackIssues.isEmpty();
      if (!hasIssues
          && !hasAck
          && (report.getInfoIssues() == null || report.getInfoIssues().isEmpty())) {
        sb.append(
            "      <p class=\"no-issues\">&#x2705; No issues detected. All queries look good.</p>\n");
      }

      sb.append("    </div>\n");
      sb.append("  </details>\n");
    }

    sb.append("</section>\n");
  }

  /**
   * Streaming variant of {@link #appendFlatTestResults} that flushes each test card to the writer
   * individually, so only one card is held in memory at a time. This prevents OOM when a class has
   * thousands of tests.
   */
  private void appendFlatTestResultsStreaming(
      StringBuilder sb, BufferedWriter writer, List<QueryAuditReport> reports) throws IOException {
    sb.append("<section class=\"section\">\n");
    sb.append("  <h2>Per-Test Results</h2>\n");

    if (reports.isEmpty()) {
      sb.append("  <p class=\"empty-message\">No test reports collected.</p>\n");
      sb.append("</section>\n");
      flushSection(sb, writer);
      return;
    }

    // Flush the section header before iterating
    flushSection(sb, writer);

    int testIndex = 0;
    for (QueryAuditReport report : reports) {
      testIndex++;
      boolean hasIssues = report.hasConfirmedIssues();
      String statusClass = hasIssues ? "test-fail" : "test-pass";
      String statusIcon = hasIssues ? "&#x2716;" : "&#x2714;";
      int errorCount = report.getErrors().size();
      int warningCount = report.getWarnings().size();
      int infoCount = report.getInfoIssues() != null ? report.getInfoIssues().size() : 0;
      int ackCount = report.getAcknowledgedCount();
      int passedCount =
          Math.max(0, report.getTotalQueryCount() - errorCount - warningCount - infoCount);

      String testAnchor = sanitizeAnchor(report.getTestName());
      sb.append("  <details class=\"test-card ").append(statusClass).append("\"");
      sb.append(" id=\"test-").append(testAnchor).append("\"");
      // All methods start collapsed — user expands what they need
      sb.append(">\n");
      sb.append("    <summary class=\"test-summary\">\n");
      sb.append("      <span class=\"test-number\">#").append(testIndex).append("</span>\n");
      sb.append("      <span class=\"test-status\">").append(statusIcon).append("</span>\n");
      sb.append("      <span class=\"test-name\">")
          .append(esc(report.getTestName()))
          .append("</span>\n");
      sb.append("      <span class=\"test-meta\">\n");
      sb.append("        <span class=\"meta-item meta-queries\">")
          .append(report.getTotalQueryCount())
          .append(" queries</span>\n");
      if (errorCount > 0) {
        sb.append("        <span class=\"meta-item badge badge-error\">")
            .append(errorCount)
            .append(" error")
            .append(errorCount > 1 ? "s" : "")
            .append("</span>\n");
      }
      if (warningCount > 0) {
        sb.append("        <span class=\"meta-item badge badge-warning\">")
            .append(warningCount)
            .append(" warning")
            .append(warningCount > 1 ? "s" : "")
            .append("</span>\n");
      }
      if (ackCount > 0) {
        sb.append("        <span class=\"meta-item badge badge-acknowledged\">")
            .append(ackCount)
            .append(" acknowledged</span>\n");
      }
      if (passedCount > 0 && !hasIssues) {
        sb.append("        <span class=\"meta-item badge badge-ok\">all passed</span>\n");
      }
      sb.append("      </span>\n");
      sb.append("    </summary>\n");

      // Stats bar inside detail
      sb.append("    <div class=\"test-detail\">\n");
      sb.append("      <div class=\"test-stats-bar\">\n");
      int total = report.getTotalQueryCount();
      if (total > 0) {
        double errPct = (double) errorCount / total * 100;
        double warnPct = (double) warningCount / total * 100;
        double passPct = 100.0 - errPct - warnPct;
        sb.append("        <div class=\"stats-track\">\n");
        if (errPct > 0)
          sb.append("          <div class=\"stats-fill stats-error\" style=\"width:")
              .append(String.format("%.1f", errPct))
              .append("%\"></div>\n");
        if (warnPct > 0)
          sb.append("          <div class=\"stats-fill stats-warning\" style=\"width:")
              .append(String.format("%.1f", warnPct))
              .append("%\"></div>\n");
        if (passPct > 0)
          sb.append("          <div class=\"stats-fill stats-pass\" style=\"width:")
              .append(String.format("%.1f", passPct))
              .append("%\"></div>\n");
        sb.append("        </div>\n");
        sb.append("        <div class=\"stats-labels\">\n");
        long ms = report.getTotalExecutionTimeNanos() / 1_000_000L;
        sb.append("          <span>")
            .append(errorCount)
            .append(" errors &middot; ")
            .append(warningCount)
            .append(" warnings &middot; ")
            .append(passedCount)
            .append(" passed</span>\n");
        sb.append("          <span>")
            .append(report.getUniquePatternCount())
            .append(" patterns &middot; ")
            .append(ms)
            .append("ms</span>\n");
        sb.append("        </div>\n");
      }
      sb.append("      </div>\n");

      // Query Timeline
      appendQueryTimeline(sb, report);

      // Query Patterns
      appendQueryPatterns(sb, report);

      // Confirmed issues
      if (report.getConfirmedIssues() != null && !report.getConfirmedIssues().isEmpty()) {
        sb.append("      <h4>Confirmed Issues</h4>\n");
        for (Issue issue : report.getConfirmedIssues()) {
          appendIssueCard(sb, issue);
        }
      }

      // Acknowledged issues
      List<Issue> ackIssues = report.getAcknowledgedIssues();
      if (!ackIssues.isEmpty()) {
        sb.append("      <h4>Acknowledged</h4>\n");
        for (Issue issue : ackIssues) {
          appendAcknowledgedIssueCard(sb, issue);
        }
      }

      // Info issues
      if (report.getInfoIssues() != null && !report.getInfoIssues().isEmpty()) {
        sb.append("      <h4>Informational</h4>\n");
        for (Issue issue : report.getInfoIssues()) {
          appendIssueCard(sb, issue);
        }
      }

      // No issues
      boolean hasAck = !ackIssues.isEmpty();
      if (!hasIssues
          && !hasAck
          && (report.getInfoIssues() == null || report.getInfoIssues().isEmpty())) {
        sb.append(
            "      <p class=\"no-issues\">&#x2705; No issues detected. All queries look good.</p>\n");
      }

      sb.append("    </div>\n");
      sb.append("  </details>\n");

      // Flush each test card to writer to keep memory bounded
      flushSection(sb, writer);
    }

    sb.append("</section>\n");
    flushSection(sb, writer);
  }

  // =========================================================================
  // Hierarchical per-test results: Method > Issues/Queries
  // =========================================================================

  /**
   * Renders class detail page with hierarchy: each test method is a collapsible section containing
   * its issues (expanded by default) and queries (collapsed by default).
   */
  private void appendHierarchicalTestResults(
      StringBuilder sb, BufferedWriter writer, List<QueryAuditReport> reports) throws IOException {
    sb.append("<section class=\"section\">\n");
    sb.append("  <h2>Test Methods</h2>\n");

    if (reports.isEmpty()) {
      sb.append("  <p class=\"empty-message\">No test reports collected.</p>\n");
      sb.append("</section>\n");
      flushSection(sb, writer);
      return;
    }

    // Flush section header
    flushSection(sb, writer);

    int methodIndex = 0;
    for (QueryAuditReport report : reports) {
      methodIndex++;
      boolean hasIssues = report.hasConfirmedIssues();
      int errorCount = report.getErrors().size();
      int warningCount = report.getWarnings().size();
      int infoCount = report.getInfoIssues() != null ? report.getInfoIssues().size() : 0;
      int ackCount = report.getAcknowledgedCount();
      int issueCount = errorCount + warningCount;
      int queryCount = report.getTotalQueryCount();
      String statusClass = hasIssues ? "test-fail" : "test-pass";
      String statusIcon = hasIssues ? "&#x2716;" : "&#x2714;";

      // Method-level collapsible: open if has issues
      sb.append("  <details class=\"method-card ").append(statusClass).append("\"");
      // All methods start collapsed — user expands what they need
      sb.append(">\n");
      sb.append("    <summary class=\"method-summary\">\n");
      sb.append("      <span class=\"test-status\">").append(statusIcon).append("</span>\n");
      sb.append("      <span class=\"method-name\">")
          .append(esc(report.getTestName()))
          .append("</span>\n");
      sb.append("      <span class=\"method-meta\">\n");
      sb.append("        <span class=\"meta-item meta-queries\">")
          .append(queryCount)
          .append(" queries</span>\n");
      if (errorCount > 0) {
        sb.append("        <span class=\"meta-item badge badge-error\">")
            .append(errorCount)
            .append(" error")
            .append(errorCount > 1 ? "s" : "")
            .append("</span>\n");
      }
      if (warningCount > 0) {
        sb.append("        <span class=\"meta-item badge badge-warning\">")
            .append(warningCount)
            .append(" warning")
            .append(warningCount > 1 ? "s" : "")
            .append("</span>\n");
      }
      if (ackCount > 0) {
        sb.append("        <span class=\"meta-item badge badge-acknowledged\">")
            .append(ackCount)
            .append(" acknowledged</span>\n");
      }
      if (!hasIssues && ackCount == 0) {
        sb.append("        <span class=\"meta-item badge badge-ok\">all passed</span>\n");
      }
      sb.append("      </span>\n");
      sb.append("    </summary>\n");

      sb.append("    <div class=\"method-detail\">\n");

      // Stats bar
      int total = report.getTotalQueryCount();
      if (total > 0) {
        double errPct = (double) errorCount / total * 100;
        double warnPct = (double) warningCount / total * 100;
        double passPct = 100.0 - errPct - warnPct;
        sb.append("      <div class=\"test-stats-bar\">\n");
        sb.append("        <div class=\"stats-track\">\n");
        if (errPct > 0)
          sb.append("          <div class=\"stats-fill stats-error\" style=\"width:")
              .append(String.format("%.1f", errPct))
              .append("%\"></div>\n");
        if (warnPct > 0)
          sb.append("          <div class=\"stats-fill stats-warning\" style=\"width:")
              .append(String.format("%.1f", warnPct))
              .append("%\"></div>\n");
        if (passPct > 0)
          sb.append("          <div class=\"stats-fill stats-pass\" style=\"width:")
              .append(String.format("%.1f", passPct))
              .append("%\"></div>\n");
        sb.append("        </div>\n");
        sb.append("        <div class=\"stats-labels\">\n");
        long ms = report.getTotalExecutionTimeNanos() / 1_000_000L;
        sb.append("          <span>")
            .append(errorCount)
            .append(" errors &middot; ")
            .append(warningCount)
            .append(" warnings &middot; ")
            .append(Math.max(0, total - errorCount - warningCount - infoCount))
            .append(" passed</span>\n");
        sb.append("          <span>")
            .append(report.getUniquePatternCount())
            .append(" patterns &middot; ")
            .append(ms)
            .append("ms</span>\n");
        sb.append("        </div>\n");
        sb.append("      </div>\n");
      }

      // --- Issues section (expanded by default) ---
      List<Issue> confirmedIssues = report.getConfirmedIssues();
      List<Issue> ackIssues = report.getAcknowledgedIssues();
      List<Issue> infoIssues = report.getInfoIssues();
      boolean hasAnyIssues =
          (confirmedIssues != null && !confirmedIssues.isEmpty())
              || !ackIssues.isEmpty()
              || (infoIssues != null && !infoIssues.isEmpty());

      if (hasAnyIssues) {
        sb.append("      <details class=\"inner-section issues-section\">\n");
        sb.append("        <summary class=\"inner-summary\">Issues (")
            .append(issueCount + infoCount + ackCount)
            .append(")</summary>\n");
        sb.append("        <div class=\"inner-content\">\n");

        if (confirmedIssues != null && !confirmedIssues.isEmpty()) {
          for (Issue issue : confirmedIssues) {
            appendIssueCard(sb, issue);
          }
        }

        if (!ackIssues.isEmpty()) {
          sb.append("          <h5 class=\"sub-heading\">Acknowledged</h5>\n");
          for (Issue issue : ackIssues) {
            appendAcknowledgedIssueCard(sb, issue);
          }
        }

        if (infoIssues != null && !infoIssues.isEmpty()) {
          sb.append("          <h5 class=\"sub-heading\">Informational</h5>\n");
          for (Issue issue : infoIssues) {
            appendIssueCard(sb, issue);
          }
        }

        sb.append("        </div>\n");
        sb.append("      </details>\n");
      } else {
        sb.append(
            "      <p class=\"no-issues\">&#x2714; No issues detected. All queries look good.</p>\n");
      }

      // --- Queries section (collapsed by default) ---
      sb.append("      <details class=\"inner-section queries-section\">\n");
      sb.append("        <summary class=\"inner-summary\">Queries (")
          .append(queryCount)
          .append(")</summary>\n");
      sb.append("        <div class=\"inner-content\">\n");

      // Query Timeline
      appendQueryTimeline(sb, report);

      // Query Patterns
      appendQueryPatterns(sb, report);

      sb.append("        </div>\n");
      sb.append("      </details>\n");

      sb.append("    </div>\n"); // method-detail
      sb.append("  </details>\n"); // method-card

      // Flush each method to keep memory bounded
      flushSection(sb, writer);
    }

    sb.append("</section>\n");
    flushSection(sb, writer);
  }

  // =========================================================================
  // Method cards (for class detail pages — new Gradle-style design)
  // =========================================================================

  /**
   * Renders method cards for the class detail page. Each method is a collapsible card. Methods with
   * issues are open by default; clean methods are collapsed.
   */
  private void appendMethodCards(
      StringBuilder sb, BufferedWriter writer, List<QueryAuditReport> reports) throws IOException {
    if (reports.isEmpty()) {
      sb.append("<p class=\"empty-message\">No test reports collected.</p>\n");
      flushSection(sb, writer);
      return;
    }

    // Flush before iterating
    flushSection(sb, writer);

    for (QueryAuditReport report : reports) {
      boolean hasIssues = report.hasConfirmedIssues();
      int errorCount = report.getErrors().size();
      int warningCount = report.getWarnings().size();
      int infoCount = report.getInfoIssues() != null ? report.getInfoIssues().size() : 0;
      int ackCount = report.getAcknowledgedCount();
      int issueCount = errorCount + warningCount + infoCount;
      int queryCount = report.getTotalQueryCount();

      // Determine status class
      String statusDot;
      String statusClass;
      if (errorCount > 0) {
        statusDot = "<span class=\"status-indicator error-dot\"></span>";
        statusClass = "method-error";
      } else if (warningCount > 0) {
        statusDot = "<span class=\"status-indicator warning-dot\"></span>";
        statusClass = "method-warning";
      } else {
        statusDot = "<span class=\"status-indicator ok-dot\"></span>";
        statusClass = "method-ok";
      }

      sb.append("<details class=\"method ").append(statusClass).append("\"");
      // All methods start collapsed — user expands what they need
      sb.append(">\n");
      sb.append("  <summary>\n");
      sb.append("    ").append(statusDot).append("\n");
      sb.append("    <span class=\"name\">")
          .append(esc(report.getTestName()))
          .append("</span>\n");
      long execTimeMs = report.getTotalExecutionTimeNanos() / 1_000_000;
      int uniquePatterns = report.getUniquePatternCount();
      sb.append("    <span class=\"meta\">")
          .append(queryCount).append(" queries")
          .append(" (").append(uniquePatterns).append(" unique)")
          .append(", ").append(issueCount).append(" issue").append(issueCount != 1 ? "s" : "")
          .append(", ").append(execTimeMs).append("ms")
          .append("</span>\n");
      sb.append("  </summary>\n");

      List<Issue> confirmedIssues = report.getConfirmedIssues();
      List<Issue> ackIssues = report.getAcknowledgedIssues();
      List<Issue> infoIssues = report.getInfoIssues();

      // 1) Queries section FIRST (always expanded)
      if (queryCount > 0) {
        sb.append("  <details class=\"queries\">\n");
        sb.append("    <summary>Queries (").append(queryCount).append(")</summary>\n");
        sb.append("    <div class=\"queries-content\">\n");
        appendQueryTimeline(sb, report);
        appendQueryPatterns(sb, report);
        sb.append("    </div>\n");
        sb.append("  </details>\n");
      }

      // 2) Issues section SECOND (warnings/errors below queries)
      if ((confirmedIssues != null && !confirmedIssues.isEmpty())
          || (infoIssues != null && !infoIssues.isEmpty())) {
        sb.append("  <div class=\"issues\">\n");
        if (confirmedIssues != null) {
          for (Issue issue : confirmedIssues) {
            appendCompactIssue(sb, issue);
          }
        }
        if (infoIssues != null) {
          for (Issue issue : infoIssues) {
            appendCompactIssue(sb, issue);
          }
        }
        sb.append("  </div>\n");
      }

      // 3) Acknowledged issues
      if (!ackIssues.isEmpty()) {
        sb.append("  <div class=\"issues acknowledged-issues\">\n");
        for (Issue issue : ackIssues) {
          appendAcknowledgedIssueCard(sb, issue);
        }
        sb.append("  </div>\n");
      }

      // No issues message
      if (!hasIssues
          && ackIssues.isEmpty()
          && (infoIssues == null || infoIssues.isEmpty())) {
        sb.append(
            "  <p class=\"no-issues\">No issues detected. All queries look good.</p>\n");
      }

      sb.append("</details>\n");

      // Flush each method to keep memory bounded
      flushSection(sb, writer);
    }
  }

  /**
   * Renders a compact issue card for the class detail page method cards.
   */
  private void appendCompactIssue(StringBuilder sb, Issue issue) {
    String sevClass = severityCssClass(issue.severity());
    String checkKey = issueCheckKey(issue);
    sb.append("    <div class=\"issue ").append(sevClass).append("\">\n");
    sb.append("      <label class=\"issue-label\"><input type=\"checkbox\" class=\"issue-check\" data-key=\"")
        .append(esc(checkKey)).append("\"> ");
    sb.append("<span class=\"badge ")
        .append(sevClass)
        .append("\">[")
        .append(issue.severity())
        .append("]</span> ")
        .append(esc(issue.type().getDescription()))
        .append("</label>\n");

    if (issue.query() != null && !issue.query().isBlank()) {
      sb.append("      <code>").append(esc(issue.query())).append("</code>\n");
    }

    if (issue.suggestion() != null && !issue.suggestion().isBlank()) {
      sb.append("      <p class=\"fix\">Fix: ")
          .append(esc(issue.suggestion()))
          .append("</p>\n");
    }

    if (issue.sourceLocation() != null && !issue.sourceLocation().isBlank()) {
      sb.append("      <p class=\"source\">Source: ")
          .append(esc(issue.sourceLocation()))
          .append("</p>\n");
    }

    sb.append("    </div>\n");
  }

  // -------------------------------------------------------------------------
  // Summary dashboard
  // -------------------------------------------------------------------------

  private void appendSummaryDashboard(
      StringBuilder sb,
      int totalTests,
      int totalQueries,
      long totalErrors,
      long totalWarnings,
      long totalInfos,
      long totalAcknowledged,
      long totalConfirmed,
      int passedTests,
      int failedTests) {
    appendSummaryDashboard(
        sb,
        totalTests,
        totalQueries,
        totalErrors,
        totalWarnings,
        totalInfos,
        totalAcknowledged,
        totalConfirmed,
        passedTests,
        failedTests,
        -1);
  }

  private void appendSummaryDashboard(
      StringBuilder sb,
      int totalTests,
      int totalQueries,
      long totalErrors,
      long totalWarnings,
      long totalInfos,
      long totalAcknowledged,
      long totalConfirmed,
      int passedTests,
      int failedTests,
      int uniqueIssueCount) {
    sb.append("<section class=\"dashboard\">\n");
    sb.append("  <div class=\"stats-grid\">\n");
    appendStatCard(sb, "Tests Analyzed", String.valueOf(totalTests), "stat-neutral");
    appendStatCard(sb, "Total Queries", String.valueOf(totalQueries), "stat-neutral");
    appendStatCard(
        sb, "Errors", String.valueOf(totalErrors), totalErrors > 0 ? "stat-error" : "stat-ok");
    appendStatCard(
        sb,
        "Warnings",
        String.valueOf(totalWarnings),
        totalWarnings > 0 ? "stat-warning" : "stat-ok");
    if (uniqueIssueCount >= 0) {
      appendStatCard(
          sb,
          "Unique Issues",
          String.valueOf(uniqueIssueCount),
          uniqueIssueCount > 0 ? "stat-unique" : "stat-ok");
    }
    appendStatCard(sb, "Acknowledged", String.valueOf(totalAcknowledged), "stat-acknowledged");
    appendStatCard(sb, "Info", String.valueOf(totalInfos), "stat-info");
    appendStatCard(sb, "Passed Tests", String.valueOf(passedTests), "stat-ok");
    sb.append("  </div>\n");

    // Donut chart
    long total = totalErrors + totalWarnings + totalInfos;
    if (total > 0 || totalTests > 0) {
      sb.append("  <div class=\"chart-section\">\n");
      sb.append("    <h2>Issue Distribution</h2>\n");
      sb.append("    <div class=\"chart-row\">\n");

      // Issues donut
      appendDonutChart(
          sb,
          "By Severity",
          new long[] {totalErrors, totalWarnings, totalInfos},
          new String[] {"Errors", "Warnings", "Info"},
          new String[] {"var(--color-error)", "var(--color-warning)", "var(--color-info)"});

      // Tests donut
      appendDonutChart(
          sb,
          "Test Results",
          new long[] {failedTests, passedTests},
          new String[] {"Failed", "Passed"},
          new String[] {"var(--color-error)", "var(--color-ok)"});

      sb.append("    </div>\n");
      sb.append("  </div>\n");
    }

    sb.append("</section>\n");
  }

  private void appendStatCard(StringBuilder sb, String label, String value, String cssClass) {
    sb.append("    <div class=\"stat-card ").append(cssClass).append("\">\n");
    sb.append("      <div class=\"stat-value\">").append(esc(value)).append("</div>\n");
    sb.append("      <div class=\"stat-label\">").append(esc(label)).append("</div>\n");
    sb.append("    </div>\n");
  }

  private void appendDonutChart(
      StringBuilder sb, String title, long[] values, String[] labels, String[] colors) {
    long total = 0;
    for (long v : values) total += v;

    sb.append("      <div class=\"donut-container\">\n");
    sb.append("        <div class=\"donut-title\">").append(esc(title)).append("</div>\n");

    if (total == 0) {
      sb.append(
          "        <div class=\"donut\" style=\"background: conic-gradient(var(--color-border) 0deg 360deg)\">\n");
      sb.append("          <div class=\"donut-hole\">0</div>\n");
      sb.append("        </div>\n");
    } else {
      sb.append("        <div class=\"donut\" style=\"background: conic-gradient(");
      double cumDeg = 0;
      for (int i = 0; i < values.length; i++) {
        if (i > 0) sb.append(", ");
        double deg = (double) values[i] / total * 360.0;
        sb.append(colors[i]).append(" ").append(String.format("%.2f", cumDeg)).append("deg ");
        cumDeg += deg;
        sb.append(String.format("%.2f", cumDeg)).append("deg");
      }
      sb.append(")\">\n");
      sb.append("          <div class=\"donut-hole\">").append(total).append("</div>\n");
      sb.append("        </div>\n");
    }

    // Legend
    sb.append("        <div class=\"donut-legend\">\n");
    for (int i = 0; i < labels.length; i++) {
      sb.append("          <span class=\"legend-item\">");
      sb.append("<span class=\"legend-dot\" style=\"background:")
          .append(colors[i])
          .append("\"></span>");
      sb.append(esc(labels[i])).append(" (").append(values[i]).append(")");
      sb.append("</span>\n");
    }
    sb.append("        </div>\n");
    sb.append("      </div>\n");
  }

  // -------------------------------------------------------------------------
  // Issue breakdown table
  // -------------------------------------------------------------------------

  private void appendIssueBreakdownTable(StringBuilder sb, Map<IssueType, int[]> breakdown) {
    sb.append("<section class=\"section\">\n");
    sb.append("  <h2>Issue Type Breakdown</h2>\n");
    sb.append("  <div class=\"table-wrapper\">\n");
    sb.append("  <table class=\"breakdown-table\">\n");
    sb.append("    <thead><tr>");
    sb.append("<th>Issue Type</th><th>Code</th><th>Severity</th><th>Count</th>");
    sb.append("</tr></thead>\n");
    sb.append("    <tbody>\n");

    // Sort: ERROR first, then WARNING, then INFO, then by count desc
    List<Map.Entry<IssueType, int[]>> sorted = new ArrayList<>(breakdown.entrySet());
    sorted.sort(
        (a, b) -> {
          int sevCmp = a.getKey().getDefaultSeverity().compareTo(b.getKey().getDefaultSeverity());
          if (sevCmp != 0) return sevCmp;
          return Integer.compare(b.getValue()[0], a.getValue()[0]);
        });

    for (Map.Entry<IssueType, int[]> entry : sorted) {
      IssueType type = entry.getKey();
      int count = entry.getValue()[0];
      String sevClass = severityCssClass(type.getDefaultSeverity());
      sb.append("    <tr>\n");
      sb.append("      <td>").append(esc(type.getDescription())).append("</td>\n");
      sb.append("      <td><code>").append(esc(type.getCode())).append("</code></td>\n");
      sb.append("      <td><span class=\"badge ")
          .append(sevClass)
          .append("\">")
          .append(type.getDefaultSeverity())
          .append("</span></td>\n");
      sb.append("      <td class=\"count-cell\">").append(count).append("</td>\n");
      sb.append("    </tr>\n");
    }

    sb.append("    </tbody>\n");
    sb.append("  </table>\n");
    sb.append("  </div>\n");
    sb.append("</section>\n");
  }

  // -------------------------------------------------------------------------
  // Table Access Frequency
  // -------------------------------------------------------------------------

  private void appendTableAccessFrequency(StringBuilder sb, List<QueryAuditReport> reports) {
    // Aggregate table name counts across all reports
    Map<String, Integer> tableCounts = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    for (QueryAuditReport report : reports) {
      List<QueryRecord> queries = report.getAllQueries();
      if (queries == null) continue;
      for (QueryRecord q : queries) {
        List<String> tables = SqlParser.extractTableNames(q.sql());
        for (String table : tables) {
          tableCounts.merge(table, 1, Integer::sum);
        }
      }
    }

    if (tableCounts.isEmpty()) return;

    // Sort by count descending
    List<Map.Entry<String, Integer>> sorted = new ArrayList<>(tableCounts.entrySet());
    sorted.sort((a, b) -> Integer.compare(b.getValue(), a.getValue()));

    int maxCount = sorted.get(0).getValue();

    sb.append("<section class=\"section\">\n");
    sb.append("  <h2>Table Access Frequency</h2>\n");
    sb.append("  <div class=\"table-freq\">\n");

    for (Map.Entry<String, Integer> entry : sorted) {
      int count = entry.getValue();
      double pct = maxCount > 0 ? (double) count / maxCount * 100.0 : 0;
      sb.append("    <div class=\"freq-row\">\n");
      sb.append("      <span class=\"freq-table\">")
          .append(esc(entry.getKey()))
          .append("</span>\n");
      sb.append("      <div class=\"freq-bar-track\">\n");
      sb.append("        <div class=\"freq-bar\" style=\"width: ")
          .append(String.format("%.1f", pct))
          .append("%\"></div>\n");
      sb.append("      </div>\n");
      sb.append("      <span class=\"freq-count\">").append(count).append("</span>\n");
      sb.append("    </div>\n");
    }

    sb.append("  </div>\n");
    sb.append("</section>\n");
  }

  // -------------------------------------------------------------------------
  // Query timeline & patterns (shared by class detail pages)
  // -------------------------------------------------------------------------

  private void appendQueryTimeline(StringBuilder sb, QueryAuditReport report) {
    List<QueryRecord> queries = report.getAllQueries();
    if (queries == null || queries.isEmpty()) return;

    // Build set of repeated normalized patterns (3+ occurrences)
    Map<String, Integer> patternCounts = new LinkedHashMap<>();
    for (QueryRecord q : queries) {
      if (q.normalizedSql() != null) {
        patternCounts.merge(q.normalizedSql(), 1, Integer::sum);
      }
    }
    Set<String> repeatedPatterns = new HashSet<>();
    for (Map.Entry<String, Integer> entry : patternCounts.entrySet()) {
      if (entry.getValue() >= 3) {
        repeatedPatterns.add(entry.getKey());
      }
    }

    sb.append("      <details>\n");
    sb.append("        <summary><h4 style=\"display:inline\">Query Timeline</h4></summary>\n");
    sb.append("        <div class=\"timeline\">\n");

    int num = 0;
    for (QueryRecord q : queries) {
      num++;
      boolean repeated = q.normalizedSql() != null && repeatedPatterns.contains(q.normalizedSql());
      String sql = q.sql() != null ? q.sql() : "";
      String truncatedSql = sql.length() > 100 ? sql.substring(0, 100) + "..." : sql;
      double ms = q.executionTimeNanos() / 1_000_000.0;
      String source = q.stackTrace() != null ? q.stackTrace() : "";
      // Extract a short source reference from stack trace (first meaningful line)
      String shortSource = extractShortSource(source);

      sb.append("          <div class=\"timeline-entry");
      if (repeated) sb.append(" timeline-repeated");
      sb.append("\">\n");
      sb.append("            <span class=\"timeline-num\">").append(num).append("</span>\n");
      sb.append("            <span class=\"timeline-time\">")
          .append(String.format("%.1f", ms))
          .append("ms</span>\n");
      sb.append("            <span class=\"timeline-sql\">")
          .append(esc(truncatedSql))
          .append("</span>\n");
      sb.append("            <span class=\"timeline-source\">")
          .append(esc(shortSource))
          .append("</span>\n");
      sb.append("          </div>\n");
    }

    sb.append("        </div>\n");
    sb.append("      </details>\n");
  }

  private void appendQueryPatterns(StringBuilder sb, QueryAuditReport report) {
    List<QueryRecord> queries = report.getAllQueries();
    if (queries == null || queries.isEmpty()) return;

    // Group by normalizedSql, count and keep one example
    Map<String, int[]> patternCounts = new LinkedHashMap<>();
    Map<String, String> patternExamples = new LinkedHashMap<>();
    for (QueryRecord q : queries) {
      String key = q.normalizedSql();
      if (key == null) key = q.sql() != null ? q.sql() : "";
      patternCounts.computeIfAbsent(key, k -> new int[1])[0]++;
      patternExamples.putIfAbsent(key, q.sql() != null ? q.sql() : "");
    }

    // Sort by count descending
    List<Map.Entry<String, int[]>> sorted = new ArrayList<>(patternCounts.entrySet());
    sorted.sort((a, b) -> Integer.compare(b.getValue()[0], a.getValue()[0]));

    sb.append("      <h4>Query Patterns</h4>\n");
    sb.append("      <div class=\"pattern-list\">\n");

    for (Map.Entry<String, int[]> entry : sorted) {
      int count = entry.getValue()[0];
      String normalized = entry.getKey();
      String truncated =
          normalized.length() > 100 ? normalized.substring(0, 100) + "..." : normalized;
      boolean hot = count >= 3;

      sb.append("        <div class=\"pattern-entry\">\n");
      sb.append("          <span class=\"pattern-count");
      if (hot) sb.append(" hot");
      sb.append("\">").append(count).append("x</span>\n");
      sb.append("          <span class=\"pattern-sql\">")
          .append(esc(truncated))
          .append("</span>\n");
      sb.append("        </div>\n");
    }

    sb.append("      </div>\n");
  }

  /**
   * Extract a short source location from a stack trace string. Attempts to find the first
   * non-framework line, otherwise returns the first line.
   */
  private static String extractShortSource(String stackTrace) {
    if (stackTrace == null || stackTrace.isBlank()) return "";
    String[] lines = stackTrace.split("\\n");
    for (String line : lines) {
      String trimmed = line.trim();
      if (trimmed.isEmpty()) continue;
      // Skip common framework prefixes
      if (trimmed.startsWith("at java.")
          || trimmed.startsWith("at sun.")
          || trimmed.startsWith("at jdk.")
          || trimmed.startsWith("at org.springframework.")
          || trimmed.startsWith("at com.zaxxer.")
          || trimmed.startsWith("at org.hibernate.")) {
        continue;
      }
      // Clean up "at " prefix
      if (trimmed.startsWith("at ")) {
        trimmed = trimmed.substring(3);
      }
      // Truncate if too long
      return trimmed.length() > 60 ? trimmed.substring(0, 60) + "..." : trimmed;
    }
    // Fallback: return first non-empty line
    for (String line : lines) {
      String trimmed = line.trim();
      if (!trimmed.isEmpty()) {
        if (trimmed.startsWith("at ")) trimmed = trimmed.substring(3);
        return trimmed.length() > 60 ? trimmed.substring(0, 60) + "..." : trimmed;
      }
    }
    return "";
  }

  private void appendIssueCard(StringBuilder sb, Issue issue) {
    String sevClass = severityCssClass(issue.severity());
    sb.append("      <div class=\"issue-card ").append(sevClass).append("\">\n");
    sb.append("        <div class=\"issue-header\">\n");
    sb.append("          <span class=\"badge ")
        .append(sevClass)
        .append("\">")
        .append(issue.severity())
        .append("</span>\n");
    sb.append("          <span class=\"issue-type\">")
        .append(esc(issue.type().getDescription()))
        .append("</span>\n");
    sb.append("          <code class=\"issue-code\">")
        .append(esc(issue.type().getCode()))
        .append("</code>\n");
    sb.append("        </div>\n");

    if (issue.query() != null && !issue.query().isBlank()) {
      sb.append("        <div class=\"issue-field\">\n");
      sb.append("          <span class=\"field-label\">Query</span>\n");
      sb.append("          <pre class=\"sql-block\">")
          .append(esc(issue.query()))
          .append("</pre>\n");
      sb.append("        </div>\n");
    }

    // Source location (if available)
    if (issue.sourceLocation() != null && !issue.sourceLocation().isBlank()) {
      sb.append("        <div class=\"issue-field\">\n");
      sb.append("          <span class=\"field-label\">Source</span>\n");
      sb.append("          <span class=\"field-value source-location\">")
          .append(esc(issue.sourceLocation()))
          .append("</span>\n");
      sb.append("        </div>\n");
    }

    if (issue.table() != null && !issue.table().isBlank()) {
      sb.append("        <div class=\"issue-field\">\n");
      sb.append("          <span class=\"field-label\">Target</span>\n");
      sb.append("          <span class=\"field-value\">");
      sb.append(esc(issue.table()));
      if (issue.column() != null && !issue.column().isBlank()) {
        sb.append(".").append(esc(issue.column()));
      }
      sb.append("</span>\n");
      sb.append("        </div>\n");
    }

    if (issue.detail() != null && !issue.detail().isBlank()) {
      sb.append("        <div class=\"issue-field\">\n");
      sb.append("          <span class=\"field-label\">Detail</span>\n");
      sb.append("          <span class=\"field-value\">")
          .append(esc(issue.detail()))
          .append("</span>\n");
      sb.append("        </div>\n");
    }

    if (issue.suggestion() != null && !issue.suggestion().isBlank()) {
      sb.append("        <div class=\"issue-field fix-field\">\n");
      sb.append("          <span class=\"field-label\">Fix</span>\n");
      sb.append("          <span class=\"field-value\">")
          .append(esc(issue.suggestion()))
          .append("</span>\n");
      sb.append("        </div>\n");
    }

    sb.append("      </div>\n");
  }

  private void appendAcknowledgedIssueCard(StringBuilder sb, Issue issue) {
    sb.append("      <div class=\"issue-card badge-acknowledged\">\n");
    sb.append("        <div class=\"issue-header\">\n");
    sb.append("          <span class=\"badge badge-acknowledged\">ACKNOWLEDGED</span>\n");
    sb.append("          <span class=\"issue-type\">")
        .append(esc(issue.type().getDescription()))
        .append("</span>\n");
    sb.append("          <code class=\"issue-code\">")
        .append(esc(issue.type().getCode()))
        .append("</code>\n");
    sb.append("        </div>\n");

    if (issue.table() != null && !issue.table().isBlank()) {
      sb.append("        <div class=\"issue-field\">\n");
      sb.append("          <span class=\"field-label\">Target</span>\n");
      sb.append("          <span class=\"field-value\">");
      sb.append(esc(issue.table()));
      if (issue.column() != null && !issue.column().isBlank()) {
        sb.append(".").append(esc(issue.column()));
      }
      sb.append("</span>\n");
      sb.append("        </div>\n");
    }

    if (issue.detail() != null && !issue.detail().isBlank()) {
      sb.append("        <div class=\"issue-field\">\n");
      sb.append("          <span class=\"field-label\">Detail</span>\n");
      sb.append("          <span class=\"field-value\">")
          .append(esc(issue.detail()))
          .append("</span>\n");
      sb.append("        </div>\n");
    }

    // Look up reason and acknowledgedBy from baseline
    BaselineEntry match = Baseline.findMatch(baseline, issue);
    if (match != null) {
      if (match.reason() != null && !match.reason().isBlank()) {
        sb.append("        <div class=\"issue-field\">\n");
        sb.append("          <span class=\"field-label\">Reason</span>\n");
        sb.append("          <span class=\"field-value ack-reason\">")
            .append(esc(match.reason()))
            .append("</span>\n");
        sb.append("        </div>\n");
      }
      if (match.acknowledgedBy() != null && !match.acknowledgedBy().isBlank()) {
        sb.append("        <div class=\"issue-field\">\n");
        sb.append("          <span class=\"field-label\">Acknowledged By</span>\n");
        sb.append("          <span class=\"field-value\">")
            .append(esc(match.acknowledgedBy()))
            .append("</span>\n");
        sb.append("        </div>\n");
      }
    }

    sb.append("      </div>\n");
  }

  // -------------------------------------------------------------------------
  // CSS
  // -------------------------------------------------------------------------

  private void appendStyles(StringBuilder sb) {
    sb.append("<style>\n");
    sb.append(
        """
                :root {
                    --color-error: #dc3545;
                    --color-warning: #fd7e14;
                    --color-info: #0d6efd;
                    --color-ok: #198754;
                    --color-bg: #f8f9fa;
                    --color-card: #ffffff;
                    --color-text: #212529;
                    --color-text-secondary: #6c757d;
                    --color-border: #dee2e6;
                    --color-header-bg: #1a1a2e;
                    --color-header-text: #ffffff;
                    --shadow-sm: 0 1px 3px rgba(0,0,0,0.08);
                    --shadow-md: 0 4px 12px rgba(0,0,0,0.1);
                    --radius: 8px;
                    --font-mono: 'SF Mono', 'Fira Code', 'Consolas', 'Monaco', monospace;
                }

                @media (prefers-color-scheme: dark) {
                    :root {
                        --color-bg: #1a1a2e;
                        --color-card: #16213e;
                        --color-text: #e0e0e0;
                        --color-text-secondary: #a0a0a0;
                        --color-border: #2a2a4a;
                        --color-header-bg: #0f0f23;
                        --color-header-text: #e0e0e0;
                        --shadow-sm: 0 1px 3px rgba(0,0,0,0.3);
                        --shadow-md: 0 4px 12px rgba(0,0,0,0.4);
                    }
                }

                * { margin: 0; padding: 0; box-sizing: border-box; }

                body {
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                    background: var(--color-bg);
                    color: var(--color-text);
                    line-height: 1.6;
                    min-height: 100vh;
                    display: flex;
                    flex-direction: column;
                }

                code, pre, .name, .timeline-sql, .pattern-sql, .freq-table {
                    font-family: var(--font-mono);
                }

                /* Header */
                .header {
                    background: var(--color-header-bg);
                    color: var(--color-header-text);
                    padding: 1.25rem 2rem;
                    box-shadow: var(--shadow-md);
                }
                .header-content {
                    max-width: 1200px;
                    margin: 0 auto;
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    flex-wrap: wrap;
                    gap: 0.5rem;
                }
                .header-title {
                    display: flex;
                    align-items: center;
                    gap: 0.75rem;
                }
                .header-title h1 { font-size: 1.4rem; font-weight: 700; }
                .logo { color: var(--color-ok); }
                .timestamp {
                    font-size: 0.8rem;
                    opacity: 0.7;
                    font-family: var(--font-mono);
                }

                /* Container */
                .container {
                    max-width: 1200px;
                    margin: 0 auto;
                    padding: 1.5rem;
                    width: 100%;
                    flex: 1;
                }

                /* ===== Summary bar ===== */
                .summary-bar {
                    display: flex;
                    flex-wrap: wrap;
                    gap: 0.75rem;
                    margin-bottom: 1.5rem;
                    padding: 1rem 1.25rem;
                    background: var(--color-card);
                    border-radius: var(--radius);
                    box-shadow: var(--shadow-sm);
                    align-items: center;
                }
                .summary-bar .stat, .summary-bar a.stat {
                    padding: 0.4rem 1rem;
                    border-radius: 6px;
                    font-weight: 700;
                    font-size: 0.9rem;
                    background: var(--color-bg);
                    color: var(--color-text);
                    white-space: nowrap;
                    text-decoration: none;
                    cursor: pointer;
                    transition: opacity 0.15s;
                }
                .summary-bar a.stat:hover { opacity: 0.8; }
                td.center { text-align: center; }
                .summary-bar .stat.error {
                    background: #fef2f2;
                    color: var(--color-error);
                    border: 1px solid #fca5a5;
                }
                .summary-bar .stat.warning {
                    background: #fff7ed;
                    color: #c2410c;
                    border: 1px solid #fdba74;
                }
                .summary-bar .stat.info {
                    background: #eff6ff;
                    color: var(--color-info);
                    border: 1px solid #93c5fd;
                }
                .summary-bar .stat.ok {
                    background: #f0fdf4;
                    color: var(--color-ok);
                    border: 1px solid #86efac;
                }
                .summary-bar .stat.acknowledged {
                    background: #f0fdf4;
                    color: var(--color-ok);
                    border: 1px solid #86efac;
                }
                @media (prefers-color-scheme: dark) {
                    .summary-bar .stat { background: rgba(255,255,255,0.06); }
                    .summary-bar .stat.error { background: rgba(220,53,69,0.15); border-color: rgba(220,53,69,0.4); }
                    .summary-bar .stat.warning { background: rgba(253,126,20,0.15); border-color: rgba(253,126,20,0.4); color: #fb923c; }
                    .summary-bar .stat.info { background: rgba(13,110,253,0.15); border-color: rgba(13,110,253,0.4); }
                    .summary-bar .stat.ok { background: rgba(25,135,84,0.15); border-color: rgba(25,135,84,0.4); }
                    .summary-bar .stat.acknowledged { background: rgba(25,135,84,0.15); border-color: rgba(25,135,84,0.4); }
                }

                /* ===== Sections ===== */
                .section {
                    margin-bottom: 2rem;
                }
                .section h2 {
                    font-size: 1.15rem;
                    font-weight: 600;
                    margin-bottom: 0.75rem;
                    padding-bottom: 0.4rem;
                    border-bottom: 2px solid var(--color-border);
                }

                /* ===== Tables (issues-table, classes-table, breakdown-table) ===== */
                .table-wrapper { overflow-x: auto; }
                .issues-table, .classes-table, .breakdown-table {
                    width: 100%;
                    border-collapse: collapse;
                    background: var(--color-card);
                    border-radius: var(--radius);
                    overflow: hidden;
                    box-shadow: var(--shadow-sm);
                    font-size: 0.88rem;
                }
                .issues-table th, .issues-table td,
                .classes-table th, .classes-table td,
                .breakdown-table th, .breakdown-table td {
                    padding: 0.65rem 1rem;
                    text-align: left;
                    border-bottom: 1px solid var(--color-border);
                }
                .issues-table th,
                .classes-table th,
                .breakdown-table th {
                    background: var(--color-header-bg);
                    color: var(--color-header-text);
                    font-weight: 600;
                    font-size: 0.8rem;
                    text-transform: uppercase;
                    letter-spacing: 0.04em;
                }
                /* Zebra striping */
                .issues-table tbody tr:nth-child(even),
                .classes-table tbody tr:nth-child(even),
                .breakdown-table tbody tr:nth-child(even) {
                    background: rgba(0,0,0,0.02);
                }
                @media (prefers-color-scheme: dark) {
                    .issues-table tbody tr:nth-child(even),
                    .classes-table tbody tr:nth-child(even),
                    .breakdown-table tbody tr:nth-child(even) {
                        background: rgba(255,255,255,0.03);
                    }
                }
                /* Hover highlight */
                .issues-table tbody tr:hover,
                .classes-table tbody tr:hover,
                .breakdown-table tbody tr:hover {
                    background: rgba(0,0,0,0.05);
                }
                @media (prefers-color-scheme: dark) {
                    .issues-table tbody tr:hover,
                    .classes-table tbody tr:hover,
                    .breakdown-table tbody tr:hover {
                        background: rgba(255,255,255,0.06);
                    }
                }
                .num-cell {
                    text-align: center;
                    font-weight: 600;
                    font-variant-numeric: tabular-nums;
                }
                .location-cell {
                    font-family: var(--font-mono);
                    font-size: 0.82rem;
                    color: var(--color-text-secondary);
                    white-space: nowrap;
                }
                .classes-table a {
                    color: var(--color-info);
                    text-decoration: none;
                    font-family: var(--font-mono);
                    font-weight: 600;
                    font-size: 0.88rem;
                }
                .classes-table a:hover { text-decoration: underline; }
                .classes-table code, .issues-table code {
                    font-family: var(--font-mono);
                    font-size: 0.82rem;
                    background: rgba(0,0,0,0.04);
                    padding: 0.15em 0.4em;
                    border-radius: 3px;
                    word-break: break-all;
                }
                @media (prefers-color-scheme: dark) {
                    .classes-table code, .issues-table code {
                        background: rgba(255,255,255,0.08);
                    }
                }
                .row-fail { }
                .row-pass { background: rgba(25, 135, 84, 0.04); }
                @media (prefers-color-scheme: dark) {
                    .row-pass { background: rgba(25, 135, 84, 0.08); }
                }

                /* Status dots */
                .status-dot {
                    display: inline-block;
                    width: 10px;
                    height: 10px;
                    border-radius: 50%;
                }
                .status-dot.error-dot { background: var(--color-error); }
                .status-dot.warning-dot { background: var(--color-warning); }
                .status-dot.ok-dot { background: var(--color-ok); }

                /* Status indicators (for method cards) */
                .status-indicator {
                    display: inline-block;
                    width: 12px;
                    height: 12px;
                    border-radius: 50%;
                    flex-shrink: 0;
                }
                .status-indicator.error-dot { background: var(--color-error); }
                .status-indicator.warning-dot { background: var(--color-warning); }
                .status-indicator.ok-dot { background: var(--color-ok); }

                /* ===== Badges ===== */
                .badge {
                    display: inline-block;
                    padding: 0.12em 0.5em;
                    border-radius: 4px;
                    font-size: 0.72rem;
                    font-weight: 700;
                    text-transform: uppercase;
                    letter-spacing: 0.03em;
                }
                .badge-error { background: var(--color-error); color: #fff; }
                .badge-warning { background: var(--color-warning); color: #fff; }
                .badge-info { background: var(--color-info); color: #fff; }
                .badge-ok { background: var(--color-ok); color: #fff; }
                .badge-acknowledged { background: var(--color-ok); color: #fff; }
                .badge-neutral { background: var(--color-border); color: var(--color-text); }

                /* ===== Method cards (class detail page) ===== */
                .method {
                    background: var(--color-card);
                    border-radius: var(--radius);
                    box-shadow: var(--shadow-sm);
                    margin-bottom: 0.75rem;
                    border-left: 4px solid var(--color-ok);
                    transition: box-shadow 0.15s ease;
                }
                .method:hover { box-shadow: var(--shadow-md); }
                .method.method-error { border-left-color: var(--color-error); }
                .method.method-warning { border-left-color: var(--color-warning); }
                .method.method-ok { border-left-color: var(--color-ok); }
                .method > summary {
                    padding: 0.85rem 1.25rem;
                    cursor: pointer;
                    display: flex;
                    align-items: center;
                    gap: 0.75rem;
                    list-style: none;
                    user-select: none;
                }
                .method > summary::-webkit-details-marker { display: none; }
                .method > summary::before {
                    content: '\\25B6';
                    font-size: 0.65rem;
                    transition: transform 0.2s ease;
                    color: var(--color-text-secondary);
                }
                .method[open] > summary::before {
                    transform: rotate(90deg);
                }
                .method .name {
                    font-weight: 600;
                    font-size: 0.9rem;
                    flex: 1;
                    min-width: 150px;
                    color: var(--color-text);
                    user-select: text;
                }
                .method .meta {
                    font-size: 0.8rem;
                    color: var(--color-text-secondary);
                    white-space: nowrap;
                }

                /* Issues inside method card */
                .method .issues {
                    padding: 0.5rem 1.25rem;
                }
                .method .issues .issue {
                    padding: 0.75rem;
                    margin-bottom: 0.5rem;
                    border-radius: 6px;
                    border-left: 3px solid var(--color-border);
                    background: rgba(0,0,0,0.02);
                }
                @media (prefers-color-scheme: dark) {
                    .method .issues .issue { background: rgba(255,255,255,0.03); }
                }
                .method .issues .issue.badge-error { border-left-color: var(--color-error); }
                .method .issues .issue.badge-warning { border-left-color: var(--color-warning); }
                .method .issues .issue.badge-info { border-left-color: var(--color-info); }
                /* Checked/resolved issue — green with strikethrough */
                .method .issues .issue.resolved {
                    border-left-color: var(--color-ok) !important;
                    background: #f0fdf4 !important;
                    opacity: 0.6;
                }
                .method .issues .issue.resolved strong,
                .method .issues .issue.resolved .issue-label {
                    text-decoration: line-through;
                    color: var(--color-ok);
                }
                @media (prefers-color-scheme: dark) {
                    .method .issues .issue.resolved { background: rgba(25,135,84,0.1) !important; }
                }
                .method.all-reviewed {
                    border-left-color: var(--color-ok) !important;
                    background: linear-gradient(135deg, rgba(25,135,84,0.05), transparent);
                }
                .method.all-reviewed > summary .review-badge {
                    display: inline-block;
                    font-size: 0.72rem;
                    color: var(--color-ok);
                    font-weight: 600;
                    padding: 0.1em 0.5em;
                    border: 1px solid var(--color-ok);
                    border-radius: 9999px;
                }
                @media (prefers-color-scheme: dark) {
                    .method.all-reviewed {
                        background: linear-gradient(135deg, rgba(25,135,84,0.1), transparent);
                    }
                }
                .issue-label { cursor: pointer; display: flex; align-items: center; gap: 0.4rem; }
                .issue-check { width: 16px; height: 16px; cursor: pointer; accent-color: var(--color-ok); }
                .method .issues .issue strong {
                    display: block;
                    font-size: 0.88rem;
                    margin-bottom: 0.3rem;
                }
                .method .issues .issue code {
                    display: block;
                    font-family: var(--font-mono);
                    font-size: 0.82rem;
                    background: #1e1e2e;
                    color: #cdd6f4;
                    border: 1px solid #313244;
                    border-radius: 4px;
                    padding: 0.5rem 0.75rem;
                    overflow-x: auto;
                    white-space: pre-wrap;
                    word-break: break-word;
                    margin-bottom: 0.3rem;
                }
                .method .issues .issue .fix {
                    font-size: 0.82rem;
                    color: var(--color-ok);
                    font-weight: 500;
                    margin: 0.25rem 0 0;
                }
                .method .issues .issue .source {
                    font-size: 0.78rem;
                    color: #8b5cf6;
                    margin: 0.2rem 0 0;
                    font-family: var(--font-mono);
                }
                @media (prefers-color-scheme: dark) {
                    .method .issues .issue .source { color: #a78bfa; }
                    .method .issues .issue .fix { color: #4ade80; }
                }

                /* Queries sub-section inside method card */
                .method .queries {
                    margin: 0.5rem 1.25rem 1rem;
                    border: 1px solid var(--color-border);
                    border-radius: 6px;
                }
                .method .queries > summary {
                    padding: 0.5rem 0.75rem;
                    cursor: pointer;
                    font-weight: 600;
                    font-size: 0.85rem;
                    color: var(--color-text-secondary);
                    background: var(--color-bg);
                    list-style: none;
                    user-select: none;
                }
                .method .queries > summary::-webkit-details-marker { display: none; }
                .method .queries > summary::before {
                    content: '\\25B6 ';
                    font-size: 0.6rem;
                    transition: transform 0.2s ease;
                }
                .method .queries[open] > summary::before {
                    content: '\\25BC ';
                }
                .method .queries-content {
                    padding: 0.5rem 0.75rem;
                }
                .method .no-issues {
                    padding: 0.5rem 1.25rem;
                    color: var(--color-ok);
                    font-size: 0.88rem;
                }
                .method .acknowledged-issues {
                    border-top: 1px dashed var(--color-border);
                    padding-top: 0.5rem;
                }

                /* ===== Breadcrumb ===== */
                .breadcrumb {
                    margin-bottom: 1rem;
                    font-size: 0.9rem;
                }
                .breadcrumb a {
                    color: var(--color-info);
                    text-decoration: none;
                }
                .breadcrumb a:hover { text-decoration: underline; }

                /* ===== Legacy shared components (kept for existing helpers) ===== */

                /* Dashboard (used by appendSummaryDashboard — retained for compatibility) */
                .dashboard { margin-bottom: 2rem; }
                .stats-grid {
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
                    gap: 0.75rem;
                    margin-bottom: 1.5rem;
                }
                .stat-card {
                    background: var(--color-card);
                    border-radius: var(--radius);
                    padding: 1rem 0.75rem;
                    text-align: center;
                    box-shadow: var(--shadow-sm);
                    border-top: 3px solid var(--color-border);
                }
                .stat-card.stat-error { border-top-color: var(--color-error); }
                .stat-card.stat-warning { border-top-color: var(--color-warning); }
                .stat-card.stat-info { border-top-color: var(--color-info); }
                .stat-card.stat-ok { border-top-color: var(--color-ok); }
                .stat-card.stat-acknowledged { border-top-color: var(--color-ok); }
                .stat-card.stat-neutral { border-top-color: var(--color-border); }
                .stat-card.stat-unique { border-top-color: #6366f1; }
                .stat-value {
                    font-size: 1.6rem;
                    font-weight: 700;
                    line-height: 1.2;
                }
                .stat-error .stat-value { color: var(--color-error); }
                .stat-warning .stat-value { color: var(--color-warning); }
                .stat-info .stat-value { color: var(--color-info); }
                .stat-ok .stat-value { color: var(--color-ok); }
                .stat-acknowledged .stat-value { color: var(--color-ok); }
                .stat-unique .stat-value { color: #6366f1; }
                .stat-label {
                    font-size: 0.75rem;
                    text-transform: uppercase;
                    letter-spacing: 0.05em;
                    color: var(--color-text-secondary);
                    margin-top: 0.2rem;
                }

                /* Chart */
                .chart-section { margin-bottom: 1rem; }
                .chart-section h2 {
                    font-size: 1rem;
                    margin-bottom: 0.75rem;
                    color: var(--color-text-secondary);
                }
                .chart-row {
                    display: flex;
                    gap: 2.5rem;
                    flex-wrap: wrap;
                    justify-content: center;
                }
                .donut-container { text-align: center; }
                .donut-title {
                    font-size: 0.8rem;
                    font-weight: 600;
                    margin-bottom: 0.5rem;
                    color: var(--color-text-secondary);
                }
                .donut {
                    width: 120px;
                    height: 120px;
                    border-radius: 50%;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    margin: 0 auto;
                }
                .donut-hole {
                    width: 70px;
                    height: 70px;
                    border-radius: 50%;
                    background: var(--color-bg);
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    font-size: 1.1rem;
                    font-weight: 700;
                }
                .donut-legend {
                    display: flex;
                    gap: 0.75rem;
                    justify-content: center;
                    margin-top: 0.5rem;
                    flex-wrap: wrap;
                }
                .legend-item {
                    font-size: 0.75rem;
                    display: flex;
                    align-items: center;
                    gap: 0.3rem;
                    color: var(--color-text-secondary);
                }
                .legend-dot {
                    width: 8px;
                    height: 8px;
                    border-radius: 50%;
                    display: inline-block;
                }

                /* Breakdown table overrides */
                .breakdown-table code {
                    font-family: var(--font-mono);
                    font-size: 0.82rem;
                    background: rgba(0,0,0,0.05);
                    padding: 0.15em 0.4em;
                    border-radius: 3px;
                }
                @media (prefers-color-scheme: dark) {
                    .breakdown-table code { background: rgba(255,255,255,0.08); }
                }
                .count-cell {
                    font-weight: 700;
                    font-variant-numeric: tabular-nums;
                }

                /* Dedup table */
                .section-desc {
                    font-size: 0.85rem;
                    color: var(--color-text-secondary);
                    margin-bottom: 0.75rem;
                }
                .dedup-table td { vertical-align: top; font-size: 0.83rem; }
                .dedup-sql {
                    font-family: var(--font-mono);
                    font-size: 0.78rem;
                    word-break: break-all;
                    white-space: pre-wrap;
                    background: rgba(0,0,0,0.04);
                    padding: 0.2em 0.4em;
                    border-radius: 3px;
                }
                @media (prefers-color-scheme: dark) {
                    .dedup-sql { background: rgba(255,255,255,0.06); }
                }
                .fix-cell { color: var(--color-ok); font-weight: 500; font-size: 0.82rem; }
                @media (prefers-color-scheme: dark) { .fix-cell { color: #4ade80; } }
                .affected-test {
                    font-family: var(--font-mono);
                    font-size: 0.72rem;
                    color: var(--color-text-secondary);
                }
                .affected-test-overflow { font-size: 0.72rem; color: var(--color-text-secondary); font-style: italic; }
                .dedup-more summary { font-size: 0.72rem; color: var(--color-info); cursor: pointer; }
                .dedup-more summary:hover { text-decoration: underline; }
                .affected-test-link {
                    color: var(--color-info); text-decoration: none; font-size: 0.78rem;
                    border-bottom: 1px dotted var(--color-info); cursor: pointer;
                }
                .affected-test-link:hover { border-bottom-style: solid; }

                /* Test cards (kept for appendFlatTestResults / appendFlatTestResultsStreaming) */
                .test-card {
                    background: var(--color-card);
                    border-radius: var(--radius);
                    box-shadow: var(--shadow-sm);
                    margin-bottom: 1rem;
                    border-left: 4px solid var(--color-ok);
                }
                .test-card:hover { box-shadow: var(--shadow-md); }
                .test-card.test-fail { border-left-color: var(--color-error); }
                .test-card.test-pass { border-left-color: var(--color-ok); }
                .test-number {
                    display: inline-flex;
                    align-items: center;
                    justify-content: center;
                    width: 1.75rem;
                    height: 1.75rem;
                    border-radius: 50%;
                    background: var(--color-bg);
                    color: var(--color-text-secondary);
                    font-size: 0.7rem;
                    font-weight: 700;
                    flex-shrink: 0;
                }
                .test-fail .test-number { background: #fef2f2; color: var(--color-error); }
                .test-pass .test-number { background: #f0fdf4; color: var(--color-ok); }
                @media (prefers-color-scheme: dark) {
                    .test-fail .test-number { background: rgba(220,53,69,0.15); }
                    .test-pass .test-number { background: rgba(25,135,84,0.15); }
                }
                .meta-queries {
                    background: var(--color-bg);
                    padding: 0.1em 0.4em;
                    border-radius: 3px;
                    font-weight: 600;
                }
                .test-stats-bar {
                    margin-bottom: 0.75rem;
                    padding-bottom: 0.75rem;
                    border-bottom: 1px solid var(--color-border);
                }
                .stats-track {
                    display: flex;
                    height: 6px;
                    border-radius: 3px;
                    overflow: hidden;
                    background: var(--color-border);
                    margin-bottom: 0.35rem;
                }
                .stats-fill { height: 100%; }
                .stats-error { background: var(--color-error); }
                .stats-warning { background: var(--color-warning); }
                .stats-pass { background: var(--color-ok); }
                .stats-labels {
                    display: flex;
                    justify-content: space-between;
                    font-size: 0.75rem;
                    color: var(--color-text-secondary);
                }
                .no-issues {
                    color: var(--color-ok);
                    font-weight: 500;
                    padding: 0.5rem 0;
                    font-size: 0.88rem;
                }
                .test-summary {
                    padding: 0.85rem 1.25rem;
                    cursor: pointer;
                    display: flex;
                    align-items: center;
                    gap: 0.75rem;
                    flex-wrap: wrap;
                    list-style: none;
                    user-select: none;
                }
                .test-summary::-webkit-details-marker { display: none; }
                .test-summary::before {
                    content: '\\25B6';
                    font-size: 0.65rem;
                    transition: transform 0.2s ease;
                    color: var(--color-text-secondary);
                }
                details[open] > .test-summary::before {
                    transform: rotate(90deg);
                }
                .test-status { font-size: 1rem; }
                .test-fail .test-status { color: var(--color-error); }
                .test-pass .test-status { color: var(--color-ok); }
                .test-name {
                    font-weight: 600;
                    font-family: var(--font-mono);
                    font-size: 0.88rem;
                    flex: 1;
                    min-width: 150px;
                    color: var(--color-text);
                    user-select: text;
                }
                .test-meta {
                    display: flex;
                    gap: 0.4rem;
                    align-items: center;
                    flex-wrap: wrap;
                }
                .meta-item {
                    font-size: 0.78rem;
                    color: var(--color-text-secondary);
                }
                .test-detail {
                    padding: 0 1.25rem 1.25rem 1.25rem;
                }
                .test-detail h4 {
                    font-size: 0.9rem;
                    margin: 0.75rem 0 0.4rem;
                    color: var(--color-text-secondary);
                }
                .empty-message {
                    color: var(--color-text-secondary);
                    font-style: italic;
                    padding: 1rem 0;
                }

                /* Issue cards (used by appendIssueCard / appendAcknowledgedIssueCard) */
                .issue-card {
                    background: #f1f3f5;
                    color: #212529;
                    border-radius: 6px;
                    padding: 0.85rem;
                    margin-bottom: 0.6rem;
                    border-left: 3px solid var(--color-border);
                }
                @media (prefers-color-scheme: dark) {
                    .issue-card { background: #1e293b; color: #e2e8f0; }
                }
                .issue-card.badge-error { border-left-color: var(--color-error); }
                .issue-card.badge-warning { border-left-color: var(--color-warning); }
                .issue-card.badge-info { border-left-color: var(--color-info); }
                .issue-card.badge-acknowledged { border-left-color: var(--color-ok); }
                .ack-reason { color: var(--color-ok); font-weight: 500; }
                @media (prefers-color-scheme: dark) { .ack-reason { color: #4ade80; } }
                .issue-header {
                    display: flex;
                    align-items: center;
                    gap: 0.4rem;
                    margin-bottom: 0.6rem;
                    flex-wrap: wrap;
                }
                .issue-type { font-weight: 600; font-size: 0.9rem; color: inherit; }
                .issue-code {
                    font-family: var(--font-mono);
                    font-size: 0.78rem;
                    color: #64748b;
                    background: rgba(0,0,0,0.07);
                    padding: 0.1em 0.35em;
                    border-radius: 3px;
                }
                @media (prefers-color-scheme: dark) { .issue-code { color: #94a3b8; background: rgba(255,255,255,0.1); } }
                .issue-field { margin-bottom: 0.4rem; }
                .field-label {
                    display: block;
                    font-size: 0.7rem;
                    text-transform: uppercase;
                    letter-spacing: 0.04em;
                    color: #64748b;
                    margin-bottom: 0.15rem;
                    font-weight: 600;
                }
                @media (prefers-color-scheme: dark) { .field-label { color: #94a3b8; } }
                .field-value { font-size: 0.88rem; color: inherit; }
                .sql-block {
                    font-family: var(--font-mono);
                    font-size: 0.82rem;
                    background: #1e1e2e;
                    color: #cdd6f4;
                    border: 1px solid #313244;
                    border-radius: 4px;
                    padding: 0.6rem;
                    overflow-x: auto;
                    white-space: pre-wrap;
                    word-break: break-word;
                    line-height: 1.5;
                    margin: 0;
                }
                .fix-field .field-value { color: var(--color-ok); font-weight: 500; }
                @media (prefers-color-scheme: dark) { .fix-field .field-value { color: #4ade80; } }
                .source-location {
                    font-family: var(--font-mono);
                    font-size: 0.82rem;
                    color: #8b5cf6;
                }
                @media (prefers-color-scheme: dark) { .source-location { color: #a78bfa; } }

                /* Method cards (legacy hierarchical — kept for appendHierarchicalTestResults) */
                .method-card {
                    background: var(--color-card);
                    border-radius: var(--radius);
                    box-shadow: var(--shadow-sm);
                    margin-bottom: 0.75rem;
                    border-left: 4px solid var(--color-ok);
                }
                .method-card:hover { box-shadow: var(--shadow-md); }
                .method-card.test-fail { border-left-color: var(--color-error); }
                .method-card.test-pass { border-left-color: var(--color-ok); }
                .method-summary {
                    padding: 0.85rem 1.25rem;
                    cursor: pointer;
                    display: flex;
                    align-items: center;
                    gap: 0.75rem;
                    flex-wrap: wrap;
                    list-style: none;
                    user-select: none;
                    font-weight: 500;
                }
                .method-summary::-webkit-details-marker { display: none; }
                .method-summary::before {
                    content: '\\25B6';
                    font-size: 0.65rem;
                    transition: transform 0.2s ease;
                    color: var(--color-text-secondary);
                }
                details[open] > .method-summary::before {
                    transform: rotate(90deg);
                }
                .method-name {
                    font-weight: 600;
                    font-family: var(--font-mono);
                    font-size: 0.9rem;
                    flex: 1;
                    min-width: 150px;
                    color: var(--color-text);
                    user-select: text;
                }
                .method-meta {
                    display: flex;
                    gap: 0.4rem;
                    align-items: center;
                    flex-wrap: wrap;
                }
                .method-detail {
                    padding: 0 1.25rem 1.25rem 1.25rem;
                }

                /* Inner collapsible sections */
                .inner-section {
                    margin-top: 0.6rem;
                    border: 1px solid var(--color-border);
                    border-radius: 6px;
                    overflow: hidden;
                }
                .inner-summary {
                    padding: 0.5rem 0.85rem;
                    cursor: pointer;
                    font-weight: 600;
                    font-size: 0.85rem;
                    color: var(--color-text);
                    background: var(--color-bg);
                    list-style: none;
                    user-select: none;
                    display: flex;
                    align-items: center;
                    gap: 0.4rem;
                }
                .inner-summary::-webkit-details-marker { display: none; }
                .inner-summary::before {
                    content: '\\25B6';
                    font-size: 0.55rem;
                    transition: transform 0.2s ease;
                    color: var(--color-text-secondary);
                }
                details[open] > .inner-summary::before { transform: rotate(90deg); }
                .inner-summary:hover { background: rgba(0,0,0,0.04); }
                @media (prefers-color-scheme: dark) { .inner-summary:hover { background: rgba(255,255,255,0.04); } }
                .inner-content { padding: 0.6rem 0.85rem; }
                .issues-section { border-left: 3px solid var(--color-error); }
                .queries-section { border-left: 3px solid var(--color-info); }
                .sub-heading {
                    font-size: 0.82rem;
                    font-weight: 600;
                    color: var(--color-text-secondary);
                    margin: 0.6rem 0 0.35rem;
                    text-transform: uppercase;
                    letter-spacing: 0.03em;
                }

                /* Query Timeline */
                .timeline { margin: 0.4rem 0; }
                .timeline-entry {
                    display: grid;
                    grid-template-columns: 2.5rem 4rem 1fr auto;
                    gap: 0.4rem;
                    padding: 0.35rem 0.5rem;
                    font-size: 0.8rem;
                    border-bottom: 1px solid var(--color-border);
                    align-items: center;
                }
                .timeline-entry:hover { background: rgba(0,0,0,0.02); }
                @media (prefers-color-scheme: dark) { .timeline-entry:hover { background: rgba(255,255,255,0.03); } }
                .timeline-num {
                    color: var(--color-text-secondary);
                    font-weight: 600;
                    text-align: center;
                }
                .timeline-time {
                    font-family: monospace;
                    color: var(--color-text-secondary);
                    text-align: right;
                }
                .timeline-sql {
                    font-size: 0.78rem;
                    white-space: nowrap;
                    overflow: hidden;
                    text-overflow: ellipsis;
                    color: var(--color-text);
                }
                .timeline-source {
                    font-size: 0.72rem;
                    color: #8b5cf6;
                    white-space: nowrap;
                }
                @media (prefers-color-scheme: dark) { .timeline-source { color: #a78bfa; } }
                .timeline-repeated {
                    background: rgba(220,53,69,0.06);
                    border-left: 2px solid var(--color-error);
                }
                @media (prefers-color-scheme: dark) { .timeline-repeated { background: rgba(220,53,69,0.12); } }

                /* Query Patterns */
                .pattern-list { margin: 0.4rem 0; }
                .pattern-entry {
                    display: flex;
                    gap: 0.6rem;
                    padding: 0.35rem 0.5rem;
                    font-size: 0.8rem;
                    border-bottom: 1px solid var(--color-border);
                    align-items: baseline;
                }
                .pattern-count {
                    font-weight: 700;
                    min-width: 3rem;
                    text-align: right;
                    color: var(--color-text-secondary);
                    font-family: monospace;
                }
                .pattern-count.hot { color: var(--color-error); }
                .pattern-sql {
                    font-size: 0.78rem;
                    white-space: nowrap;
                    overflow: hidden;
                    text-overflow: ellipsis;
                    color: var(--color-text);
                }

                /* Table Access Frequency */
                .table-freq { margin: 0.75rem 0; }
                .freq-row {
                    display: grid;
                    grid-template-columns: 10rem 1fr 3rem;
                    gap: 0.6rem;
                    padding: 0.4rem 0;
                    align-items: center;
                    border-bottom: 1px solid var(--color-border);
                }
                .freq-table {
                    font-weight: 600;
                    font-size: 0.85rem;
                    color: var(--color-text);
                }
                .freq-bar-track {
                    height: 1rem;
                    background: var(--color-border);
                    border-radius: 3px;
                    overflow: hidden;
                }
                .freq-bar {
                    height: 100%;
                    background: linear-gradient(90deg, #6366f1, #8b5cf6);
                    border-radius: 3px;
                    transition: width 0.3s ease;
                }
                .freq-count {
                    text-align: right;
                    font-weight: 600;
                    font-family: monospace;
                    color: var(--color-text);
                }

                /* Ranked Issues */
                .ranked-issues { display: flex; flex-direction: column; gap: 0.6rem; }
                .ranked-card {
                    display: flex; gap: 0.85rem; padding: 0.85rem 1.1rem;
                    background: var(--color-card); border: 1px solid var(--color-border);
                    border-radius: var(--radius); box-shadow: var(--shadow-sm);
                    transition: box-shadow 0.2s;
                }
                .ranked-card:hover { box-shadow: var(--shadow-md); }
                .ranked-highlight {
                    border-left: 4px solid var(--color-error);
                    background: color-mix(in srgb, var(--color-error) 3%, var(--color-card));
                }
                .ranked-rank {
                    font-size: 1.1rem; font-weight: 700; color: var(--color-text-secondary);
                    min-width: 2.25rem; text-align: center; align-self: flex-start; padding-top: 0.15rem;
                }
                .ranked-highlight .ranked-rank { color: var(--color-error); }
                .ranked-body { flex: 1; min-width: 0; }
                .ranked-header { display: flex; flex-wrap: wrap; align-items: center; gap: 0.4rem; margin-bottom: 0.35rem; }
                .ranked-type { font-weight: 600; }
                .ranked-target {
                    font-family: var(--font-mono); font-size: 0.82rem;
                    background: var(--color-bg); padding: 0.1rem 0.35rem; border-radius: 4px;
                }
                .ranked-freq { font-size: 0.78rem; color: var(--color-text-secondary); }
                .ranked-score-row { display: flex; align-items: center; gap: 0.6rem; margin-bottom: 0.35rem; }
                .ranked-score-value { font-weight: 700; font-size: 0.82rem; min-width: 3.5rem; }
                .ranked-score-track {
                    flex: 1; height: 6px; background: var(--color-border); border-radius: 3px; overflow: hidden;
                }
                .ranked-score-bar { height: 100%; border-radius: 3px; transition: width 0.3s ease; }
                .ranked-score-bar.badge-error { background: var(--color-error); }
                .ranked-score-bar.badge-warning { background: var(--color-warning); }
                .ranked-score-bar.badge-info { background: var(--color-info); }
                .ranked-sql {
                    font-size: 0.78rem; color: var(--color-text-secondary); margin-bottom: 0.25rem;
                    overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
                }
                .ranked-sql code { font-family: var(--font-mono); }
                .ranked-fix { font-size: 0.78rem; color: var(--color-ok); font-style: italic; }

                /* Footer */
                .footer {
                    text-align: center;
                    padding: 1.25rem;
                    color: var(--color-text-secondary);
                    font-size: 0.78rem;
                    border-top: 1px solid var(--color-border);
                    margin-top: auto;
                }

                /* Responsive */
                @media (max-width: 600px) {
                    .header-content { flex-direction: column; align-items: flex-start; }
                    .summary-bar { flex-direction: column; align-items: stretch; }
                    .summary-bar .stat { text-align: center; }
                    .stats-grid { grid-template-columns: repeat(2, 1fr); }
                    .chart-row { flex-direction: column; align-items: center; }
                    .test-name, .method .name, .method-name { min-width: auto; }
                    .ranked-card { flex-direction: column; gap: 0.4rem; }
                    .ranked-rank { align-self: flex-start; }
                    .freq-row { grid-template-columns: 6rem 1fr 2.5rem; }
                    .timeline-entry { grid-template-columns: 2rem 3rem 1fr; }
                    .timeline-source { display: none; }
                }
                """);
    sb.append("</style>\n");
  }

  // -------------------------------------------------------------------------
  // JS (minimal — expand/collapse all)
  // -------------------------------------------------------------------------

  private void appendScript(StringBuilder sb) {
    sb.append("<script>\n");
    sb.append(
        """
                // Keyboard shortcut: 'e' to expand all, 'c' to collapse all
                document.addEventListener('keydown', function(e) {
                    if (e.ctrlKey || e.metaKey || e.altKey) return;
                    if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;
                    var details = document.querySelectorAll('details.test-card, details.method-card, details.method');
                    if (e.key === 'e') {
                        details.forEach(function(d) { d.open = true; });
                    } else if (e.key === 'c') {
                        details.forEach(function(d) { d.open = false; });
                    }
                });

                document.addEventListener('DOMContentLoaded', function() {
                    var className = document.body.dataset['class'] || '';
                    var storeKey = className ? 'qg-checked:' + className : 'qg-checked';
                    var hashKey = className ? 'qg-report-hash:' + className : 'qg-report-hash';

                    var reportHash = document.querySelector('meta[name="qg-report-hash"]');
                    if (reportHash) {
                        var hash = reportHash.content;
                        var prevHash = localStorage.getItem(hashKey);
                        if (prevHash && prevHash !== hash) {
                            localStorage.removeItem(storeKey);
                            if (className) localStorage.removeItem('qg-class-reviewed:' + className);
                        }
                        localStorage.setItem(hashKey, hash);
                    }

                    var checks = document.querySelectorAll('.issue-check');
                    var store = JSON.parse(localStorage.getItem(storeKey) || '{}');

                    function updateClassStatus() {
                        if (!className) return;
                        var methods = document.querySelectorAll('.method');
                        var hasAnyIssue = false;
                        var allMethodsReviewed = true;
                        methods.forEach(function(m) {
                            var issues = m.querySelectorAll('.issues .issue');
                            if (issues.length === 0) return;
                            hasAnyIssue = true;
                            if (!m.classList.contains('all-reviewed')) allMethodsReviewed = false;
                        });
                        if (!hasAnyIssue) return;
                        var key = 'qg-class-reviewed:' + className;
                        if (allMethodsReviewed) {
                            localStorage.setItem(key, 'true');
                            var bar = document.querySelector('.summary-bar');
                            if (bar) {
                                bar.querySelectorAll('.stat.error, .stat.warning').forEach(function(s) {
                                    s.style.textDecoration = 'line-through';
                                    s.style.opacity = '0.5';
                                });
                                var reviewed = bar.querySelector('.class-reviewed-status');
                                if (reviewed) reviewed.style.display = '';
                            }
                        } else {
                            localStorage.removeItem(key);
                            var bar = document.querySelector('.summary-bar');
                            if (bar) {
                                bar.querySelectorAll('.stat.error, .stat.warning').forEach(function(s) {
                                    s.style.textDecoration = '';
                                    s.style.opacity = '';
                                });
                                var reviewed = bar.querySelector('.class-reviewed-status');
                                if (reviewed) reviewed.style.display = 'none';
                            }
                        }
                    }

                    function updateMethodCard(issueEl) {
                        var method = issueEl.closest('.method');
                        if (!method) return;
                        var allIssues = method.querySelectorAll('.issues .issue');
                        if (allIssues.length === 0) return;
                        var allResolved = true;
                        allIssues.forEach(function(iss) {
                            if (!iss.classList.contains('resolved')) allResolved = false;
                        });
                        if (allResolved) {
                            method.classList.add('all-reviewed');
                            var summary = method.querySelector(':scope > summary');
                            if (summary && !summary.querySelector('.review-badge')) {
                                var badge = document.createElement('span');
                                badge.className = 'review-badge';
                                badge.textContent = 'Reviewed';
                                summary.appendChild(badge);
                            }
                        } else {
                            method.classList.remove('all-reviewed');
                            var existing = method.querySelector('.review-badge');
                            if (existing) existing.remove();
                        }
                        updateClassStatus();
                    }

                    checks.forEach(function(cb) {
                        var key = cb.dataset.key;
                        if (store[key]) {
                            cb.checked = true;
                            cb.closest('.issue').classList.add('resolved');
                        }
                        cb.addEventListener('change', function() {
                            if (cb.checked) {
                                store[key] = true;
                                cb.closest('.issue').classList.add('resolved');
                            } else {
                                delete store[key];
                                cb.closest('.issue').classList.remove('resolved');
                            }
                            localStorage.setItem(storeKey, JSON.stringify(store));
                            updateMethodCard(cb.closest('.issue'));
                        });
                    });

                    document.querySelectorAll('.method').forEach(function(m) {
                        var firstIssue = m.querySelector('.issues .issue');
                        if (firstIssue) updateMethodCard(firstIssue);
                    });
                });

                // Deep-link handler: open the target test-card details and scroll to it
                function openAndScroll(event, href) {
                    // For same-page anchors, handle directly
                    var hashIdx = href.indexOf('#');
                    if (hashIdx < 0) return; // no anchor, let browser handle
                    var anchor = href.substring(hashIdx + 1);
                    var el = document.getElementById(anchor);
                    if (el) {
                        event.preventDefault();
                        // Open the details element if it's collapsed
                        if (el.tagName === 'DETAILS' && !el.open) {
                            el.open = true;
                        }
                        el.scrollIntoView({ behavior: 'smooth', block: 'center' });
                        // Flash highlight
                        el.style.transition = 'box-shadow 0.3s';
                        el.style.boxShadow = '0 0 0 3px var(--color-info)';
                        setTimeout(function() { el.style.boxShadow = ''; }, 2000);
                    }
                    // For cross-page links (ClassName.html#test-xxx), let browser navigate
                }

                // On page load, check if URL has a hash and open the target
                document.addEventListener('DOMContentLoaded', function() {
                    if (window.location.hash) {
                        var el = document.getElementById(window.location.hash.substring(1));
                        if (el && el.tagName === 'DETAILS') {
                            el.open = true;
                            el.scrollIntoView({ behavior: 'smooth', block: 'center' });
                            el.style.transition = 'box-shadow 0.3s';
                            el.style.boxShadow = '0 0 0 3px var(--color-info)';
                            setTimeout(function() { el.style.boxShadow = ''; }, 2000);
                        }
                    }
                });

                document.addEventListener('DOMContentLoaded', function() {
                    if (document.body.dataset['class']) return;
                    var rows = document.querySelectorAll('.classes-table tbody tr[data-class]');
                    var currentClasses = new Set();
                    rows.forEach(function(row) {
                        var cls = row.dataset['class'];
                        var expectedHash = row.dataset.hash;
                        currentClasses.add(cls);
                        var storedHash = localStorage.getItem('qg-report-hash:' + cls);
                        if (storedHash && expectedHash && storedHash !== expectedHash) {
                            localStorage.removeItem('qg-class-reviewed:' + cls);
                            localStorage.removeItem('qg-checked:' + cls);
                            localStorage.removeItem('qg-report-hash:' + cls);
                        }
                        if (localStorage.getItem('qg-class-reviewed:' + cls) === 'true') {
                            row.classList.remove('row-fail');
                            row.classList.add('row-pass');
                            var dot = row.querySelector('.status-dot');
                            if (dot) {
                                dot.classList.remove('error-dot', 'warning-dot');
                                dot.classList.add('ok-dot');
                            }
                            var badge = row.querySelector('.badge');
                            if (badge) {
                                badge.classList.remove('badge-error', 'badge-warning');
                                badge.classList.add('badge-acknowledged');
                            }
                        }
                    });
                    Object.keys(localStorage).forEach(function(k) {
                        if (k.startsWith('qg-class-reviewed:') || k.startsWith('qg-checked:') || k.startsWith('qg-report-hash:')) {
                            var cls = k.substring(k.indexOf(':') + 1);
                            if (!currentClasses.has(cls)) localStorage.removeItem(k);
                        }
                    });
                });
                """);
    sb.append("</script>\n");
  }

  // -------------------------------------------------------------------------
  // Utilities
  // -------------------------------------------------------------------------

  /**
   * Writes the contents of the StringBuilder to the writer in chunks to avoid creating a single
   * large String, then clears the buffer for reuse. This is the core mechanism that prevents OOM:
   * instead of accumulating the entire HTML document in a StringBuilder and then calling
   * toString(), we periodically flush sections to disk.
   */
  private static void flushSection(StringBuilder sb, BufferedWriter writer) throws IOException {
    // Write in chunks using a reusable char[] buffer to avoid creating
    // intermediate String objects from StringBuilder.toString()/substring().
    final int chunkSize = 8192;
    final int length = sb.length();
    char[] buf = new char[Math.min(chunkSize, length)];
    for (int offset = 0; offset < length; offset += chunkSize) {
      int end = Math.min(offset + chunkSize, length);
      int count = end - offset;
      sb.getChars(offset, end, buf, 0);
      writer.write(buf, 0, count);
    }
    sb.setLength(0);
  }

  private static String severityCssClass(Severity severity) {
    return switch (severity) {
      case ERROR -> "badge-error";
      case WARNING -> "badge-warning";
      case INFO -> "badge-info";
    };
  }

  private static String issueCheckKey(Issue issue) {
    String nq = issue.query() != null ? SqlParser.normalize(issue.query()) : "";
    return issue.type().getCode() + "-" + Integer.toHexString(nq.hashCode());
  }

  private static String computeReportHash(List<QueryAuditReport> reports) {
    int hash = 0;
    for (QueryAuditReport r : reports) {
      List<List<Issue>> allIssueLists = List.of(
          r.getErrors(), r.getWarnings(),
          r.getInfoIssues() != null ? r.getInfoIssues() : List.of());
      for (List<Issue> issues : allIssueLists) {
        for (Issue issue : issues) {
          hash = 31 * hash + issueCheckKey(issue).hashCode();
        }
      }
    }
    return Integer.toHexString(hash);
  }

  private static String esc(String text) {
    if (text == null) return "";
    return text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("\"", "&quot;")
        .replace("'", "&#39;");
  }

  /**
   * Appends a clickable link for an affected test. The test string may be:
   * <ul>
   *   <li>{@code "ClassName#testName"} — renders as a link to {@code ClassName.html#test-testName}</li>
   *   <li>{@code "testName"} (no #) — renders as plain text</li>
   * </ul>
   */
  private void appendTestLink(StringBuilder sb, String qualifiedTest, String indent) {
    int hashIdx = qualifiedTest.indexOf('#');
    if (hashIdx > 0 && hashIdx < qualifiedTest.length() - 1) {
      String className = qualifiedTest.substring(0, hashIdx);
      String testName = qualifiedTest.substring(hashIdx + 1);
      String href = classFileName(className) + "#test-" + sanitizeAnchor(testName);
      sb.append(indent)
          .append("<a class=\"affected-test-link\" href=\"")
          .append(esc(href))
          .append("\" onclick=\"openAndScroll(event, this.href)\">")
          .append(esc(testName))
          .append("</a>\n");
    } else {
      sb.append(indent)
          .append("<span class=\"affected-test\">")
          .append(esc(qualifiedTest))
          .append("</span>\n");
    }
  }

  /**
   * Converts a test name into a safe HTML anchor ID by replacing non-alphanumeric characters
   * with hyphens.
   */
  private static String sanitizeAnchor(String name) {
    if (name == null) return "unknown";
    return name.replaceAll("[^a-zA-Z0-9_-]", "-");
  }

  /** Deduplication row for the index page issues table. */
  private static class DedupIssueRow {
    final Issue issue;
    final List<String> locations = new ArrayList<>();
    int count;

    DedupIssueRow(Issue issue) {
      this.issue = issue;
      this.count = 0;
    }

    void addLocation(String location) {
      count++;
      if (!locations.contains(location) && locations.size() < 5) {
        locations.add(location);
      }
    }
  }
}
