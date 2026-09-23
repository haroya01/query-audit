package io.queryaudit.core.reporter;

import io.queryaudit.core.baseline.Baseline;
import io.queryaudit.core.baseline.BaselineEntry;
import io.queryaudit.core.dedup.DeduplicatedIssue;
import io.queryaudit.core.model.Finding;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
      writeIndexHtml(writer, byClass, rankedIssues, dedup);
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
      Map<String, List<QueryAuditReport>> byClass,
      List<RankedIssue> rankedIssues,
      List<DeduplicatedIssue> deduplicatedIssues)
      throws IOException {
    String timestamp = LocalDateTime.now().format(TIMESTAMP_FMT);

    // Build and flush each section incrementally to avoid holding the entire
    // HTML document in memory at once (prevents OOM with large test suites).
    StringBuilder sb = new StringBuilder(16_384);

    sb.append("<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n");
    sb.append("<meta charset=\"UTF-8\">\n");
    sb.append("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n");
    sb.append("<title>Query Guard Report</title>\n");
    HtmlReportAssets.appendStyles(sb);
    sb.append("</head>\n<body>\n");

    // Header
    sb.append("<header class=\"header\">\n");
    sb.append("  <div class=\"header-content\">\n");
    sb.append("    <div class=\"header-title\">\n");
    sb.append(
        "      <svg class=\"logo\" viewBox=\"0 0 24 24\" width=\"32\" height=\"32\" fill=\"none\""
            + " stroke=\"currentColor\" stroke-width=\"2\">");
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

    HtmlReportAssets.appendScript(sb);
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
    long totalErrors =
        classReports.stream().mapToLong(r -> countConfirmed(r, Severity.ERROR)).sum();
    long totalWarnings =
        classReports.stream().mapToLong(r -> countConfirmed(r, Severity.WARNING)).sum();
    long totalInfos =
        classReports.stream().mapToLong(r -> r.getFindings().informational().size()).sum();

    String timestamp = LocalDateTime.now().format(TIMESTAMP_FMT);

    // Build and flush each section incrementally to avoid holding the entire
    // HTML document in memory at once (prevents OOM with large test suites).
    StringBuilder sb = new StringBuilder(16_384);

    String reportHash = LegacyFindingPresentation.htmlReviewHash(classReports);

    sb.append("<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n");
    sb.append("<meta charset=\"UTF-8\">\n");
    sb.append("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n");
    sb.append("<meta name=\"qg-report-hash\" content=\"").append(reportHash).append("\">\n");
    sb.append("<title>Query Guard Report — ").append(esc(className)).append("</title>\n");
    HtmlReportAssets.appendStyles(sb);
    sb.append("</head>\n<body data-class=\"").append(esc(className)).append("\">\n");

    // Header
    sb.append("<header class=\"header\">\n");
    sb.append("  <div class=\"header-content\">\n");
    sb.append("    <div class=\"header-title\">\n");
    sb.append(
        "      <svg class=\"logo\" viewBox=\"0 0 24 24\" width=\"32\" height=\"32\" fill=\"none\""
            + " stroke=\"currentColor\" stroke-width=\"2\">");
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
      sb.append("  <div class=\"stat warning\">").append(totalWarnings).append(" warnings</div>\n");
    }
    if (totalInfos > 0) {
      sb.append("  <div class=\"stat info\">").append(totalInfos).append(" info</div>\n");
    }
    if (totalErrors == 0 && totalWarnings == 0) {
      sb.append("  <div class=\"stat ok\">all clean</div>\n");
    }
    sb.append(
        "  <div class=\"stat ok class-reviewed-status\" style=\"display:none\">all"
            + " reviewed</div>\n");
    sb.append("</div>\n");
    flushSection(sb, writer);

    // Methods as collapsible cards
    appendMethodCards(sb, writer, classReports);

    sb.append("</main>\n");

    // Footer
    sb.append("<footer class=\"footer\">\n");
    sb.append("  <p>Query Guard &mdash; Static &amp; Runtime SQL Analysis</p>\n");
    sb.append("</footer>\n");

    HtmlReportAssets.appendScript(sb);
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
        "  <p class=\"section-desc\">Each unique issue is shown once, with the number of tests"
            + " affected.</p>\n");
    sb.append("  <div class=\"table-wrapper\">\n");
    sb.append("  <table class=\"breakdown-table dedup-table\">\n");
    sb.append("    <thead><tr>");
    sb.append(
        "<th>Issue Type</th><th>Target</th><th>Occurrences</th><th>SQL"
            + " (truncated)</th><th>Fix</th><th>Affected Tests</th>");
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
        "  <p class=\"section-desc\">Ranked by impact score (frequency &times; severity &times;"
            + " pattern weight). ");
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

  private void appendClassesTable(StringBuilder sb, Map<String, List<QueryAuditReport>> byClass) {
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
                  long errors =
                      e.getValue().stream().mapToLong(r -> countConfirmed(r, Severity.ERROR)).sum();
                  long warnings =
                      e.getValue().stream()
                          .mapToLong(r -> countConfirmed(r, Severity.WARNING))
                          .sum();
                  return errors == 0 && warnings == 0;
                })
            .thenComparing(Map.Entry::getKey));

    for (Map.Entry<String, List<QueryAuditReport>> entry : sorted) {
      String className = entry.getKey();
      List<QueryAuditReport> classReports = entry.getValue();
      int testCount = classReports.size();
      int queryCount = classReports.stream().mapToInt(QueryAuditReport::getTotalQueryCount).sum();
      long errorCount =
          classReports.stream().mapToLong(r -> countConfirmed(r, Severity.ERROR)).sum();
      long warningCount =
          classReports.stream().mapToLong(r -> countConfirmed(r, Severity.WARNING)).sum();
      long infoCount =
          classReports.stream().mapToLong(r -> r.getFindings().informational().size()).sum();
      long issueCount = errorCount + warningCount + infoCount;
      boolean hasIssues = errorCount > 0 || warningCount > 0;
      long durationMs =
          classReports.stream().mapToLong(QueryAuditReport::getTotalExecutionTimeNanos).sum()
              / 1_000_000L;
      String durationStr =
          durationMs >= 1000 ? String.format("%.1fs", durationMs / 1000.0) : durationMs + "ms";

      String classHash = LegacyFindingPresentation.htmlReviewHash(classReports);
      sb.append("    <tr class=\"")
          .append(hasIssues ? "row-fail" : "row-pass")
          .append("\" data-class=\"")
          .append(esc(className))
          .append("\" data-hash=\"")
          .append(classHash)
          .append("\">\n");
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
  // Method cards (for class detail pages)
  // =========================================================================

  /** Renders method cards for the class detail page. All methods start collapsed. */
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
      var findings = report.getFindings();
      boolean hasIssues = findings.hasConfirmed();
      long errorCount = countConfirmed(report, Severity.ERROR);
      long warningCount = countConfirmed(report, Severity.WARNING);
      int infoCount = findings.informational().size();
      long issueCount = errorCount + warningCount + infoCount;
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
      sb.append("    <span class=\"name\">").append(esc(report.getTestName())).append("</span>\n");
      long execTimeMs = report.getTotalExecutionTimeNanos() / 1_000_000;
      int uniquePatterns = report.getUniquePatternCount();
      sb.append("    <span class=\"meta\">")
          .append(queryCount)
          .append(" queries")
          .append(" (")
          .append(uniquePatterns)
          .append(" unique)")
          .append(", ")
          .append(issueCount)
          .append(" issue")
          .append(issueCount != 1 ? "s" : "")
          .append(", ")
          .append(execTimeMs)
          .append("ms")
          .append("</span>\n");
      sb.append("  </summary>\n");

      // Queries precede findings within each method.
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
      if (hasIssues || infoCount > 0) {
        sb.append("  <div class=\"issues\">\n");
        for (Finding finding : findings.confirmed()) {
          appendFindingCard(sb, report.getTestId(), finding, false);
        }
        for (Finding finding : findings.informational()) {
          appendFindingCard(sb, report.getTestId(), finding, false);
        }
        sb.append("  </div>\n");
      }

      // 3) Acknowledged issues
      if (!findings.acknowledged().isEmpty()) {
        sb.append("  <div class=\"issues acknowledged-issues\">\n");
        for (Finding finding : findings.acknowledged()) {
          appendFindingCard(sb, report.getTestId(), finding, true);
        }
        sb.append("  </div>\n");
      }

      // No issues message
      if (!hasIssues && findings.acknowledged().isEmpty() && infoCount == 0) {
        sb.append("  <p class=\"no-issues\">No issues detected. All queries look good.</p>\n");
      }

      sb.append("</details>\n");

      // Flush each method to keep memory bounded
      flushSection(sb, writer);
    }
  }

  private static long countConfirmed(QueryAuditReport report, Severity severity) {
    return report.getFindings().confirmed().stream()
        .filter(finding -> finding.severity() == severity)
        .count();
  }

  private void appendFindingCard(
      StringBuilder sb, String testId, Finding finding, boolean acknowledged) {
    String id = FindingId.of(testId, finding);
    String badge = acknowledged ? "badge-acknowledged" : severityCssClass(finding.severity());
    sb.append("    <div class=\"")
        .append(acknowledged ? "issue-card " : "issue ")
        .append(badge)
        .append("\" data-finding-id=\"")
        .append(esc(id))
        .append("\">\n");
    if (!acknowledged) {
      sb.append(
              "      <label class=\"issue-label\"><input type=\"checkbox\" class=\"issue-check\""
                  + " data-key=\"")
          .append(esc(LegacyFindingPresentation.htmlCheckKey(testId, finding)))
          .append("\"> ");
    }
    sb.append("<span class=\"badge ")
        .append(badge)
        .append("\">")
        .append(acknowledged ? "ACKNOWLEDGED" : finding.severity().name())
        .append("</span> ")
        .append(esc(LegacyFindingPresentation.description(finding)));
    if (!acknowledged) sb.append("</label>");
    sb.append('\n');
    sb.append("      <p class=\"source finding-id\">Finding ID: <code>")
        .append(esc(id))
        .append("</code></p>\n");
    if (finding.table() != null && !finding.table().isBlank()) {
      sb.append("      <p>Target: ").append(esc(finding.table()));
      if (finding.column() != null && !finding.column().isBlank()) {
        sb.append('.').append(esc(finding.column()));
      }
      sb.append("</p>\n");
    }
    if (finding.detail() != null && !finding.detail().isBlank()) {
      sb.append("      <p>").append(esc(finding.detail())).append("</p>\n");
    }
    if (finding.query() != null && !finding.query().isBlank()) {
      sb.append("      <code>").append(esc(finding.query())).append("</code>\n");
    }
    if (finding.suggestion() != null && !finding.suggestion().isBlank()) {
      sb.append("      <p class=\"fix\">Fix: ").append(esc(finding.suggestion())).append("</p>\n");
    }
    if (finding.sourceLocation() != null && !finding.sourceLocation().isBlank()) {
      sb.append("      <p class=\"source\">Source: ")
          .append(esc(finding.sourceLocation()))
          .append("</p>\n");
    }
    if (acknowledged) appendBaselineDetails(sb, Baseline.findFindingMatch(baseline, finding));
    sb.append("    </div>\n");
  }

  // -------------------------------------------------------------------------
  // Query timeline & patterns (shared by class detail pages)
  // -------------------------------------------------------------------------

  private void appendQueryTimeline(StringBuilder sb, QueryAuditReport report) {
    List<QueryRecord> queries = report.getAllQueries();
    if (report.getOmittedQueryCount() > 0) {
      sb.append("<p class=\"query-evidence-notice\">Query evidence: ")
          .append(report.getRetainedQueryCount())
          .append(" retained, ")
          .append(report.getOmittedQueryCount())
          .append(" omitted from this report. Query totals and findings are unchanged.</p>\n");
    }
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

  private void appendBaselineDetails(StringBuilder sb, BaselineEntry match) {
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
   *
   * <ul>
   *   <li>{@code "ClassName#testName"} — renders as a link to {@code ClassName.html#test-testName}
   *   <li>{@code "testName"} (no #) — renders as plain text
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
   * Converts a test name into a safe HTML anchor ID by replacing non-alphanumeric characters with
   * hyphens.
   */
  private static String sanitizeAnchor(String name) {
    if (name == null) return "unknown";
    return name.replaceAll("[^a-zA-Z0-9_-]", "-");
  }
}
