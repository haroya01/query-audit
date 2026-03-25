package io.queryaudit.core.reporter;

import io.queryaudit.core.baseline.Baseline;
import io.queryaudit.core.baseline.BaselineEntry;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.parser.SqlParser;
import io.queryaudit.core.ranking.ImpactScorer;
import io.queryaudit.core.ranking.RankedIssue;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Prints a human-readable, ANSI-colored report to the console.
 *
 * <p>Color output is automatically disabled when {@code NO_COLOR} environment variable is set or
 * when there is no interactive console attached.
 *
 * @author haroya
 * @since 0.2.0
 */
public class ConsoleReporter implements Reporter {

  // ANSI escape codes
  private static final String RESET = "\u001B[0m";
  private static final String BOLD = "\u001B[1m";
  private static final String RED = "\u001B[31m";
  private static final String GREEN = "\u001B[32m";
  private static final String YELLOW = "\u001B[33m";
  private static final String CYAN = "\u001B[36m";
  private static final String DIM = "\u001B[2m";

  private static final int MAX_SQL_LENGTH = 120;
  private static final String DIVIDER = "─".repeat(72);

  private final PrintStream out;
  private final boolean colorsEnabled;
  private final List<BaselineEntry> baseline;

  public ConsoleReporter() {
    this(System.out, detectColorSupport());
  }

  public ConsoleReporter(PrintStream out, boolean colorsEnabled) {
    this(out, colorsEnabled, List.of());
  }

  public ConsoleReporter(PrintStream out, boolean colorsEnabled, List<BaselineEntry> baseline) {
    this.out = out;
    this.colorsEnabled = colorsEnabled;
    this.baseline = baseline != null ? baseline : List.of();
  }

  @Override
  public void report(QueryAuditReport report) {
    printHeader(report);

    List<Issue> errors = report.getErrors();
    List<Issue> warnings = report.getWarnings();
    List<Issue> infoIssues = report.getInfoIssues();
    int confirmedCount =
        report.getConfirmedIssues() != null ? report.getConfirmedIssues().size() : 0;
    int infoCount = infoIssues != null ? infoIssues.size() : 0;
    int passedCount = report.getTotalQueryCount() - confirmedCount - infoCount;
    if (passedCount < 0) {
      passedCount = 0;
    }

    // Top issues by impact (within this single test)
    printTopIssues(report);

    // Confirmed section — grouped by issue type, sorted by severity
    if (!errors.isEmpty() || !warnings.isEmpty()) {
      out.println();
      out.println(color(BOLD, "--- CONFIRMED (100% reliable, sorted by priority) ---"));
      out.println();

      // Group by IssueType, keep severity order (errors first)
      Map<IssueType, List<Issue>> grouped = new LinkedHashMap<>();
      for (Issue issue : errors) {
        grouped.computeIfAbsent(issue.type(), k -> new ArrayList<>()).add(issue);
      }
      for (Issue issue : warnings) {
        grouped.computeIfAbsent(issue.type(), k -> new ArrayList<>()).add(issue);
      }

      for (Map.Entry<IssueType, List<Issue>> entry : grouped.entrySet()) {
        List<Issue> group = entry.getValue();
        if (group.size() == 1) {
          printIssue(group.get(0));
        } else {
          // Print group header with count
          Issue first = group.get(0);
          String severityColor = colorForSeverity(first.severity());
          String tag = "[" + first.severity().name() + "]";
          out.println(
              "  "
                  + color(severityColor, BOLD, tag)
                  + " "
                  + color(severityColor, first.type().getDescription())
                  + color(DIM, " (" + group.size() + " occurrences)"));
          // Print first one in full, rest as compact
          printIssue(group.get(0));
          for (int i = 1; i < group.size() && i < 3; i++) {
            printIssueCompact(group.get(i));
          }
          if (group.size() > 3) {
            out.println("    " + color(DIM, "... and " + (group.size() - 3) + " more"));
            out.println();
          }
        }
      }
    }

    // Info section
    if (infoIssues != null && !infoIssues.isEmpty()) {
      out.println();
      out.println(color(BOLD, "--- INFO (may vary with data volume) ---"));
      out.println();

      for (Issue issue : infoIssues) {
        printIssue(issue);
      }
    }

    // Acknowledged section
    List<Issue> acknowledgedIssues = report.getAcknowledgedIssues();
    int acknowledgedCount = acknowledgedIssues.size();
    if (!acknowledgedIssues.isEmpty()) {
      out.println();
      out.println(color(GREEN, BOLD, "--- ACKNOWLEDGED (reviewed, no action needed) ---"));
      out.println();

      for (Issue issue : acknowledgedIssues) {
        printAcknowledgedIssue(issue);
      }
    }

    // Passed queries
    out.println();
    out.println(color(GREEN, "[OK] " + passedCount + " queries passed"));

    // Query Patterns
    printQueryPatterns(report);

    // Table Access Frequency
    printTableAccessFrequency(report);

    // Summary
    printSummary(report, confirmedCount, infoCount, acknowledgedCount);
  }

  // -------------------------------------------------------------------------
  // Formatting helpers
  // -------------------------------------------------------------------------

  private void printHeader(QueryAuditReport report) {
    out.println();
    out.println(color(BOLD, DIVIDER));
    out.println(color(BOLD, "  QUERY GUARD REPORT"));
    if (report.getTestName() != null && !report.getTestName().isBlank()) {
      out.println(color(DIM, "  Test: " + report.getTestName()));
    }
    out.println(color(BOLD, DIVIDER));
  }

  private void printTopIssues(QueryAuditReport report) {
    List<Issue> confirmed = report.getConfirmedIssues();
    if (confirmed == null || confirmed.isEmpty()) {
      return;
    }

    List<RankedIssue> ranked = ImpactScorer.rank(confirmed);
    if (ranked.isEmpty()) {
      return;
    }

    int limit = Math.min(ranked.size(), 5);
    out.println();
    out.println(color(BOLD, "--- TOP ISSUES BY IMPACT ---"));
    out.println();

    for (int i = 0; i < limit; i++) {
      RankedIssue ri = ranked.get(i);
      Issue issue = ri.issue();
      String severityColor = colorForSeverity(issue.severity());
      String tag = "[" + issue.severity().name() + "]";

      StringBuilder line = new StringBuilder();
      line.append("  #").append(ri.rank()).append(" ");
      line.append(color(severityColor, BOLD, tag)).append(" ");
      line.append(color(severityColor, issue.type().getDescription()));

      // Target
      if (issue.table() != null && !issue.table().isBlank()) {
        line.append(" ").append(color(DIM, issue.table()));
        if (issue.column() != null && !issue.column().isBlank()) {
          line.append(".").append(color(DIM, issue.column()));
        }
      }

      line.append("  ").append(color(BOLD, ri.impactScore() + " pts"));

      if (ri.frequency() > 1) {
        line.append(" ").append(color(DIM, "(" + ri.frequency() + "x)"));
      }

      out.println(line);

      // Fix suggestion on next line (indented)
      if (issue.suggestion() != null && !issue.suggestion().isBlank()) {
        out.println("      Fix: " + color(severityColor, issue.suggestion()));
      }
    }

    if (ranked.size() > limit) {
      out.println("  " + color(DIM, "... and " + (ranked.size() - limit) + " more issues"));
    }
  }

  private void printIssue(Issue issue) {
    String severityColor = colorForSeverity(issue.severity());
    String tag = "[" + issue.severity().name() + "]";

    out.println(
        "  "
            + color(severityColor, BOLD, tag)
            + " "
            + color(severityColor, issue.type().getDescription()));

    if (issue.query() != null && !issue.query().isBlank()) {
      out.println("    Query:  " + color(DIM, truncate(issue.query())));
    }

    if (issue.sourceLocation() != null && !issue.sourceLocation().isBlank()) {
      out.println("    Source: " + color(DIM, issue.sourceLocation()));
    }

    if (issue.table() != null && !issue.table().isBlank()) {
      String location = issue.table();
      if (issue.column() != null && !issue.column().isBlank()) {
        location += "." + issue.column();
      }
      out.println("    Target: " + location);
    }

    if (issue.detail() != null && !issue.detail().isBlank()) {
      out.println("    Detail: " + issue.detail());
    }

    if (issue.suggestion() != null && !issue.suggestion().isBlank()) {
      out.println("    Fix:    " + color(severityColor, issue.suggestion()));
    }

    out.println();
  }

  private void printAcknowledgedIssue(Issue issue) {
    String tag = "[\u2713]"; // checkmark
    out.println("  " + color(GREEN, BOLD, tag) + " " + color(GREEN, issue.type().getDescription()));

    if (issue.table() != null && !issue.table().isBlank()) {
      String location = issue.table();
      if (issue.column() != null && !issue.column().isBlank()) {
        location += "." + issue.column();
      }
      out.println("    Target: " + location);
    }

    // Look up reason and acknowledgedBy from baseline
    BaselineEntry match = Baseline.findMatch(baseline, issue);
    if (match != null) {
      if (match.reason() != null && !match.reason().isBlank()) {
        out.println("    Reason: " + color(DIM, match.reason()));
      }
      if (match.acknowledgedBy() != null && !match.acknowledgedBy().isBlank()) {
        out.println("    By:     " + color(DIM, match.acknowledgedBy()));
      }
    }

    out.println();
  }

  private void printIssueCompact(Issue issue) {
    String severityColor = colorForSeverity(issue.severity());
    StringBuilder sb = new StringBuilder("    ");
    if (issue.table() != null) {
      sb.append(issue.table());
      if (issue.column() != null) sb.append(".").append(issue.column());
      sb.append(" ");
    }
    if (issue.detail() != null) {
      sb.append(color(DIM, "— " + issue.detail()));
    }
    out.println(sb);
  }

  private void printQueryPatterns(QueryAuditReport report) {
    List<QueryRecord> allQueries = report.getAllQueries();
    if (allQueries == null || allQueries.isEmpty()) {
      return;
    }

    // Group by normalizedSql, count occurrences
    Map<String, Integer> patternCounts = new LinkedHashMap<>();
    for (QueryRecord query : allQueries) {
      String normalized = query.normalizedSql();
      if (normalized != null) {
        patternCounts.merge(normalized, 1, Integer::sum);
      }
    }

    if (patternCounts.isEmpty()) {
      return;
    }

    // Sort by count descending
    List<Map.Entry<String, Integer>> sorted = new ArrayList<>(patternCounts.entrySet());
    sorted.sort((a, b) -> Integer.compare(b.getValue(), a.getValue()));

    out.println();
    out.println(color(BOLD, "--- Query Patterns ---"));
    for (Map.Entry<String, Integer> entry : sorted) {
      String countStr = String.format("%3d", entry.getValue());
      String sql = truncatePattern(entry.getKey(), 80);
      out.println("  [" + countStr + "x] " + sql);
    }
  }

  private void printTableAccessFrequency(QueryAuditReport report) {
    List<QueryRecord> allQueries = report.getAllQueries();
    if (allQueries == null || allQueries.isEmpty()) {
      return;
    }

    // Count table references across all queries
    Map<String, Integer> tableCounts = new LinkedHashMap<>();
    for (QueryRecord query : allQueries) {
      List<String> tables = SqlParser.extractTableNames(query.sql());
      for (String table : tables) {
        tableCounts.merge(table, 1, Integer::sum);
      }
    }

    if (tableCounts.isEmpty()) {
      return;
    }

    // Sort by count descending
    List<Map.Entry<String, Integer>> sorted = new ArrayList<>(tableCounts.entrySet());
    sorted.sort((a, b) -> Integer.compare(b.getValue(), a.getValue()));

    // Find max table name length for alignment
    int maxNameLen = sorted.stream().mapToInt(e -> e.getKey().length()).max().orElse(0);

    out.println();
    out.println(color(BOLD, "--- Table Access Frequency ---"));
    for (Map.Entry<String, Integer> entry : sorted) {
      String paddedName = String.format("%-" + maxNameLen + "s", entry.getKey());
      out.printf("  %s  %d queries%n", paddedName, entry.getValue());
    }
  }

  private void printSummary(
      QueryAuditReport report, int confirmedCount, int infoCount, int acknowledgedCount) {
    long totalMs = report.getTotalExecutionTimeNanos() / 1_000_000L;

    out.println(color(BOLD, DIVIDER));
    out.printf(
        "  %d unique patterns | %d total queries | %d ms total%n",
        report.getUniquePatternCount(), report.getTotalQueryCount(), totalMs);

    StringBuilder counts = new StringBuilder("  ");
    if (confirmedCount > 0) {
      int errorCount = report.getErrors().size();
      int warnCount = report.getWarnings().size();
      if (errorCount > 0) {
        counts.append(color(RED, errorCount + " error" + (errorCount != 1 ? "s" : "")));
      }
      if (warnCount > 0) {
        if (errorCount > 0) {
          counts.append(" | ");
        }
        counts.append(color(YELLOW, warnCount + " warning" + (warnCount != 1 ? "s" : "")));
      }
      counts.append(" | ");
    }
    if (acknowledgedCount > 0) {
      counts.append(color(GREEN, acknowledgedCount + " acknowledged"));
      counts.append(" | ");
    }
    if (infoCount > 0) {
      counts.append(color(CYAN, infoCount + " info"));
      counts.append(" | ");
    }
    int passedCount = report.getTotalQueryCount() - confirmedCount - infoCount;
    if (passedCount < 0) {
      passedCount = 0;
    }
    counts.append(color(GREEN, passedCount + " passed"));

    out.println(counts);
    out.println(color(BOLD, DIVIDER));
    out.println();
  }

  // -------------------------------------------------------------------------
  // Utility
  // -------------------------------------------------------------------------

  private static String truncate(String sql) {
    String oneLine = sql.replaceAll("\\s+", " ").trim();
    if (oneLine.length() <= MAX_SQL_LENGTH) {
      return oneLine;
    }
    return oneLine.substring(0, MAX_SQL_LENGTH - 3) + "...";
  }

  private static String truncatePattern(String sql, int maxLength) {
    String oneLine = sql.replaceAll("\\s+", " ").trim();
    if (oneLine.length() <= maxLength) {
      return oneLine;
    }
    return oneLine.substring(0, maxLength - 3) + "...";
  }

  private String colorForSeverity(Severity severity) {
    return switch (severity) {
      case ERROR -> RED;
      case WARNING -> YELLOW;
      case INFO -> CYAN;
    };
  }

  /** Wraps text in ANSI codes if colors are enabled; returns plain text otherwise. */
  private String color(String code, String text) {
    if (!colorsEnabled) {
      return text;
    }
    return code + text + RESET;
  }

  /** Wraps text in two ANSI codes (e.g., bold + color) if colors are enabled. */
  private String color(String code1, String code2, String text) {
    if (!colorsEnabled) {
      return text;
    }
    return code1 + code2 + text + RESET;
  }

  /**
   * Detects whether the current terminal supports ANSI colors. Returns {@code false} when the
   * {@code NO_COLOR} environment variable is set or when there is no interactive console.
   */
  public static boolean detectColorSupport() {
    if (System.getenv("NO_COLOR") != null) {
      return false;
    }
    return System.console() != null;
  }
}
