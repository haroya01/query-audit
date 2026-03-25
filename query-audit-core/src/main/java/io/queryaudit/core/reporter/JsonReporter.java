package io.queryaudit.core.reporter;

import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import java.util.List;

/**
 * Outputs a {@link QueryAuditReport} as structured JSON suitable for dashboards, PR comments, and
 * trend tracking.
 *
 * <p>Uses {@link StringBuilder} exclusively to avoid external JSON library dependencies.
 *
 * @author haroya
 * @since 0.2.0
 */
public class JsonReporter implements Reporter {

  private String lastJson;

  @Override
  public void report(QueryAuditReport report) {
    lastJson = toJson(report);
  }

  /** Returns the JSON string produced by the most recent {@link #report} call, or {@code null}. */
  public String getJson() {
    return lastJson;
  }

  /** Converts a report to its JSON representation. */
  public static String toJson(QueryAuditReport report) {
    StringBuilder sb = new StringBuilder();
    sb.append("{\n");

    appendJsonString(sb, "  ", "testClass", report.getTestClass());
    sb.append(",\n");
    appendJsonString(sb, "  ", "testName", report.getTestName());
    sb.append(",\n");

    // summary
    sb.append("  \"summary\": {\n");
    int confirmedCount =
        report.getConfirmedIssues() != null ? report.getConfirmedIssues().size() : 0;
    int infoCount = report.getInfoIssues() != null ? report.getInfoIssues().size() : 0;
    int acknowledgedCount = report.getAcknowledgedCount();
    long executionTimeMs = report.getTotalExecutionTimeNanos() / 1_000_000L;

    sb.append("    \"confirmedIssues\": ").append(confirmedCount).append(",\n");
    sb.append("    \"infoIssues\": ").append(infoCount).append(",\n");
    sb.append("    \"acknowledgedIssues\": ").append(acknowledgedCount).append(",\n");
    sb.append("    \"uniquePatterns\": ").append(report.getUniquePatternCount()).append(",\n");
    sb.append("    \"totalQueries\": ").append(report.getTotalQueryCount()).append(",\n");
    sb.append("    \"executionTimeMs\": ").append(executionTimeMs).append("\n");
    sb.append("  },\n");

    // confirmedIssues
    sb.append("  \"confirmedIssues\": ");
    appendIssueArray(sb, report.getConfirmedIssues(), "  ");
    sb.append(",\n");

    // infoIssues
    sb.append("  \"infoIssues\": ");
    appendIssueArray(sb, report.getInfoIssues(), "  ");
    sb.append(",\n");

    // acknowledgedIssues
    sb.append("  \"acknowledgedIssues\": ");
    appendIssueArray(sb, report.getAcknowledgedIssues(), "  ");
    sb.append(",\n");

    // queries
    sb.append("  \"queries\": ");
    appendQueryArray(sb, report.getAllQueries(), "  ");
    sb.append("\n");

    sb.append("}");
    return sb.toString();
  }

  // ---------------------------------------------------------------------------
  // Array helpers
  // ---------------------------------------------------------------------------

  private static void appendIssueArray(StringBuilder sb, List<Issue> issues, String indent) {
    if (issues == null || issues.isEmpty()) {
      sb.append("[]");
      return;
    }
    sb.append("[\n");
    for (int i = 0; i < issues.size(); i++) {
      Issue issue = issues.get(i);
      String inner = indent + "  ";
      String innerField = inner + "  ";
      sb.append(inner).append("{\n");
      appendJsonString(sb, innerField, "type", issue.type().getCode());
      sb.append(",\n");
      appendJsonString(sb, innerField, "severity", issue.severity().name());
      sb.append(",\n");
      appendJsonString(sb, innerField, "query", issue.query());
      sb.append(",\n");
      appendJsonString(sb, innerField, "table", issue.table());
      sb.append(",\n");
      appendJsonString(sb, innerField, "column", issue.column());
      sb.append(",\n");
      appendJsonString(sb, innerField, "detail", issue.detail());
      sb.append(",\n");
      appendJsonString(sb, innerField, "suggestion", issue.suggestion());
      sb.append("\n");
      sb.append(inner).append("}");
      if (i < issues.size() - 1) {
        sb.append(",");
      }
      sb.append("\n");
    }
    sb.append(indent).append("]");
  }

  private static void appendQueryArray(
      StringBuilder sb, List<QueryRecord> queries, String indent) {
    if (queries == null || queries.isEmpty()) {
      sb.append("[]");
      return;
    }
    sb.append("[\n");
    for (int i = 0; i < queries.size(); i++) {
      QueryRecord q = queries.get(i);
      String inner = indent + "  ";
      String innerField = inner + "  ";
      sb.append(inner).append("{\n");
      appendJsonString(sb, innerField, "sql", q.sql());
      sb.append(",\n");
      appendJsonString(sb, innerField, "normalizedSql", q.normalizedSql());
      sb.append(",\n");
      sb.append(innerField).append("\"executionTimeNanos\": ").append(q.executionTimeNanos());
      sb.append(",\n");
      appendJsonString(sb, innerField, "stackTrace", q.stackTrace());
      sb.append("\n");
      sb.append(inner).append("}");
      if (i < queries.size() - 1) {
        sb.append(",");
      }
      sb.append("\n");
    }
    sb.append(indent).append("]");
  }

  // ---------------------------------------------------------------------------
  // JSON encoding helpers
  // ---------------------------------------------------------------------------

  private static void appendJsonString(
      StringBuilder sb, String indent, String key, String value) {
    sb.append(indent).append("\"").append(key).append("\": ");
    if (value == null) {
      sb.append("null");
    } else {
      sb.append("\"").append(escapeJson(value)).append("\"");
    }
  }

  /**
   * Escapes special characters for JSON string values according to RFC 8259.
   *
   * <p>Handles: {@code "}, {@code \}, {@code /}, and control characters ({@code \b}, {@code \f},
   * {@code \n}, {@code \r}, {@code \t}), plus any other control character as {@code \\u00XX}.
   */
  static String escapeJson(String raw) {
    if (raw == null) {
      return null;
    }
    StringBuilder sb = new StringBuilder(raw.length());
    for (int i = 0; i < raw.length(); i++) {
      char ch = raw.charAt(i);
      switch (ch) {
        case '"' -> sb.append("\\\"");
        case '\\' -> sb.append("\\\\");
        case '/' -> sb.append("\\/");
        case '\b' -> sb.append("\\b");
        case '\f' -> sb.append("\\f");
        case '\n' -> sb.append("\\n");
        case '\r' -> sb.append("\\r");
        case '\t' -> sb.append("\\t");
        default -> {
          if (ch < 0x20) {
            sb.append(String.format("\\u%04x", (int) ch));
          } else {
            sb.append(ch);
          }
        }
      }
    }
    return sb.toString();
  }
}
