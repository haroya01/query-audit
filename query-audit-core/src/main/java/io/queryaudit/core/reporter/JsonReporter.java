package io.queryaudit.core.reporter;

import io.queryaudit.core.model.AuditIncompleteReason;
import io.queryaudit.core.model.AuditRunResult;
import io.queryaudit.core.model.IndexInfo;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.TestSelector;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

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

  /**
   * Version of the {@code report.json} envelope, bumped on any breaking shape change so consumers
   * can detect incompatibilities instead of silently misparsing. Documented in the Reports guide.
   *
   * @since 0.5.0
   */
  public static final String SCHEMA_VERSION = "1.2.0";

  private static final String LEGACY_SCHEMA_VERSION = "1.0.0";

  private String lastJson;

  /**
   * Wraps per-test reports in the versioned envelope written to {@code report.json}: {@code
   * {"schemaVersion": "1.0.0", "reports": [...]}}. This compatibility overload retains the 0.5.x
   * shape because a report list alone cannot prove that the audit completed or that its policies
   * passed.
   *
   * @deprecated use {@link #toRunEnvelopeJson(AuditRunResult)} so incomplete and failed runs are
   *     preserved
   * @since 0.5.0
   */
  @Deprecated(since = "0.6.0")
  public static String toEnvelopeJson(List<QueryAuditReport> reports) {
    StringBuilder sb = new StringBuilder();
    sb.append("{\n");
    sb.append("  \"schemaVersion\": \"").append(LEGACY_SCHEMA_VERSION).append("\",\n");
    appendReports(sb, reports, false);
    sb.append("\n}");
    return sb.toString();
  }

  /**
   * Wraps per-test reports and the suite result in the versioned {@code report.json} envelope.
   *
   * @since 0.6.0
   */
  public static String toRunEnvelopeJson(AuditRunResult runResult) {
    StringBuilder sb = new StringBuilder();
    sb.append("{\n");
    sb.append("  \"schemaVersion\": \"").append(SCHEMA_VERSION).append("\",\n");
    sb.append("  \"outcome\": \"").append(runResult.outcome()).append("\",\n");
    sb.append("  \"incompleteReasons\": ");
    appendIncompleteReasons(sb, runResult.incompleteReasons());
    sb.append(",\n");
    appendReports(sb, runResult.reports(), true);
    sb.append("\n}");
    return sb.toString();
  }

  private static void appendReports(
      StringBuilder sb, List<QueryAuditReport> reports, boolean includeIdentity) {
    sb.append("  \"reports\": [\n");
    for (int i = 0; i < reports.size(); i++) {
      sb.append(toJson(reports.get(i), includeIdentity).indent(4).stripTrailing());
      if (i < reports.size() - 1) {
        sb.append(",");
      }
      sb.append("\n");
    }
    sb.append("  ]");
  }

  private static void appendIncompleteReasons(
      StringBuilder sb, List<AuditIncompleteReason> incompleteReasons) {
    if (incompleteReasons.isEmpty()) {
      sb.append("[]");
      return;
    }
    sb.append("[\n");
    for (int i = 0; i < incompleteReasons.size(); i++) {
      AuditIncompleteReason reason = incompleteReasons.get(i);
      sb.append("    {\n");
      appendJsonString(sb, "      ", "code", reason.code().name());
      sb.append(",\n");
      appendJsonString(sb, "      ", "detail", reason.detail());
      sb.append("\n    }");
      if (i < incompleteReasons.size() - 1) {
        sb.append(",");
      }
      sb.append("\n");
    }
    sb.append("  ]");
  }

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
    return toJson(report, true);
  }

  private static String toJson(QueryAuditReport report, boolean includeIdentity) {
    StringBuilder sb = new StringBuilder();
    sb.append("{\n");

    if (includeIdentity) {
      appendJsonString(sb, "  ", "testId", report.getTestId());
      sb.append(",\n");
    }
    appendJsonString(sb, "  ", "testClass", report.getTestClass());
    sb.append(",\n");
    appendJsonString(sb, "  ", "testName", report.getTestName());
    if (includeIdentity) {
      sb.append(",\n");
      appendTestSelector(sb, report.getTestSelector());
    }
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

    // indexMetadata — only tables referenced by findings, so a consumer acting on the report
    // (CI bot, remediation tooling) can see the actual index state without database access.
    sb.append("  \"indexMetadata\": ");
    appendIndexMetadata(sb, report, "  ");
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
      sb.append(",\n");
      appendJsonString(sb, innerField, "sourceLocation", issue.sourceLocation());
      RemediationHints.Remediation remediation = RemediationHints.forIssue(issue);
      if (remediation != null) {
        sb.append(",\n");
        sb.append(innerField).append("\"remediation\": {");
        sb.append("\"kind\": \"").append(escapeJson(remediation.kind())).append("\"");
        if (remediation.table() != null) {
          sb.append(", \"table\": \"").append(escapeJson(remediation.table())).append("\"");
        }
        if (!remediation.columns().isEmpty()) {
          sb.append(", \"columns\": [");
          for (int c = 0; c < remediation.columns().size(); c++) {
            if (c > 0) {
              sb.append(", ");
            }
            sb.append("\"").append(escapeJson(remediation.columns().get(c))).append("\"");
          }
          sb.append("]");
        }
        sb.append("}");
      }
      sb.append("\n");
      sb.append(inner).append("}");
      if (i < issues.size() - 1) {
        sb.append(",");
      }
      sb.append("\n");
    }
    sb.append(indent).append("]");
  }

  private static void appendQueryArray(StringBuilder sb, List<QueryRecord> queries, String indent) {
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

  private static void appendTestSelector(StringBuilder sb, TestSelector selector) {
    sb.append("  \"testSelector\": ");
    if (selector == null) {
      sb.append("null");
      return;
    }
    sb.append("{");
    appendJsonString(sb, "", "type", selector.type());
    sb.append(", ");
    appendJsonString(sb, "", "value", selector.value());
    sb.append("}");
  }

  /**
   * Serializes the index state of every table referenced by a finding, grouped per index with
   * columns in index order. {@code null} when no metadata was collected (non-database tests);
   * {@code {}} when metadata exists but no finding references a known table. Cardinality is taken
   * from the index's last column entry — the whole-index cardinality in {@code SHOW INDEX}
   * semantics.
   */
  private static void appendIndexMetadata(
      StringBuilder sb, QueryAuditReport report, String indent) {
    IndexMetadata metadata = report.getIndexMetadata();
    if (metadata == null) {
      sb.append("null");
      return;
    }

    Set<String> tables = new TreeSet<>();
    collectIssueTables(tables, report.getConfirmedIssues());
    collectIssueTables(tables, report.getInfoIssues());
    collectIssueTables(tables, report.getAcknowledgedIssues());

    StringBuilder body = new StringBuilder();
    boolean firstTable = true;
    for (String table : tables) {
      List<IndexInfo> rows = metadata.getIndexesForTable(table);
      if (rows == null || rows.isEmpty()) {
        continue;
      }
      Map<String, List<IndexInfo>> byIndex = new LinkedHashMap<>();
      rows.stream()
          .sorted(Comparator.comparingInt(IndexInfo::seqInIndex))
          .forEach(r -> byIndex.computeIfAbsent(r.indexName(), k -> new ArrayList<>()).add(r));

      if (!firstTable) {
        body.append(",\n");
      }
      firstTable = false;
      body.append(indent).append("  \"").append(escapeJson(table)).append("\": [");
      boolean firstIndex = true;
      for (Map.Entry<String, List<IndexInfo>> entry : byIndex.entrySet()) {
        if (!firstIndex) {
          body.append(", ");
        }
        firstIndex = false;
        List<IndexInfo> indexRows = entry.getValue();
        body.append("{\"name\": \"").append(escapeJson(entry.getKey())).append("\", ");
        body.append("\"unique\": ").append(!indexRows.get(0).nonUnique()).append(", ");
        body.append("\"columns\": [");
        for (int c = 0; c < indexRows.size(); c++) {
          if (c > 0) {
            body.append(", ");
          }
          body.append("\"").append(escapeJson(indexRows.get(c).columnName())).append("\"");
        }
        body.append("], \"cardinality\": ")
            .append(indexRows.get(indexRows.size() - 1).cardinality())
            .append("}");
      }
      body.append("]");
    }

    if (body.length() == 0) {
      sb.append("{}");
      return;
    }
    sb.append("{\n").append(body).append("\n").append(indent).append("}");
  }

  private static void collectIssueTables(Set<String> tables, List<Issue> issues) {
    if (issues == null) {
      return;
    }
    for (Issue issue : issues) {
      if (issue.table() != null && !issue.table().isBlank()) {
        tables.add(issue.table());
      }
    }
  }

  // ---------------------------------------------------------------------------
  // JSON encoding helpers
  // ---------------------------------------------------------------------------

  private static void appendJsonString(StringBuilder sb, String indent, String key, String value) {
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
