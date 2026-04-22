package io.queryaudit.postgresql;

import io.queryaudit.core.analyzer.ExplainAnalyzer;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.parser.SqlParser;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * PostgreSQL implementation of {@link ExplainAnalyzer}.
 *
 * <p>Runs {@code EXPLAIN (FORMAT JSON)} on captured SELECT queries and detects:
 *
 * <ul>
 *   <li>{@code "Node Type": "Seq Scan"} — full table scan ({@link IssueType#FULL_TABLE_SCAN})
 *   <li>{@code "Node Type": "Sort"} — filesort equivalent ({@link IssueType#FILESORT})
 *   <li>{@code "Node Type": "Hash"} or materialization nodes — temporary table usage ({@link
 *       IssueType#TEMPORARY_TABLE})
 * </ul>
 *
 * <p>Uses {@code EXPLAIN (FORMAT JSON)} without {@code ANALYZE} to avoid actually executing the
 * queries.
 *
 * @author haroya
 * @since 0.2.0
 */
public class PostgreSqlExplainAnalyzer implements ExplainAnalyzer {

  @Override
  public String supportedDatabase() {
    return "postgresql";
  }

  @Override
  public List<Issue> analyze(Connection connection, List<QueryRecord> queries) {
    List<Issue> issues = new ArrayList<>();
    Set<String> analyzed = new HashSet<>();

    for (QueryRecord query : queries) {
      String sql = query.sql();
      if (!SqlParser.isSelectQuery(sql)) {
        continue;
      }
      if (!analyzed.add(query.normalizedSql())) {
        continue;
      }

      try {
        String jsonOutput = runExplain(connection, sql);
        if (jsonOutput == null) {
          continue;
        }

        // Parse the JSON output with simple string matching.
        // Full JSON parsing would require a dependency; the EXPLAIN JSON structure
        // is predictable enough for substring checks.
        if (jsonOutput.contains("\"Seq Scan\"")) {
          String table = extractJsonField(jsonOutput, "Relation Name");
          long rows = extractJsonLong(jsonOutput, "Plan Rows");
          issues.add(
              new Issue(
                  IssueType.FULL_TABLE_SCAN,
                  Severity.INFO,
                  query.sql(),
                  table,
                  null,
                  String.format(
                      "EXPLAIN shows Seq Scan on table '%s', ~%d rows estimated",
                      table != null ? table : "unknown", rows),
                  "Add an index on the filtered/joined columns or rewrite the query to use an"
                      + " index."));
        }
        if (jsonOutput.contains("\"Sort\"")) {
          String table = extractJsonField(jsonOutput, "Relation Name");
          issues.add(
              new Issue(
                  IssueType.FILESORT,
                  Severity.INFO,
                  query.sql(),
                  table,
                  null,
                  "EXPLAIN shows Sort node — data must be sorted without index",
                  "Add an index that covers the ORDER BY columns to avoid sorting."));
        }
        if (jsonOutput.contains("\"Materialize\"")
            || jsonOutput.contains("\"Hash\"")
            || jsonOutput.contains("\"CTE Scan\"")) {
          String table = extractJsonField(jsonOutput, "Relation Name");
          issues.add(
              new Issue(
                  IssueType.TEMPORARY_TABLE,
                  Severity.INFO,
                  query.sql(),
                  table,
                  null,
                  "EXPLAIN shows materialization/hash node — temporary data structure used",
                  "Consider rewriting the query to avoid temporary table usage."));
        }
      } catch (Exception e) {
        // Skip queries that can't be EXPLAINed
      }
    }
    return issues;
  }

  /**
   * Replaces {@code ?} parameter placeholders with safe dummy values so that the SQL can be
   * executed as a plain {@code EXPLAIN} statement. datasource-proxy captures SQL with {@code ?}
   * placeholders which are invalid in a non-PreparedStatement context.
   *
   * <p>Using {@code 1} is safe because EXPLAIN only plans the query without executing it, and
   * PostgreSQL uses table statistics rather than actual parameter values to choose the plan.
   */
  static String prepareForExplain(String sql) {
    return sql.replaceAll("\\?", "1");
  }

  String runExplain(Connection conn, String sql) throws SQLException {
    String explainSql = prepareForExplain(sql);
    try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("EXPLAIN (FORMAT JSON) " + explainSql)) {
      if (rs.next()) {
        return rs.getString(1);
      }
    }
    return null;
  }

  /**
   * Extracts a string value from JSON output by field name. Simple substring-based extraction that
   * avoids a JSON library dependency.
   */
  static String extractJsonField(String json, String fieldName) {
    String pattern = "\"" + fieldName + "\": \"";
    int start = json.indexOf(pattern);
    if (start < 0) {
      return null;
    }
    start += pattern.length();
    int end = json.indexOf("\"", start);
    if (end < 0) {
      return null;
    }
    return json.substring(start, end);
  }

  /** Extracts a numeric value from JSON output by field name. */
  static long extractJsonLong(String json, String fieldName) {
    String pattern = "\"" + fieldName + "\": ";
    int start = json.indexOf(pattern);
    if (start < 0) {
      return 0L;
    }
    start += pattern.length();
    int end = start;
    while (end < json.length()
        && (Character.isDigit(json.charAt(end)) || json.charAt(end) == '.')) {
      end++;
    }
    if (end == start) {
      return 0L;
    }
    try {
      return (long) Double.parseDouble(json.substring(start, end));
    } catch (NumberFormatException e) {
      return 0L;
    }
  }
}
