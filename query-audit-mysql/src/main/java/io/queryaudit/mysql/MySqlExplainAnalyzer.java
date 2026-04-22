package io.queryaudit.mysql;

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
 * MySQL implementation of {@link ExplainAnalyzer}.
 *
 * <p>Runs {@code EXPLAIN} on captured SELECT queries and detects:
 *
 * <ul>
 *   <li>{@code type=ALL} — full table scan ({@link IssueType#FULL_TABLE_SCAN})
 *   <li>{@code Extra} contains "Using filesort" ({@link IssueType#FILESORT})
 *   <li>{@code Extra} contains "Using temporary" ({@link IssueType#TEMPORARY_TABLE})
 * </ul>
 *
 * @author haroya
 * @since 0.2.0
 */
public class MySqlExplainAnalyzer implements ExplainAnalyzer {

  @Override
  public String supportedDatabase() {
    return "mysql";
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
        ExplainRow row = runExplain(connection, sql);
        if (row == null) {
          continue;
        }

        if ("ALL".equalsIgnoreCase(row.type)) {
          issues.add(fullTableScanIssue(query, row));
        }
        if (row.extra != null && row.extra.contains("Using filesort")) {
          issues.add(filesortIssue(query, row));
        }
        if (row.extra != null && row.extra.contains("Using temporary")) {
          issues.add(temporaryTableIssue(query, row));
        }
      } catch (Exception e) {
        // Skip queries that can't be EXPLAINed (DDL, syntax errors, etc.)
      }
    }
    return issues;
  }

  /**
   * Replaces {@code ?} parameter placeholders with safe dummy values so that the SQL can be
   * executed as a plain {@code EXPLAIN} statement. datasource-proxy captures SQL with {@code ?}
   * placeholders which are invalid in a non-PreparedStatement context.
   *
   * <p>Using {@code 1} is safe because EXPLAIN only plans the query without executing it, and MySQL
   * uses index statistics rather than actual parameter values to choose the plan.
   */
  static String prepareForExplain(String sql) {
    return sql.replaceAll("\\?", "1");
  }

  ExplainRow runExplain(Connection conn, String sql) throws SQLException {
    String explainSql = prepareForExplain(sql);
    try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("EXPLAIN " + explainSql)) {
      if (rs.next()) {
        return new ExplainRow(
            rs.getString("table"),
            rs.getString("type"),
            rs.getString("key"),
            rs.getLong("rows"),
            rs.getString("Extra"));
      }
    }
    return null;
  }

  private Issue fullTableScanIssue(QueryRecord query, ExplainRow row) {
    String detail =
        String.format(
            "EXPLAIN type=ALL on table '%s' — full table scan, ~%d rows examined",
            row.table, row.rows);
    return new Issue(
        IssueType.FULL_TABLE_SCAN,
        Severity.INFO,
        query.sql(),
        row.table,
        null,
        detail,
        "Add an index on the filtered/joined columns or rewrite the query to use an index.");
  }

  private Issue filesortIssue(QueryRecord query, ExplainRow row) {
    String detail =
        String.format(
            "EXPLAIN shows 'Using filesort' on table '%s' (key=%s, ~%d rows)",
            row.table, row.key != null ? row.key : "none", row.rows);
    return new Issue(
        IssueType.FILESORT,
        Severity.INFO,
        query.sql(),
        row.table,
        null,
        detail,
        "Add an index that covers the ORDER BY columns to avoid filesort.");
  }

  private Issue temporaryTableIssue(QueryRecord query, ExplainRow row) {
    String detail =
        String.format(
            "EXPLAIN shows 'Using temporary' on table '%s' (key=%s, ~%d rows)",
            row.table, row.key != null ? row.key : "none", row.rows);
    return new Issue(
        IssueType.TEMPORARY_TABLE,
        Severity.INFO,
        query.sql(),
        row.table,
        null,
        detail,
        "Add an index that covers the GROUP BY/DISTINCT columns to avoid temporary table.");
  }

  /** Parsed row from MySQL EXPLAIN output. */
  static class ExplainRow {
    final String table;
    final String type;
    final String key;
    final long rows;
    final String extra;

    ExplainRow(String table, String type, String key, long rows, String extra) {
      this.table = table;
      this.type = type;
      this.key = key;
      this.rows = rows;
      this.extra = extra;
    }
  }
}
