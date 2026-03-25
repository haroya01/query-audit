package io.queryaudit.core.detector;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.parser.SqlParser;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Detects INSERT INTO ... SELECT ... FROM ... patterns.
 *
 * <p>MySQL docs: "INSERT INTO T SELECT ... FROM S sets shared next-key locks on S." This blocks
 * concurrent writes to the source table during the operation.
 *
 * <p>Note: Does NOT flag INSERT INTO ... VALUES (no SELECT). This is distinct from {@link
 * InsertSelectAllDetector} which only catches SELECT *.
 *
 * @author haroya
 * @since 0.2.0
 */
public class InsertSelectLocksSourceDetector implements DetectionRule {

  private static final Pattern INSERT_SELECT =
      Pattern.compile("\\bINSERT\\b.*\\bSELECT\\b", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  private static final Pattern SELECT_FROM =
      Pattern.compile(
          "\\bSELECT\\b.+?\\bFROM\\s+(?:`(\\w+)`|(\\w+))",
          Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  @Override
  public List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata indexMetadata) {
    List<Issue> issues = new ArrayList<>();
    Set<String> seen = new HashSet<>();

    for (QueryRecord query : queries) {
      String sql = query.sql();
      String normalized = query.normalizedSql();
      if (normalized == null || !seen.add(normalized)) {
        continue;
      }

      if (!SqlParser.isInsertQuery(sql)) {
        continue;
      }

      if (!INSERT_SELECT.matcher(sql).find()) {
        continue;
      }

      // Extract source table from SELECT ... FROM <table>
      String sourceTable = extractSourceTable(sql);

      issues.add(
          new Issue(
              IssueType.INSERT_SELECT_LOCKS_SOURCE,
              Severity.INFO,
              normalized,
              sourceTable,
              null,
              "INSERT ... SELECT sets shared next-key locks on the source table"
                  + (sourceTable != null ? " '" + sourceTable + "'" : "")
                  + ", blocking concurrent writes",
              "INSERT ... SELECT sets shared next-key locks on the source table rows, "
                  + "blocking concurrent writes. Consider batching with application-level "
                  + "reads or using a temporary table.",
              query.stackTrace()));
    }
    return issues;
  }

  /** Extracts the source table from the SELECT portion of an INSERT ... SELECT statement. */
  private String extractSourceTable(String sql) {
    Matcher m = SELECT_FROM.matcher(sql);
    if (m.find()) {
      String table = m.group(1) != null ? m.group(1) : m.group(2);
      if (table != null && !isKeyword(table)) {
        return table;
      }
    }
    return null;
  }

  private static final Set<String> SQL_KEYWORDS =
      Set.of(
          "select",
          "from",
          "where",
          "and",
          "or",
          "not",
          "in",
          "on",
          "join",
          "left",
          "right",
          "inner",
          "outer",
          "cross",
          "set",
          "values",
          "into",
          "insert",
          "update",
          "delete",
          "order",
          "group",
          "having",
          "limit",
          "offset",
          "union",
          "all",
          "distinct",
          "as",
          "case",
          "when",
          "then",
          "else",
          "end");

  private static boolean isKeyword(String word) {
    return SQL_KEYWORDS.contains(word.toLowerCase());
  }
}
