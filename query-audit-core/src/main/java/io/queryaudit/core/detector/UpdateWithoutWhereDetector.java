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

/**
 * Detects UPDATE or DELETE statements without a WHERE clause. These statements affect every row in
 * the table and are almost always unintentional, potentially causing catastrophic data loss in
 * production.
 *
 * <p>JOIN and USING clauses count as row restrictions, but only when the parser finds a real
 * top-level clause. The same literal-aware scan that recognises WHERE also decides JOIN and USING,
 * so keyword text inside a string literal or a comment leaves the statement reportable instead of
 * being mistaken for filtered DML.
 *
 * @author haroya
 * @since 0.2.0
 */
public class UpdateWithoutWhereDetector implements DetectionRule {

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

      if (SqlParser.isUpdateQuery(sql)
          && !SqlParser.hasOuterWhereClause(sql)
          && !SqlParser.hasOuterJoinClause(sql)) {
        String table = SqlParser.extractUpdateTable(sql);
        issues.add(
            new Issue(
                IssueType.UPDATE_WITHOUT_WHERE,
                Severity.ERROR,
                normalized,
                table,
                null,
                "UPDATE without WHERE clause will modify all rows in "
                    + (table != null ? "table '" + table + "'" : "the table"),
                "Add a WHERE clause to limit the affected rows, "
                    + "or use TRUNCATE if you intend to clear the entire table."));
      }

      if (SqlParser.isDeleteQuery(sql)
          && !SqlParser.hasOuterWhereClause(sql)
          && !SqlParser.hasOuterJoinClause(sql)
          && !SqlParser.hasOuterUsingClause(sql)) {
        String table = SqlParser.extractDeleteTable(sql);
        issues.add(
            new Issue(
                IssueType.UPDATE_WITHOUT_WHERE,
                Severity.ERROR,
                normalized,
                table,
                null,
                "DELETE without WHERE clause will remove all rows from "
                    + (table != null ? "table '" + table + "'" : "the table"),
                "Add a WHERE clause to limit the affected rows, "
                    + "or use TRUNCATE if you intend to clear the entire table."));
      }
    }
    return issues;
  }
}
