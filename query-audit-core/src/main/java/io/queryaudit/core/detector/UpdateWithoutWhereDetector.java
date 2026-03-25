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
import java.util.regex.Pattern;

/**
 * Detects UPDATE or DELETE statements without a WHERE clause. These statements affect every row in
 * the table and are almost always unintentional, potentially causing catastrophic data loss in
 * production.
 *
 * @author haroya
 * @since 0.2.0
 */
public class UpdateWithoutWhereDetector implements DetectionRule {

  private static final Pattern JOIN_PATTERN =
      Pattern.compile("\\bJOIN\\b", Pattern.CASE_INSENSITIVE);

  private static final Pattern USING_PATTERN =
      Pattern.compile("\\bUSING\\b", Pattern.CASE_INSENSITIVE);

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
          && !hasJoinClause(sql)) {
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
          && !hasJoinClause(sql)
          && !hasUsingClause(sql)) {
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

  /**
   * Returns true if the SQL contains a JOIN clause, which provides row filtering via the ON
   * condition even without an explicit WHERE clause.
   */
  private static boolean hasJoinClause(String sql) {
    return JOIN_PATTERN.matcher(sql).find();
  }

  /**
   * Returns true if the SQL contains a USING clause (PostgreSQL DELETE ... USING pattern), which
   * provides row filtering similar to JOIN.
   */
  private static boolean hasUsingClause(String sql) {
    return USING_PATTERN.matcher(sql).find();
  }
}
