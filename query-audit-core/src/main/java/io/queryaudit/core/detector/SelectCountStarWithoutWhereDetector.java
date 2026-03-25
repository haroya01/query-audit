package io.queryaudit.core.detector;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.parser.SqlParser;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Detects {@code SELECT COUNT(*)} or {@code SELECT COUNT(1)} without a WHERE clause and without
 * GROUP BY. On InnoDB, this forces a full clustered index scan which is very slow on large tables.
 *
 * @author haroya
 * @since 0.2.0
 */
public class SelectCountStarWithoutWhereDetector implements DetectionRule {

  private static final Pattern COUNT_STAR =
      Pattern.compile("\\bCOUNT\\s*\\(\\s*(?:\\*|1)\\s*\\)", Pattern.CASE_INSENSITIVE);

  private static final Pattern GROUP_BY =
      Pattern.compile("\\bGROUP\\s+BY\\b", Pattern.CASE_INSENSITIVE);

  @Override
  public List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata indexMetadata) {
    List<Issue> issues = new ArrayList<>();
    Set<String> seen = new LinkedHashSet<>();

    for (QueryRecord query : queries) {
      String sql = query.sql();
      if (sql == null) {
        continue;
      }

      String normalized = query.normalizedSql();
      if (normalized != null && !seen.add(normalized)) {
        continue;
      }

      if (!SqlParser.isSelectQuery(sql)) {
        continue;
      }

      // Analyze only the outer query — COUNT(*) in subqueries is fine
      String outerSql = SqlParser.removeSubqueries(sql);

      if (!COUNT_STAR.matcher(outerSql).find()) {
        continue;
      }

      // Don't flag if the outer query has a WHERE clause
      if (SqlParser.hasWhereClause(outerSql)) {
        continue;
      }

      // Don't flag if there is a GROUP BY (legitimate aggregation)
      if (GROUP_BY.matcher(outerSql).find()) {
        continue;
      }

      List<String> tables = SqlParser.extractTableNames(sql);
      String table = tables.isEmpty() ? null : tables.get(0);

      issues.add(
          new Issue(
              IssueType.COUNT_STAR_WITHOUT_WHERE,
              Severity.INFO,
              normalized,
              table,
              null,
              "COUNT(*) without WHERE scans the entire table",
              "COUNT(*) without WHERE scans the entire table. "
                  + "Consider using approximate counts (information_schema.tables) "
                  + "or caching the count.",
              query.stackTrace()));
    }

    return issues;
  }
}
