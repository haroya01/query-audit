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
 * Detects UPDATE/DELETE statements with subqueries in the WHERE clause.
 *
 * <p>MySQL docs: "A limitation on UPDATE and DELETE statements that use a subquery to modify a
 * single table is that the optimizer does not use semijoin or materialization subquery
 * optimizations."
 *
 * <p>Pattern: {@code UPDATE ... WHERE col IN (SELECT ...)} or {@code DELETE ... WHERE col IN
 * (SELECT ...)}
 *
 * @author haroya
 * @since 0.2.0
 */
public class SubqueryInDmlDetector implements DetectionRule {

  private static final Pattern IN_SUBQUERY =
      Pattern.compile("\\bIN\\s*\\(\\s*SELECT\\b", Pattern.CASE_INSENSITIVE);

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

      if (!SqlParser.isUpdateQuery(sql) && !SqlParser.isDeleteQuery(sql)) {
        continue;
      }

      String whereBody = SqlParser.extractWhereBody(sql);
      if (whereBody == null) {
        continue;
      }

      if (IN_SUBQUERY.matcher(whereBody).find()) {
        List<String> tables = SqlParser.extractTableNames(sql);
        String table = tables.isEmpty() ? null : tables.get(0);

        issues.add(
            new Issue(
                IssueType.SUBQUERY_IN_DML,
                Severity.WARNING,
                normalized,
                table,
                null,
                "Subquery in "
                    + (SqlParser.isUpdateQuery(sql) ? "UPDATE" : "DELETE")
                    + " WHERE clause cannot use semijoin optimization",
                "Subquery in UPDATE/DELETE WHERE clause cannot use semijoin optimization. "
                    + "Rewrite as multi-table UPDATE/DELETE with JOIN for better performance.",
                query.stackTrace()));
      }
    }
    return issues;
  }
}
