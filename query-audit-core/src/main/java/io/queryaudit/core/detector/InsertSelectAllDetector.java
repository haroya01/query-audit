package io.queryaudit.core.detector;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.parser.EnhancedSqlParser;
import io.queryaudit.core.parser.SqlParser;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Detects INSERT ... SELECT * patterns.
 *
 * <p>This pattern is fragile because adding a column to the source table silently changes the data
 * being inserted. It also transfers unnecessary columns, increasing I/O and memory usage.
 *
 * @author haroya
 * @since 0.2.0
 */
public class InsertSelectAllDetector implements DetectionRule {

  private static final Pattern INSERT_SELECT_ALL =
      Pattern.compile(
          "\\bINSERT\\b.*\\bSELECT\\s+(?:ALL\\s+|DISTINCT\\s+)?(?:\\w+\\.)?\\*",
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

      // Remove subqueries to avoid matching SELECT * inside a subquery
      String outerSql = EnhancedSqlParser.removeSubqueries(sql);
      if (INSERT_SELECT_ALL.matcher(outerSql).find()) {
        String table = SqlParser.extractInsertTable(sql);
        issues.add(
            new Issue(
                IssueType.INSERT_SELECT_ALL,
                Severity.WARNING,
                normalized,
                table,
                null,
                "INSERT ... SELECT * transfers all columns from the source table. "
                    + "Schema changes will silently affect the inserted data.",
                "Explicitly list the columns in both INSERT and SELECT clauses."));
      }
    }
    return issues;
  }
}
