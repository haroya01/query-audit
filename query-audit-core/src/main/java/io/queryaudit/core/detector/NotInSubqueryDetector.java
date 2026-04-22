package io.queryaudit.core.detector;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.parser.EnhancedSqlParser;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Detects {@code NOT IN (SELECT ...)} patterns.
 *
 * <p>This is a correctness bug: if the subquery returns any NULL value, the NOT IN condition
 * returns zero rows regardless of the data.
 *
 * @author haroya
 * @since 0.2.0
 */
public class NotInSubqueryDetector implements DetectionRule {

  private static final Pattern NOT_IN_SUBQUERY =
      Pattern.compile("NOT\\s+IN\\s*\\(\\s*SELECT\\b", Pattern.CASE_INSENSITIVE);

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

      if (NOT_IN_SUBQUERY.matcher(sql).find()) {
        List<String> tables = EnhancedSqlParser.extractTableNames(sql);
        String table = tables.isEmpty() ? null : tables.get(0);
        issues.add(
            new Issue(
                IssueType.NOT_IN_SUBQUERY,
                Severity.ERROR,
                normalized,
                table,
                null,
                "NOT IN (SELECT ...) returns no rows if the subquery contains any NULL value. "
                    + "This is a correctness bug, not just a performance issue.",
                "Replace NOT IN (SELECT ...) with NOT EXISTS (SELECT 1 FROM ... WHERE ...). "
                    + "NOT IN returns no rows if the subquery contains any NULL value."));
      }
    }
    return issues;
  }
}
