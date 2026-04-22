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
 * Detects {@code REGEXP} or {@code RLIKE} usage in queries.
 *
 * <p>Source: BigQuery BQ-5, MySQL performance docs
 *
 * <p>REGEXP always causes a full table scan — it can never use indexes. LIKE with a trailing
 * wildcard can use indexes.
 *
 * @author haroya
 * @since 0.2.0
 */
public class RegexpInsteadOfLikeDetector implements DetectionRule {

  private static final Pattern REGEXP_PATTERN =
      Pattern.compile("\\b(REGEXP|RLIKE)\\b", Pattern.CASE_INSENSITIVE);

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

      // Only flag REGEXP/RLIKE when it appears in WHERE or JOIN ON conditions,
      // not in SELECT, ORDER BY, or HAVING clauses where it has no index impact.
      boolean found = false;

      String whereBody = EnhancedSqlParser.extractWhereBody(sql);
      if (whereBody != null && REGEXP_PATTERN.matcher(whereBody).find()) {
        found = true;
      }

      if (!found) {
        for (String joinOn : EnhancedSqlParser.extractJoinOnBodies(sql)) {
          if (REGEXP_PATTERN.matcher(joinOn).find()) {
            found = true;
            break;
          }
        }
      }

      if (found) {
        List<String> tables = EnhancedSqlParser.extractTableNames(sql);
        String table = tables.isEmpty() ? null : tables.get(0);
        issues.add(
            new Issue(
                IssueType.REGEXP_INSTEAD_OF_LIKE,
                Severity.WARNING,
                normalized,
                table,
                null,
                "REGEXP/RLIKE usage detected. This operator always causes a full table scan "
                    + "because it cannot use indexes.",
                "REGEXP/RLIKE prevents index usage and causes full table scan. "
                    + "Use LIKE if the pattern is a simple wildcard match.",
                query.stackTrace()));
      }
    }
    return issues;
  }
}
