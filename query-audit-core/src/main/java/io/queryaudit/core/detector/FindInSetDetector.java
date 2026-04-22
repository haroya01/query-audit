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
 * Detects {@code FIND_IN_SET()} usage in queries — indicates comma-separated values stored in a
 * column.
 *
 * <p>Source: SQLCheck 1001 (Multi-valued attribute), MySQL docs
 *
 * <p>FIND_IN_SET cannot use indexes, violates 1NF, and indicates a schema design problem.
 *
 * @author haroya
 * @since 0.2.0
 */
public class FindInSetDetector implements DetectionRule {

  private static final Pattern FIND_IN_SET_PATTERN =
      Pattern.compile("\\bFIND_IN_SET\\s*\\(", Pattern.CASE_INSENSITIVE);

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

      // Only flag FIND_IN_SET when it appears in WHERE or JOIN ON conditions,
      // not in SELECT, ORDER BY, or HAVING clauses where it has no index impact.
      boolean found = false;

      String whereBody = EnhancedSqlParser.extractWhereBody(sql);
      if (whereBody != null && FIND_IN_SET_PATTERN.matcher(whereBody).find()) {
        found = true;
      }

      if (!found) {
        for (String joinOn : EnhancedSqlParser.extractJoinOnBodies(sql)) {
          if (FIND_IN_SET_PATTERN.matcher(joinOn).find()) {
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
                IssueType.FIND_IN_SET_USAGE,
                Severity.WARNING,
                normalized,
                table,
                null,
                "FIND_IN_SET() usage detected. This indicates comma-separated values stored "
                    + "in a column, which violates first normal form.",
                "FIND_IN_SET() indicates comma-separated values in a column, which violates "
                    + "first normal form and prevents index usage. Consider normalizing to a junction table.",
                query.stackTrace()));
      }
    }
    return issues;
  }
}
