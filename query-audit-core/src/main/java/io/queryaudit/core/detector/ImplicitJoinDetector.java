package io.queryaudit.core.detector;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.parser.SqlParser;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Detects old-style implicit comma-separated joins in the FROM clause.
 *
 * <p>Pre-ANSI-92 syntax like {@code FROM a, b WHERE a.id = b.id} makes accidental Cartesian joins
 * likely and is a code quality issue.
 *
 * <p>Note: {@link CartesianJoinDetector} catches cases where there is no WHERE linking the tables.
 * This detector catches cases where there IS a WHERE clause but the syntax is old-style
 * comma-separated.
 *
 * @author haroya
 * @since 0.2.0
 */
public class ImplicitJoinDetector implements DetectionRule {

  // Matches: FROM table1, table2 — handles aliases, backtick-quoted names, schema-qualified names
  // Examples: FROM users u, orders o | FROM `users`, `orders` | FROM mydb.users, mydb.orders
  private static final Pattern IMPLICIT_JOIN =
      Pattern.compile(
          "\\bFROM\\s+(?:`[^`]+`|\\w+(?:\\.\\w+)?)(?:\\s+(?:AS\\s+)?\\w+)?\\s*,\\s*(?:`[^`]+`|\\(\\?\\)|\\w+(?:\\.\\w+)?)",
          Pattern.CASE_INSENSITIVE);

  /**
   * Detects function calls in the FROM clause (e.g., generate_series(1, 10)), which contain commas
   * inside parentheses that should not be treated as implicit joins.
   */
  private static final Pattern FROM_FUNCTION_CALL =
      Pattern.compile("\\bFROM\\s+\\w+\\s*\\(", Pattern.CASE_INSENSITIVE);

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

      String cleaned = SqlParser.removeSubqueries(sql);

      // Skip queries with function calls in FROM clause (e.g., generate_series(1, 10))
      // where the comma is inside function arguments, not between tables
      if (FROM_FUNCTION_CALL.matcher(cleaned).find()) {
        continue;
      }

      if (IMPLICIT_JOIN.matcher(cleaned).find()) {
        List<String> tables = SqlParser.extractTableNames(sql);
        String table = tables.isEmpty() ? null : tables.get(0);
        issues.add(
            new Issue(
                IssueType.IMPLICIT_JOIN,
                IssueType.IMPLICIT_JOIN.getDefaultSeverity(),
                normalized,
                table,
                null,
                "Implicit comma-separated join syntax (pre-ANSI-92) makes accidental "
                    + "Cartesian joins likely and reduces readability.",
                "Use explicit JOIN syntax instead of implicit comma-separated joins "
                    + "for better readability and safety."));
      }
    }
    return issues;
  }
}
