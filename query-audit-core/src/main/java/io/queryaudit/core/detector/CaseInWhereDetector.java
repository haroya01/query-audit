package io.queryaudit.core.detector;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.parser.EnhancedSqlParser;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Detects CASE expressions in WHERE clauses, which prevent the database from using indexes.
 *
 * <p>Similar to function wrapping (WhereFunctionDetector), a CASE expression in WHERE forces the
 * database to evaluate the expression for every row, making index usage impossible.
 *
 * @author haroya
 * @since 0.2.0
 */
public class CaseInWhereDetector implements DetectionRule {

  /** Pre-compiled pattern for detecting CASE keyword. */
  private static final Pattern CASE_KEYWORD =
      Pattern.compile("\\bCASE\\b", Pattern.CASE_INSENSITIVE);

  /**
   * Matches CASE that appears on the right-hand side of a comparison operator, optionally preceded
   * by an opening parenthesis or IN keyword. When CASE is on the RHS (e.g. {@code WHERE status =
   * CASE WHEN ...} or {@code WHERE status = (CASE WHEN ...)} or {@code WHERE status IN (CASE WHEN
   * ...)}), the index on the left-hand column is still usable, so this should NOT be flagged.
   */
  private static final Pattern CASE_ON_RHS =
      Pattern.compile(
          "(?:(?:=|!=|<>|<=|>=|<|>)\\s*\\(?\\s*CASE\\b|\\bIN\\s*\\(\\s*CASE\\b)",
          Pattern.CASE_INSENSITIVE);

  @Override
  public List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata indexMetadata) {
    List<Issue> issues = new ArrayList<>();
    Set<String> seen = new LinkedHashSet<>();

    for (QueryRecord query : queries) {
      String normalized = query.normalizedSql();
      if (normalized == null || seen.contains(normalized)) {
        continue;
      }
      seen.add(normalized);

      String sql = query.sql();
      if (sql == null) {
        continue;
      }

      String whereBody = EnhancedSqlParser.extractWhereBody(sql);
      if (whereBody == null) {
        continue;
      }

      // Check for CASE in the WHERE clause body
      if (!CASE_KEYWORD.matcher(whereBody).find()) {
        continue;
      }

      // If every CASE occurrence is on the right-hand side of a comparison operator,
      // the index on the left-hand column is still usable — skip.
      String withoutRhs = CASE_ON_RHS.matcher(whereBody).replaceAll("");
      if (!CASE_KEYWORD.matcher(withoutRhs).find()) {
        continue;
      }

      List<String> tables = EnhancedSqlParser.extractTableNames(sql);
      String table = tables.isEmpty() ? null : tables.get(0);

      issues.add(
          new Issue(
              IssueType.CASE_IN_WHERE,
              Severity.WARNING,
              normalized,
              table,
              null,
              "CASE expression in WHERE clause prevents index usage"
                  + (table != null ? " on table '" + table + "'" : ""),
              "Rewrite the CASE expression as multiple OR conditions or separate queries. "
                  + "Alternatively, create a generated/computed column for the CASE logic "
                  + "and index it.",
              query.stackTrace()));
    }

    return issues;
  }
}
