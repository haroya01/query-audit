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
 * Detects {@code SELECT COUNT(*)} queries that are likely used only for boolean existence checks,
 * where {@code EXISTS} would be more efficient.
 *
 * <p>Flags queries that match all of:
 *
 * <ul>
 *   <li>SELECT COUNT(*) or SELECT COUNT(column)
 *   <li>Has a WHERE clause (checking existence of specific rows)
 *   <li>Does NOT have GROUP BY (that's aggregation, not existence)
 *   <li>Does NOT have HAVING
 * </ul>
 *
 * @author haroya
 * @since 0.2.0
 */
public class CountInsteadOfExistsDetector implements DetectionRule {

  private static final Pattern COUNT_PATTERN =
      Pattern.compile(
          "\\bSELECT\\s+COUNT\\s*\\(\\s*(?:\\*|\\w+(?:\\.\\w+)?)\\s*\\)", Pattern.CASE_INSENSITIVE);

  // COUNT(DISTINCT ...) computes a distinct count value, not an existence check
  private static final Pattern COUNT_DISTINCT_PATTERN =
      Pattern.compile("\\bCOUNT\\s*\\(\\s*DISTINCT\\b", Pattern.CASE_INSENSITIVE);

  // COUNT inside a subquery SELECT clause is used as a column value, not an existence check
  private static final Pattern COUNT_IN_SUBQUERY_PATTERN =
      Pattern.compile("\\(\\s*SELECT\\s+COUNT\\s*\\(", Pattern.CASE_INSENSITIVE);

  // COUNT(...) followed by a comparison operator (>, >=) already expresses boolean intent
  // in SQL itself — e.g. Hibernate translates existsBy* into "count(col) > ?"
  private static final Pattern COUNT_COMPARISON_PATTERN =
      Pattern.compile(
          "\\bCOUNT\\s*\\(\\s*(?:\\*|\\w+(?:\\.\\w+)?)\\s*\\)\\s*(?:>|>=)\\s*(?:\\?|\\d+)",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern WHERE_PATTERN =
      Pattern.compile("\\bWHERE\\b", Pattern.CASE_INSENSITIVE);

  private static final Pattern GROUP_BY_PATTERN =
      Pattern.compile("\\bGROUP\\s+BY\\b", Pattern.CASE_INSENSITIVE);

  private static final Pattern HAVING_PATTERN =
      Pattern.compile("\\bHAVING\\b", Pattern.CASE_INSENSITIVE);

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

      if (!COUNT_PATTERN.matcher(sql).find()) {
        continue;
      }

      // COUNT(DISTINCT ...) computes a distinct count value, not an existence check
      if (COUNT_DISTINCT_PATTERN.matcher(sql).find()) {
        continue;
      }

      // COUNT inside a subquery SELECT is used as a column value
      if (COUNT_IN_SUBQUERY_PATTERN.matcher(sql).find()) {
        continue;
      }

      // COUNT(...) > ? or COUNT(...) >= N — already a boolean expression in SQL
      // (e.g. Hibernate existsBy* generates "count(col) > ?")
      if (COUNT_COMPARISON_PATTERN.matcher(sql).find()) {
        continue;
      }

      if (!WHERE_PATTERN.matcher(sql).find()) {
        continue;
      }

      if (GROUP_BY_PATTERN.matcher(sql).find()) {
        continue;
      }

      if (HAVING_PATTERN.matcher(sql).find()) {
        continue;
      }

      List<String> tables = EnhancedSqlParser.extractTableNames(sql);
      String table = tables.isEmpty() ? null : tables.get(0);

      issues.add(
          new Issue(
              IssueType.COUNT_INSTEAD_OF_EXISTS,
              Severity.INFO,
              normalized,
              table,
              null,
              "COUNT query may be replaceable with EXISTS for better performance",
              "If you only check whether rows exist (count > 0), use EXISTS (SELECT 1 FROM ... WHERE ...) instead. "
                  + "EXISTS short-circuits on the first matching row. Ignore if the actual count value is needed.",
              query.stackTrace()));
    }

    return issues;
  }
}
