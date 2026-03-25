package io.queryaudit.core.detector;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.parser.SqlParser;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Detects HAVING conditions that reference only non-aggregate columns.
 *
 * <p>HAVING filters rows <em>after</em> GROUP BY aggregation, while WHERE filters <em>before</em>.
 * When a HAVING condition does not use an aggregate function (COUNT, SUM, AVG, MAX, MIN), it should
 * be moved to WHERE for better performance because it reduces the data processed by the GROUP BY
 * operation.
 *
 * @author haroya
 * @since 0.2.0
 */
public class HavingMisuseDetector implements DetectionRule {

  /** Aggregate function names (case-insensitive). */
  private static final Pattern AGGREGATE_FUNCTION =
      Pattern.compile("\\b(?:COUNT|SUM|AVG|MAX|MIN)\\s*\\(", Pattern.CASE_INSENSITIVE);

  /** Splits HAVING body into individual conditions on AND/OR boundaries. */
  private static final Pattern CONDITION_SPLITTER =
      Pattern.compile("\\s+(?:AND|OR)\\s+", Pattern.CASE_INSENSITIVE);

  /**
   * Extracts a column name from a simple condition like "col = value" or "col > 10". Captures
   * optional table/alias prefix and column name on the left side of a comparison.
   */
  private static final Pattern CONDITION_COLUMN =
      Pattern.compile(
          "^\\s*(?:(\\w+)\\.)?(\\w+)\\s*(?:=|!=|<>|<=|>=|<|>|\\bLIKE\\b|\\bIN\\b|\\bIS\\b|\\bBETWEEN\\b|\\bNOT\\b)",
          Pattern.CASE_INSENSITIVE);

  /** Extracts the SELECT column list (between SELECT [DISTINCT] and FROM). */
  private static final Pattern SELECT_COLUMNS =
      Pattern.compile(
          "\\bSELECT\\s+(?:DISTINCT\\s+)?(.+?)\\bFROM\\b",
          Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  /**
   * Matches a SELECT item that is an aggregate function with an alias. e.g. "COUNT(*) as cnt",
   * "SUM(total) total_sum", "AVG(price) AS avg_price"
   */
  private static final Pattern AGGREGATE_ALIAS =
      Pattern.compile(
          "\\b(?:COUNT|SUM|AVG|MAX|MIN)\\s*\\([^)]*\\)\\s+(?:AS\\s+)?(\\w+)",
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
      if (sql == null || !SqlParser.isSelectQuery(sql)) {
        continue;
      }

      String havingBody = SqlParser.extractHavingClause(sql);
      if (havingBody == null || havingBody.isBlank()) {
        continue;
      }

      // Collect SELECT aliases that refer to aggregate functions
      Set<String> aggregateAliases = extractAggregateAliases(sql);

      // Split HAVING into individual conditions
      String[] conditions = CONDITION_SPLITTER.split(havingBody);
      for (String condition : conditions) {
        String trimmed = condition.trim();
        if (trimmed.isEmpty()) {
          continue;
        }

        // If the condition contains an aggregate function, it's valid HAVING usage
        if (AGGREGATE_FUNCTION.matcher(trimmed).find()) {
          continue;
        }

        // Extract the column name from the non-aggregate condition
        String columnName = extractColumnName(trimmed);

        // If the column references a SELECT alias for an aggregate function, skip it
        if (columnName != null && aggregateAliases.contains(columnName.toLowerCase())) {
          continue;
        }

        List<String> tables = SqlParser.extractTableNames(sql);
        String table = tables.isEmpty() ? null : tables.get(0);

        // Build a cleaned version of the condition for the suggestion
        String conditionText = trimmed.length() > 60 ? trimmed.substring(0, 60) + "..." : trimmed;

        String detail =
            columnName != null
                ? "HAVING condition on non-aggregate column '"
                    + columnName
                    + "' should be a WHERE condition"
                : "HAVING condition '"
                    + conditionText
                    + "' does not use an aggregate function and should be a WHERE condition";

        String moveText = trimmed.length() > 80 ? trimmed.substring(0, 80) + "..." : trimmed;
        String suggestion =
            "Move '"
                + moveText
                + "' from HAVING to WHERE. "
                + "HAVING filters after aggregation; WHERE filters before, reducing the data processed.";

        issues.add(
            new Issue(
                IssueType.HAVING_MISUSE,
                Severity.WARNING,
                normalized,
                table,
                columnName,
                detail,
                suggestion,
                query.stackTrace()));
      }
    }

    return issues;
  }

  /** Extract the column name from the left side of a simple condition. */
  private String extractColumnName(String condition) {
    Matcher m = CONDITION_COLUMN.matcher(condition);
    if (m.find()) {
      return m.group(2);
    }
    return null;
  }

  /**
   * Extracts aliases from the SELECT clause that refer to aggregate functions. For example, {@code
   * SELECT COUNT(*) as cnt} would return a set containing "cnt".
   *
   * @return a set of lowercase alias names that are aggregate function aliases
   */
  private Set<String> extractAggregateAliases(String sql) {
    Set<String> aliases = new HashSet<>();
    Matcher selectMatcher = SELECT_COLUMNS.matcher(sql);
    if (!selectMatcher.find()) {
      return aliases;
    }
    String selectList = selectMatcher.group(1);
    Matcher aliasMatcher = AGGREGATE_ALIAS.matcher(selectList);
    while (aliasMatcher.find()) {
      aliases.add(aliasMatcher.group(1).toLowerCase());
    }
    return aliases;
  }
}
