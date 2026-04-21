package io.queryaudit.core.detector;

import io.queryaudit.core.model.IndexInfo;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.parser.EnhancedSqlParser;
import io.queryaudit.core.parser.SqlParser;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Detects potentially unnecessary DISTINCT usage:
 *
 * <ul>
 *   <li>DISTINCT with GROUP BY: redundant since GROUP BY already produces unique groups
 *   <li>DISTINCT with JOIN: may indicate a missing JOIN condition or could use EXISTS
 *   <li>DISTINCT on primary key column: always unnecessary since PK is unique by definition
 * </ul>
 *
 * @author haroya
 * @since 0.2.0
 */
public class DistinctMisuseDetector implements DetectionRule {

  private static final Pattern SELECT_DISTINCT =
      Pattern.compile("\\bSELECT\\s+DISTINCT\\b", Pattern.CASE_INSENSITIVE);

  /**
   * Detects DISTINCT usage only inside a subquery context (e.g., IN (SELECT DISTINCT ...),
   * EXISTS (SELECT DISTINCT ...)). Such usage is typically intentional.
   */
  private static final Pattern DISTINCT_IN_SUBQUERY =
      Pattern.compile(
          "\\b(?:IN|EXISTS|ANY|ALL)\\s*\\(\\s*SELECT\\s+DISTINCT\\b", Pattern.CASE_INSENSITIVE);

  private static final Pattern GROUP_BY =
      Pattern.compile("\\bGROUP\\s+BY\\b", Pattern.CASE_INSENSITIVE);

  private static final Pattern JOIN_PATTERN =
      Pattern.compile("\\bJOIN\\b", Pattern.CASE_INSENSITIVE);

  /** Matches the start of SELECT DISTINCT to find column list start position. */
  private static final Pattern DISTINCT_START =
      Pattern.compile("\\bSELECT\\s+DISTINCT\\s+", Pattern.CASE_INSENSITIVE);

  /** Matches the FROM keyword boundary to find column list end position. */
  private static final Pattern FROM_KEYWORD =
      Pattern.compile("\\bFROM\\b", Pattern.CASE_INSENSITIVE);

  private static final Pattern COLUMN_NAME =
      Pattern.compile("(?:(\\w+)\\.)?(\\w+)", Pattern.CASE_INSENSITIVE);

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
      if (sql == null || !SELECT_DISTINCT.matcher(sql).find()) {
        continue;
      }

      // If DISTINCT only appears inside a subquery, skip — it is often intentional
      // (e.g., WHERE id IN (SELECT DISTINCT category_id FROM ...))
      String outerSql = EnhancedSqlParser.removeSubqueries(sql);
      if (!SELECT_DISTINCT.matcher(outerSql).find()) {
        continue;
      }

      boolean hasGroupBy = GROUP_BY.matcher(sql).find();
      boolean hasJoin = JOIN_PATTERN.matcher(sql).find();

      // (b) DISTINCT + GROUP BY is almost always redundant
      if (hasGroupBy) {
        List<String> tables = EnhancedSqlParser.extractTableNames(sql);
        String table = tables.isEmpty() ? null : tables.get(0);

        issues.add(
            new Issue(
                IssueType.DISTINCT_MISUSE,
                Severity.WARNING,
                normalized,
                table,
                null,
                "DISTINCT with GROUP BY is redundant — GROUP BY already produces unique groups",
                "Remove DISTINCT when GROUP BY is present. "
                    + "GROUP BY guarantees uniqueness of the grouped columns.",
                query.stackTrace()));
        continue; // Don't report multiple issues for the same query
      }

      // (c) DISTINCT on primary key column
      if (indexMetadata != null && !indexMetadata.isEmpty()) {
        List<String> distinctCols = extractDistinctColumnNames(sql);
        List<String> tables = EnhancedSqlParser.extractTableNames(sql);
        String table = tables.isEmpty() ? null : tables.get(0);

        if (table != null && indexMetadata.hasTable(table)) {
          Set<String> pkColumns = getPrimaryKeyColumns(indexMetadata, table);
          boolean selectsPK =
              distinctCols.stream().anyMatch(col -> pkColumns.contains(col.toLowerCase()));

          if (selectsPK) {
            issues.add(
                new Issue(
                    IssueType.DISTINCT_MISUSE,
                    Severity.WARNING,
                    normalized,
                    table,
                    null,
                    "DISTINCT on primary key column is unnecessary",
                    "Remove DISTINCT — the primary key column is already unique by definition.",
                    query.stackTrace()));
            continue;
          }
        }
      }

      // (a) DISTINCT + JOIN may indicate a missing JOIN condition
      if (hasJoin) {
        List<String> tables = EnhancedSqlParser.extractTableNames(sql);
        String table = tables.isEmpty() ? null : tables.get(0);

        issues.add(
            new Issue(
                IssueType.DISTINCT_MISUSE,
                Severity.WARNING,
                normalized,
                table,
                null,
                "DISTINCT with JOIN may indicate a missing JOIN condition or could use EXISTS",
                "Review JOIN conditions for correctness. "
                    + "If checking existence, use EXISTS (SELECT 1 FROM ...) instead.",
                query.stackTrace()));
      }
    }

    return issues;
  }

  /** Extract column names from the SELECT DISTINCT clause. */
  private List<String> extractDistinctColumnNames(String sql) {
    List<String> columns = new ArrayList<>();
    // Use manual boundary scanning to avoid (.+?) backtracking
    Matcher startMatcher = DISTINCT_START.matcher(sql);
    if (!startMatcher.find()) {
      return columns;
    }
    int colStart = startMatcher.end();
    Matcher fromMatcher = FROM_KEYWORD.matcher(sql);
    if (!fromMatcher.find(colStart)) {
      return columns;
    }
    if (colStart >= fromMatcher.start()) {
      return columns;
    }

    String colList = sql.substring(colStart, fromMatcher.start()).trim();
    String[] parts = colList.split(",");
    for (String part : parts) {
      String trimmed = part.trim();
      if (trimmed.contains("(")) {
        continue; // Skip function expressions
      }
      Matcher colMatcher = COLUMN_NAME.matcher(trimmed);
      if (colMatcher.matches()) {
        columns.add(colMatcher.group(2));
      }
    }
    return columns;
  }

  private Set<String> getPrimaryKeyColumns(IndexMetadata metadata, String table) {
    return metadata.getIndexesForTable(table).stream()
        .filter(idx -> "PRIMARY".equalsIgnoreCase(idx.indexName()))
        .map(IndexInfo::columnName)
        .filter(name -> name != null)
        .map(String::toLowerCase)
        .collect(Collectors.toSet());
  }
}
