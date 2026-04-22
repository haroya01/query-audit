package io.queryaudit.core.detector;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.parser.EnhancedSqlParser;
import io.queryaudit.core.parser.FunctionUsage;
import io.queryaudit.core.parser.SqlParser;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Detects function calls wrapping columns in WHERE and JOIN ON conditions, which prevent index
 * usage. When a function like {@code LOWER()}, {@code DATE()}, or {@code CONCAT()} wraps an indexed
 * column, the database cannot use the index and falls back to a full table scan.
 *
 * <p>Index-safe functions like {@code COALESCE}, {@code IFNULL}, {@code IF}, and {@code NULLIF} are
 * excluded from detection because MySQL 8.0.13+ and other modern databases can utilize indexes with
 * these functions.
 *
 * @author haroya
 * @since 0.2.0
 */
public class WhereFunctionDetector implements DetectionRule {

  /**
   * Index-safe functions that MySQL 8.0.13+ and modern databases can use with indexes. Flagging
   * these produces false positives.
   */
  static final Set<String> INDEX_SAFE_FUNCTIONS = Set.of("COALESCE", "IFNULL", "IF", "NULLIF");

  /**
   * Function-specific rewrite suggestions. Maps function name (uppercase) to a descriptive
   * suggestion string.
   */
  private static final Map<String, String> FUNCTION_SUGGESTIONS =
      Map.ofEntries(
          Map.entry("DATE", "Use range condition: `col >= '2024-01-01' AND col < '2024-01-02'`"),
          Map.entry(
              "LOWER",
              "Use case-insensitive collation or functional index: `ALTER TABLE t ADD INDEX idx ((LOWER(col)))`"),
          Map.entry(
              "UPPER",
              "Use case-insensitive collation or functional index: `ALTER TABLE t ADD INDEX idx ((UPPER(col)))`"),
          Map.entry("YEAR", "Use range condition: `col >= '2024-01-01' AND col < '2025-01-01'`"),
          Map.entry("MONTH", "Use range condition instead of MONTH() extraction"),
          Map.entry("DAY", "Use range condition instead of DAY() extraction"),
          Map.entry(
              "COALESCE",
              "Set column NOT NULL with a default value, or use `col IS NOT NULL AND col > ?`"),
          Map.entry(
              "IFNULL",
              "Set column NOT NULL with a default value, or use `col IS NOT NULL AND col > ?`"),
          Map.entry("TRIM", "Store pre-trimmed values, or create a functional index on TRIM(col)"),
          Map.entry(
              "SUBSTRING",
              "Use LIKE 'prefix%' for prefix matching, or create a generated column with index"),
          Map.entry("CAST", "Store data in the target type directly, or create a functional index"),
          Map.entry("LENGTH", "Create a generated/virtual column for LENGTH(col) and index it"),
          Map.entry(
              "CONCAT", "Create a generated/virtual column for the concatenation and index it"),
          Map.entry("MD5", "Store pre-computed hash in a separate indexed column"),
          Map.entry("SHA1", "Store pre-computed hash in a separate indexed column"),
          Map.entry("SHA2", "Store pre-computed hash in a separate indexed column"),
          Map.entry("UNIX_TIMESTAMP", "Use range condition on the datetime column directly"),
          Map.entry("STR_TO_DATE", "Store data as DATE/DATETIME type instead of string"),
          Map.entry(
              "TO_CHAR",
              "Compare against the original column type instead of converting to string"),
          Map.entry("TO_DATE", "Store data as DATE/DATETIME type instead of string"),
          Map.entry("JSON_EXTRACT", "Create a generated column from JSON path and index it"),
          Map.entry("JSON_VALUE", "Create a generated column from JSON path and index it"),
          Map.entry("EXTRACT", "Use range condition instead of EXTRACT()"),
          Map.entry(
              "ABS", "Create a functional index on ABS(col), or rewrite condition without ABS"),
          Map.entry("ROUND", "Store pre-rounded values, or create a functional index"),
          Map.entry("CEIL", "Rewrite as a range condition, or create a functional index"),
          Map.entry("FLOOR", "Rewrite as a range condition, or create a functional index"));

  private static final String DEFAULT_SUGGESTION =
      "Rewrite without function wrapper, or create a functional index";

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

      // Kept on SqlParser: the regex path handles CAST / TRIM / EXTRACT / unknown-function
      // skips that the AST version does not yet reproduce.
      List<FunctionUsage> functions = SqlParser.detectWhereFunctions(query.sql());
      functions.removeIf(f -> INDEX_SAFE_FUNCTIONS.contains(f.functionName()));
      List<FunctionUsage> joinFunctions = SqlParser.detectJoinFunctions(query.sql());
      joinFunctions.removeIf(f -> INDEX_SAFE_FUNCTIONS.contains(f.functionName()));

      if (functions.isEmpty() && joinFunctions.isEmpty()) {
        continue;
      }

      List<String> tables = EnhancedSqlParser.extractTableNames(query.sql());
      String table = tables.isEmpty() ? null : tables.get(0);

      for (FunctionUsage func : functions) {
        issues.add(
            new Issue(
                IssueType.WHERE_FUNCTION,
                Severity.ERROR,
                normalized,
                table,
                func.columnName(),
                func.functionName() + "() wrapping disables index on column " + func.columnName(),
                getSuggestion(func.functionName()),
                query.stackTrace()));
      }

      for (FunctionUsage func : joinFunctions) {
        issues.add(
            new Issue(
                IssueType.WHERE_FUNCTION,
                Severity.WARNING,
                normalized,
                table,
                func.columnName(),
                func.functionName()
                    + "() in JOIN ON condition disables index on column "
                    + func.columnName(),
                getSuggestion(func.functionName()),
                query.stackTrace()));
      }
    }

    return issues;
  }

  /** Returns a function-specific suggestion for rewriting the query. */
  private static String getSuggestion(String functionName) {
    return FUNCTION_SUGGESTIONS.getOrDefault(functionName, DEFAULT_SUGGESTION);
  }
}
