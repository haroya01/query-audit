package io.queryaudit.core.detector;

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

/**
 * Detects WHERE conditions where a string-like column is compared to a bare numeric literal, which
 * causes implicit type conversion in MySQL and disables index usage on the column.
 *
 * <p>Only flags columns whose names contain obvious string indicators (e.g., {@code _name}, {@code
 * _email}, {@code _code}, etc.) to minimize false positives.
 *
 * @author haroya
 * @since 0.2.0
 */
public class ImplicitTypeConversionDetector implements DetectionRule {

  /** Suffixes/substrings in column names that strongly suggest a string type. */
  private static final Set<String> STRING_COLUMN_INDICATORS =
      Set.of(
          "_name",
          "_email",
          "_phone",
          "_code",
          "_token",
          "_key",
          "_slug",
          "_handle",
          "_address",
          "_title",
          "_url",
          "_path",
          "_type");

  /**
   * Pattern to match: column_name = bare_number in a WHERE context. Captures group(1) = column
   * name, group(2) = numeric literal. Works on raw SQL (before normalization replaces literals).
   */
  private static final Pattern STRING_COL_EQUALS_NUMBER =
      Pattern.compile("(?i)\\b(\\w+)\\s*=\\s*(\\d+)\\b");

  @Override
  public List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata indexMetadata) {
    List<Issue> issues = new ArrayList<>();
    Set<String> seen = new LinkedHashSet<>();

    for (QueryRecord query : queries) {
      String sql = query.sql();
      if (sql == null) {
        continue;
      }

      // Use EnhancedSqlParser.extractWhereBody to properly extract only the WHERE clause,
      // excluding ORDER BY, GROUP BY, HAVING, LIMIT, and other trailing clauses
      // that could cause false positives.
      String whereClause = EnhancedSqlParser.extractWhereBody(sql);
      if (whereClause == null) {
        continue;
      }

      Matcher matcher = STRING_COL_EQUALS_NUMBER.matcher(whereClause);
      while (matcher.find()) {
        String columnName = matcher.group(1).toLowerCase();
        String numericLiteral = matcher.group(2);

        if (isStringColumn(columnName)) {
          String key = columnName + "=" + numericLiteral;
          if (!seen.add(key)) {
            continue;
          }

          issues.add(
              new Issue(
                  IssueType.IMPLICIT_TYPE_CONVERSION,
                  Severity.WARNING,
                  query.normalizedSql(),
                  null,
                  columnName,
                  "Possible implicit type conversion: string column '"
                      + columnName
                      + "' compared to numeric literal "
                      + numericLiteral,
                  "Use string comparison: WHERE "
                      + columnName
                      + " = '"
                      + numericLiteral
                      + "' instead of WHERE "
                      + columnName
                      + " = "
                      + numericLiteral
                      + ". Implicit conversion disables index usage on the column.",
                  query.stackTrace()));
        }
      }
    }

    return issues;
  }

  /** Check if the column name suggests a string type based on known indicators. */
  private boolean isStringColumn(String columnName) {
    String lower = columnName.toLowerCase();
    for (String indicator : STRING_COLUMN_INDICATORS) {
      if (lower.contains(indicator)) {
        return true;
      }
    }
    return false;
  }
}
