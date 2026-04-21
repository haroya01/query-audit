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

  /**
   * Tokens (in the snake_case or camelCase-ends-with sense) in column names that strongly suggest
   * a string type. Matched at three positions:
   *
   * <ul>
   *   <li>as an entire {@code _}-separated segment (e.g. {@code user_name}, {@code email_address})
   *   <li>as the end of a non-separated identifier (e.g. {@code ucode}, {@code firstname})
   *   <li>as the beginning of a non-separated identifier (e.g. {@code codebook})
   * </ul>
   */
  private static final Set<String> STRING_COLUMN_TOKENS =
      Set.of(
          "name",
          "email",
          "phone",
          "code",
          "token",
          "key",
          "slug",
          "handle",
          "address",
          "title",
          "url",
          "path",
          "type",
          "description",
          "desc",
          "note",
          "comment",
          "text",
          "content",
          "message",
          "label",
          "tag",
          "remark");

  /**
   * Tokens that strongly suggest a numeric/identifier column. If a column ends with one of these,
   * we trust the numeric hint even if another token earlier in the name would otherwise match.
   * Without this guard, {@code description_id} (FK INT column) would be flagged because it
   * contains the {@code description} token.
   */
  private static final Set<String> NUMERIC_COLUMN_TOKENS =
      Set.of("id", "count", "num", "no", "seq", "order", "size", "length");

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
  // Package-private for testability.
  boolean isStringColumn(String columnName) {
    String lower = columnName.toLowerCase();

    // If the last snake_case segment is a numeric hint (e.g. *_id, *_count, *_order), bail out
    // before the string-token check. This prevents false positives on FK-style column names
    // whose leading segment happens to look string-like (e.g. "description_id").
    int lastUnderscore = lower.lastIndexOf('_');
    if (lastUnderscore >= 0) {
      String lastSegment = lower.substring(lastUnderscore + 1);
      if (NUMERIC_COLUMN_TOKENS.contains(lastSegment)) {
        return false;
      }
    }

    // Match as any _-separated segment.
    for (String segment : lower.split("_")) {
      if (STRING_COLUMN_TOKENS.contains(segment)) {
        return true;
      }
    }

    // Match as end or start of a non-separated identifier (ucode, firstname, codebook).
    for (String token : STRING_COLUMN_TOKENS) {
      if (lower.endsWith(token) || lower.startsWith(token)) {
        return true;
      }
    }

    return false;
  }
}
