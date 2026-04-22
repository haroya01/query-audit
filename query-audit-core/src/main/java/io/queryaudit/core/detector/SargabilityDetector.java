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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Detects non-sargable expressions where arithmetic on a column prevents index usage.
 *
 * <p>Based on "Use The Index Luke" / MySQL documentation on sargable expressions.
 *
 * <p>Patterns detected:
 *
 * <ul>
 *   <li>{@code WHERE col + 1 = ?} &rarr; should be {@code WHERE col = ? - 1}
 *   <li>{@code WHERE col * 2 > ?} &rarr; should be {@code WHERE col > ? / 2}
 *   <li>{@code WHERE col / 10 = ?} &rarr; should be {@code WHERE col = ? * 10}
 *   <li>{@code WHERE col - 1 = ?}
 * </ul>
 *
 * @author haroya
 * @since 0.2.0
 */
public class SargabilityDetector implements DetectionRule {

  /**
   * Pattern: column [+\-*\/] literal/param followed by comparison operator. E.g., "col + 1 =",
   * "t.col * 2 >", "col - ? <"
   */
  private static final Pattern COL_ARITHMETIC_THEN_CMP =
      Pattern.compile(
          "\\b([a-zA-Z_]\\w*(?:\\.[a-zA-Z_]\\w*)?)\\s*([+\\-*/])\\s*[\\d?]+\\s*([=<>!]+|<=|>=|<>|!=)",
          Pattern.CASE_INSENSITIVE);

  /**
   * Pattern: comparison operator then literal/param [+\-*\/] column. E.g., "= ? + col", "> 10 *
   * col" This catches reversed forms like: "? = col + 1" rewritten as "= col + 1" after
   * normalization.
   */
  private static final Pattern CMP_THEN_ARITHMETIC_COL =
      Pattern.compile(
          "([=<>!]+|<=|>=|<>|!=)\\s*[\\d?]+\\s*([+\\-*/])\\s*([a-zA-Z_]\\w*(?:\\.[a-zA-Z_]\\w*)?)\\b",
          Pattern.CASE_INSENSITIVE);

  // WHERE clause extraction delegated to EnhancedSqlParser.extractWhereBody() to avoid
  // catastrophic backtracking from (.+?) with DOTALL patterns.

  private static final Set<String> SQL_KEYWORDS =
      Set.of(
          "select",
          "from",
          "where",
          "and",
          "or",
          "not",
          "in",
          "is",
          "null",
          "between",
          "like",
          "join",
          "inner",
          "left",
          "right",
          "outer",
          "on",
          "order",
          "by",
          "group",
          "having",
          "limit",
          "offset",
          "as",
          "asc",
          "desc",
          "insert",
          "update",
          "delete",
          "set",
          "into",
          "values",
          "create",
          "drop",
          "alter",
          "table",
          "index",
          "exists",
          "case",
          "when",
          "then",
          "else",
          "end",
          "union",
          "all",
          "distinct");

  private static final Pattern INVERSE_OP = Pattern.compile("^[+\\-*/]$");

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

      String whereBody =
          EnhancedSqlParser.extractWhereBody(EnhancedSqlParser.removeSubqueries(sql));
      if (whereBody == null) {
        continue;
      }
      Set<String> reportedColumns = new LinkedHashSet<>();

      // Check pattern: col op literal cmp
      Matcher m1 = COL_ARITHMETIC_THEN_CMP.matcher(whereBody);
      while (m1.find()) {
        String colRef = m1.group(1);
        String col = extractColumnName(colRef);
        if (col != null
            && !SQL_KEYWORDS.contains(col.toLowerCase())
            && reportedColumns.add(col.toLowerCase())) {
          String arithmeticOp = m1.group(2);
          String inverseOp = invertOp(arithmeticOp);

          List<String> tables = EnhancedSqlParser.extractTableNames(sql);
          String table = tables.isEmpty() ? null : tables.get(0);

          issues.add(
              new Issue(
                  IssueType.NON_SARGABLE_EXPRESSION,
                  Severity.ERROR,
                  normalized,
                  table,
                  col,
                  "Arithmetic on column '" + col + "' prevents index usage",
                  "Move the operation to the other side of the comparison. "
                      + "Instead of '"
                      + col
                      + " "
                      + arithmeticOp
                      + " value', "
                      + "use '"
                      + col
                      + " = value "
                      + inverseOp
                      + " ...'.",
                  query.stackTrace()));
        }
      }

      // Check reversed pattern: cmp literal op col
      Matcher m2 = CMP_THEN_ARITHMETIC_COL.matcher(whereBody);
      while (m2.find()) {
        String colRef = m2.group(3);
        String col = extractColumnName(colRef);
        if (col != null
            && !SQL_KEYWORDS.contains(col.toLowerCase())
            && reportedColumns.add(col.toLowerCase())) {
          String arithmeticOp = m2.group(2);
          String inverseOp = invertOp(arithmeticOp);

          List<String> tables = EnhancedSqlParser.extractTableNames(sql);
          String table = tables.isEmpty() ? null : tables.get(0);

          issues.add(
              new Issue(
                  IssueType.NON_SARGABLE_EXPRESSION,
                  Severity.ERROR,
                  normalized,
                  table,
                  col,
                  "Arithmetic on column '" + col + "' prevents index usage",
                  "Move the operation to the other side of the comparison. "
                      + "Instead of 'value "
                      + arithmeticOp
                      + " "
                      + col
                      + "', "
                      + "isolate the column on one side.",
                  query.stackTrace()));
        }
      }
    }

    return issues;
  }

  /** Extract the column name from a possibly qualified reference (e.g., "t.col" -> "col"). */
  private String extractColumnName(String ref) {
    if (ref == null) return null;
    int dot = ref.lastIndexOf('.');
    return dot >= 0 ? ref.substring(dot + 1) : ref;
  }

  /** Return the inverse arithmetic operator. */
  private String invertOp(String op) {
    return switch (op) {
      case "+" -> "-";
      case "-" -> "+";
      case "*" -> "/";
      case "/" -> "*";
      default -> op;
    };
  }
}
