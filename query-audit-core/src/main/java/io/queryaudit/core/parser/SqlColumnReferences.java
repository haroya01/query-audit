package io.queryaudit.core.parser;

import static io.queryaudit.core.parser.SqlClauseBodies.extractWhereBody;
import static io.queryaudit.core.parser.SqlIdentifiers.firstNonNull;
import static io.queryaudit.core.parser.SqlIdentifiers.isKeyword;
import static io.queryaudit.core.parser.SqlIdentifiers.isLiteralValue;
import static io.queryaudit.core.parser.SqlIdentifiers.unquoteIdentifier;
import static io.queryaudit.core.parser.SqlStatementScope.removeSubqueries;
import static io.queryaudit.core.parser.SqlText.replaceStringLiterals;
import static io.queryaudit.core.parser.SqlText.splitByTopLevelCommas;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Baseline extraction of WHERE, JOIN, ORDER BY and GROUP BY column references, without detector
 * policy.
 */
final class SqlColumnReferences {
  private SqlColumnReferences() {}

  private static final Pattern WHERE_COLUMN_WITH_OP =
      Pattern.compile(
          "(?:(?:(\"[^\"]+\"|\\w+)\\.)?(?:\"([^\"]+)\"|(\\w+)))\\s*(=|!=|<>|<=|>=|<|>|\\bNOT\\s+LIKE\\b|\\bLIKE\\b|\\bNOT\\s+IN\\b|\\bIN\\b|\\bIS\\s+NOT\\b|\\bIS\\b|\\bILIKE\\b|\\bBETWEEN\\b)",
          Pattern.CASE_INSENSITIVE);

  static List<ColumnReference> extractWhereColumns(String sql) {
    List<ColumnReference> result = new ArrayList<>();
    for (WhereColumnReference ref : extractWhereColumnsWithOperators(sql)) {
      result.add(ref.toColumnReference());
    }
    return result;
  }

  /**
   * Extract WHERE columns along with their operators (e.g., "=", "IS", "LIKE"). This enables
   * smarter analysis such as soft-delete detection (IS NULL) and equality vs range discrimination.
   */
  static List<WhereColumnReference> extractWhereColumnsWithOperators(String sql) {
    List<WhereColumnReference> result = new ArrayList<>();
    if (sql == null) {
      return result;
    }

    String cleaned = removeSubqueries(sql);
    String whereBody = extractWhereBody(cleaned);
    if (whereBody == null) {
      return result;
    }

    Matcher colMatcher = WHERE_COLUMN_WITH_OP.matcher(whereBody);
    while (colMatcher.find()) {
      String table = unquoteIdentifier(colMatcher.group(1));
      String column = firstNonNull(colMatcher.group(2), colMatcher.group(3));
      String operator = colMatcher.group(4).trim();
      if (column != null && !isKeyword(column) && !isLiteralValue(column)) {
        result.add(new WhereColumnReference(table, column, operator));
      }
    }
    return result;
  }

  // ── extractJoinColumns ─────────────────────────────────────────────

  private static final Pattern JOIN_ON =
      Pattern.compile(
          "\\bJOIN\\s+(?:\"[^\"]+\"|\\w+)(?:\\s+(?:AS\\s+)?(?:\"[^\"]+\"|\\w+))?\\s+ON\\s+"
              + "(?:(\"[^\"]+\"|\\w+)\\.)?(\"[^\"]+\"|\\w+)\\s*=\\s*(?:(\"[^\"]+\"|\\w+)\\.)?(\"[^\"]+\"|\\w+)",
          Pattern.CASE_INSENSITIVE);

  static List<JoinColumnPair> extractJoinColumns(String sql) {
    List<JoinColumnPair> result = new ArrayList<>();
    if (sql == null) {
      return result;
    }

    Matcher m = JOIN_ON.matcher(sql);
    while (m.find()) {
      ColumnReference left =
          new ColumnReference(unquoteIdentifier(m.group(1)), unquoteIdentifier(m.group(2)));
      ColumnReference right =
          new ColumnReference(unquoteIdentifier(m.group(3)), unquoteIdentifier(m.group(4)));
      result.add(new JoinColumnPair(left, right));
    }
    return result;
  }

  // ── extractOrderByColumns ──────────────────────────────────────────

  private static final Pattern COLUMN_REF =
      Pattern.compile(
          "(?:(\"[^\"]+\"|\\w+)\\.)?(\"[^\"]+\"|\\w+)(?:\\s+(?:ASC|DESC))?",
          Pattern.CASE_INSENSITIVE);

  static List<ColumnReference> extractOrderByColumns(String sql) {
    return extractColumnsFromBody(SqlClauseBodies.orderByBody(sql));
  }

  // ── extractGroupByColumns ──────────────────────────────────────────

  static List<ColumnReference> extractGroupByColumns(String sql) {
    return extractColumnsFromBody(SqlClauseBodies.groupByBody(sql));
  }

  /**
   * Extracts column references from a SQL clause (ORDER BY, GROUP BY, etc.) using manual clause
   * boundary scanning to avoid regex backtracking.
   */
  private static List<ColumnReference> extractColumnsFromBody(String body) {
    List<ColumnReference> result = new ArrayList<>();
    if (body == null) {
      return result;
    }
    // Replace single-quoted literals with '?' so a comma inside a literal
    // (e.g. ORDER BY name, 'a,b', created_at) is not treated as a separator.
    body = replaceStringLiterals(body);
    // Split by commas that are NOT inside parentheses
    List<String> parts = splitByTopLevelCommas(body);
    for (String part : parts) {
      String trimmed = part.trim();
      // Skip function expressions (e.g. COALESCE(...), COUNT(...), ROLLUP(...))
      // Only extract plain column references like "col" or "alias.col"
      if (trimmed.contains("(")) {
        continue;
      }
      Matcher colMatcher = COLUMN_REF.matcher(trimmed);
      if (colMatcher.find()) {
        String table = unquoteIdentifier(colMatcher.group(1));
        String column = unquoteIdentifier(colMatcher.group(2));
        if (column != null && !isKeyword(column)) {
          result.add(new ColumnReference(table, column));
        }
      }
    }
    return result;
  }
}
