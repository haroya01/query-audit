package io.queryaudit.core.parser;

import static io.queryaudit.core.parser.SqlClauseBodies.extractWhereBody;
import static io.queryaudit.core.parser.SqlIdentifiers.firstNonNull;
import static io.queryaudit.core.parser.SqlIdentifiers.isKeyword;
import static io.queryaudit.core.parser.SqlStatementScope.removeSubqueries;
import static io.queryaudit.core.parser.SqlText.replaceStringLiterals;
import static io.queryaudit.core.parser.SqlText.stripComments;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** OR-branch analysis, including optional-parameter and IN-list exemptions. */
final class SqlOrPredicates {
  private SqlOrPredicates() {}

  // ── countOrConditions ──────────────────────────────────────────────

  private static final Pattern OR_PATTERN = Pattern.compile("\\bOR\\b", Pattern.CASE_INSENSITIVE);

  // Pre-compiled pattern for extracting column names from OR branches.
  // Was previously compiled inside isSameColumnOrPattern() on every call.
  private static final Pattern OR_BRANCH_COL =
      Pattern.compile(
          "(?:(?:\"[^\"]+\"|\\w+)\\.)?(?:\"([^\"]+)\"|(\\w+))\\s*(?:=|!=|<>|<=|>=|<|>|\\bIS\\b|\\bLIKE\\b)",
          Pattern.CASE_INSENSITIVE);

  static int countOrConditions(String sql) {
    return countOperators(outerPredicateBody(sql));
  }

  private static int countOperators(String whereBody) {
    if (whereBody == null) return 0;
    Matcher orMatcher = OR_PATTERN.matcher(whereBody);
    int count = 0;
    while (orMatcher.find()) {
      count++;
    }
    return count;
  }

  /**
   * Pattern to match optional parameter conditions: {@code (? IS NULL OR column = ?)}. JPA dynamic
   * queries use this pattern for optional parameters — it is NOT OR abuse because the DB
   * short-circuits at bind time.
   */
  private static final Pattern OPTIONAL_PARAM_PATTERN =
      Pattern.compile("\\(\\s*\\?\\s+IS\\s+NULL\\s+OR\\s+[^)]+\\)", Pattern.CASE_INSENSITIVE);

  /**
   * Counts effective OR conditions by excluding optional parameter patterns ({@code (? IS NULL OR
   * column = ?)}) which are short-circuited at bind time and are not real OR abuse.
   *
   * @return the number of OR conditions excluding optional parameter patterns
   */
  static int countEffectiveOrConditions(String sql) {
    String whereBody = outerPredicateBody(sql);
    if (whereBody == null) {
      return 0;
    }

    // Remove optional parameter patterns so their ORs aren't counted
    whereBody = OPTIONAL_PARAM_PATTERN.matcher(whereBody).replaceAll("?");

    return countOperators(whereBody);
  }

  /**
   * Extracts the column name referenced in each OR branch of the WHERE clause. Used to determine
   * whether every OR-branched column has its own index, enabling index_merge optimisation.
   *
   * @return list of lowercase column names (one per OR branch); empty list if unparseable
   */
  static List<String> extractOrBranchColumns(String sql) {
    String whereBody = outerPredicateBody(sql);
    if (whereBody == null) {
      return List.of();
    }

    String[] orParts = OR_PATTERN.split(whereBody);
    if (orParts.length < 2) {
      return List.of();
    }

    List<String> columns = new ArrayList<>();
    for (String part : orParts) {
      Matcher m = OR_BRANCH_COL.matcher(part.trim());
      if (!m.find()) {
        return List.of();
      }
      String col = m.group(2);
      if (col == null || isKeyword(col)) {
        return List.of();
      }
      columns.add(col.toLowerCase(Locale.ROOT));
    }
    return columns;
  }

  /**
   * Check whether all OR conditions in the WHERE clause reference the same column. This is
   * equivalent to an IN clause (e.g., "type = 'A' OR type = 'B'" is the same as "type IN ('A',
   * 'B')"), which MySQL optimizes identically.
   *
   * @return true if all OR-separated conditions reference the same column
   */
  static boolean allOrConditionsOnSameColumn(String sql) {
    String whereBody = outerPredicateBody(sql);
    if (whereBody == null) {
      return false;
    }

    // Split by OR (top level)
    // Use pre-compiled OR_PATTERN instead of String.split() with inline regex
    String[] orParts = OR_PATTERN.split(whereBody);
    if (orParts.length < 2) {
      return false;
    }

    // Extract column name from each OR part
    String firstColumn = null;
    for (String part : orParts) {
      Matcher m = OR_BRANCH_COL.matcher(part.trim());
      if (!m.find()) {
        return false;
      }
      String column = firstNonNull(m.group(1), m.group(2));
      if (column == null || isKeyword(column)) {
        return false;
      }
      if (firstColumn == null) {
        firstColumn = column.toLowerCase();
      } else if (!firstColumn.equals(column.toLowerCase())) {
        return false;
      }
    }
    return firstColumn != null;
  }

  /** Every OR operation uses the same scope and excludes literal/IN-list contents. */
  private static String outerPredicateBody(String sql) {
    if (sql == null) return null;
    String cleaned = removeSubqueries(stripComments(sql));
    String whereBody = extractWhereBody(cleaned);
    return whereBody == null
        ? null
        : replaceStringLiterals(whereBody).replaceAll("(?i)\\bIN\\s*\\([^)]*\\)", "IN (?)");
  }
}
