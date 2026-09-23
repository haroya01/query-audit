package io.queryaudit.core.parser;

import static io.queryaudit.core.parser.SqlClauseBodies.extractHavingBody;
import static io.queryaudit.core.parser.SqlClauseBodies.extractWhereBody;
import static io.queryaudit.core.parser.SqlIdentifiers.firstNonNull;
import static io.queryaudit.core.parser.SqlIdentifiers.isKeyword;
import static io.queryaudit.core.parser.SqlIdentifiers.isLiteralValue;
import static io.queryaudit.core.parser.SqlIdentifiers.unquoteIdentifier;
import static io.queryaudit.core.parser.SqlStatementScope.stripCtePrefix;
import static io.queryaudit.core.parser.SqlText.stripComments;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Function-on-column analysis for WHERE, HAVING and JOIN lookup sides. Clause boundaries belong to
 * SqlClauseBodies.
 */
final class SqlFunctionExpressions {
  private SqlFunctionExpressions() {}

  /**
   * Detects function usage in HAVING clause that may disable index usage or indicate non-sargable
   * expressions. Works like {@link #detectWhereFunctions(String)} but for HAVING.
   */
  static List<FunctionUsage> detectHavingFunctions(String sql) {
    List<FunctionUsage> result = new ArrayList<>();
    if (sql == null) {
      return result;
    }
    String havingBody = extractHavingBody(sql);
    if (havingBody == null) {
      return result;
    }
    addFunctionsFromExpression(havingBody, result);
    return result;
  }

  // ── detectWhereFunctions ───────────────────────────────────────────

  private static final String FUNC_NAMES =
      "DATE|LOWER|UPPER|YEAR|MONTH|DAY|TRIM|SUBSTRING|CAST|LENGTH|COALESCE|IFNULL|CONCAT|ABS|ROUND|CEIL|FLOOR|EXTRACT|MD5|SHA1|SHA2|UNIX_TIMESTAMP|STR_TO_DATE|TO_CHAR|TO_DATE|JSON_EXTRACT|JSON_VALUE";

  private static final Pattern FUNCTION_IN_WHERE =
      Pattern.compile(
          "\\b(" + FUNC_NAMES + ")\\s*\\(\\s*(?:(\"[^\"]+\"|\\w+)\\.)?(\"[^\"]+\"|\\w+)",
          Pattern.CASE_INSENSITIVE);

  /**
   * Pattern to split a WHERE clause into individual conditions. Splits on AND/OR at the top level.
   */
  private static final Pattern CONDITION_SPLITTER =
      Pattern.compile("\\s+(?:AND|OR)\\s+", Pattern.CASE_INSENSITIVE);

  /**
   * Pattern to detect a comparison operator in a condition. Captures: left-side, operator,
   * right-side. Uses [^=!<>]+ instead of (.+?) to avoid backtracking — the left side of a
   * comparison cannot contain operator characters.
   */
  private static final Pattern COMPARISON =
      Pattern.compile("([^=!<>]+?)\\s*(=|!=|<>|<=|>=|<|>)\\s*(.+)");

  /**
   * Detects function usage in WHERE clause that disables index usage.
   *
   * <p>Improvement: If a function wraps a column on the comparison-value side (not the column being
   * searched/indexed), it is skipped. E.g., {@code WHERE m.id > COALESCE(rm.last_read, 0)} - the
   * index is on {@code m.id}, the function wraps {@code rm.last_read} which is the comparison
   * value, so no issue.
   */
  static List<FunctionUsage> detectWhereFunctions(String sql) {
    List<FunctionUsage> result = new ArrayList<>();
    if (sql == null) {
      return result;
    }

    String whereBody = extractWhereBody(sql);
    if (whereBody == null) {
      return result;
    }

    // Split into individual conditions and analyze each
    String[] conditions = CONDITION_SPLITTER.split(whereBody);
    for (String condition : conditions) {
      String trimmed = condition.trim();
      Matcher compMatcher = COMPARISON.matcher(trimmed);
      if (compMatcher.matches()) {
        String leftSide = compMatcher.group(1).trim();
        String rightSide = compMatcher.group(3).trim();

        boolean leftHasFunc = FUNCTION_IN_WHERE.matcher(leftSide).find();
        boolean rightHasFunc = FUNCTION_IN_WHERE.matcher(rightSide).find();
        boolean leftHasPlainColumn = hasPlainColumnReference(leftSide);

        // If function is on the right side and left side has a plain column,
        // the function wraps the comparison value, not the indexed column -> skip
        if (rightHasFunc && leftHasPlainColumn && !leftHasFunc) {
          continue;
        }
        // If function is on the left side and right side has a plain column or literal,
        // the function wraps the indexed column -> flag it
        if (leftHasFunc) {
          addFunctionsFromExpression(leftSide, result);
        }
        // If both sides have functions, flag both
        if (rightHasFunc && !leftHasPlainColumn) {
          addFunctionsFromExpression(rightSide, result);
        }
      } else {
        // No comparison operator found (e.g., IS NULL, BETWEEN, IN, LIKE)
        // Fall back to detecting all functions in the condition
        addFunctionsFromExpression(trimmed, result);
      }
    }
    return result;
  }

  // Pre-compiled pattern for simple column references like "table.column" or "column".
  private static final Pattern SIMPLE_COL =
      Pattern.compile("^(?:(?:\"[^\"]+\"|\\w+)\\.)?(?:\"([^\"]+)\"|(\\w+))$");

  static boolean hasPlainColumnReference(String expr) {
    String trimmed = expr.trim();
    // If the expression IS a function call, there is no plain column
    if (FUNCTION_IN_WHERE.matcher(trimmed).matches()) {
      return false;
    }
    // Check if it looks like a column reference (not a literal)
    Matcher m = SIMPLE_COL.matcher(trimmed);
    if (m.matches()) {
      String col = firstNonNull(m.group(1), m.group(2));
      return col != null && !isKeyword(col) && !isLiteralValue(col);
    }
    return false;
  }

  static void addFunctionsFromExpression(String expression, List<FunctionUsage> result) {
    Matcher fm = FUNCTION_IN_WHERE.matcher(expression);
    while (fm.find()) {
      String funcName = fm.group(1).toUpperCase();
      String tableOrAlias = unquoteIdentifier(fm.group(2));
      String column = unquoteIdentifier(fm.group(3));
      if (column != null && !isKeyword(column)) {
        result.add(new FunctionUsage(funcName, column, tableOrAlias));
      }
    }
  }

  /** Pattern to extract the FROM table and its optional alias. */
  private static final Pattern FROM_WITH_ALIAS =
      Pattern.compile(
          "\\bFROM\\s+[`\"]?(\\w+)[`\"]?(?:\\s+(?:AS\\s+)?[`\"]?(\\w+)[`\"]?)?",
          Pattern.CASE_INSENSITIVE);

  /**
   * Detects function usage in JOIN ON conditions that disable index usage.
   *
   * <p>Only flags functions that wrap columns on the LOOKUP side (the table that needs index
   * access), not the DRIVING side.
   *
   * <ul>
   *   <li>For LEFT JOIN: the right (joined) table is the lookup table
   *   <li>For RIGHT JOIN: the left (FROM) table is the lookup table
   *   <li>For INNER/CROSS JOIN: both sides need index access, flag both
   * </ul>
   */
  static List<FunctionUsage> detectJoinFunctions(String sql) {
    List<FunctionUsage> result = new ArrayList<>();
    if (sql == null) {
      return result;
    }

    sql = stripComments(sql);
    sql = stripCtePrefix(sql);

    // Extract driving table info from FROM clause
    Matcher fromMatcher = FROM_WITH_ALIAS.matcher(sql);
    String fromTable = null;
    String fromAlias = null;
    if (fromMatcher.find()) {
      fromTable = fromMatcher.group(1);
      fromAlias = fromMatcher.group(2); // may be null
    }

    for (SqlClauseBodies.JoinClause join : SqlClauseBodies.joinClauses(sql)) {
      String joinType = join.type();
      String joinedTable = join.table();
      String joinedAlias = join.alias();
      String onBody = join.body();

      // Determine which table/alias is the lookup table
      String lookupTable = joinedTable;
      String lookupAlias = joinedAlias != null ? joinedAlias : joinedTable;

      boolean isLeft = joinType != null && joinType.equalsIgnoreCase("LEFT");
      boolean isRight = joinType != null && joinType.equalsIgnoreCase("RIGHT");

      // For RIGHT JOIN, the driving table (FROM) is the lookup side
      if (isRight) {
        lookupTable = fromTable;
        lookupAlias = fromAlias != null ? fromAlias : fromTable;
      }

      Matcher fm = FUNCTION_IN_WHERE.matcher(onBody);
      while (fm.find()) {
        String funcName = fm.group(1).toUpperCase();
        String colTableOrAlias = fm.group(2); // table/alias prefix of the column
        String column = fm.group(3);
        if (isKeyword(column)) {
          continue;
        }

        if (isLeft || isRight) {
          // Only flag if the function wraps a column from the lookup table
          if (colTableOrAlias != null) {
            if (colTableOrAlias.equalsIgnoreCase(lookupTable)
                || colTableOrAlias.equalsIgnoreCase(lookupAlias)) {
              result.add(new FunctionUsage(funcName, column, colTableOrAlias));
            }
            // else: function is on driving table side -> skip
          } else {
            // No table qualifier: conservatively flag it
            result.add(new FunctionUsage(funcName, column, null));
          }
        } else {
          // INNER/CROSS JOIN: both sides need index access -> flag all
          result.add(new FunctionUsage(funcName, column, colTableOrAlias));
        }
      }
    }
    return result;
  }
}
