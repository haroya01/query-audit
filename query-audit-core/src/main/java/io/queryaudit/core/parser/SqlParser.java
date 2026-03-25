package io.queryaudit.core.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Fast regex-based SQL parser. Always available, no external dependencies.
 *
 * <p>Use this class for simple pattern checks ({@code isSelectQuery}, {@code hasWhereClause},
 * {@code hasSelectAll}, etc.) where regex is perfectly adequate.
 *
 * <p>For complex structural extraction (WHERE columns, JOIN columns, table names), prefer {@link
 * EnhancedSqlParser} instead — it uses JSqlParser for accurate AST-level parsing when available and
 * falls back to the regex methods in this class automatically.
 *
 * @author haroya
 * @since 0.2.0
 */
public final class SqlParser {

  private SqlParser() {}

  // -- CTE (WITH clause) stripping --

  private static final Pattern CTE_PREFIX =
      Pattern.compile("^\\s*WITH\\b", Pattern.CASE_INSENSITIVE);

  /**
   * Strips CTE (WITH ... AS (...)) prefix from a SQL query, returning only the main query body.
   * Handles multiple CTEs, nested parentheses, and the RECURSIVE keyword.
   */
  public static String stripCtePrefix(String sql) {
    if (sql == null) {
      return null;
    }
    if (!CTE_PREFIX.matcher(sql).find()) {
      return sql;
    }
    String upper = sql.toUpperCase();
    int idx = upper.indexOf("WITH") + 4;
    int len = sql.length();
    while (idx < len) {
      while (idx < len && Character.isWhitespace(sql.charAt(idx))) {
        idx++;
      }
      if (idx + 9 <= len && upper.substring(idx, idx + 9).equals("RECURSIVE")) {
        idx += 9;
        while (idx < len && Character.isWhitespace(sql.charAt(idx))) {
          idx++;
        }
      }
      while (idx < len
          && (Character.isLetterOrDigit(sql.charAt(idx))
              || sql.charAt(idx) == '_'
              || sql.charAt(idx) == '`')) {
        idx++;
      }
      while (idx < len && Character.isWhitespace(sql.charAt(idx))) {
        idx++;
      }
      if (idx < len && sql.charAt(idx) == '(') {
        int depth = 1;
        idx++;
        while (idx < len && depth > 0) {
          if (sql.charAt(idx) == '(') depth++;
          else if (sql.charAt(idx) == ')') depth--;
          idx++;
        }
        while (idx < len && Character.isWhitespace(sql.charAt(idx))) {
          idx++;
        }
      }
      if (idx + 2 <= len && upper.substring(idx, idx + 2).equals("AS")) {
        idx += 2;
      } else {
        return sql;
      }
      while (idx < len && Character.isWhitespace(sql.charAt(idx))) {
        idx++;
      }
      if (idx < len && sql.charAt(idx) == '(') {
        int depth = 1;
        idx++;
        while (idx < len && depth > 0) {
          char ch = sql.charAt(idx);
          if (ch == SINGLE_QUOTE_CHAR) {
            idx++;
            while (idx < len) {
              if (sql.charAt(idx) == SINGLE_QUOTE_CHAR
                  && idx + 1 < len
                  && sql.charAt(idx + 1) == SINGLE_QUOTE_CHAR) {
                idx += 2;
              } else if (sql.charAt(idx) == SINGLE_QUOTE_CHAR) {
                idx++;
                break;
              } else {
                idx++;
              }
            }
            continue;
          }
          if (ch == '(') depth++;
          else if (ch == ')') depth--;
          idx++;
        }
      } else {
        return sql;
      }
      while (idx < len && Character.isWhitespace(sql.charAt(idx))) {
        idx++;
      }
      if (idx < len && sql.charAt(idx) == ',') {
        idx++;
        continue;
      }
      break;
    }
    if (idx >= len) {
      return sql;
    }
    return sql.substring(idx).trim();
  }

  private static final char SINGLE_QUOTE_CHAR = 39;

  // -- stripComments ----------------------------------------------------------

  /**
   * Strips SQL comments from the input while preserving content inside string literals. Handles
   * block comments (including nested), line comments, and preserves comment-like content inside
   * string literals.
   *
   * @param sql the SQL string to strip comments from
   * @return the SQL with comments replaced by a single space, or null if input is null
   */
  public static String stripComments(String sql) {
    if (sql == null) {
      return null;
    }
    StringBuilder sb = new StringBuilder(sql.length());
    int i = 0;
    int len = sql.length();
    while (i < len) {
      char c = sql.charAt(i);
      if (c == '\'') {
        sb.append(c);
        i++;
        while (i < len) {
          char inner = sql.charAt(i);
          if (inner == '\\' && i + 1 < len) {
            sb.append(inner);
            sb.append(sql.charAt(i + 1));
            i += 2;
          } else if (inner == '\'' && i + 1 < len && sql.charAt(i + 1) == '\'') {
            sb.append('\'');
            sb.append('\'');
            i += 2;
          } else if (inner == '\'') {
            sb.append(inner);
            i++;
            break;
          } else {
            sb.append(inner);
            i++;
          }
        }
        continue;
      }
      if (c == '"') {
        sb.append(c);
        i++;
        while (i < len) {
          char inner = sql.charAt(i);
          if (inner == '"' && i + 1 < len && sql.charAt(i + 1) == '"') {
            sb.append('"');
            sb.append('"');
            i += 2;
          } else if (inner == '"') {
            sb.append(inner);
            i++;
            break;
          } else {
            sb.append(inner);
            i++;
          }
        }
        continue;
      }
      if (c == '/' && i + 1 < len && sql.charAt(i + 1) == '*') {
        int depth = 1;
        i += 2;
        while (i < len && depth > 0) {
          if (sql.charAt(i) == '/' && i + 1 < len && sql.charAt(i + 1) == '*') {
            depth++;
            i += 2;
          } else if (sql.charAt(i) == '*' && i + 1 < len && sql.charAt(i + 1) == '/') {
            depth--;
            i += 2;
          } else {
            i++;
          }
        }
        sb.append(' ');
        continue;
      }
      if (c == '-' && i + 1 < len && sql.charAt(i + 1) == '-') {
        i += 2;
        while (i < len && sql.charAt(i) != '\n' && sql.charAt(i) != '\r') {
          i++;
        }
        sb.append(' ');
        continue;
      }
      sb.append(c);
      i++;
    }
    return sb.toString();
  }


  // ── normalize ──────────────────────────────────────────────────────

  private static final Pattern SINGLE_QUOTED =
      Pattern.compile("'[^'\\\\]*(?:(?:''|\\\\.)[^'\\\\]*)*'");
  private static final Pattern DOUBLE_QUOTED = Pattern.compile("\"[^\"]*\"");
  private static final Pattern NUMBERS =
      Pattern.compile("\\b(?:0x[0-9a-fA-F]+|\\d+(?:\\.\\d+)?(?:[eE][+-]?\\d+)?)\\b");
  private static final Pattern IN_LIST =
      Pattern.compile("\\bIN\\s*\\(\\s*\\?(?:\\s*,\\s*\\?)*+\\s*\\)", Pattern.CASE_INSENSITIVE);
  private static final Pattern WHITESPACE = Pattern.compile("\\s+");

  /**
   * Normalize a SQL query by replacing literal values with {@code ?}, collapsing whitespace, and
   * lowercasing. Useful for grouping identical query patterns (e.g. N+1 detection).
   */
  public static String normalize(String sql) {
    if (sql == null) {
      return null;
    }
    String result = stripComments(sql);
    result = replaceStringLiterals(result);
    result = DOUBLE_QUOTED.matcher(result).replaceAll("?");
    result = NUMBERS.matcher(result).replaceAll("?");
    result = IN_LIST.matcher(result).replaceAll("IN (?)");
    result = WHITESPACE.matcher(result).replaceAll(" ");
    return result.trim().toLowerCase();
  }

  /**
   * Replaces single-quoted string literals with {@code ?}, handling SQL-standard escaped quotes
   * ({@code ''}) and MySQL backslash escaping ({@code \'}). Uses a manual loop instead of regex to
   * avoid StackOverflowError on large inputs.
   */
  private static String replaceStringLiterals(String sql) {
    StringBuilder sb = new StringBuilder(sql.length());
    int i = 0;
    while (i < sql.length()) {
      char c = sql.charAt(i);
      if (c == '\'') {
        // Found opening quote — skip to closing quote
        i++;
        while (i < sql.length()) {
          char inner = sql.charAt(i);
          if (inner == '\\' && i + 1 < sql.length()) {
            // MySQL backslash escape: skip next char
            i += 2;
          } else if (inner == '\'' && i + 1 < sql.length() && sql.charAt(i + 1) == '\'') {
            // SQL-standard escaped quote (''): skip both
            i += 2;
          } else if (inner == '\'') {
            // Closing quote
            i++;
            break;
          } else {
            i++;
          }
        }
        sb.append('?');
      } else if (c == '"') {
        i++;
        while (i < sql.length()) {
          char inner = sql.charAt(i);
          if (inner == '\\' && i + 1 < sql.length()) {
            i += 2;
          } else if (inner == '"' && i + 1 < sql.length() && sql.charAt(i + 1) == '"') {
            i += 2;
          } else if (inner == '"') {
            i++;
            break;
          } else {
            i++;
          }
        }
        sb.append('?');
      } else {
        sb.append(c);
        i++;
      }
    }
    return sb.toString();
  }

  // ── isSelectQuery ──────────────────────────────────────────────────

  // Query type detection uses simple string prefix checks instead of regex.
  // These methods are called for every single query, so avoiding Matcher
  // allocation and regex engine overhead is a meaningful win.

  public static boolean isSelectQuery(String sql) {
    return sql != null && startsWithKeyword(sql, "SELECT");
  }

  // ── DML query type detection ────────────────────────────────────────

  public static boolean isInsertQuery(String sql) {
    return sql != null && startsWithKeyword(sql, "INSERT");
  }

  public static boolean isUpdateQuery(String sql) {
    return sql != null && startsWithKeyword(sql, "UPDATE");
  }

  public static boolean isDeleteQuery(String sql) {
    return sql != null && startsWithKeyword(sql, "DELETE");
  }

  public static boolean isDmlQuery(String sql) {
    return isInsertQuery(sql) || isUpdateQuery(sql) || isDeleteQuery(sql);
  }

  /**
   * Checks if sql starts with the given keyword (case-insensitive) after skipping leading
   * whitespace, followed by a non-word character or end of string. Replaces regex-based
   * Pattern.compile("^\\s*KEYWORD\\b") checks.
   */
  private static boolean startsWithKeyword(String sql, String keyword) {
    int i = 0;
    int len = sql.length();
    // skip leading whitespace
    while (i < len && Character.isWhitespace(sql.charAt(i))) {
      i++;
    }
    // check keyword
    if (i + keyword.length() > len) {
      return false;
    }
    for (int j = 0; j < keyword.length(); j++) {
      char c = Character.toUpperCase(sql.charAt(i + j));
      if (c != keyword.charAt(j)) {
        return false;
      }
    }
    // check word boundary: next char must be non-word or end of string
    int afterKeyword = i + keyword.length();
    if (afterKeyword >= len) {
      return true;
    }
    char next = sql.charAt(afterKeyword);
    return !Character.isLetterOrDigit(next) && next != '_';
  }

  // ── extractUpdateTable ──────────────────────────────────────────────

  private static final Pattern UPDATE_TABLE =
      Pattern.compile("^\\s*UPDATE\\s+(?:`(\\w+)`|(\\w+))", Pattern.CASE_INSENSITIVE);

  /** Extracts the target table name from an UPDATE statement. */
  public static String extractUpdateTable(String sql) {
    if (sql == null) return null;
    Matcher m = UPDATE_TABLE.matcher(sql);
    if (m.find()) {
      return m.group(1) != null ? m.group(1) : m.group(2);
    }
    return null;
  }

  // ── extractDeleteTable ──────────────────────────────────────────────

  private static final Pattern DELETE_TABLE =
      Pattern.compile("^\\s*DELETE\\s+FROM\\s+(?:`(\\w+)`|(\\w+))", Pattern.CASE_INSENSITIVE);

  /** Extracts the target table name from a DELETE statement. */
  public static String extractDeleteTable(String sql) {
    if (sql == null) return null;
    Matcher m = DELETE_TABLE.matcher(sql);
    if (m.find()) {
      return m.group(1) != null ? m.group(1) : m.group(2);
    }
    return null;
  }

  // ── extractInsertTable ──────────────────────────────────────────────

  private static final Pattern INSERT_TABLE =
      Pattern.compile("^\\s*INSERT\\s+INTO\\s+(?:`(\\w+)`|(\\w+))", Pattern.CASE_INSENSITIVE);

  /** Extracts the target table name from an INSERT statement. */
  public static String extractInsertTable(String sql) {
    if (sql == null) return null;
    Matcher m = INSERT_TABLE.matcher(sql);
    if (m.find()) {
      return m.group(1) != null ? m.group(1) : m.group(2);
    }
    return null;
  }

  // ── hasWhereClause ──────────────────────────────────────────────────

  /** Returns true if the SQL contains a WHERE clause. */
  public static boolean hasWhereClause(String sql) {
    if (sql == null) return false;
    return WHERE_START.matcher(sql).find();
  }

  /**
   * Returns true if the outer SQL (ignoring subqueries) contains a WHERE clause. This avoids false
   * negatives where a WHERE inside a subquery masks a missing outer WHERE.
   */
  public static boolean hasOuterWhereClause(String sql) {
    if (sql == null) return false;
    String cleaned = removeSubqueries(sql);
    return WHERE_START.matcher(cleaned).find();
  }

  // ── hasSelectAll ───────────────────────────────────────────────────

  private static final Pattern SELECT_ALL =
      Pattern.compile(
          "\\bSELECT\\s+(?:ALL\\s+|DISTINCT\\s+)?(?:(?!COUNT\\s*\\(|EXISTS\\s*\\()(?:\\w+\\.)?\\*)",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern EXISTS_SELECT_STAR =
      Pattern.compile("\\bEXISTS\\s*\\(\\s*SELECT\\s+\\*", Pattern.CASE_INSENSITIVE);

  public static boolean hasSelectAll(String sql) {
    if (sql == null) {
      return false;
    }
    sql = stripComments(sql);
    // Neutralise EXISTS(SELECT * ...) — it is idiomatic SQL and not a real SELECT *
    String sanitized = EXISTS_SELECT_STAR.matcher(sql).replaceAll("EXISTS(SELECT 1");
    return SELECT_ALL.matcher(sanitized).find();
  }

  // ── extractWhereColumns ────────────────────────────────────────────

  /** Keyword boundary pattern for finding WHERE keyword start position. */
  private static final Pattern WHERE_START =
      Pattern.compile("\\bWHERE\\b", Pattern.CASE_INSENSITIVE);

  /**
   * Clause terminators that end a WHERE body. Searched via manual scanning to avoid catastrophic
   * backtracking from (.+?) patterns with DOTALL.
   */
  private static final Pattern[] WHERE_TERMINATORS = {
    Pattern.compile("\\bGROUP\\s+BY\\b", Pattern.CASE_INSENSITIVE),
    Pattern.compile("\\bORDER\\s+BY\\b", Pattern.CASE_INSENSITIVE),
    Pattern.compile("\\bLIMIT\\b", Pattern.CASE_INSENSITIVE),
    Pattern.compile("\\bHAVING\\b", Pattern.CASE_INSENSITIVE),
  };

  /**
   * Extracts the WHERE clause body by finding WHERE keyword and then scanning for the nearest
   * clause terminator. This is O(n) per terminator pattern, avoiding the O(n^2) worst-case of (.+?)
   * with alternation terminators.
   *
   * @return the WHERE clause body (without the WHERE keyword), or null if no WHERE found
   */
  public static String extractWhereBody(String sql) {
    if (sql == null) return null;
    String effective = stripComments(sql);
    effective = stripCtePrefix(effective);
    Matcher m = WHERE_START.matcher(effective);
    if (!m.find()) return null;
    int bodyStart = m.end();
    return extractClauseBody(effective, bodyStart, WHERE_TERMINATORS);
  }

  /**
   * Given a start position inside the SQL string, finds the nearest terminator and returns the
   * substring between start and the terminator (or end of string).
   */
  private static String extractClauseBody(String sql, int bodyStart, Pattern[] terminators) {
    int bodyEnd = sql.length();
    for (Pattern terminator : terminators) {
      Matcher tm = terminator.matcher(sql);
      if (tm.find(bodyStart) && tm.start() < bodyEnd) {
        bodyEnd = tm.start();
      }
    }
    if (bodyStart >= bodyEnd) return null;
    return sql.substring(bodyStart, bodyEnd);
  }

  private static final Pattern WHERE_COLUMN =
      Pattern.compile(
          "(?:(?:(\\w+)\\.)?(\\w+))\\s*(?:=|!=|<>|<=|>=|<|>|\\bNOT\\s+LIKE\\b|\\bLIKE\\b|\\bNOT\\s+IN\\b|\\bIN\\b|\\bIS\\s+NOT\\b|\\bIS\\b|\\bILIKE\\b|\\bBETWEEN\\b)",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern WHERE_COLUMN_WITH_OP =
      Pattern.compile(
          "(?:(?:(\\w+)\\.)?(\\w+))\\s*(=|!=|<>|<=|>=|<|>|\\bNOT\\s+LIKE\\b|\\bLIKE\\b|\\bNOT\\s+IN\\b|\\bIN\\b|\\bIS\\s+NOT\\b|\\bIS\\b|\\bILIKE\\b|\\bBETWEEN\\b)",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern LITERAL_VALUE =
      Pattern.compile(
          "^(?:\\d+(?:\\.\\d+)?(?:[eE][+-]?\\d+)?|0x[0-9a-fA-F]+|'[^']*'|\"[^\"]*\"|\\?|true|false|null)$",
          Pattern.CASE_INSENSITIVE);

  public static List<ColumnReference> extractWhereColumns(String sql) {
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
  public static List<WhereColumnReference> extractWhereColumnsWithOperators(String sql) {
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
      String table = colMatcher.group(1);
      String column = colMatcher.group(2);
      String operator = colMatcher.group(3).trim();
      if (!isKeyword(column) && !isLiteralValue(column)) {
        result.add(new WhereColumnReference(table, column, operator));
      }
    }
    return result;
  }

  /**
   * Returns true if the given string looks like a literal value (number, quoted string,
   * placeholder, or boolean keyword) rather than a column name.
   */
  private static boolean isLiteralValue(String value) {
    return value != null && LITERAL_VALUE.matcher(value).matches();
  }

  // ── extractJoinColumns ─────────────────────────────────────────────

  private static final Pattern JOIN_ON =
      Pattern.compile(
          "\\bJOIN\\s+\\w+(?:\\s+(?:AS\\s+)?\\w+)?\\s+ON\\s+"
              + "(?:(\\w+)\\.)?(\\w+)\\s*=\\s*(?:(\\w+)\\.)?(\\w+)",
          Pattern.CASE_INSENSITIVE);

  public static List<JoinColumnPair> extractJoinColumns(String sql) {
    List<JoinColumnPair> result = new ArrayList<>();
    if (sql == null) {
      return result;
    }

    Matcher m = JOIN_ON.matcher(sql);
    while (m.find()) {
      ColumnReference left = new ColumnReference(m.group(1), m.group(2));
      ColumnReference right = new ColumnReference(m.group(3), m.group(4));
      result.add(new JoinColumnPair(left, right));
    }
    return result;
  }

  // ── extractOrderByColumns ──────────────────────────────────────────

  private static final Pattern ORDER_BY_START =
      Pattern.compile("\\bORDER\\s+BY\\b", Pattern.CASE_INSENSITIVE);

  private static final Pattern[] ORDER_BY_TERMINATORS = {
    Pattern.compile("\\bLIMIT\\b", Pattern.CASE_INSENSITIVE),
    Pattern.compile("\\bOFFSET\\b", Pattern.CASE_INSENSITIVE),
  };

  private static final Pattern COLUMN_REF =
      Pattern.compile("(?:(\\w+)\\.)?(\\w+)(?:\\s+(?:ASC|DESC))?", Pattern.CASE_INSENSITIVE);

  public static List<ColumnReference> extractOrderByColumns(String sql) {
    return extractColumnsFromClause(sql, ORDER_BY_START, ORDER_BY_TERMINATORS);
  }

  // ── extractGroupByColumns ──────────────────────────────────────────

  private static final Pattern GROUP_BY_START =
      Pattern.compile("\\bGROUP\\s+BY\\b", Pattern.CASE_INSENSITIVE);

  private static final Pattern[] GROUP_BY_TERMINATORS = {
    Pattern.compile("\\bHAVING\\b", Pattern.CASE_INSENSITIVE),
    Pattern.compile("\\bORDER\\s+BY\\b", Pattern.CASE_INSENSITIVE),
    Pattern.compile("\\bLIMIT\\b", Pattern.CASE_INSENSITIVE),
  };

  public static List<ColumnReference> extractGroupByColumns(String sql) {
    return extractColumnsFromClause(sql, GROUP_BY_START, GROUP_BY_TERMINATORS);
  }

  // ── extractHavingClause ──────────────────────────────────────────

  private static final Pattern HAVING_START =
      Pattern.compile("\\bHAVING\\b", Pattern.CASE_INSENSITIVE);

  private static final Pattern[] HAVING_TERMINATORS = {
    Pattern.compile("\\bORDER\\s+BY\\b", Pattern.CASE_INSENSITIVE),
    Pattern.compile("\\bLIMIT\\b", Pattern.CASE_INSENSITIVE),
    Pattern.compile("\\bUNION\\b", Pattern.CASE_INSENSITIVE),
  };

  /**
   * Extracts the HAVING clause body from a SQL query, or null if not present. Uses manual clause
   * boundary scanning to avoid regex backtracking.
   */
  public static String extractHavingClause(String sql) {
    if (sql == null) {
      return null;
    }
    Matcher m = HAVING_START.matcher(sql);
    if (!m.find()) {
      return null;
    }
    String body = extractClauseBody(sql, m.end(), HAVING_TERMINATORS);
    return body != null ? body.trim() : null;
  }

  /**
   * Extracts the HAVING clause body, similar to {@link #extractWhereBody(String)}. Handles HAVING
   * after GROUP BY, HAVING before ORDER BY/LIMIT, and subqueries within HAVING.
   *
   * @return the HAVING clause body (without the HAVING keyword), or null if no HAVING found
   */
  public static String extractHavingBody(String sql) {
    return extractHavingClause(sql);
  }

  /**
   * Detects function usage in HAVING clause that may disable index usage or indicate non-sargable
   * expressions. Works like {@link #detectWhereFunctions(String)} but for HAVING.
   */
  public static List<FunctionUsage> detectHavingFunctions(String sql) {
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

  /**
   * Extracts the JOIN ON clause bodies for all JOINs found in the SQL.
   *
   * @return list of ON clause body strings
   */
  public static List<String> extractJoinOnBodies(String sql) {
    List<String> result = new ArrayList<>();
    if (sql == null) {
      return result;
    }
    String effective = stripCtePrefix(sql);
    Matcher joinMatcher = JOIN_HEADER.matcher(effective);
    while (joinMatcher.find()) {
      String onBody = extractClauseBody(effective, joinMatcher.end(), JOIN_ON_TERMINATORS);
      if (onBody != null) {
        result.add(onBody);
      }
    }
    return result;
  }

  /**
   * Extracts column references from a SQL clause (ORDER BY, GROUP BY, etc.) using manual clause
   * boundary scanning to avoid regex backtracking.
   */
  private static List<ColumnReference> extractColumnsFromClause(
      String sql, Pattern clauseStart, Pattern[] terminators) {
    List<ColumnReference> result = new ArrayList<>();
    if (sql == null) {
      return result;
    }

    Matcher startMatcher = clauseStart.matcher(sql);
    if (!startMatcher.find()) {
      return result;
    }

    String body = extractClauseBody(sql, startMatcher.end(), terminators);
    if (body == null) {
      return result;
    }
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
        String table = colMatcher.group(1);
        String column = colMatcher.group(2);
        if (!isKeyword(column)) {
          result.add(new ColumnReference(table, column));
        }
      }
    }
    return result;
  }

  /** Split a string by commas that are at the top level (not inside parentheses). */
  private static List<String> splitByTopLevelCommas(String s) {
    List<String> parts = new ArrayList<>();
    int depth = 0;
    int start = 0;
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c == '(') {
        depth++;
      } else if (c == ')') {
        depth--;
      } else if (c == ',' && depth == 0) {
        parts.add(s.substring(start, i));
        start = i + 1;
      }
    }
    parts.add(s.substring(start));
    return parts;
  }

  // ── detectWhereFunctions ───────────────────────────────────────────

  private static final String FUNC_NAMES =
      "DATE|LOWER|UPPER|YEAR|MONTH|DAY|TRIM|SUBSTRING|CAST|LENGTH|COALESCE|IFNULL|CONCAT|ABS|ROUND|CEIL|FLOOR|EXTRACT|MD5|SHA1|SHA2|UNIX_TIMESTAMP|STR_TO_DATE|TO_CHAR|TO_DATE|JSON_EXTRACT|JSON_VALUE";

  private static final Pattern FUNCTION_IN_WHERE =
      Pattern.compile(
          "\\b(" + FUNC_NAMES + ")\\s*\\(\\s*(?:(\\w+)\\.)?(\\w+)", Pattern.CASE_INSENSITIVE);

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
  public static List<FunctionUsage> detectWhereFunctions(String sql) {
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
        boolean rightHasPlainColumn = hasPlainColumnReference(rightSide);

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

  /** Check if expression contains a plain (non-function-wrapped) column reference. */
  private static final Pattern PLAIN_COLUMN =
      Pattern.compile("(?:^|[^\\w])(?:(\\w+)\\.)?(\\w+)\\s*$");

  // Pre-compiled pattern for simple column references like "table.column" or "column".
  // Was previously compiled inside hasPlainColumnReference() on every call.
  private static final Pattern SIMPLE_COL = Pattern.compile("^(?:(\\w+)\\.)?(\\w+)$");

  private static boolean hasPlainColumnReference(String expr) {
    String trimmed = expr.trim();
    // If the expression IS a function call, there is no plain column
    if (FUNCTION_IN_WHERE.matcher(trimmed).matches()) {
      return false;
    }
    // Check if it looks like a column reference (not a literal)
    Matcher m = SIMPLE_COL.matcher(trimmed);
    if (m.matches()) {
      String col = m.group(2);
      return !isKeyword(col) && !isLiteralValue(col);
    }
    return false;
  }

  private static void addFunctionsFromExpression(String expression, List<FunctionUsage> result) {
    Matcher fm = FUNCTION_IN_WHERE.matcher(expression);
    while (fm.find()) {
      String funcName = fm.group(1).toUpperCase();
      String tableOrAlias = fm.group(2);
      String column = fm.group(3);
      if (!isKeyword(column)) {
        result.add(new FunctionUsage(funcName, column, tableOrAlias));
      }
    }
  }

  // ── detectJoinFunctions ─────────────────────────────────────────────

  /**
   * Pattern to match JOIN header: type, table name, optional alias, and ON keyword. The ON clause
   * body is extracted via manual scanning (extractJoinOnBody) to avoid catastrophic backtracking
   * from (.+?) with DOTALL.
   */
  private static final Pattern JOIN_HEADER =
      Pattern.compile(
          "\\b(LEFT|RIGHT|INNER|CROSS|FULL)?\\s*(?:OUTER\\s+)?JOIN\\s+`?(\\w+)`?(?:\\s+(?:AS\\s+)?`?(\\w+)`?)?\\s+ON\\s+",
          Pattern.CASE_INSENSITIVE);

  /**
   * Terminators that end a JOIN ON clause body. Used by extractJoinOnBody for manual boundary
   * scanning, replacing the previous (.+?) with DOTALL approach that caused catastrophic
   * backtracking.
   */
  private static final Pattern[] JOIN_ON_TERMINATORS = {
    Pattern.compile(
        "\\b(?:LEFT|RIGHT|INNER|CROSS|FULL)?\\s*(?:OUTER\\s+)?JOIN\\b",
        Pattern.CASE_INSENSITIVE),
    Pattern.compile("\\bWHERE\\b", Pattern.CASE_INSENSITIVE),
    Pattern.compile("\\bGROUP\\s+BY\\b", Pattern.CASE_INSENSITIVE),
    Pattern.compile("\\bORDER\\s+BY\\b", Pattern.CASE_INSENSITIVE),
    Pattern.compile("\\bLIMIT\\b", Pattern.CASE_INSENSITIVE),
    Pattern.compile("\\bHAVING\\b", Pattern.CASE_INSENSITIVE),
  };

  /** Pattern to extract the FROM table and its optional alias. */
  private static final Pattern FROM_WITH_ALIAS =
      Pattern.compile("\\bFROM\\s+(\\w+)(?:\\s+(?:AS\\s+)?(\\w+))?", Pattern.CASE_INSENSITIVE);

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
  public static List<FunctionUsage> detectJoinFunctions(String sql) {
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

    Matcher joinMatcher = JOIN_HEADER.matcher(sql);
    while (joinMatcher.find()) {
      String joinType = joinMatcher.group(1); // LEFT, RIGHT, INNER, or null
      String joinedTable = joinMatcher.group(2);
      String joinedAlias = joinMatcher.group(3); // may be null
      String onBody = extractClauseBody(sql, joinMatcher.end(), JOIN_ON_TERMINATORS);
      if (onBody == null) continue;

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

  // ── countOrConditions ──────────────────────────────────────────────

  private static final Pattern OR_PATTERN = Pattern.compile("\\bOR\\b", Pattern.CASE_INSENSITIVE);

  // Pre-compiled pattern for extracting column names from OR branches.
  // Was previously compiled inside isSameColumnOrPattern() on every call.
  private static final Pattern OR_BRANCH_COL =
      Pattern.compile(
          "(?:(\\w+)\\.)?(\\w+)\\s*(?:=|!=|<>|<=|>=|<|>|\\bIS\\b|\\bLIKE\\b)",
          Pattern.CASE_INSENSITIVE);

  public static int countOrConditions(String sql) {
    if (sql == null) {
      return 0;
    }

    sql = stripComments(sql);
    String cleaned = removeSubqueries(sql);
    String whereBody = extractWhereBody(cleaned);
    if (whereBody == null) {
      return 0;
    }

    // Remove string literals to avoid counting OR inside string values
    whereBody = replaceStringLiterals(whereBody);
    whereBody = DOUBLE_QUOTED.matcher(whereBody).replaceAll("?");
    // Remove content inside IN(...)
    whereBody = removeInLists(whereBody);

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
  public static int countEffectiveOrConditions(String sql) {
    if (sql == null) {
      return 0;
    }

    sql = stripComments(sql);
    String cleaned = removeSubqueries(sql);
    String whereBody = extractWhereBody(cleaned);
    if (whereBody == null) {
      return 0;
    }

    // Remove string literals to avoid counting OR inside string values
    whereBody = replaceStringLiterals(whereBody);
    whereBody = DOUBLE_QUOTED.matcher(whereBody).replaceAll("?");
    // Remove content inside IN(...)
    whereBody = removeInLists(whereBody);

    // Remove optional parameter patterns so their ORs aren't counted
    whereBody = OPTIONAL_PARAM_PATTERN.matcher(whereBody).replaceAll("?");

    Matcher orMatcher = OR_PATTERN.matcher(whereBody);
    int count = 0;
    while (orMatcher.find()) {
      count++;
    }
    return count;
  }

  /**
   * Check whether all OR conditions in the WHERE clause reference the same column. This is
   * equivalent to an IN clause (e.g., "type = 'A' OR type = 'B'" is the same as "type IN ('A',
   * 'B')"), which MySQL optimizes identically.
   *
   * @return true if all OR-separated conditions reference the same column
   */
  public static boolean allOrConditionsOnSameColumn(String sql) {
    if (sql == null) {
      return false;
    }

    sql = stripComments(sql);
    String cleaned = removeSubqueries(sql);
    String whereBody = extractWhereBody(cleaned);
    if (whereBody == null) {
      return false;
    }

    whereBody = replaceStringLiterals(whereBody);
    whereBody = DOUBLE_QUOTED.matcher(whereBody).replaceAll("?");
    whereBody = removeInLists(whereBody);

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
      String column = m.group(2);
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

  // ── extractOffsetValue ─────────────────────────────────────────────

  // LIMIT count OFFSET offset
  private static final Pattern LIMIT_OFFSET =
      Pattern.compile("\\bLIMIT\\s+\\d+\\s+OFFSET\\s+(\\d+)", Pattern.CASE_INSENSITIVE);

  // LIMIT offset, count (MySQL style)
  private static final Pattern LIMIT_COMMA =
      Pattern.compile("\\bLIMIT\\s+(\\d+)\\s*,\\s*\\d+", Pattern.CASE_INSENSITIVE);

  // OFFSET offset (standalone, e.g. PostgreSQL without LIMIT before OFFSET)
  private static final Pattern OFFSET_STANDALONE =
      Pattern.compile("\\bOFFSET\\s+(\\d+)", Pattern.CASE_INSENSITIVE);

  public static OptionalLong extractOffsetValue(String sql) {
    if (sql == null) {
      return OptionalLong.empty();
    }

    // Try LIMIT ... OFFSET ... first
    Matcher m = LIMIT_OFFSET.matcher(sql);
    if (m.find()) {
      return OptionalLong.of(Long.parseLong(m.group(1)));
    }

    // Try LIMIT offset, count
    m = LIMIT_COMMA.matcher(sql);
    if (m.find()) {
      return OptionalLong.of(Long.parseLong(m.group(1)));
    }

    // Try standalone OFFSET
    m = OFFSET_STANDALONE.matcher(sql);
    if (m.find()) {
      return OptionalLong.of(Long.parseLong(m.group(1)));
    }

    return OptionalLong.empty();
  }

  // ── hasOffsetClause ───────────────────────────────────────────────

  /**
   * Matches OFFSET with either a literal number or a parameterized placeholder (?). JPA always uses
   * parameterized placeholders for OFFSET, so we cannot see the actual value at SQL interception
   * time.
   */
  private static final Pattern OFFSET_ANY =
      Pattern.compile("\\bOFFSET\\s+(?:\\d+|\\?)", Pattern.CASE_INSENSITIVE);

  /** Matches MySQL-style LIMIT offset, count with parameterized placeholder. */
  private static final Pattern LIMIT_COMMA_PARAM =
      Pattern.compile("\\bLIMIT\\s+(?:\\d+|\\?)\\s*,\\s*(?:\\d+|\\?)", Pattern.CASE_INSENSITIVE);

  /**
   * Returns true if the SQL contains an OFFSET clause (either with a literal value or a
   * parameterized placeholder). This is useful for detecting potential large OFFSET usage in JPA
   * queries where the actual value is always parameterized.
   */
  public static boolean hasOffsetClause(String sql) {
    if (sql == null) {
      return false;
    }
    if (OFFSET_ANY.matcher(sql).find()) {
      return true;
    }
    // MySQL-style LIMIT offset, count
    return LIMIT_COMMA_PARAM.matcher(sql).find();
  }

  // ── extractTableNames ──────────────────────────────────────────────

  private static final Pattern FROM_TABLE =
      Pattern.compile(
          "\\bFROM\\s+(?:`(\\w+)`|(\\w+(?:\\.\\w+)?))(?:\\s+(?:AS\\s+)?(?:`?\\w+`?))?",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern JOIN_TABLE =
      Pattern.compile(
          "\\bJOIN\\s+(?:`(\\w+)`|(\\w+(?:\\.\\w+)?))(?:\\s+(?:AS\\s+)?(?:`?\\w+`?))?",
          Pattern.CASE_INSENSITIVE);

  public static List<String> extractTableNames(String sql) {
    List<String> result = new ArrayList<>();
    if (sql == null) {
      return result;
    }

    sql = stripComments(sql);
    sql = stripCtePrefix(sql);

    Matcher fromMatcher = FROM_TABLE.matcher(sql);
    while (fromMatcher.find()) {
      String table = fromMatcher.group(1) != null ? fromMatcher.group(1) : fromMatcher.group(2);
      // For schema-qualified names like "schema.table", take just the table part
      if (table != null && table.contains(".")) {
        table = table.substring(table.lastIndexOf('.') + 1);
      }
      if (table != null && !isKeyword(table) && !result.contains(table)) {
        result.add(table);
      }
    }

    Matcher joinMatcher = JOIN_TABLE.matcher(sql);
    while (joinMatcher.find()) {
      String table = joinMatcher.group(1) != null ? joinMatcher.group(1) : joinMatcher.group(2);
      if (table != null && table.contains(".")) {
        table = table.substring(table.lastIndexOf('.') + 1);
      }
      if (table != null && !isKeyword(table) && !result.contains(table)) {
        result.add(table);
      }
    }

    return result;
  }

  // ── helpers ─────────────────────────────────────────────────────────

  /**
   * Remove subqueries (nested SELECT statements) from SQL to avoid parsing their internals. Uses a
   * simple parenthesis-depth approach.
   */
  public static String removeSubqueries(String sql) {
    StringBuilder sb = new StringBuilder();
    int depth = 0;
    boolean inSubquery = false;
    String upper = sql.toUpperCase();

    for (int i = 0; i < sql.length(); i++) {
      char c = sql.charAt(i);
      if (c == '(') {
        // Check if this opens a subquery
        int ahead = i + 1;
        while (ahead < sql.length() && Character.isWhitespace(sql.charAt(ahead))) {
          ahead++;
        }
        if (ahead + 6 <= upper.length() && upper.substring(ahead, ahead + 6).equals("SELECT")) {
          inSubquery = true;
          depth = 1;
          sb.append('('); // preserve opening parenthesis
          i = ahead; // skip to SELECT
          continue;
        }
        if (inSubquery) {
          depth++;
          continue;
        }
        sb.append(c);
      } else if (c == ')') {
        if (inSubquery) {
          depth--;
          if (depth == 0) {
            inSubquery = false;
            sb.append("?)"); // placeholder (opening paren already appended)
          }
          continue;
        }
        sb.append(c);
      } else {
        if (!inSubquery) {
          sb.append(c);
        }
      }
    }
    return sb.toString();
  }

  /** Remove content inside IN(...) to avoid false positives when counting OR etc. */
  private static String removeInLists(String sql) {
    return sql.replaceAll("(?i)\\bIN\\s*\\([^)]*\\)", "IN (?)");
  }

  private static final java.util.Set<String> SQL_KEYWORDS =
      java.util.Set.of(
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
          "distinct",
          "count",
          "sum",
          "avg",
          "min",
          "max");

  private static boolean isKeyword(String word) {
    return word != null && SQL_KEYWORDS.contains(word.toLowerCase());
  }
}
