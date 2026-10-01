package io.queryaudit.core.parser;

import java.util.regex.Pattern;

/**
 * Finds or masks statement boundaries while preserving the baseline scanner's legacy SQL semantics.
 */
final class SqlStatementScope {
  private SqlStatementScope() {}

  // -- CTE (WITH clause) stripping --

  private static final Pattern CTE_PREFIX =
      Pattern.compile("^\\s*WITH\\b", Pattern.CASE_INSENSITIVE);

  /**
   * Strips CTE (WITH ... AS (...)) prefix from a SQL query, returning only the main query body.
   * Handles multiple CTEs, nested parentheses, and the RECURSIVE keyword.
   */
  static String stripCtePrefix(String sql) {
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

  /**
   * Remove subqueries (nested SELECT statements) from SQL to avoid parsing their internals. Uses a
   * simple parenthesis-depth approach.
   */
  static String removeSubqueries(String sql) {
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
}
