package io.queryaudit.core.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Lexical transformations: comments, literals, normalization and delimiter scans. No statement or
 * detector policy.
 */
final class SqlText {
  private SqlText() {}

  // -- stripComments ----------------------------------------------------------

  /**
   * Strips SQL comments from the input while preserving content inside string literals. Handles
   * block comments (including nested), line comments, and preserves comment-like content inside
   * string literals.
   *
   * @param sql the SQL string to strip comments from
   * @return the SQL with comments replaced by a single space, or null if input is null
   */
  static String stripComments(String sql) {
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

  private static final Pattern NUMBERS =
      Pattern.compile("\\b(?:0x[0-9a-fA-F]+|\\d+(?:\\.\\d+)?(?:[eE][+-]?\\d+)?)\\b");

  private static final Pattern IN_LIST =
      Pattern.compile("\\bIN\\s*\\(\\s*\\?(?:\\s*,\\s*\\?)*+\\s*\\)", Pattern.CASE_INSENSITIVE);

  private static final Pattern WHITESPACE = Pattern.compile("\\s+");

  /**
   * Normalize a SQL query by replacing literal values with {@code ?}, collapsing whitespace, and
   * lowercasing. Useful for grouping identical query patterns (e.g. N+1 detection).
   */
  static String normalize(String sql) {
    if (sql == null) {
      return null;
    }
    String result = stripComments(sql);
    result = replaceStringLiterals(result);
    result = NUMBERS.matcher(result).replaceAll("?");
    result = IN_LIST.matcher(result).replaceAll("IN (?)");
    result = WHITESPACE.matcher(result).replaceAll(" ");
    return result.trim().toLowerCase();
  }

  /**
   * Replaces single-quoted string literals with {@code ?}, handling SQL-standard escaped quotes
   * ({@code ''}) and MySQL backslash escaping ({@code \'}). Double-quoted identifiers are preserved
   * per SQL standard (PostgreSQL, Oracle, SQL Server). Uses a manual loop instead of regex to avoid
   * StackOverflowError on large inputs.
   *
   * <p>Public so other modules (notably suppression-pattern matching in {@code QueryAuditConfig})
   * can mask literals before doing structural comparisons against the SQL.
   *
   * @since 0.4.0
   */
  static String replaceStringLiterals(String sql) {
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
        // Double-quoted identifier (SQL standard): preserve content, will be lowercased later
        sb.append('"');
        i++;
        while (i < sql.length()) {
          char inner = sql.charAt(i);
          if (inner == '"' && i + 1 < sql.length() && sql.charAt(i + 1) == '"') {
            // Escaped quote inside identifier (""): keep both
            sb.append("\"\"");
            i += 2;
          } else if (inner == '"') {
            // Closing quote
            sb.append('"');
            i++;
            break;
          } else {
            sb.append(inner);
            i++;
          }
        }
      } else {
        sb.append(c);
        i++;
      }
    }
    return sb.toString();
  }

  /** Split a string by commas that are at the top level (not inside parentheses). */
  static List<String> splitByTopLevelCommas(String s) {
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
}
