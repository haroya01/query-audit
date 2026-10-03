package io.queryaudit.core.parser;

/** Literal-aware scans used when AST rendering would lose source text or recurse too deeply. */
final class SqlSourceScanner {
  /** Source-preserving clause rendering after AST validation; never calls Expression.toString(). */
  static String clauseBody(String sql, String keyword, String... terminators) {
    int start = scanForKeyword(sql, 0, keyword);
    if (start < 0) return null;
    int bodyStart = start + keyword.length();
    return clauseBodyFrom(sql, bodyStart, terminators);
  }

  /**
   * Extracts a clause body starting from a given position, scanning for the nearest terminator.
   * Uses literal-aware scanning with parenthesis depth tracking.
   *
   * @param sql the SQL string
   * @param bodyStart the position where the clause body starts (after the keyword)
   * @param terminators clause terminator keywords
   * @return the clause body, or null if no body found
   */
  static String clauseBodyFrom(String sql, int bodyStart, String... terminators) {
    int bodyEnd = sql.length();
    for (String terminator : terminators) {
      int position = scanForKeyword(sql, bodyStart, terminator);
      if (position >= 0 && position < bodyEnd) bodyEnd = position;
    }
    if (bodyStart >= bodyEnd) return null;
    return sql.substring(bodyStart, bodyEnd).trim();
  }

  /**
   * Returns true when {@code keyword} is a real top-level clause of the statement rather than text
   * that merely spells it.
   *
   * <p>Reuses the literal-aware scan, so the keyword is only recognised outside single-quoted
   * literals (including SQL-standard and backslash escapes), double-quoted and backtick-quoted
   * identifiers, and line and block comments, and only at parenthesis depth zero so a nested
   * subquery cannot stand in for the outer statement's clause.
   *
   * <p>A keyword also has to be followed by a table reference to count. That rejects a bare token
   * such as {@code DELETE FROM t USING} with nothing after it, which is not a clause of any
   * statement form.
   */
  static boolean hasTopLevelClause(String sql, String keyword) {
    if (sql == null) {
      return false;
    }
    int len = sql.length();
    int from = 0;
    while (from < len) {
      int index = scanForKeyword(sql, from, keyword);
      if (index < 0) {
        return false;
      }
      int bodyStart = index + getKeywordEffectiveLength(sql, index, keyword);
      if (startsTableReference(sql, bodyStart)) {
        return true;
      }
      from = bodyStart;
    }
    return false;
  }

  /**
   * Returns true when the next non-whitespace, non-comment character can begin the table reference
   * that a JOIN or USING clause must be followed by.
   */
  private static boolean startsTableReference(String sql, int from) {
    int len = sql.length();
    int i = from;
    while (i < len) {
      char c = sql.charAt(i);
      if (Character.isWhitespace(c)) {
        i++;
        continue;
      }
      if (c == '/' && i + 1 < len && sql.charAt(i + 1) == '*') {
        i = skipBlockComment(sql, i + 2);
        continue;
      }
      if (c == '-' && i + 1 < len && sql.charAt(i + 1) == '-') {
        i = skipLineComment(sql, i + 2);
        continue;
      }
      return isSqlIdentifierPart(c) || c == '"' || c == '`' || c == '[' || c == '(';
    }
    return false;
  }

  static boolean containsNestedSelect(String sql) {
    int len = sql.length();
    for (int i = 0; i < len; i++) {
      if (sql.charAt(i) != '(') continue;
      int j = i + 1;
      while (j < len && Character.isWhitespace(sql.charAt(j))) j++;
      if (j + 6 > len) return false;
      char c0 = sql.charAt(j);
      char c1 = sql.charAt(j + 1);
      char c2 = sql.charAt(j + 2);
      char c3 = sql.charAt(j + 3);
      char c4 = sql.charAt(j + 4);
      char c5 = sql.charAt(j + 5);
      if ((c0 == 'S' || c0 == 's')
          && (c1 == 'E' || c1 == 'e')
          && (c2 == 'L' || c2 == 'l')
          && (c3 == 'E' || c3 == 'e')
          && (c4 == 'C' || c4 == 'c')
          && (c5 == 'T' || c5 == 't')) {
        return true;
      }
    }
    return false;
  }

  private SqlSourceScanner() {}

  /**
   * Find the next case-insensitive whole-word occurrence of {@code keyword} starting at {@code
   * from}, skipping content inside single-quoted string literals, double-quoted identifiers,
   * and comments. Tracks parenthesis depth to avoid matching keywords inside nested subqueries.
   * Supports multiword keywords with flexible whitespace between words.
   * Returns -1 if not found at top level (parenthesis depth 0).
   */
  static int scanForKeyword(String sql, int from, String keyword) {
    int len = sql.length();
    int klen = keyword.length();
    int i = from;
    int parenDepth = 0;

    while (i < len) {
      char c = sql.charAt(i);

      // Track parenthesis depth for nested query detection
      if (c == '(') {
        parenDepth++;
        i++;
        continue;
      }
      if (c == ')') {
        if (parenDepth > 0) parenDepth--;
        i++;
        continue;
      }

      // Skip single-quoted string literals
      if (c == '\'') {
        i = skipSingleQuotedLiteral(sql, i + 1);
        continue;
      }

      // Skip double-quoted identifiers
      if (c == '"') {
        i = skipDoubleQuotedIdentifier(sql, i + 1);
        continue;
      }

      // Skip backtick-quoted identifiers (MySQL)
      if (c == '`') {
        i = skipBacktickQuotedIdentifier(sql, i + 1);
        continue;
      }

      // Skip block comments /* ... */
      if (c == '/' && i + 1 < len && sql.charAt(i + 1) == '*') {
        i = skipBlockComment(sql, i + 2);
        continue;
      }

      // Skip line comments -- ...
      if (c == '-' && i + 1 < len && sql.charAt(i + 1) == '-') {
        i = skipLineComment(sql, i + 2);
        continue;
      }

      // Only match keywords at top level (parenDepth == 0)
      if (parenDepth == 0 && matchesKeyword(sql, i, keyword)) {
        if (isKeywordBoundary(sql, i, keyword)) {
          return i;
        }
      }
      i++;
    }
    return -1;
  }

  /**
   * Check if the keyword matches at position i, supporting multiword keywords with flexible whitespace.
   */
  private static boolean matchesKeyword(String sql, int i, String keyword) {
    int len = sql.length();
    int kwIdx = 0;
    int pos = i;

    while (kwIdx < keyword.length() && pos < len) {
      char kwChar = keyword.charAt(kwIdx);
      char sqlChar = sql.charAt(pos);

      if (Character.isWhitespace(kwChar)) {
        // Skip whitespace in keyword, match any whitespace in SQL
        while (kwIdx < keyword.length() && Character.isWhitespace(keyword.charAt(kwIdx))) {
          kwIdx++;
        }
        // Match at least one whitespace character in SQL
        if (pos >= len || !Character.isWhitespace(sqlChar)) {
          return false;
        }
        while (pos < len && Character.isWhitespace(sql.charAt(pos))) {
          pos++;
        }
      } else {
        // Case-insensitive character match
        if (Character.toLowerCase(kwChar) != Character.toLowerCase(sqlChar)) {
          return false;
        }
        kwIdx++;
        pos++;
      }
    }

    // Successfully matched all keyword characters
    return kwIdx == keyword.length();
  }

  /**
   * Check if the keyword at position i has proper word boundaries.
   * SQL identifier characters: letters, digits, underscore, $, #
   */
  private static boolean isKeywordBoundary(String sql, int i, String keyword) {
    int len = sql.length();
    int kwLen = getKeywordEffectiveLength(sql, i, keyword);

    // Left boundary: start of string or non-identifier character
    boolean leftOk = i == 0 || !isSqlIdentifierPart(sql.charAt(i - 1));

    // Right boundary: end of string or non-identifier character
    boolean rightOk = i + kwLen >= len || !isSqlIdentifierPart(sql.charAt(i + kwLen));

    return leftOk && rightOk;
  }

  /**
   * Get the effective length of the keyword as it appears in the SQL (including any whitespace).
   */
  private static int getKeywordEffectiveLength(String sql, int i, String keyword) {
    int len = sql.length();
    int kwIdx = 0;
    int pos = i;

    while (kwIdx < keyword.length() && pos < len) {
      char kwChar = keyword.charAt(kwIdx);
      if (Character.isWhitespace(kwChar)) {
        while (kwIdx < keyword.length() && Character.isWhitespace(keyword.charAt(kwIdx))) {
          kwIdx++;
        }
        while (pos < len && Character.isWhitespace(sql.charAt(pos))) {
          pos++;
        }
      } else {
        kwIdx++;
        pos++;
      }
    }

    return pos - i;
  }

  /**
   * Check if a character is a valid SQL identifier part (letter, digit, underscore, $, #).
   */
  private static boolean isSqlIdentifierPart(char c) {
    return Character.isLetterOrDigit(c) || c == '_' || c == '$' || c == '#';
  }

  private static int skipSingleQuotedLiteral(String sql, int i) {
    int len = sql.length();
    while (i < len) {
      char c = sql.charAt(i);
      if (c == '\\' && i + 1 < len) {
        i += 2; // MySQL backslash escape
      } else if (c == '\'' && i + 1 < len && sql.charAt(i + 1) == '\'') {
        i += 2; // SQL-standard escaped quote
      } else if (c == '\'') {
        i++; // Closing quote
        break;
      } else {
        i++;
      }
    }
    return i;
  }

  private static int skipDoubleQuotedIdentifier(String sql, int i) {
    int len = sql.length();
    while (i < len) {
      char c = sql.charAt(i);
      if (c == '"' && i + 1 < len && sql.charAt(i + 1) == '"') {
        i += 2; // Escaped quote inside identifier
      } else if (c == '"') {
        i++; // Closing quote
        break;
      } else {
        i++;
      }
    }
    return i;
  }

  private static int skipBacktickQuotedIdentifier(String sql, int i) {
    int len = sql.length();
    while (i < len) {
      char c = sql.charAt(i);
      if (c == '`') {
        i++; // Closing backtick
        break;
      }
      i++;
    }
    return i;
  }

  private static int skipBlockComment(String sql, int i) {
    int len = sql.length();
    int depth = 1;
    while (i < len && depth > 0) {
      if (i + 1 < len && sql.charAt(i) == '/' && sql.charAt(i + 1) == '*') {
        depth++;
        i += 2;
      } else if (i + 1 < len && sql.charAt(i) == '*' && sql.charAt(i + 1) == '/') {
        depth--;
        i += 2;
      } else {
        i++;
      }
    }
    return i;
  }

  private static int skipLineComment(String sql, int i) {
    int len = sql.length();
    while (i < len) {
      char c = sql.charAt(i);
      if (c == '\n' || c == '\r') break;
      i++;
    }
    return i;
  }
}
