package io.queryaudit.core.parser;

/** Literal-aware scans used when AST rendering would lose source text or recurse too deeply. */
final class SqlSourceScanner {
  /** Source-preserving clause rendering after AST validation; never calls Expression.toString(). */
  static String clauseBody(String sql, String keyword, String... terminators) {
    int start = scanForKeyword(sql, 0, keyword);
    if (start < 0) return null;
    int bodyStart = start + keyword.length();
    int bodyEnd = sql.length();
    for (String terminator : terminators) {
      int position = scanForKeyword(sql, bodyStart, terminator);
      if (position >= 0 && position < bodyEnd) bodyEnd = position;
    }
    return sql.substring(bodyStart, bodyEnd).trim();
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
   * from}, skipping content inside single-quoted string literals and double-quoted identifiers.
   * Returns -1 if not found.
   */
  private static int scanForKeyword(String sql, int from, String keyword) {
    int len = sql.length();
    int klen = keyword.length();
    int i = from;
    while (i < len) {
      char c = sql.charAt(i);
      if (c == '\'') {
        i++;
        while (i < len) {
          char q = sql.charAt(i);
          if (q == '\\' && i + 1 < len) {
            i += 2;
          } else if (q == '\'' && i + 1 < len && sql.charAt(i + 1) == '\'') {
            i += 2;
          } else if (q == '\'') {
            i++;
            break;
          } else {
            i++;
          }
        }
        continue;
      }
      if (c == '"') {
        i++;
        while (i < len && sql.charAt(i) != '"') i++;
        if (i < len) i++;
        continue;
      }
      // Word boundary check + keyword match
      if (i + klen <= len && sql.regionMatches(true, i, keyword, 0, klen)) {
        boolean leftOk = i == 0 || !Character.isLetterOrDigit(sql.charAt(i - 1));
        boolean rightOk = i + klen == len || !Character.isLetterOrDigit(sql.charAt(i + klen));
        if (leftOk && rightOk) {
          return i;
        }
      }
      i++;
    }
    return -1;
  }
}
