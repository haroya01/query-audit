package io.queryaudit.core.reporter;

import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** A lexical identity for SQL shapes, without interpreting or rewriting SQL expressions. */
final class FindingQuery {
  private static final Pattern NUMBER =
      Pattern.compile(
          "(?:0[xX][0-9a-fA-F_]+|0[bB][01_]+|0[oO][0-7_]+|"
              + "(?:\\d[\\d_]*(?:\\.[\\d_]*)?|\\.[\\d_]+)(?:[eE][+-]?[\\d_]+)?)");
  private static final Pattern DOLLAR_QUOTE =
      Pattern.compile("\\$(?:[\\p{L}_][\\p{L}\\p{N}_]*)?\\$");
  private static final Set<String> KEYWORDS =
      Set.of(
          "select",
          "from",
          "where",
          "and",
          "or",
          "not",
          "as",
          "join",
          "on",
          "group",
          "by",
          "having",
          "order",
          "asc",
          "desc",
          "insert",
          "into",
          "values",
          "update",
          "set",
          "delete",
          "distinct",
          "union",
          "all",
          "is",
          "in",
          "exists",
          "between",
          "like",
          "case",
          "when",
          "then",
          "else",
          "end");

  private FindingQuery() {}

  static String canonicalize(String sql) {
    StringBuilder tokens = new StringBuilder();
    for (int index = 0; index < sql.length(); ) {
      char current = sql.charAt(index);
      if (Character.isWhitespace(current)) {
        index++;
        continue;
      }
      if (lineComment(sql, index)) {
        index = lineEnd(sql, index + 2);
        continue;
      }
      if (sql.startsWith("/*", index)) {
        int end = commentEnd(sql, index + 2);
        if (end < 0) {
          token(tokens, sql.substring(index));
          break;
        }
        // MySQL executable comments and optimizer hints can change execution behavior.
        if (sql.startsWith("/*!", index) || sql.startsWith("/*+", index)) {
          token(tokens, sql.substring(index, end));
        }
        index = end;
        continue;
      }
      int literalStart = index;
      boolean explicitEscape = false;
      if ((current == 'E' || current == 'e')
          && index + 1 < sql.length()
          && sql.charAt(index + 1) == '\'') {
        literalStart++;
        explicitEscape = true;
      }
      if (sql.charAt(literalStart) == '\'') {
        int end = quotedEnd(sql, literalStart, '\'', explicitEscape);
        if (!explicitEscape && end != quotedEnd(sql, literalStart, '\'', true)) {
          end = -1;
        }
        if (end < 0) {
          // Unknown or mode-dependent quoting cannot establish equivalent statement structure.
          token(tokens, sql.substring(index));
          break;
        }
        token(tokens, "?");
        index = end;
        continue;
      }
      if (current == '$') {
        Matcher delimiter = DOLLAR_QUOTE.matcher(sql).region(index, sql.length());
        if (delimiter.lookingAt()) {
          String marker = delimiter.group();
          int end = sql.indexOf(marker, delimiter.end());
          if (end < 0) {
            token(tokens, sql.substring(index));
            break;
          }
          token(tokens, "?");
          index = end + marker.length();
          continue;
        }
      }
      if (current == '"' || current == '`' || current == '[') {
        char close = current == '[' ? ']' : current;
        int end = quotedEnd(sql, index, close, false);
        if (end < 0) {
          token(tokens, sql.substring(index));
          break;
        }
        token(tokens, sql.substring(index, end));
        index = end;
        continue;
      }
      Matcher number = NUMBER.matcher(sql).region(index, sql.length());
      if (number.lookingAt()
          && (number.end() == sql.length() || !wordPart(sql.charAt(number.end())))) {
        token(tokens, "?");
        index = number.end();
        continue;
      }
      if (wordPart(current)) {
        int end = index + 1;
        while (end < sql.length() && wordPart(sql.charAt(end))) {
          end++;
        }
        String word = sql.substring(index, end);
        String lowercase = word.toLowerCase(Locale.ROOT);
        token(
            tokens,
            switch (lowercase) {
              case "true", "false", "null" -> "?";
                // Unquoted table names and aliases can be case-sensitive in some databases.
              default -> KEYWORDS.contains(lowercase) ? lowercase : word;
            });
        index = end;
        continue;
      }
      int end = operatorEnd(sql, index);
      token(tokens, sql.substring(index, end));
      index = end;
    }
    return tokens.toString();
  }

  private static void token(StringBuilder tokens, String token) {
    tokens.append(token.length()).append(':').append(token);
  }

  private static boolean wordPart(char value) {
    return Character.isLetterOrDigit(value) || value == '_' || value == '$';
  }

  private static int operatorEnd(String sql, int start) {
    // A bind marker is its own token; PostgreSQL also uses these two question-mark operators.
    if (sql.startsWith("?|", start) || sql.startsWith("?&", start)) {
      return start + 2;
    }
    int end = start + 1;
    if (operator(sql.charAt(start))) {
      while (end < sql.length()
          && operator(sql.charAt(end))
          && !lineComment(sql, end)
          && !sql.startsWith("/*", end)) {
        end++;
      }
    }
    return end;
  }

  private static boolean operator(char value) {
    return "+-*/<>=!~^|&:#@".indexOf(value) >= 0;
  }

  private static boolean lineComment(String sql, int start) {
    // Without whitespace after --, MySQL interprets this as subtraction rather than a comment.
    return sql.startsWith("--", start)
        && start + 2 < sql.length()
        && (Character.isWhitespace(sql.charAt(start + 2))
            || Character.isISOControl(sql.charAt(start + 2)));
  }

  private static int lineEnd(String sql, int start) {
    int index = start;
    while (index < sql.length() && sql.charAt(index) != '\n' && sql.charAt(index) != '\r') {
      index++;
    }
    return index;
  }

  private static int commentEnd(String sql, int start) {
    int index = start;
    while (index < sql.length()) {
      if (sql.startsWith("/*", index)) {
        // Databases disagree on nested comments. Do not discard uncertain statement structure.
        return -1;
      } else if (sql.startsWith("*/", index)) {
        return index + 2;
      } else {
        index++;
      }
    }
    return -1;
  }

  private static int quotedEnd(String sql, int start, char quote, boolean backslashEscapes) {
    for (int index = start + 1; index < sql.length(); index++) {
      char value = sql.charAt(index);
      if (value == '\\' && backslashEscapes) {
        index++;
      } else if (value == quote) {
        if (index + 1 < sql.length() && sql.charAt(index + 1) == quote) {
          index++;
        } else {
          return index + 1;
        }
      }
    }
    return -1;
  }
}
