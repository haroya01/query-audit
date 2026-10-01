package io.queryaudit.core.parser;

import java.util.Set;
import java.util.regex.Pattern;

/** Shared identifier/literal recognition used by baseline extractors. */
final class SqlIdentifiers {
  private SqlIdentifiers() {}

  private static final Pattern LITERAL_VALUE =
      Pattern.compile(
          "^(?:\\d+(?:\\.\\d+)?(?:[eE][+-]?\\d+)?|0x[0-9a-fA-F]+|'[^']*'|\\?|true|false|null)$",
          Pattern.CASE_INSENSITIVE);

  /**
   * Returns true if the given string looks like a literal value (number, quoted string,
   * placeholder, or boolean keyword) rather than a column name.
   */
  static boolean isLiteralValue(String value) {
    return value != null && LITERAL_VALUE.matcher(value).matches();
  }

  // ── helpers ─────────────────────────────────────────────────────────

  /** Returns the first non-null value among the given strings. */
  static String firstNonNull(String... values) {
    for (String v : values) {
      if (v != null) return v;
    }
    return null;
  }

  /** Strips surrounding double quotes or backticks from an identifier. */
  static String unquoteIdentifier(String identifier) {
    if (identifier == null) return null;
    if (identifier.length() >= 2
        && ((identifier.charAt(0) == '"' && identifier.charAt(identifier.length() - 1) == '"')
            || (identifier.charAt(0) == '`'
                && identifier.charAt(identifier.length() - 1) == '`'))) {
      return identifier.substring(1, identifier.length() - 1);
    }
    return identifier;
  }

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
          "distinct",
          "count",
          "sum",
          "avg",
          "min",
          "max");

  static boolean isKeyword(String word) {
    return word != null && SQL_KEYWORDS.contains(word.toLowerCase());
  }
}
