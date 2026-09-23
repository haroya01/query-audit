package io.queryaudit.core.parser;

import static io.queryaudit.core.parser.SqlIdentifiers.firstNonNull;
import static io.queryaudit.core.parser.SqlText.replaceStringLiterals;
import static io.queryaudit.core.parser.SqlText.stripComments;

import java.util.OptionalLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Cheap statement classification, DML targets, SELECT-star and pagination patterns; no AST
 * allocation.
 */
final class SqlStatementPatterns {
  private SqlStatementPatterns() {}

  // ── isSelectQuery ──────────────────────────────────────────────────

  // Query type detection uses simple string prefix checks instead of regex.
  // These methods are called for every single query, so avoiding Matcher
  // allocation and regex engine overhead is a meaningful win.

  static boolean isSelectQuery(String sql) {
    return sql != null && startsWithKeyword(sql, "SELECT");
  }

  // ── DML query type detection ────────────────────────────────────────

  static boolean isInsertQuery(String sql) {
    return sql != null && startsWithKeyword(sql, "INSERT");
  }

  static boolean isUpdateQuery(String sql) {
    return sql != null && startsWithKeyword(sql, "UPDATE");
  }

  static boolean isDeleteQuery(String sql) {
    return sql != null && startsWithKeyword(sql, "DELETE");
  }

  static boolean isDmlQuery(String sql) {
    return isInsertQuery(sql) || isUpdateQuery(sql) || isDeleteQuery(sql);
  }

  /**
   * Checks if sql starts with the given keyword (case-insensitive) after skipping leading
   * whitespace, followed by a non-word character or end of string. Replaces regex-based
   * Pattern.compile("^\\s*KEYWORD\\b") checks.
   */
  static boolean startsWithKeyword(String sql, String keyword) {
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
      Pattern.compile("^\\s*UPDATE\\s+(?:`(\\w+)`|\"(\\w+)\"|(\\w+))", Pattern.CASE_INSENSITIVE);

  /** Extracts the target table name from an UPDATE statement. */
  static String extractUpdateTable(String sql) {
    if (sql == null) return null;
    Matcher m = UPDATE_TABLE.matcher(sql);
    if (m.find()) {
      return firstNonNull(m.group(1), m.group(2), m.group(3));
    }
    return null;
  }

  // ── extractDeleteTable ──────────────────────────────────────────────

  private static final Pattern DELETE_TABLE =
      Pattern.compile(
          "^\\s*DELETE\\s+FROM\\s+(?:`(\\w+)`|\"(\\w+)\"|(\\w+))", Pattern.CASE_INSENSITIVE);

  /** Extracts the target table name from a DELETE statement. */
  static String extractDeleteTable(String sql) {
    if (sql == null) return null;
    Matcher m = DELETE_TABLE.matcher(sql);
    if (m.find()) {
      return firstNonNull(m.group(1), m.group(2), m.group(3));
    }
    return null;
  }

  // ── extractInsertTable ──────────────────────────────────────────────

  private static final Pattern INSERT_TABLE =
      Pattern.compile(
          "^\\s*INSERT\\s+INTO\\s+(?:`(\\w+)`|\"(\\w+)\"|(\\w+))", Pattern.CASE_INSENSITIVE);

  /** Extracts the target table name from an INSERT statement. */
  static String extractInsertTable(String sql) {
    if (sql == null) return null;
    Matcher m = INSERT_TABLE.matcher(sql);
    if (m.find()) {
      return firstNonNull(m.group(1), m.group(2), m.group(3));
    }
    return null;
  }

  // ── hasSelectAll ───────────────────────────────────────────────────

  private static final Pattern SELECT_ALL =
      Pattern.compile(
          "\\bSELECT\\s+(?:ALL\\s+|DISTINCT\\s+)?(?:(?!COUNT\\s*\\(|EXISTS\\s*\\()(?:\\w+\\.)?\\*)",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern EXISTS_SELECT_STAR =
      Pattern.compile("\\bEXISTS\\s*\\(\\s*SELECT\\s+\\*", Pattern.CASE_INSENSITIVE);

  static boolean hasSelectAll(String sql) {
    if (sql == null) {
      return false;
    }
    sql = stripComments(sql);
    // Mask string literals so 'SELECT *' inside a literal (e.g. an audit-log row or stored
    // SQL template) does not produce a false positive.
    sql = replaceStringLiterals(sql);
    // Neutralise EXISTS(SELECT * ...) — it is idiomatic SQL and not a real SELECT *
    String sanitized = EXISTS_SELECT_STAR.matcher(sql).replaceAll("EXISTS(SELECT 1");
    return SELECT_ALL.matcher(sanitized).find();
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

  static OptionalLong extractOffsetValue(String sql) {
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
  static boolean hasOffsetClause(String sql) {
    if (sql == null) {
      return false;
    }
    if (OFFSET_ANY.matcher(sql).find()) {
      return true;
    }
    // MySQL-style LIMIT offset, count
    return LIMIT_COMMA_PARAM.matcher(sql).find();
  }
}
