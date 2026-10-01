package io.queryaudit.core.parser;

import static io.queryaudit.core.parser.SqlIdentifiers.isKeyword;
import static io.queryaudit.core.parser.SqlIdentifiers.unquoteIdentifier;
import static io.queryaudit.core.parser.SqlStatementScope.stripCtePrefix;
import static io.queryaudit.core.parser.SqlText.stripComments;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Baseline FROM/JOIN table-name extraction and qualified-name handling. */
final class SqlTableNames {
  private SqlTableNames() {}

  // ── extractTableNames ──────────────────────────────────────────────

  // An identifier segment: backtick-quoted, double-quoted, or bare.
  private static final String IDENT_SEGMENT = "(?:`\\w+`|\"\\w+\"|\\w+)";

  // schema-qualified name: optional schema segment followed by a table segment.
  // Each segment may independently be quoted (supports "schema"."table", `s`.`t`,
  // and mixed forms like "schema".table).
  private static final String QUALIFIED_NAME =
      "(" + IDENT_SEGMENT + "(?:\\." + IDENT_SEGMENT + ")?)";

  private static final Pattern FROM_TABLE =
      Pattern.compile(
          "\\bFROM\\s+" + QUALIFIED_NAME + "(?:\\s+(?:AS\\s+)?(?:[`\"]?\\w+[`\"]?))?",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern JOIN_TABLE =
      Pattern.compile(
          "\\bJOIN\\s+" + QUALIFIED_NAME + "(?:\\s+(?:AS\\s+)?(?:[`\"]?\\w+[`\"]?))?",
          Pattern.CASE_INSENSITIVE);

  static List<String> extractTableNames(String sql) {
    List<String> result = new ArrayList<>();
    if (sql == null) {
      return result;
    }

    sql = stripComments(sql);
    sql = stripCtePrefix(sql);

    Matcher fromMatcher = FROM_TABLE.matcher(sql);
    while (fromMatcher.find()) {
      addTableFromQualifiedMatch(fromMatcher.group(1), result);
    }

    Matcher joinMatcher = JOIN_TABLE.matcher(sql);
    while (joinMatcher.find()) {
      addTableFromQualifiedMatch(joinMatcher.group(1), result);
    }

    return result;
  }

  /**
   * Extracts the table name from a (possibly schema-qualified, possibly quoted) identifier and adds
   * it to {@code result} if it is a non-keyword not already present. Each segment is unquoted
   * independently so {@code "schema"."table"}, {@code `schema`.`table`}, and mixed forms all
   * resolve to the table segment.
   */
  static void addTableFromQualifiedMatch(String qualified, List<String> result) {
    if (qualified == null) {
      return;
    }
    // A '.' inside a quoted segment would be illegal under our regex (segments are \w+),
    // so splitting on the last unquoted dot is equivalent to lastIndexOf('.').
    int dot = qualified.lastIndexOf('.');
    String tableSegment = dot >= 0 ? qualified.substring(dot + 1) : qualified;
    String table = unquoteIdentifier(tableSegment);
    if (table != null && !isKeyword(table) && !result.contains(table)) {
      result.add(table);
    }
  }
}
