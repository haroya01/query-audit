package io.queryaudit.core.parser;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Shared alias lookup for detectors using the same lightweight SQL reference rules. */
public final class SqlTableReferences {
  private SqlTableReferences() {}

  // A single identifier segment: bare \w+, backtick-quoted, or double-quoted.
  // Hibernate emits backtick- or double-quoted identifiers under
  // hibernate.globally_quoted_identifiers=true or for reserved-word table names.
  private static final String IDENT_SEGMENT = "(?:\\w+|`[^`]+`|\"[^\"]+\")";

  // Matches "FROM <table>" with optional schema/database prefixes (e.g. "myschema.users",
  // "db.schema.users", "`messages`", "\"users\"") and an optional bare alias.
  private static final Pattern FROM_ALIAS =
      Pattern.compile(
          "\\bFROM\\s+((?:"
              + IDENT_SEGMENT
              + "\\.){0,2}"
              + IDENT_SEGMENT
              + ")(?:\\s+(?:AS\\s+)?(\\w+))?",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern JOIN_ALIAS =
      Pattern.compile(
          "\\bJOIN\\s+((?:"
              + IDENT_SEGMENT
              + "\\.){0,2}"
              + IDENT_SEGMENT
              + ")(?:\\s+(?:AS\\s+)?(\\w+))?",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern QUOTED_IDENT = Pattern.compile("`([^`]+)`|\"([^\"]+)\"");

  /**
   * Build a mapping from alias (or table name) to actual table name. For example: "FROM orders o
   * JOIN users u ON ..." produces {o -> orders, u -> users, orders -> orders, users -> users}.
   */
  public static Map<String, String> resolveAliases(String sql) {
    Map<String, String> aliasToTable = new HashMap<>();

    Matcher fromMatcher = FROM_ALIAS.matcher(sql);
    while (fromMatcher.find()) {
      registerAlias(aliasToTable, fromMatcher.group(1), fromMatcher.group(2));
    }

    Matcher joinMatcher = JOIN_ALIAS.matcher(sql);
    while (joinMatcher.find()) {
      registerAlias(aliasToTable, joinMatcher.group(1), joinMatcher.group(2));
    }

    return aliasToTable;
  }

  private static void registerAlias(
      Map<String, String> aliasToTable, String tableToken, String aliasToken) {
    if (tableToken == null) {
      return;
    }
    // Strip backticks / double-quotes from each segment so the canonical key matches the
    // bare names produced by the WHERE-column extractor (JSqlParser already unquotes).
    String normalized = unquoteSegments(tableToken).toLowerCase();
    // The token may be schema-qualified ("myschema.users", "db.schema.users"). Drop the prefix so
    // detectors look up metadata under the canonical bare name; preserve the qualified form too so
    // `WHERE myschema.users.col` resolves through the same map.
    String unqualified = stripSchemaPrefix(normalized);
    if (isKeyword(unqualified)) {
      return;
    }
    aliasToTable.put(unqualified, unqualified);
    if (!normalized.equals(unqualified)) {
      aliasToTable.put(normalized, unqualified);
    }
    if (aliasToken != null && !isKeyword(aliasToken)) {
      aliasToTable.put(aliasToken.toLowerCase(), unqualified);
    }
  }

  private static String stripSchemaPrefix(String token) {
    int lastDot = token.lastIndexOf('.');
    return lastDot < 0 ? token : token.substring(lastDot + 1);
  }

  private static String unquoteSegments(String token) {
    if (token == null || token.indexOf('`') < 0 && token.indexOf('"') < 0) {
      return token;
    }
    Matcher m = QUOTED_IDENT.matcher(token);
    StringBuilder out = new StringBuilder();
    while (m.find()) {
      String inner = m.group(1) != null ? m.group(1) : m.group(2);
      m.appendReplacement(out, Matcher.quoteReplacement(inner));
    }
    m.appendTail(out);
    return out.toString();
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
          "cross",
          "natural",
          "full",
          "using");

  private static boolean isKeyword(String word) {
    return word != null && SQL_KEYWORDS.contains(word.toLowerCase());
  }
}
