package io.queryaudit.core.detector;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.parser.ColumnReference;
import io.queryaudit.core.parser.EnhancedSqlParser;
import io.queryaudit.core.parser.SqlParser;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Detects LEFT JOIN tables that are never referenced in SELECT, WHERE, ORDER BY, GROUP BY, or
 * HAVING clauses.
 *
 * <p>Source: ICSME 2020 AP-29
 *
 * <p>Only LEFT JOINs are flagged because INNER JOINs affect row filtering even without explicit
 * column references.
 *
 * @author haroya
 * @since 0.2.0
 */
public class UnusedJoinDetector implements DetectionRule {

  /** Extracts LEFT JOIN table name and alias. Group 1 = table name, group 2 = alias (optional). */
  private static final Pattern LEFT_JOIN_TABLE =
      Pattern.compile(
          "\\bLEFT\\s+(?:OUTER\\s+)?JOIN\\s+`?(\\w+)`?(?:\\s+(?:AS\\s+)?`?(\\w+)`?)?",
          Pattern.CASE_INSENSITIVE);

  /** Extracts the SELECT clause body (between SELECT and FROM). */
  private static final Pattern SELECT_BODY =
      Pattern.compile(
          "\\bSELECT\\s+(?:DISTINCT\\s+|ALL\\s+)?(.+?)\\bFROM\\b",
          Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  /** Matches a qualified column reference like {@code alias.column} or {@code alias.*}. */
  private static final Pattern QUALIFIED_REF =
      Pattern.compile("\\b(\\w+)\\s*\\.\\s*(?:\\w+|\\*)", Pattern.CASE_INSENSITIVE);

  /** Matches unqualified SELECT * which implicitly references all joined tables. */
  private static final Pattern UNQUALIFIED_SELECT_STAR =
      Pattern.compile(
          "\\bSELECT\\s+(?:DISTINCT\\s+|ALL\\s+)?\\*\\s+FROM\\b", Pattern.CASE_INSENSITIVE);

  @Override
  public List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata indexMetadata) {
    List<Issue> issues = new ArrayList<>();
    Set<String> seen = new LinkedHashSet<>();

    for (QueryRecord query : queries) {
      String normalized = query.normalizedSql();
      if (normalized == null || seen.contains(normalized)) {
        continue;
      }
      seen.add(normalized);

      String sql = query.sql();
      if (sql == null || !SqlParser.isSelectQuery(sql)) {
        continue;
      }

      // SELECT * implicitly references all joined tables — no unused joins possible
      if (UNQUALIFIED_SELECT_STAR.matcher(sql).find()) {
        continue;
      }

      // Extract all LEFT JOINed table aliases
      List<String[]> leftJoins = extractLeftJoinAliases(sql);
      if (leftJoins.isEmpty()) {
        continue;
      }

      // Collect all referenced aliases/table qualifiers from the query clauses
      Set<String> referencedAliases = collectReferencedAliases(sql);

      // Check each LEFT JOINed table
      for (String[] join : leftJoins) {
        String tableName = join[0];
        String alias = join[1]; // may be same as tableName if no alias

        if (!referencedAliases.contains(alias.toLowerCase())
            && !referencedAliases.contains(tableName.toLowerCase())) {
          List<String> tables = EnhancedSqlParser.extractTableNames(sql);
          String mainTable = tables.isEmpty() ? null : tables.get(0);

          issues.add(
              new Issue(
                  IssueType.UNUSED_JOIN,
                  Severity.WARNING,
                  normalized,
                  mainTable,
                  null,
                  "LEFT JOIN table '"
                      + alias
                      + "' is never referenced in SELECT, WHERE, ORDER BY, GROUP BY, or HAVING",
                  "Table '"
                      + alias
                      + "' is LEFT JOINed but never referenced. Remove the unnecessary JOIN to reduce query cost.",
                  query.stackTrace()));
        }
      }
    }

    return issues;
  }

  /**
   * Extracts LEFT JOINed table names and their aliases.
   *
   * @return list of [tableName, effectiveAlias] pairs
   */
  private List<String[]> extractLeftJoinAliases(String sql) {
    List<String[]> result = new ArrayList<>();
    Matcher m = LEFT_JOIN_TABLE.matcher(sql);
    while (m.find()) {
      String tableName = m.group(1);
      String alias = m.group(2);
      if (alias == null) {
        alias = tableName;
      }
      result.add(new String[] {tableName, alias});
    }
    return result;
  }

  /**
   * Collects all table/alias qualifiers referenced in SELECT, WHERE, ORDER BY, GROUP BY, and HAVING
   * clauses.
   */
  private Set<String> collectReferencedAliases(String sql) {
    Set<String> aliases = new HashSet<>();

    // From SELECT clause
    Matcher selectMatcher = SELECT_BODY.matcher(sql);
    if (selectMatcher.find()) {
      collectQualifiedRefs(selectMatcher.group(1), aliases);
    }

    // From WHERE columns
    List<ColumnReference> whereCols = EnhancedSqlParser.extractWhereColumns(sql);
    for (ColumnReference col : whereCols) {
      if (col.tableOrAlias() != null) {
        aliases.add(col.tableOrAlias().toLowerCase());
      }
    }

    // From ORDER BY columns
    List<ColumnReference> orderByCols = EnhancedSqlParser.extractOrderByColumns(sql);
    for (ColumnReference col : orderByCols) {
      if (col.tableOrAlias() != null) {
        aliases.add(col.tableOrAlias().toLowerCase());
      }
    }

    // From GROUP BY columns
    List<ColumnReference> groupByCols = EnhancedSqlParser.extractGroupByColumns(sql);
    for (ColumnReference col : groupByCols) {
      if (col.tableOrAlias() != null) {
        aliases.add(col.tableOrAlias().toLowerCase());
      }
    }

    // From HAVING clause
    String havingClause = EnhancedSqlParser.extractHavingClause(sql);
    if (havingClause != null) {
      collectQualifiedRefs(havingClause, aliases);
    }

    return aliases;
  }

  /** Extracts all table/alias qualifiers from qualified column references (e.g., {@code t.col}). */
  private void collectQualifiedRefs(String clause, Set<String> aliases) {
    Matcher m = QUALIFIED_REF.matcher(clause);
    while (m.find()) {
      aliases.add(m.group(1).toLowerCase());
    }
  }
}
