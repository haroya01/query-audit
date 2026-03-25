package io.queryaudit.core.detector;

import io.queryaudit.core.model.IndexInfo;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.parser.ColumnReference;
import io.queryaudit.core.parser.SqlParser;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Detects ORDER BY + LIMIT where ORDER BY column is not unique, leading to non-deterministic
 * pagination (inconsistent results across pages).
 *
 * <p>Source: MySQL LIMIT optimization docs
 *
 * <p>Only flags when index metadata is available and can determine that no ORDER BY column has a
 * unique index.
 *
 * @author haroya
 * @since 0.2.0
 */
public class NonDeterministicPaginationDetector implements DetectionRule {

  private static final Pattern ORDER_BY_PATTERN =
      Pattern.compile("\\bORDER\\s+BY\\b", Pattern.CASE_INSENSITIVE);

  private static final Pattern LIMIT_PATTERN =
      Pattern.compile("\\bLIMIT\\b", Pattern.CASE_INSENSITIVE);

  private static final Pattern FROM_ALIAS =
      Pattern.compile(
          "\\bFROM\\s+`?(\\w+)`?(?:\\s+(?:AS\\s+)?`?(\\w+)`?)?", Pattern.CASE_INSENSITIVE);

  private static final Pattern JOIN_ALIAS =
      Pattern.compile(
          "\\bJOIN\\s+`?(\\w+)`?(?:\\s+(?:AS\\s+)?`?(\\w+)`?)?", Pattern.CASE_INSENSITIVE);

  @Override
  public List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata indexMetadata) {
    List<Issue> issues = new ArrayList<>();

    // Cannot determine uniqueness without index metadata
    if (indexMetadata == null || indexMetadata.isEmpty()) {
      return issues;
    }

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

      // Must have both ORDER BY and LIMIT
      if (!ORDER_BY_PATTERN.matcher(sql).find() || !LIMIT_PATTERN.matcher(sql).find()) {
        continue;
      }

      List<ColumnReference> orderByCols = SqlParser.extractOrderByColumns(sql);
      if (orderByCols.isEmpty()) {
        continue;
      }

      Map<String, String> aliasToTable = resolveAliases(sql);

      // Check if any ORDER BY column has a unique index
      boolean hasUniqueTiebreaker = false;
      for (ColumnReference col : orderByCols) {
        String table = resolveTable(col.tableOrAlias(), aliasToTable);
        if (table == null || !indexMetadata.hasTable(table)) {
          // Can't determine uniqueness — skip entire query to avoid false positives
          hasUniqueTiebreaker = true;
          break;
        }
        if (hasUniqueIndex(indexMetadata, table, col.columnName())) {
          hasUniqueTiebreaker = true;
          break;
        }
      }

      if (!hasUniqueTiebreaker) {
        List<String> tables = SqlParser.extractTableNames(sql);
        String mainTable = tables.isEmpty() ? null : tables.get(0);

        String orderByColNames =
            orderByCols.stream()
                .map(
                    c ->
                        c.tableOrAlias() != null
                            ? c.tableOrAlias() + "." + c.columnName()
                            : c.columnName())
                .reduce((a, b) -> a + ", " + b)
                .orElse("unknown");

        issues.add(
            new Issue(
                IssueType.NON_DETERMINISTIC_PAGINATION,
                Severity.INFO,
                normalized,
                mainTable,
                null,
                "ORDER BY "
                    + orderByColNames
                    + " with LIMIT but no unique tiebreaker column — pagination results may be inconsistent",
                "Add a unique tiebreaker column (e.g., id) to ORDER BY for consistent pagination results.",
                query.stackTrace()));
      }
    }

    return issues;
  }

  /** Returns true if the column has a unique (non-nonUnique) index on the given table. */
  private boolean hasUniqueIndex(IndexMetadata indexMetadata, String table, String column) {
    List<IndexInfo> indexes = indexMetadata.getIndexesForTable(table);
    for (IndexInfo idx : indexes) {
      if (idx.columnName() != null
          && idx.columnName().equalsIgnoreCase(column)
          && !idx.nonUnique()) {
        return true;
      }
    }
    return false;
  }

  private Map<String, String> resolveAliases(String sql) {
    Map<String, String> aliasToTable = new HashMap<>();

    Matcher fromMatcher = FROM_ALIAS.matcher(sql);
    while (fromMatcher.find()) {
      String table = fromMatcher.group(1);
      String alias = fromMatcher.group(2);
      aliasToTable.put(table.toLowerCase(), table.toLowerCase());
      if (alias != null) {
        aliasToTable.put(alias.toLowerCase(), table.toLowerCase());
      }
    }

    Matcher joinMatcher = JOIN_ALIAS.matcher(sql);
    while (joinMatcher.find()) {
      String table = joinMatcher.group(1);
      String alias = joinMatcher.group(2);
      aliasToTable.put(table.toLowerCase(), table.toLowerCase());
      if (alias != null) {
        aliasToTable.put(alias.toLowerCase(), table.toLowerCase());
      }
    }

    return aliasToTable;
  }

  private String resolveTable(String tableOrAlias, Map<String, String> aliasToTable) {
    if (tableOrAlias != null) {
      String resolved = aliasToTable.get(tableOrAlias.toLowerCase());
      if (resolved != null) return resolved;
      return tableOrAlias.toLowerCase();
    }
    if (aliasToTable.size() <= 2) {
      return aliasToTable.values().stream().findFirst().orElse(null);
    }
    return null;
  }
}
