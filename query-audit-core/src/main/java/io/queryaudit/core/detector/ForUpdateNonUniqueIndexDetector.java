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
 * Detects {@code FOR UPDATE} or {@code FOR SHARE} on columns that have a non-unique index.
 *
 * <p>MySQL docs: "Gap locking is not needed for statements that lock rows using a unique index to
 * search for a unique row." A non-unique index with FOR UPDATE causes next-key locks (record +
 * gap), blocking inserts in the locked range.
 *
 * <p>This complements {@link ForUpdateWithoutIndexDetector} which checks for columns with NO index
 * at all. This detector checks for columns that HAVE an index but it is non-unique.
 *
 * @author haroya
 * @since 0.2.0
 */
public class ForUpdateNonUniqueIndexDetector implements DetectionRule {

  private static final Pattern FOR_UPDATE_OR_SHARE =
      Pattern.compile("\\bFOR\\s+(?:UPDATE|SHARE)\\b", Pattern.CASE_INSENSITIVE);

  private static final Pattern FROM_ALIAS =
      Pattern.compile("\\bFROM\\s+(\\w+)(?:\\s+(?:AS\\s+)?(\\w+))?", Pattern.CASE_INSENSITIVE);

  private static final Pattern JOIN_ALIAS =
      Pattern.compile("\\bJOIN\\s+(\\w+)(?:\\s+(?:AS\\s+)?(\\w+))?", Pattern.CASE_INSENSITIVE);

  @Override
  public List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata indexMetadata) {
    List<Issue> issues = new ArrayList<>();

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
      if (sql == null) {
        continue;
      }

      if (!FOR_UPDATE_OR_SHARE.matcher(sql).find()) {
        continue;
      }

      List<ColumnReference> whereColumns = SqlParser.extractWhereColumns(sql);
      if (whereColumns.isEmpty()) {
        continue;
      }

      Map<String, String> aliasToTable = resolveAliases(sql);

      for (ColumnReference col : whereColumns) {
        String table = resolveTable(col.tableOrAlias(), aliasToTable);
        if (table == null || !indexMetadata.hasTable(table)) {
          continue;
        }

        if (!indexMetadata.hasIndexOn(table, col.columnName())) {
          // No index at all -- handled by ForUpdateWithoutIndexDetector
          continue;
        }

        // Column has an index -- check if ALL indexes on this column are non-unique
        if (isOnlyNonUniqueIndexed(indexMetadata, table, col.columnName())) {
          issues.add(
              new Issue(
                  IssueType.FOR_UPDATE_NON_UNIQUE,
                  Severity.WARNING,
                  normalized,
                  table,
                  col.columnName(),
                  "FOR UPDATE on non-unique index '"
                      + col.columnName()
                      + "' causes next-key locks (record + gap), blocking inserts in the locked range",
                  "FOR UPDATE on non-unique index '"
                      + col.columnName()
                      + "' causes gap locks, blocking inserts in the locked range. "
                      + "Consider using a unique key or reducing the locked scope.",
                  query.stackTrace()));
        }
      }
    }

    return issues;
  }

  /**
   * Returns true if the column has at least one index and ALL indexes on it are non-unique (i.e.,
   * no unique index or PRIMARY key covers this column).
   */
  private boolean isOnlyNonUniqueIndexed(IndexMetadata indexMetadata, String table, String column) {
    List<IndexInfo> indexes = indexMetadata.getIndexesForTable(table);
    boolean hasIndex = false;
    for (IndexInfo idx : indexes) {
      if (idx.columnName() != null && idx.columnName().equalsIgnoreCase(column)) {
        hasIndex = true;
        if (!idx.nonUnique()) {
          // Found a unique index or PRIMARY key on this column
          return false;
        }
      }
    }
    return hasIndex;
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
