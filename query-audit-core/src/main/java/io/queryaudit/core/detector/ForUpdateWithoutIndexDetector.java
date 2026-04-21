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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Detects {@code SELECT ... FOR UPDATE} or {@code SELECT ... FOR SHARE} without index coverage on
 * WHERE columns.
 *
 * <p>Based on InnoDB gap lock behavior: when a {@code FOR UPDATE} query's WHERE columns lack
 * indexes, InnoDB may escalate to table-level locking, blocking all concurrent writes.
 *
 * @author haroya
 * @since 0.2.0
 */
public class ForUpdateWithoutIndexDetector implements DetectionRule {

  private static final Pattern FOR_UPDATE_OR_SHARE =
      Pattern.compile("\\bFOR\\s+(?:UPDATE|SHARE)\\b", Pattern.CASE_INSENSITIVE);

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

      // Check if query contains FOR UPDATE or FOR SHARE
      if (!FOR_UPDATE_OR_SHARE.matcher(sql).find()) {
        continue;
      }

      // Extract WHERE columns and check index coverage
      List<ColumnReference> whereColumns = EnhancedSqlParser.extractWhereColumns(sql);
      if (whereColumns.isEmpty()) {
        // FOR UPDATE without any WHERE clause at all -- locks entire table
        List<String> tables = EnhancedSqlParser.extractTableNames(sql);
        String table = tables.isEmpty() ? null : tables.get(0);

        issues.add(
            new Issue(
                IssueType.FOR_UPDATE_WITHOUT_INDEX,
                Severity.ERROR,
                normalized,
                table,
                null,
                "FOR UPDATE without WHERE clause locks entire table",
                "Add a WHERE clause with indexed columns to narrow the lock scope.",
                query.stackTrace()));
        continue;
      }

      Map<String, String> aliasToTable = MissingIndexDetector.resolveAliases(sql);

      for (ColumnReference col : whereColumns) {
        String table = resolveTable(col.tableOrAlias(), aliasToTable);
        if (table != null
            && indexMetadata.hasTable(table)
            && !indexMetadata.hasIndexOn(table, col.columnName())) {
          issues.add(
              new Issue(
                  IssueType.FOR_UPDATE_WITHOUT_INDEX,
                  Severity.ERROR,
                  normalized,
                  table,
                  col.columnName(),
                  "FOR UPDATE without index on WHERE column '"
                      + col.columnName()
                      + "' may lock entire table in InnoDB",
                  "Add an index on '"
                      + col.columnName()
                      + "' to enable row-level locking: ALTER TABLE "
                      + table
                      + " ADD INDEX idx_"
                      + col.columnName()
                      + " ("
                      + col.columnName()
                      + ");",
                  query.stackTrace()));
        }
      }
    }

    return issues;
  }

  private String resolveTable(String tableOrAlias, Map<String, String> aliasToTable) {
    if (tableOrAlias != null) {
      String resolved = aliasToTable.get(tableOrAlias.toLowerCase());
      if (resolved != null) return resolved;
      // Don't use unresolved Hibernate aliases (e.g., m1_0, r1_0, us1_0) as table names
      if (tableOrAlias.matches("(?i)[a-z]{1,3}\\d+_\\d+")) return null;
      return tableOrAlias.toLowerCase();
    }
    if (aliasToTable.size() <= 2) {
      return aliasToTable.values().stream().findFirst().orElse(null);
    }
    return null;
  }
}
