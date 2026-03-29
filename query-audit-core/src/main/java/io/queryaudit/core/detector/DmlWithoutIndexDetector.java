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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Detects UPDATE or DELETE statements where the WHERE clause columns are not covered by an index,
 * causing a full table scan + row-level locks.
 *
 * <p>Unlike a SELECT full scan which is just slow, a DML full scan also acquires locks on every
 * scanned row, which can cause severe lock contention.
 *
 * @author haroya
 * @since 0.2.0
 */
public class DmlWithoutIndexDetector implements DetectionRule {

  @Override
  public List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata indexMetadata) {
    List<Issue> issues = new ArrayList<>();
    if (indexMetadata == null || indexMetadata.isEmpty()) {
      return issues;
    }

    Set<String> seen = new HashSet<>();

    for (QueryRecord query : queries) {
      String sql = query.sql();
      String normalized = query.normalizedSql();
      if (normalized == null || !seen.add(normalized)) {
        continue;
      }

      boolean isUpdate = SqlParser.isUpdateQuery(sql);
      boolean isDelete = SqlParser.isDeleteQuery(sql);
      if (!isUpdate && !isDelete) {
        continue;
      }

      if (!SqlParser.hasWhereClause(sql)) {
        continue; // handled by UpdateWithoutWhereDetector
      }

      String table =
          isUpdate ? SqlParser.extractUpdateTable(sql) : SqlParser.extractDeleteTable(sql);
      if (table == null) {
        continue;
      }

      String tableLower = table.toLowerCase();
      List<IndexInfo> tableIndexes = indexMetadata.getIndexesForTable(tableLower);
      if (tableIndexes == null || tableIndexes.isEmpty()) {
        continue; // no index info available for this table
      }

      List<ColumnReference> whereColumns = SqlParser.extractWhereColumns(sql);
      if (whereColumns.isEmpty()) {
        continue;
      }

      // Check if at least one WHERE column is the leading column of any index
      Set<String> leadingIndexColumns = new HashSet<>();
      for (IndexInfo idx : tableIndexes) {
        if (idx.seqInIndex() == 1 && idx.columnName() != null) {
          leadingIndexColumns.add(idx.columnName().toLowerCase());
        }
      }

      boolean hasIndexedColumn = false;
      for (ColumnReference col : whereColumns) {
        if (leadingIndexColumns.contains(col.columnName().toLowerCase())) {
          hasIndexedColumn = true;
          break;
        }
      }

      if (!hasIndexedColumn) {
        String dmlType = isUpdate ? "UPDATE" : "DELETE";
        String columnList =
            whereColumns.stream()
                .map(ColumnReference::columnName)
                .distinct()
                .reduce((a, b) -> a + ", " + b)
                .orElse("?");

        issues.add(
            new Issue(
                IssueType.DML_WITHOUT_INDEX,
                Severity.WARNING,
                normalized,
                table,
                columnList,
                dmlType
                    + " on '"
                    + table
                    + "' WHERE columns ["
                    + columnList
                    + "] have no matching index. This causes a full table scan with row locks.",
                "Add an index on the WHERE columns to avoid full table scan. "
                    + "DML full scans are worse than SELECT scans because they lock every scanned row."));
      }
    }
    return issues;
  }
}
