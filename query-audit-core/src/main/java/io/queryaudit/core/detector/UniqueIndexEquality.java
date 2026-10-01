package io.queryaudit.core.detector;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.parser.ColumnReference;
import io.queryaudit.core.parser.EnhancedSqlParser;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Conservative proof for suppressing index advice, not an estimate of query selectivity. */
final class UniqueIndexEquality {
  private UniqueIndexEquality() {}

  static Set<String> singleRowTables(String sql, IndexMetadata metadata) {
    Map<String, Set<String>> columnsByTable = new HashMap<>();
    for (ColumnReference equality : EnhancedSqlParser.extractRequiredScalarEqualities(sql)) {
      columnsByTable
          .computeIfAbsent(equality.tableOrAlias(), ignored -> new HashSet<>())
          .add(equality.columnName());
    }
    Set<String> singleRowTables = new HashSet<>();
    for (Map.Entry<String, Set<String>> entry : columnsByTable.entrySet()) {
      if (metadata.hasUniqueIndexCoveredBy(entry.getKey(), entry.getValue())) {
        singleRowTables.add(entry.getKey());
      }
    }
    return Set.copyOf(singleRowTables);
  }
}
