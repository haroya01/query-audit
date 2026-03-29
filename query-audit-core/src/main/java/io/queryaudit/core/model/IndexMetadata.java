package io.queryaudit.core.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Aggregates index information for all tables and provides lookup methods to check whether
 * a given column is indexed, uniquely indexed, or part of a composite index. Serves as the
 * central index metadata store consumed by various issue detectors.
 *
 * @author haroya
 * @since 0.2.0
 */
public class IndexMetadata {

  private final Map<String, List<IndexInfo>> indexesByTable;

  public IndexMetadata(Map<String, List<IndexInfo>> indexesByTable) {
    this.indexesByTable = indexesByTable;
  }

  public boolean hasIndexOn(String table, String column) {
    if (table == null) {
      return false;
    }
    List<IndexInfo> indexes = indexesByTable.get(table);
    if (indexes == null) {
      return false;
    }
    return indexes.stream()
        .anyMatch(idx -> idx.columnName() != null && idx.columnName().equalsIgnoreCase(column));
  }

  /**
   * Returns {@code true} if the given column on the given table has a UNIQUE (or PRIMARY KEY)
   * single-column index. Convenience delegate to {@link #hasUniqueIndexCoveredBy(String, Set)}.
   */
  public boolean hasUniqueIndexOn(String table, String column) {
    if (column == null) {
      return false;
    }
    return hasUniqueIndexCoveredBy(table, Set.of(column));
  }

  /**
   * Returns {@code true} if there exists a UNIQUE (or PRIMARY KEY) index on the given table
   * whose columns are all contained in the provided set of equality columns. Handles both
   * single-column and composite unique indexes — when all columns of such an index appear as
   * equality conditions, the query is guaranteed to return at most one row.
   */
  public boolean hasUniqueIndexCoveredBy(String table, Set<String> columns) {
    if (table == null || columns == null || columns.isEmpty()) {
      return false;
    }
    List<IndexInfo> indexes = indexesByTable.get(table);
    if (indexes == null) {
      return false;
    }

    // Group unique index entries by index name
    Map<String, List<IndexInfo>> uniqueIndexes =
        indexes.stream()
            .filter(idx -> !idx.nonUnique() && idx.indexName() != null)
            .collect(Collectors.groupingBy(IndexInfo::indexName));

    // Check if any unique index has all its columns covered by the equality columns
    for (List<IndexInfo> indexEntries : uniqueIndexes.values()) {
      boolean allCovered =
          indexEntries.stream()
              .allMatch(
                  idx ->
                      idx.columnName() != null
                          && columns.stream()
                              .anyMatch(col -> col.equalsIgnoreCase(idx.columnName())));
      if (allCovered) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns {@code true} if the given set of columns fully covers all columns of any UNIQUE (or
   * PRIMARY KEY) index on the given table. This handles both single-column and composite unique
   * indexes. A full cover guarantees at most one row for equality predicates on all those columns.
   */
  public boolean columnsMatchUniqueIndex(String table, java.util.Set<String> columns) {
    if (table == null || columns == null || columns.isEmpty()) {
      return false;
    }
    List<IndexInfo> indexes = indexesByTable.get(table);
    if (indexes == null) {
      return false;
    }

    // Group unique indexes by name
    java.util.Map<String, List<IndexInfo>> uniqueIndexes =
        indexes.stream()
            .filter(idx -> !idx.nonUnique() && idx.indexName() != null)
            .collect(Collectors.groupingBy(IndexInfo::indexName));

    // Check if any unique index has all its columns covered by the given column set
    for (List<IndexInfo> indexColumns : uniqueIndexes.values()) {
      boolean allCovered =
          indexColumns.stream()
              .allMatch(
                  idx ->
                      idx.columnName() != null
                          && columns.stream()
                              .anyMatch(c -> c.equalsIgnoreCase(idx.columnName())));
      if (allCovered) {
        return true;
      }
    }
    return false;
  }

  public List<IndexInfo> getIndexesForTable(String table) {
    if (table == null) {
      return Collections.emptyList();
    }
    return indexesByTable.getOrDefault(table, Collections.emptyList());
  }

  public Map<String, List<IndexInfo>> getCompositeIndexes(String table) {
    List<IndexInfo> indexes = getIndexesForTable(table);
    return indexes.stream()
        .filter(idx -> idx.indexName() != null)
        .collect(Collectors.groupingBy(IndexInfo::indexName))
        .entrySet()
        .stream()
        .filter(entry -> entry.getValue().size() > 1)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * Returns {@code true} if the metadata contains index information for the given table. When a
   * table is absent from the metadata we cannot tell whether its columns are indexed, so detectors
   * should skip it to avoid false positives.
   */
  public boolean hasTable(String table) {
    return table != null && indexesByTable.containsKey(table);
  }

  public boolean isEmpty() {
    return indexesByTable.isEmpty();
  }

  /**
   * Merges another IndexMetadata into this one, returning a new combined instance. When index name
   * conflicts exist for the same table, entries from {@code this} (the primary/authoritative
   * source) are preferred over entries from {@code other}. Indexes that only exist in {@code other}
   * are added.
   *
   * @param other the supplementary metadata to merge (e.g., from JPA annotations)
   * @return a new IndexMetadata containing indexes from both sources
   */
  public IndexMetadata merge(IndexMetadata other) {
    if (other == null || other.isEmpty()) {
      return this;
    }
    if (this.isEmpty()) {
      return other;
    }

    Map<String, List<IndexInfo>> merged = new HashMap<>();

    // Copy all entries from this (primary source)
    for (Map.Entry<String, List<IndexInfo>> entry : this.indexesByTable.entrySet()) {
      merged.put(entry.getKey(), new ArrayList<>(entry.getValue()));
    }

    // Add entries from other, skipping index names that already exist in primary
    for (Map.Entry<String, List<IndexInfo>> entry : other.indexesByTable.entrySet()) {
      String table = entry.getKey();
      List<IndexInfo> otherIndexes = entry.getValue();

      if (!merged.containsKey(table)) {
        merged.put(table, new ArrayList<>(otherIndexes));
      } else {
        List<IndexInfo> existing = merged.get(table);
        Set<String> existingNames =
            existing.stream()
                .map(IndexInfo::indexName)
                .filter(Objects::nonNull)
                .map(String::toLowerCase)
                .collect(Collectors.toSet());

        for (IndexInfo info : otherIndexes) {
          String name = info.indexName();
          if (name == null || !existingNames.contains(name.toLowerCase())) {
            existing.add(info);
          }
        }
      }
    }

    return new IndexMetadata(merged);
  }
}
