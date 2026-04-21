package io.queryaudit.core.detector;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.parser.EnhancedSqlParser;
import io.queryaudit.core.parser.SqlParser;
import io.queryaudit.core.parser.WhereColumnReference;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Detects the DELETE-all + re-INSERT pattern that occurs with unidirectional {@code @OneToMany} or
 * {@code @ManyToMany} using {@code List<>} in Hibernate/JPA.
 *
 * <p>When a collection mapped this way is modified, Hibernate deletes all rows from the join table
 * for the parent entity, then re-inserts every element. This is extremely wasteful for large
 * collections.
 *
 * <p>Detection pattern: a DELETE with a single FK column in WHERE, followed by multiple INSERTs to
 * the same table.
 *
 * @author haroya
 * @since 0.2.0
 */
public class CollectionManagementDetector implements DetectionRule {

  private static final int DEFAULT_MIN_INSERTS = 2;

  private final int minInserts;

  public CollectionManagementDetector() {
    this(DEFAULT_MIN_INSERTS);
  }

  public CollectionManagementDetector(int minInserts) {
    this.minInserts = minInserts;
  }

  @Override
  public List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata indexMetadata) {
    List<Issue> issues = new ArrayList<>();
    Set<String> flaggedTables = new HashSet<>();

    for (int i = 0; i < queries.size(); i++) {
      String sql = queries.get(i).sql();
      if (!SqlParser.isDeleteQuery(sql)) {
        continue;
      }

      String deleteTable = SqlParser.extractDeleteTable(sql);
      if (deleteTable == null) {
        continue;
      }

      // Already flagged this table in this test run
      if (flaggedTables.contains(deleteTable.toLowerCase())) {
        continue;
      }

      // Extract WHERE columns from the DELETE
      List<WhereColumnReference> whereColumns = EnhancedSqlParser.extractWhereColumnsWithOperators(sql);
      if (whereColumns.size() != 1) {
        continue;
      }

      // Count subsequent INSERTs to the same table
      int insertCount = 0;
      for (int j = i + 1; j < queries.size(); j++) {
        String nextSql = queries.get(j).sql();
        if (SqlParser.isInsertQuery(nextSql)) {
          String insertTable = SqlParser.extractInsertTable(nextSql);
          if (deleteTable.equalsIgnoreCase(insertTable)) {
            insertCount++;
          } else {
            break;
          }
        } else {
          break;
        }
      }

      if (insertCount >= minInserts) {
        flaggedTables.add(deleteTable.toLowerCase());
        issues.add(
            new Issue(
                IssueType.COLLECTION_DELETE_REINSERT,
                Severity.WARNING,
                sql,
                deleteTable,
                whereColumns.get(0).columnName(),
                "DELETE-all + "
                    + insertCount
                    + " re-INSERTs detected on table '"
                    + deleteTable
                    + "'. The DELETE has a single FK column '"
                    + whereColumns.get(0).columnName()
                    + "' in WHERE, followed by "
                    + insertCount
                    + " INSERTs.",
                "DELETE-all + re-INSERT pattern on table '"
                    + deleteTable
                    + "'. This typically happens with unidirectional @OneToMany or @ManyToMany using List<>. "
                    + "Use bidirectional mapping with @JoinColumn, or change the collection type to Set<>."));
      }
    }

    return issues;
  }
}
