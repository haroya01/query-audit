package io.queryaudit.core.detector;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.parser.SqlParser;
import io.queryaudit.core.parser.WhereColumnReference;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Detects the Spring Data derived delete anti-pattern where entities are first loaded via SELECT,
 * then deleted individually.
 *
 * <p>Spring Data derived delete methods (e.g., {@code deleteByStatus}) load all matching entities
 * into memory first, then issue individual DELETE statements for each entity. This is N+1 for
 * deletes.
 *
 * <p>Detection pattern: a SELECT with WHERE on a table, followed by 3+ individual DELETEs from the
 * same table with a single PK-style WHERE condition.
 *
 * @author haroya
 * @since 0.2.0
 */
public class DerivedDeleteDetector implements DetectionRule {

  private static final int DEFAULT_THRESHOLD = 3;

  /**
   * Pattern to detect a simple PK-style WHERE clause: WHERE column = ? Matches patterns like: WHERE
   * id = ? or WHERE id = 1
   */
  private static final Pattern PK_DELETE_PATTERN =
      Pattern.compile(
          "^\\s*DELETE\\s+FROM\\s+\\S+\\s+WHERE\\s+\\w+\\s*=\\s*\\S+\\s*$",
          Pattern.CASE_INSENSITIVE);

  private final int threshold;

  public DerivedDeleteDetector() {
    this(DEFAULT_THRESHOLD);
  }

  public DerivedDeleteDetector(int threshold) {
    this.threshold = threshold;
  }

  @Override
  public List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata indexMetadata) {
    List<Issue> issues = new ArrayList<>();
    Set<String> flaggedTables = new HashSet<>();

    for (int i = 0; i < queries.size(); i++) {
      String sql = queries.get(i).sql();
      if (!SqlParser.isSelectQuery(sql)) {
        continue;
      }

      if (!SqlParser.hasWhereClause(sql)) {
        continue;
      }

      // Extract the main table from the SELECT
      List<String> selectTables = SqlParser.extractTableNames(sql);
      if (selectTables.isEmpty()) {
        continue;
      }

      String selectTable = selectTables.get(0);
      if (flaggedTables.contains(selectTable.toLowerCase())) {
        continue;
      }

      // Look for subsequent DELETEs to the same table with single PK condition
      int deleteCount = 0;
      for (int j = i + 1; j < queries.size(); j++) {
        String nextSql = queries.get(j).sql();
        if (SqlParser.isDeleteQuery(nextSql)) {
          String deleteTable = SqlParser.extractDeleteTable(nextSql);
          if (selectTable.equalsIgnoreCase(deleteTable)) {
            // Check if DELETE has a single PK-style WHERE
            List<WhereColumnReference> whereCols =
                SqlParser.extractWhereColumnsWithOperators(nextSql);
            if (whereCols.size() == 1 && "=".equals(whereCols.get(0).operator())) {
              deleteCount++;
            } else {
              break;
            }
          } else {
            break;
          }
        } else if (SqlParser.isSelectQuery(nextSql)) {
          // Allow interleaved SELECTs (Hibernate may load associations)
          continue;
        } else {
          break;
        }
      }

      if (deleteCount >= threshold) {
        flaggedTables.add(selectTable.toLowerCase());
        issues.add(
            new Issue(
                IssueType.DERIVED_DELETE_LOADS_ENTITIES,
                Severity.WARNING,
                sql,
                selectTable,
                null,
                "SELECT followed by "
                    + deleteCount
                    + " individual DELETEs on table '"
                    + selectTable
                    + "'. Entities are loaded before being deleted one by one.",
                "SELECT then individual DELETEs pattern on table '"
                    + selectTable
                    + "'. Spring Data derived delete (deleteByStatus) loads all entities before deleting. "
                    + "Use @Modifying @Query(\"DELETE FROM entity WHERE condition\") for bulk deletes."));
      }
    }

    return issues;
  }
}
