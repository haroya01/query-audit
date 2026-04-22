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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

  /** Max WHERE columns we treat as a collection-DELETE shape (single FK or small composite). */
  private static final int MAX_WHERE_COLUMNS_FOR_COLLECTION = 4;

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

      // Allow composite owner keys (e.g. parent_id + discriminator) up to a small cardinality.
      List<WhereColumnReference> whereColumns =
          EnhancedSqlParser.extractWhereColumnsWithOperators(sql);
      if (whereColumns.isEmpty() || whereColumns.size() > MAX_WHERE_COLUMNS_FOR_COLLECTION) {
        continue;
      }

      // For composite WHERE, require each INSERT to carry the same (col, value) pairs as the
      // DELETE — otherwise it's a specific-row DELETE, not a collection reinsertion.
      Map<String, String> deleteWherePairs =
          whereColumns.size() > 1 ? extractWhereEqualityPairs(sql) : Map.of();
      if (whereColumns.size() > 1 && deleteWherePairs.size() != whereColumns.size()) {
        continue;
      }

      // Count subsequent INSERTs to the same table
      int insertCount = 0;
      for (int j = i + 1; j < queries.size(); j++) {
        String nextSql = queries.get(j).sql();
        if (SqlParser.isInsertQuery(nextSql)) {
          String insertTable = SqlParser.extractInsertTable(nextSql);
          if (deleteTable.equalsIgnoreCase(insertTable)) {
            if (!deleteWherePairs.isEmpty()
                && !insertMatchesDeleteWhereValues(nextSql, deleteWherePairs)) {
              insertCount = 0;
              break;
            }
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
        StringBuilder cols = new StringBuilder();
        for (int k = 0; k < whereColumns.size(); k++) {
          if (k > 0) cols.append(", ");
          cols.append(whereColumns.get(k).columnName());
        }
        String whereLabel =
            whereColumns.size() == 1
                ? "a single FK column '" + whereColumns.get(0).columnName() + "'"
                : "composite owner key (" + cols + ")";
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
                    + "'. The DELETE has "
                    + whereLabel
                    + " in WHERE, followed by "
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

  private static final Pattern WHERE_EQUALITY_PAIR =
      Pattern.compile("(?i)(?:\\w+\\.)?(\\w+)\\s*=\\s*('[^']*'|\\?|-?\\d+)");

  private static final Pattern INSERT_COLS_VALUES =
      Pattern.compile("(?i)INSERT\\s+INTO\\s+\\w+\\s*\\(([^)]+)\\)\\s+VALUES\\s*\\(([^)]+)\\)");

  /** Extracts {@code col = literal-or-?} pairs from a WHERE joined by AND, or an empty map. */
  private static Map<String, String> extractWhereEqualityPairs(String sql) {
    String where = EnhancedSqlParser.extractWhereBody(sql);
    if (where == null) {
      return Map.of();
    }
    Map<String, String> out = new LinkedHashMap<>();
    Matcher m = WHERE_EQUALITY_PAIR.matcher(where);
    while (m.find()) {
      out.put(m.group(1).toLowerCase(), m.group(2).trim());
    }
    return out;
  }

  private static boolean insertMatchesDeleteWhereValues(
      String insertSql, Map<String, String> deleteWherePairs) {
    Matcher m = INSERT_COLS_VALUES.matcher(insertSql);
    if (!m.find()) {
      return false;
    }
    String[] colTokens = m.group(1).split(",");
    String[] valTokens = m.group(2).split(",");
    if (colTokens.length != valTokens.length) {
      return false;
    }
    Map<String, String> insertColToVal = new HashMap<>();
    for (int i = 0; i < colTokens.length; i++) {
      insertColToVal.put(colTokens[i].trim().toLowerCase(), valTokens[i].trim());
    }
    for (Map.Entry<String, String> entry : deleteWherePairs.entrySet()) {
      String actual = insertColToVal.get(entry.getKey());
      if (actual == null) {
        return false;
      }
      // ? in the DELETE matches any INSERT value; otherwise literals must match exactly.
      if (!"?".equals(entry.getValue()) && !actual.equalsIgnoreCase(entry.getValue())) {
        return false;
      }
    }
    return true;
  }
}
