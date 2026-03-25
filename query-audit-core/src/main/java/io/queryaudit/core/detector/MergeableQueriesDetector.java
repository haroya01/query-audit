package io.queryaudit.core.detector;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.parser.SqlParser;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Detects multiple SELECT queries to the same table with the same columns but different WHERE
 * conditions that could be merged into a single query using IN clause or batch query.
 *
 * <p>Source: ICSME 2020 AP-10
 *
 * <p>Distinguished from N+1: N+1 has identical normalized SQL, while this detector finds queries
 * with the same table and SELECT columns but different WHERE columns or structure.
 *
 * @author haroya
 * @since 0.2.0
 */
public class MergeableQueriesDetector implements DetectionRule {

  private static final int DEFAULT_THRESHOLD = 3;

  /** Extracts the SELECT columns part (between SELECT and FROM). */
  private static final Pattern SELECT_COLUMNS =
      Pattern.compile(
          "\\bSELECT\\s+(?:DISTINCT\\s+|ALL\\s+)?(.+?)\\bFROM\\b", Pattern.CASE_INSENSITIVE);

  /** Extracts the first table name from the FROM clause. */
  private static final Pattern FROM_TABLE =
      Pattern.compile("\\bFROM\\s+`?(\\w+)`?", Pattern.CASE_INSENSITIVE);

  /** Extracts the WHERE clause body for grouping key comparison. */
  private static final Pattern WHERE_BODY =
      Pattern.compile(
          "\\bWHERE\\b(.+?)(?:\\bGROUP\\s+BY\\b|\\bORDER\\s+BY\\b|\\bLIMIT\\b|\\bHAVING\\b|$)",
          Pattern.CASE_INSENSITIVE);

  private final int threshold;

  public MergeableQueriesDetector(int threshold) {
    this.threshold = threshold;
  }

  public MergeableQueriesDetector() {
    this(DEFAULT_THRESHOLD);
  }

  @Override
  public List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata indexMetadata) {
    List<Issue> issues = new ArrayList<>();

    // Group queries by: table name + normalized SELECT columns
    // Key = "table|normalizedColumns"
    Map<String, List<QueryRecord>> groups = new LinkedHashMap<>();

    for (QueryRecord query : queries) {
      String sql = query.sql();
      if (sql == null || !SqlParser.isSelectQuery(sql)) {
        continue;
      }

      String table = extractFromTable(sql);
      if (table == null) {
        continue;
      }

      String selectCols = extractNormalizedSelectColumns(sql);
      if (selectCols == null) {
        continue;
      }

      // Include JOIN structure in the group key — queries with different JOINs are
      // structurally different and cannot simply be merged with IN clause.
      String joinStructure = extractJoinStructure(sql);
      String groupKey = table.toLowerCase() + "|" + selectCols + "|" + joinStructure;
      groups.computeIfAbsent(groupKey, k -> new ArrayList<>()).add(query);
    }

    // Check each group
    for (var entry : groups.entrySet()) {
      List<QueryRecord> group = entry.getValue();
      if (group.size() < threshold) {
        continue;
      }

      // Distinguish from N+1: if all queries have the same normalized SQL,
      // it's N+1 (already handled by NPlusOneDetector), not mergeable queries.
      if (allSameNormalizedSql(group)) {
        continue;
      }

      // Different WHERE conditions detected for same table + columns
      String firstSql = group.get(0).sql();
      String table = extractFromTable(firstSql);

      issues.add(
          new Issue(
              IssueType.MERGEABLE_QUERIES,
              Severity.INFO,
              group.get(0).normalizedSql(),
              table,
              null,
              group.size()
                  + " queries to '"
                  + table
                  + "' with same columns but different conditions",
              group.size()
                  + " queries to '"
                  + table
                  + "' with same columns but different conditions. Consider using IN clause or batch query.",
              group.get(0).stackTrace()));
    }

    return issues;
  }

  private String extractFromTable(String sql) {
    Matcher m = FROM_TABLE.matcher(sql);
    if (m.find()) {
      return m.group(1);
    }
    return null;
  }

  /**
   * Extracts and normalizes the SELECT columns part. Normalizes by lowercasing, collapsing
   * whitespace, and sorting columns to handle different column orderings as equivalent.
   */
  private String extractNormalizedSelectColumns(String sql) {
    Matcher m = SELECT_COLUMNS.matcher(sql);
    if (!m.find()) {
      return null;
    }
    String cols = m.group(1).trim().toLowerCase();
    // Normalize whitespace
    cols = cols.replaceAll("\\s+", " ");
    return cols;
  }

  /**
   * Extracts a normalized representation of the JOIN structure from a SQL query. Queries with
   * different JOINs (e.g., different tables or JOIN types) should not be considered mergeable.
   */
  private static final Pattern JOIN_CLAUSE =
      Pattern.compile(
          "\\b(?:LEFT\\s+(?:OUTER\\s+)?|RIGHT\\s+(?:OUTER\\s+)?|INNER\\s+|FULL\\s+(?:OUTER\\s+)?|CROSS\\s+)?JOIN\\s+\\w+",
          Pattern.CASE_INSENSITIVE);

  private String extractJoinStructure(String sql) {
    StringBuilder sb = new StringBuilder();
    Matcher m = JOIN_CLAUSE.matcher(sql);
    while (m.find()) {
      if (sb.length() > 0) sb.append(",");
      sb.append(m.group().toLowerCase().replaceAll("\\s+", " ").trim());
    }
    return sb.toString();
  }

  /** Returns true if all queries in the group have the same normalized SQL. */
  private boolean allSameNormalizedSql(List<QueryRecord> group) {
    String first = group.get(0).normalizedSql();
    if (first == null) {
      return false;
    }
    for (int i = 1; i < group.size(); i++) {
      if (!first.equals(group.get(i).normalizedSql())) {
        return false;
      }
    }
    return true;
  }
}
