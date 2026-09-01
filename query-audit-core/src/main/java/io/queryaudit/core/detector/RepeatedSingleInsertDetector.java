package io.queryaudit.core.detector;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.parser.SqlParser;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Detects repeated single-row INSERT statements to the same table that could benefit from batch
 * insertion.
 *
 * <p>Each single INSERT incurs a network round-trip and a separate transaction log flush. Batching
 * multiple inserts significantly reduces overhead.
 *
 * @author haroya
 * @since 0.2.0
 */
public class RepeatedSingleInsertDetector implements DetectionRule {

  private static final int DEFAULT_THRESHOLD = 3;

  /**
   * Default table-name globs treated as deliberate staging targets — repeated single-row inserts
   * into these tables are intentional (test fixtures, ETL stage, session-local temp tables) and the
   * round-trip / log-flush cost the rule warns about does not apply.
   */
  public static final Set<String> DEFAULT_EXCLUDE_TABLES =
      TableNameGlobMatcher.DEFAULT_STAGING_TABLES;

  private static final Pattern MULTI_ROW_INSERT =
      Pattern.compile(
          "\\bVALUES\\s*\\(.*\\)\\s*,\\s*\\(", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  private final int threshold;
  private final TableNameGlobMatcher excludedTables;

  public RepeatedSingleInsertDetector() {
    this(DEFAULT_THRESHOLD, DEFAULT_EXCLUDE_TABLES);
  }

  public RepeatedSingleInsertDetector(int threshold) {
    this(threshold, DEFAULT_EXCLUDE_TABLES);
  }

  public RepeatedSingleInsertDetector(int threshold, Collection<String> excludeTablePatterns) {
    this.threshold = threshold;
    this.excludedTables = new TableNameGlobMatcher(excludeTablePatterns);
  }

  @Override
  public List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata indexMetadata) {
    List<Issue> issues = new ArrayList<>();

    // Group insert queries by normalized SQL pattern (same table + same columns)
    Map<String, InsertGroup> groups = new HashMap<>();

    for (QueryRecord query : queries) {
      String sql = query.sql();
      if (!SqlParser.isInsertQuery(sql)) {
        continue;
      }

      // Skip multi-row inserts — they're already batched
      if (MULTI_ROW_INSERT.matcher(sql).find()) {
        continue;
      }

      String normalized = query.normalizedSql();
      if (normalized == null) {
        continue;
      }

      String table = SqlParser.extractInsertTable(sql);
      if (excludedTables.matches(table)) {
        continue;
      }
      groups.computeIfAbsent(normalized, k -> new InsertGroup(table, normalized)).count++;
    }

    for (InsertGroup group : groups.values()) {
      if (group.count >= threshold) {
        issues.add(
            new Issue(
                IssueType.REPEATED_SINGLE_INSERT,
                Severity.WARNING,
                group.normalizedSql,
                group.table,
                null,
                "Single-row INSERT executed "
                    + group.count
                    + " times on "
                    + (group.table != null ? "table '" + group.table + "'" : "the same table")
                    + ". Each INSERT causes a separate network round-trip and log flush.",
                "Use batch INSERT (addBatch/executeBatch in JDBC, or saveAll() in JPA with "
                    + "spring.jpa.properties.hibernate.jdbc.batch_size). "
                    + "This can reduce overhead by 5-10x. "
                    + "Note: batch inserts are NOT possible with GenerationType.IDENTITY — "
                    + "consider SEQUENCE or TABLE strategy if batching is critical."));
      }
    }
    return issues;
  }

  private static class InsertGroup {
    final String table;
    final String normalizedSql;
    int count;

    InsertGroup(String table, String normalizedSql) {
      this.table = table;
      this.normalizedSql = normalizedSql;
      this.count = 0;
    }
  }
}
