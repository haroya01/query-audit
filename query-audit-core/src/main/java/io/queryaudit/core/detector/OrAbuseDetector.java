package io.queryaudit.core.detector;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.parser.SqlParser;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Detects excessive OR conditions on different columns in WHERE clauses. Multiple OR conditions
 * across different columns prevent the optimizer from using indexes effectively, often resulting
 * in full table scans.
 *
 * @author haroya
 * @since 0.2.0
 */
public class OrAbuseDetector implements DetectionRule {

  private final int threshold;

  public OrAbuseDetector(int threshold) {
    this.threshold = threshold;
  }

  public OrAbuseDetector() {
    this(3);
  }

  @Override
  public List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata indexMetadata) {
    List<Issue> issues = new ArrayList<>();
    Set<String> seen = new LinkedHashSet<>();

    for (QueryRecord query : queries) {
      String normalized = query.normalizedSql();
      if (normalized == null || seen.contains(normalized)) {
        continue;
      }
      seen.add(normalized);

      int orCount = SqlParser.countEffectiveOrConditions(query.sql());
      if (orCount >= threshold && !SqlParser.allOrConditionsOnSameColumn(query.sql())) {
        List<String> tables = SqlParser.extractTableNames(query.sql());
        String table = tables.isEmpty() ? null : tables.get(0);

        if (table != null && indexMetadata != null && indexMetadata.hasTable(table)) {
          List<String> orColumns = SqlParser.extractOrBranchColumns(query.sql());
          if (!orColumns.isEmpty()
              && orColumns.stream().allMatch(col -> indexMetadata.hasIndexOn(table, col))) {
            continue;
          }
        }

        issues.add(
            new Issue(
                IssueType.OR_ABUSE,
                Severity.WARNING,
                normalized,
                table,
                null,
                orCount
                    + " OR conditions on different columns found"
                    + (table != null ? " on table '" + table + "'" : ""),
                "Rewrite multiple OR conditions as IN (...) when they reference the same column, "
                    + "or split into separate queries with UNION ALL for different columns.",
                query.stackTrace()));
      }
    }

    return issues;
  }
}
