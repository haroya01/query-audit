package io.queryaudit.core.detector;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.parser.EnhancedSqlParser;
import io.queryaudit.core.parser.SqlParser;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Detects {@code SELECT *} usage which fetches all columns unnecessarily. Selecting all columns
 * increases network I/O and prevents covering index optimizations, leading to degraded performance
 * on wide tables.
 *
 * @author haroya
 * @since 0.2.0
 */
public class SelectAllDetector implements DetectionRule {

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

      if (SqlParser.hasSelectAll(query.sql())) {
        List<String> tables = EnhancedSqlParser.extractTableNames(query.sql());
        String table = tables.isEmpty() ? null : tables.get(0);

        issues.add(
            new Issue(
                IssueType.SELECT_ALL,
                Severity.INFO,
                normalized,
                table,
                null,
                "SELECT * usage detected"
                    + (table != null ? " on table '" + table + "'" : ""),
                "Replace SELECT * with an explicit column list to reduce network I/O and enable covering index optimization.",
                query.stackTrace()));
      }
    }

    return issues;
  }
}
