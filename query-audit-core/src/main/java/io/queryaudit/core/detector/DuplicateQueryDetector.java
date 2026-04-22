package io.queryaudit.core.detector;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.parser.EnhancedSqlParser;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Detects exact duplicate queries — same SQL with same parameters executed multiple times.
 *
 * <p>Important distinction from N+1:
 *
 * <ul>
 *   <li>N+1 detects same <b>pattern</b> with different parameters (e.g., SELECT ... WHERE id=1,
 *       id=2, id=3)
 *   <li>Duplicate detects same <b>exact SQL</b> with same parameters (e.g., identical query run
 *       twice)
 * </ul>
 *
 * <p>Since datasource-proxy provides SQL with '?' placeholders, we can only detect duplicates when
 * the parameterized SQL is identical. To avoid overlap with N+1, we exclude patterns that are
 * already flagged as N+1 (normalized pattern appears 3+ times).
 *
 * @author haroya
 * @since 0.2.0
 */
public class DuplicateQueryDetector implements DetectionRule {

  private final int nPlusOneThreshold;

  public DuplicateQueryDetector(int nPlusOneThreshold) {
    this.nPlusOneThreshold = nPlusOneThreshold;
  }

  public DuplicateQueryDetector() {
    this(3);
  }

  @Override
  public List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata indexMetadata) {
    List<Issue> issues = new ArrayList<>();

    // First, find normalized patterns that qualify as N+1 (to exclude them)
    Map<String, Long> normalizedCounts =
        queries.stream()
            .filter(q -> q.normalizedSql() != null)
            .collect(Collectors.groupingBy(QueryRecord::normalizedSql, Collectors.counting()));

    Set<String> nPlusOnePatterns =
        normalizedCounts.entrySet().stream()
            .filter(e -> e.getValue() >= nPlusOneThreshold)
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());

    // Group by exact SQL, but skip queries whose normalized form is already N+1
    Map<String, List<QueryRecord>> grouped = new LinkedHashMap<>();
    for (QueryRecord query : queries) {
      if (query.sql() == null) continue;
      if (query.normalizedSql() != null && nPlusOnePatterns.contains(query.normalizedSql())) {
        continue; // Already caught by N+1 detector
      }
      grouped.computeIfAbsent(query.sql(), k -> new ArrayList<>()).add(query);
    }

    for (Map.Entry<String, List<QueryRecord>> entry : grouped.entrySet()) {
      int count = entry.getValue().size();
      if (count > 1) {
        String sql = entry.getKey();
        List<String> tables = EnhancedSqlParser.extractTableNames(sql);
        String table = tables.isEmpty() ? null : tables.get(0);
        QueryRecord first = entry.getValue().get(0);

        issues.add(
            new Issue(
                IssueType.DUPLICATE_QUERY,
                Severity.WARNING,
                sql,
                table,
                null,
                "Identical query executed "
                    + count
                    + " times within the same test — likely redundant",
                "Cache the result, or check if the caller is invoking this unnecessarily",
                first.stackTrace()));
      }
    }

    return issues;
  }
}
