package io.queryaudit.core.detector;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.parser.EnhancedSqlParser;
import io.queryaudit.core.parser.SqlParser;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Detects queries that are consistently slow by grouping executions of the same normalizedSql
 * pattern and checking the <strong>median</strong> execution time.
 *
 * <p>A single slow execution could be JVM warmup, GC pause, or CI noise. By using the median across
 * all executions of the same query pattern, we eliminate one-off spikes and only flag queries that
 * are <em>consistently</em> slow.
 *
 * <p>Default thresholds:
 *
 * <ul>
 *   <li>{@code median > 500ms} = WARNING
 *   <li>{@code median > 3000ms} = ERROR
 * </ul>
 *
 * Query patterns with 0 execution time (test mocks) are skipped.
 *
 * @author haroya
 * @since 0.2.0
 */
public class SlowQueryDetector implements DetectionRule {

  private final long warningThresholdMs;
  private final long errorThresholdMs;

  public SlowQueryDetector(long warningThresholdMs, long errorThresholdMs) {
    this.warningThresholdMs = warningThresholdMs;
    this.errorThresholdMs = errorThresholdMs;
  }

  public SlowQueryDetector() {
    this(500, 3000);
  }

  @Override
  public List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata indexMetadata) {
    // Group all executions by normalizedSql to compute median per pattern
    Map<String, List<QueryRecord>> grouped = new LinkedHashMap<>();
    for (QueryRecord query : queries) {
      String normalized = query.normalizedSql();
      if (normalized == null) {
        continue;
      }
      grouped.computeIfAbsent(normalized, k -> new ArrayList<>()).add(query);
    }

    List<Issue> issues = new ArrayList<>();

    for (Map.Entry<String, List<QueryRecord>> entry : grouped.entrySet()) {
      String normalized = entry.getKey();
      List<QueryRecord> records = entry.getValue();

      // Collect execution times, skipping zero (test mocks)
      List<Long> timesMs = new ArrayList<>();
      for (QueryRecord r : records) {
        long ms = TimeUnit.NANOSECONDS.toMillis(r.executionTimeNanos());
        if (ms > 0) {
          timesMs.add(ms);
        }
      }

      if (timesMs.isEmpty()) {
        continue;
      }

      long medianMs = median(timesMs);

      Severity severity;
      long threshold;
      if (medianMs > errorThresholdMs) {
        severity = Severity.ERROR;
        threshold = errorThresholdMs;
      } else if (medianMs > warningThresholdMs) {
        severity = Severity.WARNING;
        threshold = warningThresholdMs;
      } else {
        continue;
      }

      // Use the first record for table extraction and stack trace
      QueryRecord representative = records.get(0);
      List<String> tables = EnhancedSqlParser.extractTableNames(representative.sql());
      String table = tables.isEmpty() ? null : tables.get(0);

      String execCountInfo =
          timesMs.size() > 1 ? " (median of " + timesMs.size() + " executions)" : "";

      issues.add(
          new Issue(
              IssueType.SLOW_QUERY,
              severity,
              normalized,
              table,
              null,
              "Query median "
                  + medianMs
                  + "ms"
                  + execCountInfo
                  + " (threshold: "
                  + threshold
                  + "ms)",
              "Check EXPLAIN output for full table scans or filesort. "
                  + "Add indexes on WHERE/JOIN/ORDER BY columns, reduce fetched columns, "
                  + "or add LIMIT for pagination.",
              representative.stackTrace()));
    }

    return issues;
  }

  private static long median(List<Long> values) {
    List<Long> sorted = new ArrayList<>(values);
    Collections.sort(sorted);
    int size = sorted.size();
    if (size % 2 == 1) {
      return sorted.get(size / 2);
    }
    return (sorted.get(size / 2 - 1) + sorted.get(size / 2)) / 2;
  }
}
