package io.queryaudit.core.detector;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.parser.EnhancedSqlParser;
import io.queryaudit.core.parser.SqlParser;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * SQL-level repeated query detector (supplementary information only).
 *
 * <p>This detector reports as {@link Severity#INFO} only. It serves as supplementary information,
 * not a confirmed detection. The Hibernate event-based {@link LazyLoadNPlusOneDetector} is the
 * authoritative source for N+1 errors.
 *
 * <p>Groups queries by normalizedSql and reports any group with count &gt;= threshold as INFO to
 * help developers identify repeated query patterns.
 *
 * @author haroya
 * @since 0.2.0
 */
public class NPlusOneDetector implements DetectionRule {

  private final int threshold;

  public NPlusOneDetector(int threshold) {
    this.threshold = threshold;
  }

  public NPlusOneDetector() {
    this(3);
  }

  @Override
  public List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata indexMetadata) {
    List<Issue> issues = new ArrayList<>();
    Set<String> seen = new HashSet<>();

    Map<String, List<QueryRecord>> bySql = new LinkedHashMap<>();
    for (QueryRecord q : queries) {
      if (q.normalizedSql() == null) continue;
      // N+1 is a SELECT problem — repeated INSERTs are handled by RepeatedSingleInsertDetector
      if (!SqlParser.isSelectQuery(q.sql())) continue;
      bySql.computeIfAbsent(q.normalizedSql(), k -> new ArrayList<>()).add(q);
    }

    for (var entry : bySql.entrySet()) {
      if (entry.getValue().size() < threshold) continue;
      if (!seen.add(entry.getKey())) continue;

      String table =
          EnhancedSqlParser.extractTableNames(entry.getKey()).stream().findFirst().orElse(null);

      issues.add(
          new Issue(
              IssueType.N_PLUS_ONE_SUSPECT,
              Severity.INFO,
              entry.getValue().get(0).sql(),
              table,
              null,
              String.format(
                  "Same query pattern repeated %d times (SQL-level detection, see Hibernate-level N+1 for confirmed issues)",
                  entry.getValue().size()),
              "SQL-level repeated query detection. Check Hibernate-level N+1 issues for confirmed lazy loading problems.",
              entry.getValue().get(0).stackTrace()));
    }

    return issues;
  }

  /** Extracts the first (top) frame from a stack trace string. */
  static String extractFirstFrame(String stackTrace) {
    if (stackTrace == null || stackTrace.isEmpty()) {
      return null;
    }
    String[] lines = stackTrace.split("\n");
    for (String line : lines) {
      String trimmed = line.trim();
      if (!trimmed.isEmpty()) {
        return trimmed;
      }
    }
    return null;
  }
}
