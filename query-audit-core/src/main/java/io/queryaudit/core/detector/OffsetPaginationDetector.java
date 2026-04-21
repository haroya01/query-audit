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
import java.util.OptionalLong;
import java.util.Set;

/**
 * Detects OFFSET-based pagination that may cause performance issues.
 *
 * <p>Two detection modes:
 *
 * <ol>
 *   <li><b>Literal OFFSET</b>: When the SQL contains a literal OFFSET value >= threshold, emit a
 *       WARNING (the value is known to be large).
 *   <li><b>Parameterized OFFSET</b>: When the SQL contains OFFSET with a placeholder ({@code ?}),
 *       the actual value is unknown at interception time (JPA always parameterizes OFFSET). Emit an
 *       INFO because the value <em>could</em> be large at runtime.
 * </ol>
 *
 * @author haroya
 * @since 0.2.0
 */
public class OffsetPaginationDetector implements DetectionRule {

  private final int threshold;

  public OffsetPaginationDetector(int threshold) {
    this.threshold = threshold;
  }

  public OffsetPaginationDetector() {
    this(1000);
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

      // First, try to extract a literal OFFSET value
      OptionalLong offset = SqlParser.extractOffsetValue(query.sql());
      if (offset.isPresent() && offset.getAsLong() >= threshold) {
        List<String> tables = EnhancedSqlParser.extractTableNames(query.sql());
        String table = tables.isEmpty() ? null : tables.get(0);

        issues.add(
            new Issue(
                IssueType.OFFSET_PAGINATION,
                Severity.WARNING,
                normalized,
                table,
                null,
                "OFFSET value: " + offset.getAsLong(),
                "Use cursor-based pagination (WHERE id > last_id)",
                query.stackTrace()));
        continue;
      }

      // If no literal OFFSET found, check for parameterized OFFSET (JPA uses ? placeholders).
      // We cannot know the actual value, but flag it as INFO since it could be large at runtime.
      if (!offset.isPresent() && SqlParser.hasOffsetClause(query.sql())) {
        List<String> tables = EnhancedSqlParser.extractTableNames(query.sql());
        String table = tables.isEmpty() ? null : tables.get(0);

        issues.add(
            new Issue(
                IssueType.OFFSET_PAGINATION,
                Severity.INFO,
                normalized,
                table,
                null,
                "Parameterized OFFSET detected. The actual value is unknown at analysis time "
                    + "but could be large at runtime, causing full table scan up to the offset row.",
                "Use cursor-based pagination (WHERE id > last_id) instead of OFFSET for large datasets.",
                query.stackTrace()));
      }
    }

    return issues;
  }
}
