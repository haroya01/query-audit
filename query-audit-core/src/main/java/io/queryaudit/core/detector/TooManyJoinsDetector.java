package io.queryaudit.core.detector;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.parser.EnhancedSqlParser;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Detects queries with more than N JOINs (default: 5).
 *
 * <p>Too many JOINs multiply the optimizer search space, cause plan explosion, and often indicate
 * missing denormalization.
 *
 * @author haroya
 * @since 0.2.0
 */
public class TooManyJoinsDetector implements DetectionRule {

  private static final int DEFAULT_THRESHOLD = 5;

  private static final Pattern JOIN_PATTERN =
      Pattern.compile("\\bJOIN\\b", Pattern.CASE_INSENSITIVE);

  private final int threshold;

  public TooManyJoinsDetector() {
    this(DEFAULT_THRESHOLD);
  }

  public TooManyJoinsDetector(int threshold) {
    this.threshold = threshold;
  }

  @Override
  public List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata indexMetadata) {
    List<Issue> issues = new ArrayList<>();
    Set<String> seen = new HashSet<>();

    for (QueryRecord query : queries) {
      String sql = query.sql();
      String normalized = query.normalizedSql();
      if (normalized == null || !seen.add(normalized)) {
        continue;
      }

      String cleaned = EnhancedSqlParser.removeSubqueries(sql);
      int joinCount = countMatches(JOIN_PATTERN, cleaned);

      if (joinCount > threshold) {
        List<String> tables = EnhancedSqlParser.extractTableNames(sql);
        String table = tables.isEmpty() ? null : tables.get(0);
        issues.add(
            new Issue(
                IssueType.TOO_MANY_JOINS,
                Severity.WARNING,
                normalized,
                table,
                null,
                "Query has "
                    + joinCount
                    + " JOINs (threshold: "
                    + threshold
                    + "). "
                    + "This multiplies the optimizer search space and may cause plan explosion.",
                "Consider splitting into multiple simpler queries with application-side joining, "
                    + "using DTO projections to fetch only needed columns, or denormalizing the schema."));
      }
    }
    return issues;
  }

  private static int countMatches(Pattern pattern, String text) {
    Matcher matcher = pattern.matcher(text);
    int count = 0;
    while (matcher.find()) {
      count++;
    }
    return count;
  }
}
