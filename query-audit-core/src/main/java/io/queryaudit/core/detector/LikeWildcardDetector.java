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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Detects LIKE patterns with a leading wildcard ({@code LIKE '%...'}) which prevent B-tree index
 * usage.
 *
 * @author haroya
 * @since 0.2.0
 */
public class LikeWildcardDetector implements DetectionRule {

  /**
   * Matches LIKE followed by a string literal starting with '%'. Covers: LIKE '%...', LIKE '%...%',
   * etc. Parameterized queries (LIKE ?) are intentionally skipped.
   */
  private static final Pattern LIKE_LEADING_WILDCARD =
      Pattern.compile("\\bLIKE\\s+'%", Pattern.CASE_INSENSITIVE);

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

      String sql = query.sql();
      if (sql == null) {
        continue;
      }

      Matcher matcher = LIKE_LEADING_WILDCARD.matcher(sql);
      if (matcher.find()) {
        List<String> tables = EnhancedSqlParser.extractTableNames(sql);
        String table = tables.isEmpty() ? null : tables.get(0);

        issues.add(
            new Issue(
                IssueType.LIKE_LEADING_WILDCARD,
                Severity.WARNING,
                normalized,
                table,
                null,
                "LIKE with leading wildcard detected"
                    + (table != null ? " on table '" + table + "'" : ""),
                "Leading wildcard (LIKE '%...') prevents B-tree index usage and causes a full table scan. "
                    + "Use a fulltext index (MATCH ... AGAINST), or move the search to the application layer.",
                query.stackTrace()));
      }
    }

    return issues;
  }
}
