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
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Detects {@code ORDER BY RAND()}, {@code ORDER BY RANDOM()}, {@code ORDER BY NEWID()}, and {@code
 * ORDER BY DBMS_RANDOM} patterns.
 *
 * <p>These force random number generation for every row followed by a full sort, which is
 * catastrophic on large tables.
 *
 * @author haroya
 * @since 0.2.0
 */
public class OrderByRandDetector implements DetectionRule {

  private static final Pattern ORDER_BY_RAND =
      Pattern.compile(
          "ORDER\\s+BY\\s+(?:RAND|RANDOM|NEWID|DBMS_RANDOM)\\s*\\(", Pattern.CASE_INSENSITIVE);

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

      if (ORDER_BY_RAND.matcher(sql).find()) {
        List<String> tables = EnhancedSqlParser.extractTableNames(sql);
        String table = tables.isEmpty() ? null : tables.get(0);
        issues.add(
            new Issue(
                IssueType.ORDER_BY_RAND,
                Severity.ERROR,
                normalized,
                table,
                null,
                "ORDER BY RAND() generates a random number for every row and then performs "
                    + "a full sort. This is catastrophic on large tables.",
                "Use application-side random offset or a pre-computed random column instead of ORDER BY RAND()."));
      }
    }
    return issues;
  }
}
