package io.queryaudit.core.detector;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Detects UNION without ALL, which forces MySQL to perform a deduplication sort on the combined
 * result set. In most JPA/application scenarios, duplicate rows are either impossible or
 * acceptable, making UNION ALL the better choice.
 *
 * @author haroya
 * @since 0.2.0
 */
public class UnionWithoutAllDetector implements DetectionRule {

  /**
   * Matches the keyword UNION that is NOT immediately followed by ALL. Uses a negative lookahead to
   * exclude UNION ALL.
   */
  private static final Pattern UNION_WITHOUT_ALL =
      Pattern.compile("\\bUNION\\b(?!\\s+ALL\\b)", Pattern.CASE_INSENSITIVE);

  /** Matches SELECT DISTINCT — deduplication via UNION is intentional when DISTINCT is used. */
  private static final Pattern SELECT_DISTINCT =
      Pattern.compile("\\bSELECT\\s+DISTINCT\\b", Pattern.CASE_INSENSITIVE);

  @Override
  public List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata indexMetadata) {
    List<Issue> issues = new ArrayList<>();
    Set<String> seen = new LinkedHashSet<>();

    for (QueryRecord query : queries) {
      String sql = query.sql();
      if (sql == null) {
        continue;
      }

      String normalized = query.normalizedSql();
      if (normalized != null && !seen.add(normalized)) {
        continue;
      }

      Matcher matcher = UNION_WITHOUT_ALL.matcher(sql);
      if (matcher.find()) {
        // If any branch uses SELECT DISTINCT, dedup via UNION is intentional
        if (SELECT_DISTINCT.matcher(sql).find()) {
          continue;
        }

        issues.add(
            new Issue(
                IssueType.UNION_WITHOUT_ALL,
                Severity.INFO,
                normalized,
                null,
                null,
                "UNION (without ALL) forces deduplication sort",
                "If duplicate rows are impossible or acceptable, use UNION ALL "
                    + "to avoid the deduplication overhead.",
                query.stackTrace()));
      }
    }

    return issues;
  }
}
