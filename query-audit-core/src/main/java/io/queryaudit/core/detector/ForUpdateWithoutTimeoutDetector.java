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
import java.util.regex.Pattern;

/**
 * Detects FOR UPDATE/FOR SHARE clauses without NOWAIT or SKIP LOCKED.
 *
 * <p>Without NOWAIT or SKIP LOCKED, a FOR UPDATE query will block indefinitely if the target rows
 * are already locked by another transaction. This is a common source of production deadlocks and
 * timeouts that are difficult to reproduce in testing.
 *
 * @author haroya
 * @since 0.2.0
 */
public class ForUpdateWithoutTimeoutDetector implements DetectionRule {

  private static final Pattern FOR_UPDATE =
      Pattern.compile("\\bFOR\\s+(?:UPDATE|SHARE)\\b", Pattern.CASE_INSENSITIVE);

  private static final Pattern NOWAIT =
      Pattern.compile("\\bNOWAIT\\b", Pattern.CASE_INSENSITIVE);

  private static final Pattern SKIP_LOCKED =
      Pattern.compile("\\bSKIP\\s+LOCKED\\b", Pattern.CASE_INSENSITIVE);

  private static final Pattern WAIT_TIMEOUT =
      Pattern.compile("\\bWAIT\\s+\\d+\\b", Pattern.CASE_INSENSITIVE);

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

      if (!FOR_UPDATE.matcher(sql).find()) {
        continue;
      }

      // Check for NOWAIT, SKIP LOCKED, or WAIT N
      if (NOWAIT.matcher(sql).find()
          || SKIP_LOCKED.matcher(sql).find()
          || WAIT_TIMEOUT.matcher(sql).find()) {
        continue;
      }

      List<String> tables = SqlParser.extractTableNames(sql);
      String table = tables.isEmpty() ? null : tables.get(0);

      issues.add(
          new Issue(
              IssueType.FOR_UPDATE_WITHOUT_TIMEOUT,
              Severity.WARNING,
              normalized,
              table,
              null,
              "FOR UPDATE without NOWAIT or SKIP LOCKED may block indefinitely"
                  + (table != null ? " on table '" + table + "'" : ""),
              "Add NOWAIT (fail immediately if locked), SKIP LOCKED (skip locked rows), "
                  + "or WAIT N (timeout after N seconds) to prevent indefinite blocking. "
                  + "Example: SELECT ... FOR UPDATE NOWAIT",
              query.stackTrace()));
    }

    return issues;
  }
}
