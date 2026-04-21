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
import java.util.regex.Pattern;

/**
 * Detects {@code INSERT ... ON DUPLICATE KEY UPDATE} and {@code REPLACE INTO} patterns. These can
 * cause deadlocks under concurrent execution due to gap lock interactions on unique keys
 * (MySQL-specific).
 *
 * @author haroya
 * @since 0.2.0
 */
public class InsertOnDuplicateKeyDetector implements DetectionRule {

  private static final Pattern ON_DUPLICATE_KEY =
      Pattern.compile(
          "\\bINSERT\\b.*\\bON\\s+DUPLICATE\\s+KEY\\s+UPDATE\\b",
          Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  private static final Pattern REPLACE_INTO =
      Pattern.compile("^\\s*REPLACE\\s+INTO\\b", Pattern.CASE_INSENSITIVE);

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

      boolean isOnDuplicate = ON_DUPLICATE_KEY.matcher(sql).find();
      boolean isReplace = REPLACE_INTO.matcher(sql).find();

      if (!isOnDuplicate && !isReplace) {
        continue;
      }

      List<String> tables = EnhancedSqlParser.extractTableNames(sql);
      String table = tables.isEmpty() ? null : tables.get(0);

      String detail =
          isOnDuplicate
              ? "INSERT ON DUPLICATE KEY UPDATE can cause deadlocks under concurrent execution"
              : "REPLACE INTO can cause deadlocks under concurrent execution";

      issues.add(
          new Issue(
              IssueType.INSERT_ON_DUPLICATE_KEY,
              Severity.WARNING,
              normalized,
              table,
              null,
              detail,
              "INSERT ON DUPLICATE KEY UPDATE can cause deadlocks under concurrent execution. "
                  + "Consider using SELECT ... FOR UPDATE first, or application-level locking.",
              query.stackTrace()));
    }

    return issues;
  }
}
