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
 * Detects queries that use FORCE INDEX, USE INDEX, or IGNORE INDEX hints.
 *
 * <p>Index hints hard-code the optimizer's index choice into the application code. As the schema
 * evolves (indexes added, dropped, or renamed), these hints become stale and can cause the optimizer
 * to make <em>worse</em> decisions than it would without the hint.
 *
 * <p>In most cases, fixing the query or schema is better than forcing an index. Hints should only be
 * used as a last resort with a comment explaining why.
 *
 * @author haroya
 * @since 0.2.0
 */
public class ForceIndexHintDetector implements DetectionRule {

  private static final Pattern MIGRATION_PATTERN =
      Pattern.compile(
          "(?i)(flyway|liquibase|migrate|migration|dbmigrat|schema\\.sql|changelog)",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern INDEX_HINT =
      Pattern.compile(
          "\\b(FORCE\\s+INDEX|USE\\s+INDEX|IGNORE\\s+INDEX)\\s*\\(",
          Pattern.CASE_INSENSITIVE);

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

      Matcher matcher = INDEX_HINT.matcher(sql);
      if (!matcher.find()) {
        continue;
      }

      // Skip index hints in migration/schema scripts — these are acceptable
      // because migrations run once and the hint is specific to that schema version.
      String stackTrace = query.stackTrace();
      if (isMigrationContext(stackTrace)) {
        continue;
      }

      String hintType = matcher.group(1).toUpperCase().replaceAll("\\s+", " ");
      List<String> tables = EnhancedSqlParser.extractTableNames(sql);
      String table = tables.isEmpty() ? null : tables.get(0);

      issues.add(
          new Issue(
              IssueType.FORCE_INDEX_HINT,
              Severity.INFO,
              normalized,
              table,
              null,
              hintType + " hint detected — this overrides the query optimizer's index choice"
                  + (table != null ? " on table '" + table + "'" : ""),
              "Index hints become stale as the schema evolves. Prefer fixing the query or "
                  + "updating statistics (ANALYZE TABLE) instead. If a hint is truly needed, "
                  + "document why and review it during schema changes.",
              query.stackTrace()));
    }

    return issues;
  }

  /**
   * Returns true if the stack trace indicates a migration or schema script context.
   * FORCE INDEX in migrations is acceptable because migrations run once and the
   * hint is specific to that schema version.
   */
  private boolean isMigrationContext(String stackTrace) {
    if (stackTrace == null || stackTrace.isEmpty()) {
      return false;
    }
    return MIGRATION_PATTERN.matcher(stackTrace).find();
  }
}
