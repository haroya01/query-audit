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
 * Detects Cartesian JOIN anti-patterns where a JOIN is missing an ON or USING clause,
 * or where an implicit join (comma-separated tables in FROM) lacks a WHERE clause.
 * Cartesian products produce a cross product of all rows, leading to massive result sets
 * and severe performance degradation.
 *
 * @author haroya
 * @since 0.2.0
 */
public class CartesianJoinDetector implements DetectionRule {

  /**
   * Detects explicit JOIN without ON or USING clause. Matches: JOIN table_name [alias] NOT followed
   * by ON/USING before the next JOIN/WHERE/ORDER/GROUP/LIMIT/HAVING or end of string. Excludes
   * CROSS JOIN (intentional Cartesian product) and NATURAL JOIN (implicit join on matching column
   * names).
   */
  private static final Pattern JOIN_WITHOUT_ON =
      Pattern.compile(
          "(?<!\\bCROSS\\s)(?<!\\bNATURAL\\s)\\bJOIN\\s+\\w+(?:\\s+(?:AS\\s+)?(?!ON\\b|USING\\b)\\w+)?\\s*"
              + "(?=\\bJOIN\\b|\\bWHERE\\b|\\bORDER\\b|\\bGROUP\\b|\\bLIMIT\\b|\\bHAVING\\b|$)",
          Pattern.CASE_INSENSITIVE);

  /** Pattern to detect LATERAL keyword after JOIN, which is a legitimate usage. */
  private static final Pattern LATERAL_JOIN =
      Pattern.compile("\\bJOIN\\s+LATERAL\\b", Pattern.CASE_INSENSITIVE);

  /**
   * Detects implicit Cartesian join: FROM a, b without WHERE clause. Pattern: FROM table1 [alias],
   * table2 [alias] (comma-separated tables in FROM).
   */
  private static final Pattern IMPLICIT_JOIN =
      Pattern.compile(
          "\\bFROM\\s+\\w+(?:\\s+(?:AS\\s+)?\\w+)?\\s*,\\s*\\w+", Pattern.CASE_INSENSITIVE);

  private static final Pattern WHERE_CLAUSE =
      Pattern.compile("\\bWHERE\\b", Pattern.CASE_INSENSITIVE);

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

      boolean detected = false;

      // Check for explicit JOIN without ON/USING
      // Skip LATERAL joins — they are a legitimate usage (e.g., CROSS JOIN LATERAL, JOIN LATERAL)
      if (!LATERAL_JOIN.matcher(sql).find()) {
        Matcher joinMatcher = JOIN_WITHOUT_ON.matcher(sql);
        if (joinMatcher.find()) {
          detected = true;
        }
      }

      // Check for implicit Cartesian join: FROM a, b without WHERE
      if (!detected) {
        Matcher implicitMatcher = IMPLICIT_JOIN.matcher(sql);
        if (implicitMatcher.find() && !WHERE_CLAUSE.matcher(sql).find()) {
          detected = true;
        }
      }

      if (detected) {
        List<String> tables = EnhancedSqlParser.extractTableNames(sql);
        String table = tables.isEmpty() ? null : tables.get(0);

        issues.add(
            new Issue(
                IssueType.CARTESIAN_JOIN,
                Severity.ERROR,
                normalized,
                table,
                null,
                "Cartesian product detected — missing JOIN condition",
                "Add JOIN condition (ON clause) to avoid Cartesian product",
                query.stackTrace()));
      }
    }

    return issues;
  }
}
