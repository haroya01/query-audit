package io.queryaudit.core.detector;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.parser.EnhancedSqlParser;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Detects SELECT queries that return potentially unbounded result sets.
 *
 * <p>A finding is suppressed only when the <em>outer</em> result set is bounded by evidence that
 * survives scrutiny:
 *
 * <ul>
 *   <li>a row-limiting clause on the outermost statement — {@code LIMIT}, {@code LIMIT ... OFFSET}
 *       or SQL:2008 {@code FETCH FIRST n ROWS} — read from the parsed statement, so the same words
 *       inside a string literal, quoted identifier or comment do not count ({@link
 *       EnhancedSqlParser#hasOuterRowLimit(String)});
 *   <li>a single-row proof from real uniqueness metadata: every column of a PRIMARY KEY or UNIQUE
 *       index on the queried table appears as a necessary equality predicate ({@link
 *       UniqueIndexEquality});
 *   <li>a repository return type that cannot carry many rows — {@code Optional}, a single entity,
 *       or a {@code Page}/{@code Slice} ({@link RepositoryReturnTypeResolver}).
 * </ul>
 *
 * <p>Deliberately <em>not</em> treated as a bound on the outer result:
 *
 * <ul>
 *   <li>a column name such as {@code id}, {@code tenant_id}, {@code email} or {@code username}.
 *       Naming conventions are not uniqueness: without a proven constraint those queries can still
 *       match many rows, so {@code WHERE tenant_id = ?} stays reportable;
 *   <li>{@code EXISTS (...)} or {@code IN (SELECT ...)} in the outer WHERE. Those bound the
 *       subquery's rows, not the rows the outer statement returns;
 *   <li>a {@code LIMIT} on a derived table, scalar subquery or set operand.
 * </ul>
 *
 * <p>Other exclusions that are not row bounds but not result sets either: aggregate-only
 * projections (a single value regardless of input rows), queries without {@code FROM}, {@code
 * SELECT ... INTO} variable assignment, and {@code FOR UPDATE}/{@code FOR SHARE} locking reads.
 *
 * @author haroya
 * @since 0.2.0
 */
public class UnboundedResultSetDetector implements DetectionRule {

  private final RepositoryReturnTypeResolver returnTypeResolver;

  public UnboundedResultSetDetector() {
    this(null);
  }

  public UnboundedResultSetDetector(RepositoryReturnTypeResolver returnTypeResolver) {
    this.returnTypeResolver = returnTypeResolver;
  }

  private static final Pattern SELECT_PATTERN =
      Pattern.compile("^\\s*SELECT\\b", Pattern.CASE_INSENSITIVE);

  private static final Pattern AGGREGATE_PATTERN =
      Pattern.compile(
          "\\bSELECT\\s+(?:COUNT|EXISTS|MAX|MIN|SUM|AVG)\\s*\\(", Pattern.CASE_INSENSITIVE);

  private static final Pattern FROM_PATTERN =
      Pattern.compile("\\bFROM\\b", Pattern.CASE_INSENSITIVE);

  private static final Pattern FOR_UPDATE_PATTERN =
      Pattern.compile("\\bFOR\\s+UPDATE\\b", Pattern.CASE_INSENSITIVE);

  private static final Pattern FOR_SHARE_PATTERN =
      Pattern.compile("\\bFOR\\s+SHARE\\b", Pattern.CASE_INSENSITIVE);

  /** SELECT ... INTO (variable assignment, not a result set) */
  private static final Pattern SELECT_INTO_PATTERN =
      Pattern.compile("\\bSELECT\\b.+\\bINTO\\b", Pattern.CASE_INSENSITIVE);

  /** Detects presence of a WHERE clause (used for Collection return type downgrade). */
  private static final Pattern WHERE_PATTERN =
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

      if (!SELECT_PATTERN.matcher(sql).find()) {
        continue;
      }

      if (AGGREGATE_PATTERN.matcher(sql).find()) {
        continue;
      }

      if (!FROM_PATTERN.matcher(sql).find()) {
        continue;
      }

      // SELECT ... INTO is variable assignment, not a result set
      if (SELECT_INTO_PATTERN.matcher(sql).find()) {
        continue;
      }

      if (FOR_UPDATE_PATTERN.matcher(sql).find()) {
        continue;
      }

      if (FOR_SHARE_PATTERN.matcher(sql).find()) {
        continue;
      }

      // Bound 1: a real row-limiting clause on the outermost statement. Parsed, not text-matched.
      if (EnhancedSqlParser.hasOuterRowLimit(sql)) {
        continue;
      }

      List<String> tables = EnhancedSqlParser.extractTableNames(sql);
      String table = tables.isEmpty() ? null : tables.get(0);

      // Bound 2: metadata proves at most one row. A column name alone never proves this.
      if (isProvenSingleRow(sql, indexMetadata, table)) {
        continue;
      }

      // Return type analysis: suppress or downgrade based on repository method return type
      Severity severity = Severity.WARNING;
      String detail = "SELECT query without LIMIT could return unbounded rows";
      String suggestion =
          "Add LIMIT to prevent unbounded result sets in production. "
              + "For JPA: use Pageable parameter or setMaxResults().";

      if (returnTypeResolver != null
          && query.stackTrace() != null
          && !query.stackTrace().isEmpty()) {
        RepositoryReturnType returnType;
        try {
          returnType = returnTypeResolver.resolve(query.stackTrace());
        } catch (Exception e) {
          returnType = RepositoryReturnType.UNKNOWN;
        }

        switch (returnType) {
          case OPTIONAL, SINGLE_ENTITY, PAGE_OR_SLICE -> {
            continue;
          }
          case COLLECTION -> {
            if (WHERE_PATTERN.matcher(sql).find()) {
              severity = Severity.INFO;
              detail =
                  "Collection-returning repository method with WHERE clause "
                      + "(intentional fetch, not unbounded)";
              suggestion = "If the result set could grow large, consider adding Pageable or LIMIT.";
            }
          }
          case UNKNOWN -> {
            // fall through — keep WARNING
          }
        }
      }

      issues.add(
          new Issue(
              IssueType.UNBOUNDED_RESULT_SET,
              severity,
              normalized,
              table,
              null,
              detail,
              suggestion,
              query.stackTrace()));
    }

    return issues;
  }

  /**
   * Returns true when uniqueness metadata proves the outer result holds at most one row. Without
   * collected metadata — or without every column of some unique index matched by an equality — this
   * stays false and the query is reported.
   */
  private static boolean isProvenSingleRow(
      String sql, IndexMetadata indexMetadata, String reportedTable) {
    if (indexMetadata == null || reportedTable == null) {
      return false;
    }
    String reported = reportedTable.toLowerCase(Locale.ROOT);
    for (String proven : UniqueIndexEquality.singleRowTables(sql, indexMetadata)) {
      if (proven.toLowerCase(Locale.ROOT).equals(reported)) {
        return true;
      }
    }
    return false;
  }
}
