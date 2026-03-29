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
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Detects SELECT queries that return potentially unbounded result sets (no LIMIT clause).
 *
 * <p>Exclusions to reduce false positives:
 *
 * <ul>
 *   <li>Aggregate queries (COUNT, EXISTS, MAX, MIN, SUM, AVG)
 *   <li>Queries without a FROM clause (e.g., {@code SELECT 1})
 *   <li>Queries with {@code FOR UPDATE} or {@code FOR SHARE} (deliberate locking, typically
 *       single-row)
 *   <li>Single-row lookups via primary key or common unique column pattern ({@code WHERE
 *       id/email/uuid/username = ?})
 *   <li>Any single equality condition with {@code LIMIT 1}
 *   <li>Queries with {@code IN (?)} subquery (bounded by subquery)
 *   <li>Queries with {@code EXISTS (SELECT ...)} subquery (bounded by EXISTS check)
 *   <li>{@code SELECT ... INTO} patterns (variable assignment, not returning result sets)
 *   <li>Queries where the WHERE column has a UNIQUE index according to {@link IndexMetadata}
 * </ul>
 *
 * @author haroya
 * @since 0.2.0
 */
public class UnboundedResultSetDetector implements DetectionRule {

  private static final Pattern SELECT_PATTERN =
      Pattern.compile("^\\s*SELECT\\b", Pattern.CASE_INSENSITIVE);

  private static final Pattern AGGREGATE_PATTERN =
      Pattern.compile(
          "\\bSELECT\\s+(?:COUNT|EXISTS|MAX|MIN|SUM|AVG)\\s*\\(", Pattern.CASE_INSENSITIVE);

  private static final Pattern FROM_PATTERN =
      Pattern.compile("\\bFROM\\b", Pattern.CASE_INSENSITIVE);

  private static final Pattern LIMIT_PATTERN =
      Pattern.compile("\\bLIMIT\\b", Pattern.CASE_INSENSITIVE);

  /** SQL:2008 standard row-limiting clause used by Hibernate 6 / H2 / PostgreSQL. */
  private static final Pattern FETCH_FIRST_PATTERN =
      Pattern.compile("\\bFETCH\\s+FIRST\\b", Pattern.CASE_INSENSITIVE);

  private static final Pattern FOR_UPDATE_PATTERN =
      Pattern.compile("\\bFOR\\s+UPDATE\\b", Pattern.CASE_INSENSITIVE);

  private static final Pattern FOR_SHARE_PATTERN =
      Pattern.compile("\\bFOR\\s+SHARE\\b", Pattern.CASE_INSENSITIVE);

  /**
   * Matches a simple primary key or common unique column lookup pattern: WHERE (alias.)column = ?
   * with no additional OR/AND conditions. Recognises id, *_id, email, uuid, and username columns.
   */
  private static final Pattern PK_LOOKUP_PATTERN =
      Pattern.compile(
          "\\bWHERE\\s+(?:\\w+\\.)?(?:id|\\w+_id|email|uuid|username)\\s*=\\s*\\?\\s*$",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern IN_SUBQUERY_PATTERN =
      Pattern.compile("\\bIN\\s*\\(\\s*SELECT\\b", Pattern.CASE_INSENSITIVE);

  /** EXISTS subquery pattern — the inner SELECT is bounded by the EXISTS check. */
  private static final Pattern EXISTS_SUBQUERY_PATTERN =
      Pattern.compile("\\bEXISTS\\s*\\(\\s*SELECT\\b", Pattern.CASE_INSENSITIVE);

  /** SELECT ... INTO (variable assignment, not a result set) */
  private static final Pattern SELECT_INTO_PATTERN =
      Pattern.compile("\\bSELECT\\b.+\\bINTO\\b", Pattern.CASE_INSENSITIVE);

  /**
   * Matches any single equality condition: WHERE col = ? (end of string or followed only by
   * whitespace / FOR UPDATE / FOR SHARE). Captures the column name in group 1 for index metadata
   * lookups.
   */
  private static final Pattern SINGLE_EQUALITY_PATTERN =
      Pattern.compile("\\bWHERE\\s+(?:\\w+\\.)?(\\w+)\\s*=\\s*\\?\\s*$", Pattern.CASE_INSENSITIVE);

  /**
   * Extracts column names from equality conditions: {@code (alias.)column = ?}. Used to collect
   * all equality columns in a WHERE clause for unique index checks.
   */
  private static final Pattern EQUALITY_COLUMN_PATTERN =
      Pattern.compile("(?:\\w+\\.)?(\\w+)\\s*=\\s*\\?", Pattern.CASE_INSENSITIVE);

  /** Matches OR — unique index check is unsafe when OR is present in the WHERE clause. */
  private static final Pattern OR_PATTERN =
      Pattern.compile("\\bOR\\b", Pattern.CASE_INSENSITIVE);

  /** Extracts the WHERE clause from a SQL statement. */
  private static final Pattern WHERE_CLAUSE_PATTERN =
      Pattern.compile("\\bWHERE\\b(.+)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  /** Matches parenthesized subqueries to strip before column extraction. */
  private static final Pattern SUBQUERY_PATTERN =
      Pattern.compile("\\([^()]*\\bSELECT\\b[^()]*\\)", Pattern.CASE_INSENSITIVE);

  /** Matches a single equality condition followed by LIMIT 1. */
  private static final Pattern SINGLE_EQUALITY_LIMIT1_PATTERN =
      Pattern.compile(
          "\\bWHERE\\s+(?:\\w+\\.)?\\w+\\s*=\\s*\\?\\s+LIMIT\\s+1\\s*$", Pattern.CASE_INSENSITIVE);

  /**
   * Extracts equality column names from WHERE clause conditions joined by AND. Matches patterns
   * like {@code (alias.)column = ?} within compound WHERE clauses.
   */
  private static final Pattern WHERE_EQUALITY_COLUMN_PATTERN =
      Pattern.compile("(?:\\w+\\.)?(\\w+)\\s*=\\s*\\?", Pattern.CASE_INSENSITIVE);

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

      if (LIMIT_PATTERN.matcher(sql).find()) {
        continue;
      }

      if (FETCH_FIRST_PATTERN.matcher(sql).find()) {
        continue;
      }

      if (FOR_UPDATE_PATTERN.matcher(sql).find()) {
        continue;
      }

      if (FOR_SHARE_PATTERN.matcher(sql).find()) {
        continue;
      }

      if (PK_LOOKUP_PATTERN.matcher(sql).find()) {
        continue;
      }

      if (IN_SUBQUERY_PATTERN.matcher(sql).find()) {
        continue;
      }

      // EXISTS subquery — the inner SELECT is bounded by the EXISTS check
      if (EXISTS_SUBQUERY_PATTERN.matcher(sql).find()) {
        continue;
      }

      // Check index metadata: if all columns of a unique index (single or composite)
      // appear as AND-connected equality conditions, the result is at most one row.
      List<String> tables = SqlParser.extractTableNames(sql);
      String table = tables.isEmpty() ? null : tables.get(0);

      if (indexMetadata != null && table != null) {
        String whereClause = extractWhereClause(sql);
        if (whereClause != null && !OR_PATTERN.matcher(whereClause).find()) {
          String cleaned = stripSubqueries(whereClause);
          Set<String> eqColumns = extractEqualityColumns(cleaned);
          if (!eqColumns.isEmpty()
              && indexMetadata.hasUniqueIndexCoveredBy(table, eqColumns)) {
            continue;
          }
        }
      }

      // Single equality + LIMIT 1 is already handled by LIMIT_PATTERN above,
      // but this catches normalized queries where LIMIT 1 is present.
      if (SINGLE_EQUALITY_LIMIT1_PATTERN.matcher(sql).find()) {
        continue;
      }

      issues.add(
          new Issue(
              IssueType.UNBOUNDED_RESULT_SET,
              Severity.WARNING,
              normalized,
              table,
              null,
              "SELECT query without LIMIT could return unbounded rows",
              "Add LIMIT to prevent unbounded result sets in production. "
                  + "For JPA: use Pageable parameter or setMaxResults().",
              query.stackTrace()));
    }

    return issues;
  }

  private static String extractWhereClause(String sql) {
    Matcher m = WHERE_CLAUSE_PATTERN.matcher(sql);
    return m.find() ? m.group(1).trim() : null;
  }

  /** Removes parenthesized subqueries so that inner columns are not extracted. */
  private static String stripSubqueries(String whereClause) {
    String result = whereClause;
    while (SUBQUERY_PATTERN.matcher(result).find()) {
      result = SUBQUERY_PATTERN.matcher(result).replaceAll("");
    }
    return result;
  }

  /**
   * Extracts all equality column names from the WHERE clause of the given SQL. Only considers
   * {@code column = ?} patterns connected by AND. Returns an empty set if no WHERE clause is found.
   */
  private static Set<String> extractEqualityColumns(String whereClause) {
    Set<String> columns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    Matcher m = EQUALITY_COLUMN_PATTERN.matcher(whereClause);
    while (m.find()) {
      columns.add(m.group(1));
    }
    return columns;
  }
}
