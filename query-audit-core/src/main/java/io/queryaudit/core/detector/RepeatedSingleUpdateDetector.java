package io.queryaudit.core.detector;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.parser.SqlParser;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Detects repeated UPDATE statements that target one row at a time and share the same SQL shape.
 *
 * <p>The detector requires equality predicates that cover a known unique index. It stays silent
 * when index metadata is unavailable or the WHERE clause could match more than one row. This keeps
 * range and set-based updates out of the results.
 *
 * @author haroya
 * @since 0.6.0
 */
public final class RepeatedSingleUpdateDetector implements DetectionRule {

  private static final int DEFAULT_THRESHOLD = 3;
  private static final String IDENTIFIER = "(?:`\\w+`|\"\\w+\"|[a-z_][a-z0-9_$]*)";
  private static final Pattern SCALAR_EQUALITY =
      Pattern.compile(
          "^(?:" + IDENTIFIER + "\\.)?(" + IDENTIFIER + ")\\s*=\\s*\\?$", Pattern.CASE_INSENSITIVE);
  private static final Pattern BETWEEN = Pattern.compile("\\bbetween\\b", Pattern.CASE_INSENSITIVE);
  private static final Pattern UPDATE_TARGETS =
      Pattern.compile("^update\\s+(.+?)\\s+set\\b", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
  private static final Pattern JOIN =
      Pattern.compile("\\b(?:join|straight_join)\\b", Pattern.CASE_INSENSITIVE);
  private static final Pattern OUTER_FROM = Pattern.compile("\\sfrom\\s", Pattern.CASE_INSENSITIVE);

  /** Table-name globs excluded by default because they usually represent staging work. */
  public static final Set<String> DEFAULT_EXCLUDE_TABLES =
      TableNameGlobMatcher.DEFAULT_STAGING_TABLES;

  private final int threshold;
  private final TableNameGlobMatcher excludedTables;

  public RepeatedSingleUpdateDetector() {
    this(DEFAULT_THRESHOLD, DEFAULT_EXCLUDE_TABLES);
  }

  public RepeatedSingleUpdateDetector(int threshold) {
    this(threshold, DEFAULT_EXCLUDE_TABLES);
  }

  public RepeatedSingleUpdateDetector(int threshold, Collection<String> excludeTablePatterns) {
    this.threshold = threshold;
    this.excludedTables = new TableNameGlobMatcher(excludeTablePatterns);
  }

  @Override
  public List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata indexMetadata) {
    Map<UpdateShape, Integer> occurrences = new LinkedHashMap<>();

    for (QueryRecord query : queries) {
      String sql = query.sql();
      String normalizedSql = query.normalizedSql();
      if (!isSingleRowUpdate(sql, normalizedSql, indexMetadata)) {
        continue;
      }

      String table = SqlParser.extractUpdateTable(sql);
      if (excludedTables.matches(table)) {
        continue;
      }
      occurrences.merge(new UpdateShape(table, normalizedSql), 1, Integer::sum);
    }

    List<Issue> issues = new ArrayList<>();
    occurrences.forEach(
        (shape, count) -> {
          if (count >= threshold) {
            issues.add(toIssue(shape, count));
          }
        });
    return issues;
  }

  @Override
  public String getRuleCode() {
    return IssueType.REPEATED_SINGLE_UPDATE.getCode();
  }

  private boolean isSingleRowUpdate(String sql, String normalizedSql, IndexMetadata indexMetadata) {
    if (!SqlParser.isUpdateQuery(sql)
        || normalizedSql == null
        || hasUnverifiableTargetScope(normalizedSql)
        || !SqlParser.hasOuterWhereClause(sql)
        || SqlParser.countOrConditions(sql) > 0) {
      return false;
    }

    String table = SqlParser.extractUpdateTable(sql);
    if (table == null || indexMetadata == null) {
      return false;
    }

    String metadataTable = resolveMetadataTable(indexMetadata, table);
    if (metadataTable == null) {
      return false;
    }
    Set<String> equalityColumns = scalarEqualityColumns(normalizedSql);
    return indexMetadata.hasUniqueIndexCoveredBy(metadataTable, equalityColumns);
  }

  private static String resolveMetadataTable(IndexMetadata indexMetadata, String table) {
    if (indexMetadata.hasTable(table)) {
      return table;
    }
    String lowercaseTable = table.toLowerCase(Locale.ROOT);
    return indexMetadata.hasTable(lowercaseTable) ? lowercaseTable : null;
  }

  private static boolean hasUnverifiableTargetScope(String normalizedSql) {
    String outerSql = SqlParser.removeSubqueries(normalizedSql);
    Matcher targetMatcher = UPDATE_TARGETS.matcher(outerSql);
    if (!targetMatcher.find()) {
      return true;
    }

    String targetClause = targetMatcher.group(1);
    return targetClause.indexOf('.') >= 0
        || targetClause.indexOf(',') >= 0
        || JOIN.matcher(targetClause).find()
        || OUTER_FROM.matcher(outerSql).find();
  }

  private static Set<String> scalarEqualityColumns(String normalizedSql) {
    String outerSql = SqlParser.removeSubqueries(normalizedSql);
    String whereBody = SqlParser.extractWhereBody(outerSql);
    if (whereBody == null || BETWEEN.matcher(whereBody).find()) {
      return Set.of();
    }

    Set<String> columns = new LinkedHashSet<>();
    String[] predicates =
        stripEnclosingParentheses(stripTrailingSemicolon(whereBody)).split("\\s+and\\s+");
    for (String predicate : predicates) {
      Matcher matcher = SCALAR_EQUALITY.matcher(stripEnclosingParentheses(predicate.trim()));
      if (matcher.matches()) {
        columns.add(unquote(matcher.group(1)));
      }
    }
    return columns;
  }

  private static String stripTrailingSemicolon(String expression) {
    String trimmed = expression.trim();
    return trimmed.endsWith(";") ? trimmed.substring(0, trimmed.length() - 1).trim() : trimmed;
  }

  private static String stripEnclosingParentheses(String expression) {
    String result = expression.trim();
    while (isWrappedInParentheses(result)) {
      result = result.substring(1, result.length() - 1).trim();
    }
    return result;
  }

  private static boolean isWrappedInParentheses(String expression) {
    if (expression.length() < 2 || expression.charAt(0) != '(') {
      return false;
    }

    int depth = 0;
    for (int i = 0; i < expression.length(); i++) {
      char character = expression.charAt(i);
      if (character == '(') {
        depth++;
      } else if (character == ')') {
        depth--;
        if (depth == 0) {
          return i == expression.length() - 1;
        }
      }
    }
    return false;
  }

  private static String unquote(String identifier) {
    if (identifier.length() >= 2) {
      char first = identifier.charAt(0);
      char last = identifier.charAt(identifier.length() - 1);
      if ((first == '`' && last == '`') || (first == '"' && last == '"')) {
        return identifier.substring(1, identifier.length() - 1);
      }
    }
    return identifier;
  }

  private static Issue toIssue(UpdateShape shape, int count) {
    return new Issue(
        IssueType.REPEATED_SINGLE_UPDATE,
        Severity.WARNING,
        shape.normalizedSql(),
        shape.table(),
        null,
        "Single-row UPDATE executed "
            + count
            + " times on table '"
            + shape.table()
            + "'. Repeating the same statement adds avoidable database round trips.",
        "Use one set-based UPDATE when every row receives the same change. When values differ per "
            + "row, execute a JDBC batch; Hibernate users can configure hibernate.jdbc.batch_size "
            + "and hibernate.order_updates.");
  }

  private record UpdateShape(String table, String normalizedSql) {}
}
