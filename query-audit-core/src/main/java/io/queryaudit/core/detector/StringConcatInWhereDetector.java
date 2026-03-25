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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Detects string concatenation on columns in WHERE clause using the {@code ||} operator. The {@code
 * ||} operator prevents index usage on the column.
 *
 * <p>Note: {@code CONCAT()} function calls are already caught by {@link WhereFunctionDetector}, so
 * this detector focuses exclusively on the {@code ||} operator.
 *
 * @author haroya
 * @since 0.2.0
 */
public class StringConcatInWhereDetector implements DetectionRule {

  /**
   * Matches {@code column || ...} in WHERE body where the left operand is a column name (word
   * character sequence), not a string/number literal.
   */
  private static final Pattern CONCAT_OPERATOR =
      Pattern.compile("(\\w+)\\s*\\|\\|", Pattern.CASE_INSENSITIVE);

  private static final Pattern LITERAL_VALUE =
      Pattern.compile(
          "^(?:\\d+(?:\\.\\d+)?|'[^']*'|\"[^\"]*\"|\\?|true|false|null)$",
          Pattern.CASE_INSENSITIVE);

  private static final Set<String> KEYWORDS =
      Set.of(
          "select", "from", "where", "and", "or", "not", "in", "is", "null", "true", "false",
          "between", "like", "as", "on", "join", "left", "right", "inner", "outer", "cross",
          "order", "group", "by", "having", "limit", "offset", "insert", "update", "delete", "set",
          "into", "values");

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

      String whereBody = SqlParser.extractWhereBody(sql);
      if (whereBody == null) {
        continue;
      }

      Matcher matcher = CONCAT_OPERATOR.matcher(whereBody);
      while (matcher.find()) {
        String leftOperand = matcher.group(1);
        // Skip if left operand is a literal or keyword
        if (LITERAL_VALUE.matcher(leftOperand).matches()) {
          continue;
        }
        if (KEYWORDS.contains(leftOperand.toLowerCase())) {
          continue;
        }

        List<String> tables = SqlParser.extractTableNames(sql);
        String table = tables.isEmpty() ? null : tables.get(0);

        issues.add(
            new Issue(
                IssueType.STRING_CONCAT_IN_WHERE,
                Severity.WARNING,
                normalized,
                table,
                leftOperand,
                "String concatenation (||) on column '" + leftOperand + "' prevents index usage",
                "String concatenation on column prevents index usage. "
                    + "Split into separate conditions on individual columns.",
                query.stackTrace()));
        break; // one issue per query
      }
    }

    return issues;
  }
}
