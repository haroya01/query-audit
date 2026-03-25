package io.queryaudit.core.detector;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.parser.SqlParser;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Detects {@code INSERT INTO table VALUES (...)} without specifying column names.
 *
 * <p>Source: SQLCheck 3011, Nagy &amp; Cleve SCAM 2017
 *
 * <p>Adding, removing, or reordering columns silently breaks the INSERT. Same fragility as SELECT
 * *.
 *
 * @author haroya
 * @since 0.2.0
 */
public class ImplicitColumnsInsertDetector implements DetectionRule {

  private static final Pattern IMPLICIT_INSERT =
      Pattern.compile("INSERT\\s+INTO\\s+(?:`\\w+`|\\w+)\\s+VALUES", Pattern.CASE_INSENSITIVE);

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

      if (IMPLICIT_INSERT.matcher(sql).find()) {
        String table = SqlParser.extractInsertTable(sql);
        issues.add(
            new Issue(
                IssueType.IMPLICIT_COLUMNS_INSERT,
                Severity.WARNING,
                normalized,
                table,
                null,
                "INSERT without explicit column list detected. "
                    + "Adding, removing, or reordering columns will silently break this statement.",
                "INSERT without explicit column list is fragile. "
                    + "Specify column names: INSERT INTO table (col1, col2) VALUES (?, ?)",
                query.stackTrace()));
      }
    }
    return issues;
  }
}
