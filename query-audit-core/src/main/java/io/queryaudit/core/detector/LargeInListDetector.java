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
 * Detects IN clauses with too many values, which can cause optimizer issues.
 *
 * <p>Large IN lists (>100 values) can cause the query optimizer to spend excessive time evaluating
 * execution plans, and extremely large lists (>1000) can exceed parser limits or cause memory
 * issues in some databases.
 *
 * <p>Thresholds:
 *
 * <ul>
 *   <li>&gt; 100 values: WARNING
 *   <li>&gt; 1000 values: ERROR
 * </ul>
 *
 * @author haroya
 * @since 0.2.0
 */
public class LargeInListDetector implements DetectionRule {

  private static final int DEFAULT_WARNING_THRESHOLD = 100;
  private static final int ERROR_MULTIPLIER = 10;

  /**
   * Matches an IN clause and captures its contents (including nested parentheses). The content
   * group captures everything between the outer parentheses.
   */
  private static final Pattern IN_CLAUSE_PATTERN =
      Pattern.compile("\\bIN\\s*\\(([^)]+)\\)", Pattern.CASE_INSENSITIVE);

  /**
   * Matches an IN list that contains only parameter placeholders ({@code ?}). When all values
   * are placeholders, the actual count at runtime is unknown and may differ from the analyzed
   * SQL (e.g., Hibernate generates a fixed-size padded list). Flagging these is a false positive.
   */
  private static final Pattern ALL_PLACEHOLDERS =
      Pattern.compile("^[\\s?,]+$");

  /**
   * Counts the number of comma-separated values (placeholders or literals) in an IN list. Counts
   * occurrences of '?' placeholders and literal values separated by commas.
   */
  private static final Pattern VALUE_PATTERN =
      Pattern.compile("\\?|'[^']*'|\\b\\d+(?:\\.\\d+)?\\b");

  private final int warningThreshold;
  private final int errorThreshold;

  public LargeInListDetector() {
    this(DEFAULT_WARNING_THRESHOLD);
  }

  public LargeInListDetector(int warningThreshold) {
    this.warningThreshold = warningThreshold;
    this.errorThreshold = warningThreshold * ERROR_MULTIPLIER;
  }

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

      Matcher inMatcher = IN_CLAUSE_PATTERN.matcher(sql);
      while (inMatcher.find()) {
        String inContent = inMatcher.group(1);

        // Skip IN lists composed entirely of parameter placeholders — the actual count
        // at runtime is unknown (e.g., Hibernate IN-clause padding)
        if (ALL_PLACEHOLDERS.matcher(inContent).matches()) {
          continue;
        }

        int valueCount = countValues(inContent);

        if (valueCount > warningThreshold) {
          Severity severity = valueCount > errorThreshold ? Severity.ERROR : Severity.WARNING;

          List<String> tables = SqlParser.extractTableNames(sql);
          String table = tables.isEmpty() ? null : tables.get(0);

          issues.add(
              new Issue(
                  IssueType.LARGE_IN_LIST,
                  severity,
                  normalized,
                  table,
                  null,
                  "IN clause with " + valueCount + " values may cause optimizer issues",
                  "Consider using a temporary table JOIN, batch processing, "
                      + "or EXISTS subquery for large IN lists.",
                  query.stackTrace()));
          break; // Report once per query
        }
      }
    }

    return issues;
  }

  /**
   * Count the number of values in an IN list content string. Counts by splitting on commas (simple
   * but effective for typical SQL).
   */
  private int countValues(String inContent) {
    if (inContent == null || inContent.trim().isEmpty()) {
      return 0;
    }
    // Count commas + 1 gives the number of values
    int count = 1;
    int depth = 0;
    for (int i = 0; i < inContent.length(); i++) {
      char c = inContent.charAt(i);
      if (c == '(') depth++;
      else if (c == ')') depth--;
      else if (c == ',' && depth == 0) count++;
    }
    return count;
  }
}
