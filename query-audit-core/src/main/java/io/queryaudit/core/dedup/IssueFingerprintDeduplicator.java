package io.queryaudit.core.dedup;

import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.Severity;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Deduplicates issues across multiple test reports by generating a fingerprint for each issue and
 * grouping identical ones together.
 *
 * <p>Fingerprint format: {@code issueType|table|column|normalizedQuery(truncated to 200 chars)}
 *
 * <p>The result is sorted by severity DESC, then occurrence count DESC.
 *
 * @author haroya
 * @since 0.2.0
 */
public final class IssueFingerprintDeduplicator {

  private static final int MAX_AFFECTED_TESTS = 10;
  private static final int MAX_QUERY_LENGTH = 200;

  private static final Pattern SINGLE_QUOTED =
      Pattern.compile("'[^'\\\\]*(?:(?:''|\\\\.)[^'\\\\]*)*'");
  private static final Pattern NUMERIC = Pattern.compile("\\b\\d+\\.?\\d*\\b");
  private static final Pattern WHITESPACE = Pattern.compile("\\s+");

  private IssueFingerprintDeduplicator() {}

  /**
   * Deduplicates all confirmed and info issues from the given reports.
   *
   * @param reports list of reports from multiple test methods
   * @return deduplicated issues sorted by severity DESC, then occurrence count DESC
   */
  public static List<DeduplicatedIssue> deduplicate(List<QueryAuditReport> reports) {
    if (reports == null || reports.isEmpty()) {
      return List.of();
    }

    // fingerprint -> accumulated data
    Map<String, AccumulatedIssue> map = new LinkedHashMap<>();

    for (QueryAuditReport report : reports) {
      String testName = report.getTestName() != null ? report.getTestName() : "unknown";
      String testClass = report.getTestClass();
      // Store as "className#testName" to enable linking from dedup summary to class pages
      String qualifiedTest =
          (testClass != null && !testClass.isBlank()) ? testClass + "#" + testName : testName;

      // Process confirmed issues
      if (report.getConfirmedIssues() != null) {
        for (Issue issue : report.getConfirmedIssues()) {
          accumulate(map, issue, qualifiedTest);
        }
      }

      // Process info issues
      if (report.getInfoIssues() != null) {
        for (Issue issue : report.getInfoIssues()) {
          accumulate(map, issue, qualifiedTest);
        }
      }
    }

    // Convert to DeduplicatedIssue and sort
    List<DeduplicatedIssue> result = new ArrayList<>(map.size());
    for (Map.Entry<String, AccumulatedIssue> entry : map.entrySet()) {
      AccumulatedIssue acc = entry.getValue();
      result.add(
          new DeduplicatedIssue(
              acc.representative,
              entry.getKey(),
              acc.count,
              List.copyOf(acc.affectedTests),
              acc.highestSeverity));
    }

    result.sort(
        Comparator.comparing(
                DeduplicatedIssue
                    ::highestSeverity) // ERROR(0) < WARNING(1) < INFO(2), so natural order =
            // severity DESC
            .thenComparing(d -> -d.occurrenceCount())); // count DESC

    return result;
  }

  /** Generates a fingerprint for the given issue. */
  static String fingerprint(Issue issue) {
    String type = issue.type() != null ? issue.type().name() : "";
    String table = issue.table() != null ? issue.table() : "";
    String column = issue.column() != null ? issue.column() : "";
    String normalizedQuery = normalizeQuery(issue.query());
    return type + "|" + table + "|" + column + "|" + normalizedQuery;
  }

  /**
   * Normalizes a SQL query for fingerprinting: lowercase, strip literals to ?, collapse whitespace,
   * truncate to 200 chars.
   */
  static String normalizeQuery(String sql) {
    if (sql == null || sql.isBlank()) {
      return "";
    }
    String result = sql;
    result = SINGLE_QUOTED.matcher(result).replaceAll("?");
    result = NUMERIC.matcher(result).replaceAll("?");
    result = result.toLowerCase();
    result = WHITESPACE.matcher(result.trim()).replaceAll(" ");
    if (result.length() > MAX_QUERY_LENGTH) {
      result = result.substring(0, MAX_QUERY_LENGTH);
    }
    return result;
  }

  private static void accumulate(Map<String, AccumulatedIssue> map, Issue issue, String testName) {
    String fp = fingerprint(issue);
    AccumulatedIssue acc = map.get(fp);
    if (acc == null) {
      acc = new AccumulatedIssue(issue);
      map.put(fp, acc);
    }
    acc.count++;
    if (acc.affectedTests.size() < MAX_AFFECTED_TESTS && !acc.affectedTests.contains(testName)) {
      acc.affectedTests.add(testName);
    }
    // Track highest severity (ERROR < WARNING < INFO in enum ordinal, so lower ordinal = higher
    // severity)
    if (issue.severity().ordinal() < acc.highestSeverity.ordinal()) {
      acc.highestSeverity = issue.severity();
    }
  }

  private static final class AccumulatedIssue {
    final Issue representative;
    int count;
    final List<String> affectedTests = new ArrayList<>();
    Severity highestSeverity;

    AccumulatedIssue(Issue representative) {
      this.representative = representative;
      this.highestSeverity = representative.severity();
    }
  }
}
