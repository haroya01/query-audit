package io.queryaudit.core.model;

import java.util.List;

/**
 * Encapsulates the analysis results for a single test method. Contains confirmed issues,
 * informational issues, acknowledged (baselined) issues, all captured queries, and summary
 * statistics such as unique pattern count, total query count, and total execution time.
 *
 * @author haroya
 * @since 0.2.0
 */
public class QueryAuditReport {

  private final String testClass;
  private final String testName;
  private final List<Issue> confirmedIssues;
  private final List<Issue> infoIssues;
  private final List<Issue> acknowledgedIssues;
  private final List<QueryRecord> allQueries;
  private final int uniquePatternCount;
  private final int totalQueryCount;
  private final long totalExecutionTimeNanos;

  /** Full 9-arg constructor including acknowledgedIssues. */
  public QueryAuditReport(
      String testClass,
      String testName,
      List<Issue> confirmedIssues,
      List<Issue> infoIssues,
      List<Issue> acknowledgedIssues,
      List<QueryRecord> allQueries,
      int uniquePatternCount,
      int totalQueryCount,
      long totalExecutionTimeNanos) {
    this.testClass = testClass;
    this.testName = testName;
    this.confirmedIssues = confirmedIssues;
    this.infoIssues = infoIssues;
    this.acknowledgedIssues = acknowledgedIssues;
    this.allQueries = allQueries;
    this.uniquePatternCount = uniquePatternCount;
    this.totalQueryCount = totalQueryCount;
    this.totalExecutionTimeNanos = totalExecutionTimeNanos;
  }

  /** Backward-compatible 8-arg constructor (testClass + no acknowledgedIssues). */
  public QueryAuditReport(
      String testClass,
      String testName,
      List<Issue> confirmedIssues,
      List<Issue> infoIssues,
      List<QueryRecord> allQueries,
      int uniquePatternCount,
      int totalQueryCount,
      long totalExecutionTimeNanos) {
    this(
        testClass,
        testName,
        confirmedIssues,
        infoIssues,
        List.of(),
        allQueries,
        uniquePatternCount,
        totalQueryCount,
        totalExecutionTimeNanos);
  }

  /** Backward-compatible 7-arg constructor (testClass defaults to null, no acknowledgedIssues). */
  public QueryAuditReport(
      String testName,
      List<Issue> confirmedIssues,
      List<Issue> infoIssues,
      List<QueryRecord> allQueries,
      int uniquePatternCount,
      int totalQueryCount,
      long totalExecutionTimeNanos) {
    this(
        null,
        testName,
        confirmedIssues,
        infoIssues,
        List.of(),
        allQueries,
        uniquePatternCount,
        totalQueryCount,
        totalExecutionTimeNanos);
  }

  public boolean hasConfirmedIssues() {
    return confirmedIssues != null && !confirmedIssues.isEmpty();
  }

  public List<Issue> getErrors() {
    if (confirmedIssues == null) return List.of();
    return confirmedIssues.stream().filter(issue -> issue.severity() == Severity.ERROR).toList();
  }

  public List<Issue> getWarnings() {
    if (confirmedIssues == null) return List.of();
    return confirmedIssues.stream().filter(issue -> issue.severity() == Severity.WARNING).toList();
  }

  public String getTestClass() {
    return testClass;
  }

  public String getTestName() {
    return testName;
  }

  public List<Issue> getConfirmedIssues() {
    return confirmedIssues;
  }

  public List<Issue> getInfoIssues() {
    return infoIssues;
  }

  public List<Issue> getAcknowledgedIssues() {
    return acknowledgedIssues != null ? acknowledgedIssues : List.of();
  }

  public int getAcknowledgedCount() {
    return acknowledgedIssues != null ? acknowledgedIssues.size() : 0;
  }

  public List<QueryRecord> getAllQueries() {
    return allQueries;
  }

  public int getUniquePatternCount() {
    return uniquePatternCount;
  }

  public int getTotalQueryCount() {
    return totalQueryCount;
  }

  public long getTotalExecutionTimeNanos() {
    return totalExecutionTimeNanos;
  }
}
