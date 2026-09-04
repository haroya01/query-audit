package io.queryaudit.core.model;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
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

  private static final String CORE_ID_PREFIX = "query-audit:core:v1:";

  private final String testId;
  private final TestSelector testSelector;
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
    this(
        fallbackTestId(testClass, testName),
        null,
        testClass,
        testName,
        confirmedIssues,
        infoIssues,
        acknowledgedIssues,
        allQueries,
        uniquePatternCount,
        totalQueryCount,
        totalExecutionTimeNanos);
  }

  private QueryAuditReport(
      String testId,
      TestSelector testSelector,
      String testClass,
      String testName,
      List<Issue> confirmedIssues,
      List<Issue> infoIssues,
      List<Issue> acknowledgedIssues,
      List<QueryRecord> allQueries,
      int uniquePatternCount,
      int totalQueryCount,
      long totalExecutionTimeNanos) {
    if (testId == null || testId.isBlank()) {
      throw new IllegalArgumentException("testId must not be blank");
    }
    this.testId = testId;
    this.testSelector = testSelector;
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

  // Attached via withIndexMetadata() after all analysis merges, not in the constructors — the
  // report is rebuilt several times during afterEach (regression/EXPLAIN/Hibernate merges) and
  // threading a tenth constructor argument through every rebuild site is worse than one late copy.
  private IndexMetadata indexMetadata;

  /**
   * Returns a copy carrying an identity supplied by a test framework. Core-only callers may keep
   * using the existing constructors, which derive a deterministic ID from the exact {@code
   * testClass} and {@code testName} values they receive.
   *
   * @since 0.6.0
   */
  public QueryAuditReport withTestIdentity(String testId, TestSelector testSelector) {
    QueryAuditReport copy =
        new QueryAuditReport(
            testId,
            testSelector,
            testClass,
            testName,
            confirmedIssues,
            infoIssues,
            getAcknowledgedIssues(),
            allQueries,
            uniquePatternCount,
            totalQueryCount,
            totalExecutionTimeNanos);
    copy.indexMetadata = indexMetadata;
    return copy;
  }

  /**
   * Returns a copy of this report carrying the index metadata collected for the test's DataSource,
   * or {@code this} when {@code metadata} is {@code null}. The JSON reporter serializes the subset
   * relevant to the findings so report consumers can act without separate database access.
   *
   * @since 0.5.0
   */
  public QueryAuditReport withIndexMetadata(IndexMetadata metadata) {
    if (metadata == null) {
      return this;
    }
    QueryAuditReport copy =
        new QueryAuditReport(
            testId,
            testSelector,
            testClass,
            testName,
            confirmedIssues,
            infoIssues,
            getAcknowledgedIssues(),
            allQueries,
            uniquePatternCount,
            totalQueryCount,
            totalExecutionTimeNanos);
    copy.indexMetadata = metadata;
    return copy;
  }

  /**
   * Returns the index metadata attached to this report, or {@code null} when none was collected
   * (non-database tests, or reports built before {@link #withIndexMetadata}).
   *
   * @since 0.5.0
   */
  public IndexMetadata getIndexMetadata() {
    return indexMetadata;
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

  /** Stable identity used by machine reports, comparisons, contracts, and count baselines. */
  public String getTestId() {
    return testId;
  }

  /** Returns a reproducible framework selector, or {@code null} for core-only reports. */
  public TestSelector getTestSelector() {
    return testSelector;
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

  /** Number of captured query records retained in this report. */
  public int getRetainedQueryCount() {
    return allQueries == null ? 0 : allQueries.size();
  }

  /** Number of captured queries whose records are no longer retained. */
  public int getOmittedQueryCount() {
    return Math.max(0, totalQueryCount - getRetainedQueryCount());
  }

  /** Describes query evidence retention, independently of the audit verdict. */
  public QueryEvidenceStatus getQueryEvidenceStatus() {
    if (getOmittedQueryCount() == 0) {
      return QueryEvidenceStatus.COMPLETE;
    }
    return getRetainedQueryCount() == 0 ? QueryEvidenceStatus.OMITTED : QueryEvidenceStatus.PARTIAL;
  }

  /**
   * Returns a compact copy that keeps findings, identity, and metadata but releases query records.
   */
  public QueryAuditReport withoutQueryEvidence() {
    QueryAuditReport copy =
        new QueryAuditReport(
            testId,
            testSelector,
            testClass,
            testName,
            confirmedIssues,
            infoIssues,
            getAcknowledgedIssues(),
            List.of(),
            uniquePatternCount,
            totalQueryCount,
            totalExecutionTimeNanos);
    copy.indexMetadata = indexMetadata;
    return copy;
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

  private static String fallbackTestId(String testClass, String testName) {
    String source = lengthPrefixed(testClass) + lengthPrefixed(testName);
    try {
      byte[] digest =
          MessageDigest.getInstance("SHA-256").digest(source.getBytes(StandardCharsets.UTF_8));
      return CORE_ID_PREFIX + HexFormat.of().formatHex(digest);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is unavailable", e);
    }
  }

  private static String lengthPrefixed(String value) {
    return value == null ? "-:" : value.length() + ":" + value;
  }
}
