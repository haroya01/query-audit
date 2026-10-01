package io.queryaudit.core.model;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;

/**
 * Encapsulates the analysis results for a single test method. Contains confirmed issues,
 * informational issues, acknowledged (baselined) issues, all captured queries, and summary
 * statistics such as unique pattern count, total query count, and total execution time.
 *
 * <p>Non-null collection inputs are snapshotted and exposed as unmodifiable lists. Copy operations
 * share those snapshots, so caller changes cannot alter a completed report or its copies.
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
  private final ReportFindings findings;
  private final List<QueryRecord> allQueries;
  private final int uniquePatternCount;
  private final int totalQueryCount;
  private final long totalExecutionTimeNanos;
  private final IndexMetadata indexMetadata;

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
        ReportFindings.legacy(confirmedIssues, infoIssues, acknowledgedIssues),
        snapshot(allQueries),
        uniquePatternCount,
        totalQueryCount,
        totalExecutionTimeNanos,
        null);
  }

  private QueryAuditReport(
      String testId,
      TestSelector testSelector,
      String testClass,
      String testName,
      ReportFindings findings,
      List<QueryRecord> allQueries,
      int uniquePatternCount,
      int totalQueryCount,
      long totalExecutionTimeNanos,
      IndexMetadata indexMetadata) {
    if (testId == null || testId.isBlank()) {
      throw new IllegalArgumentException("testId must not be blank");
    }
    this.testId = testId;
    this.testSelector = testSelector;
    this.testClass = testClass;
    this.testName = testName;
    this.findings = findings;
    this.allQueries = allQueries;
    this.uniquePatternCount = uniquePatternCount;
    this.totalQueryCount = totalQueryCount;
    this.totalExecutionTimeNanos = totalExecutionTimeNanos;
    this.indexMetadata = indexMetadata;
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

  /**
   * Returns a copy carrying an identity supplied by a test framework. Core-only callers may keep
   * using the existing constructors, which derive a deterministic ID from the exact {@code
   * testClass} and {@code testName} values they receive.
   *
   * @since 0.6.0
   */
  public QueryAuditReport withTestIdentity(String testId, TestSelector testSelector) {
    return copy(testId, testSelector, allQueries, indexMetadata);
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
    return copy(testId, testSelector, allQueries, metadata);
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
    return findings.hasConfirmed();
  }

  /** The recommended result API: all built-in and custom kinds in one immutable view. */
  public AuditFindings getFindings() {
    return findings.view();
  }

  /**
   * Replaces every finding category, keeping identity, evidence, and metadata. This is a data-copy
   * operation, not policy evaluation; use the analyzer to classify raw rule output.
   */
  public QueryAuditReport withFindings(AuditFindings replacement) {
    return copyWithFindings(ReportFindings.from(replacement));
  }

  /** Replaces only the informational category, retaining the other immutable snapshots. */
  public QueryAuditReport withInformationalFindings(List<Finding> informational) {
    return copyWithFindings(findings.withInformational(informational));
  }

  /** A human-facing projection; canonical machine reports should retain informational findings. */
  public QueryAuditReport withoutInformationalFindings() {
    return withInformationalFindings(List.of());
  }

  /**
   * Returns a copy with replacement custom buckets. Legacy issue buckets remain unchanged. Inputs
   * must be non-null and contain no null elements. This is a data-copy operation, not a policy
   * evaluation; hosts should use the analyzer to classify raw findings. Compatibility bridge for
   * older hosts; new code should use {@link #withFindings(AuditFindings)}.
   */
  public QueryAuditReport withCustomFindings(
      List<Finding> confirmed, List<Finding> info, List<Finding> acknowledged) {
    return copyWithFindings(findings.withCustom(confirmed, info, acknowledged));
  }

  /** Compatibility view containing only custom findings; prefer {@link #getFindings()}. */
  public List<Finding> getCustomConfirmedFindings() {
    return findings.customConfirmed();
  }

  /** Compatibility view containing only custom findings; prefer {@link #getFindings()}. */
  public List<Finding> getCustomInfoFindings() {
    return findings.customInfo();
  }

  /** Compatibility view containing only custom findings; prefer {@link #getFindings()}. */
  public List<Finding> getCustomAcknowledgedFindings() {
    return findings.customAcknowledged();
  }

  /** Convenience alias for {@code getFindings().confirmed()}. */
  public List<Finding> getConfirmedFindings() {
    return findings.confirmed();
  }

  public List<Finding> getInfoFindings() {
    return findings.informational();
  }

  public List<Finding> getAcknowledgedFindings() {
    return findings.acknowledged();
  }

  /** Legacy built-in errors only; use {@code getFindings().errors()} for all kinds. */
  public List<Issue> getErrors() {
    List<Issue> confirmedIssues = getConfirmedIssues();
    if (confirmedIssues == null) return List.of();
    return confirmedIssues.stream().filter(issue -> issue.severity() == Severity.ERROR).toList();
  }

  /** Legacy built-in warnings only; use {@code getFindings().warnings()} for all kinds. */
  public List<Issue> getWarnings() {
    List<Issue> confirmedIssues = getConfirmedIssues();
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

  /** Legacy enum-only view; use {@code getFindings().confirmed()} for all kinds. */
  public List<Issue> getConfirmedIssues() {
    return findings.legacyConfirmed();
  }

  /** Legacy enum-only view; use {@code getFindings().informational()} for all kinds. */
  public List<Issue> getInfoIssues() {
    return findings.legacyInfo();
  }

  /** Legacy enum-only view; use {@code getFindings().acknowledged()} for all kinds. */
  public List<Issue> getAcknowledgedIssues() {
    return findings.legacyAcknowledged();
  }

  public int getAcknowledgedCount() {
    return getAcknowledgedIssues().size();
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
    return copy(testId, testSelector, List.of(), indexMetadata);
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

  private QueryAuditReport copy(
      String testId,
      TestSelector testSelector,
      List<QueryRecord> queryEvidence,
      IndexMetadata metadata) {
    // Copy operations only share snapshots owned by this report or an immutable empty list.
    return new QueryAuditReport(
        testId,
        testSelector,
        testClass,
        testName,
        findings,
        queryEvidence,
        uniquePatternCount,
        totalQueryCount,
        totalExecutionTimeNanos,
        metadata);
  }

  private QueryAuditReport copyWithFindings(ReportFindings replacement) {
    return new QueryAuditReport(
        testId,
        testSelector,
        testClass,
        testName,
        replacement,
        allQueries,
        uniquePatternCount,
        totalQueryCount,
        totalExecutionTimeNanos,
        indexMetadata);
  }

  private static <T> List<T> snapshot(List<T> values) {
    // Preserve the existing nullable-list and nullable-element constructor contract.
    return values == null ? null : Collections.unmodifiableList(new ArrayList<>(values));
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
