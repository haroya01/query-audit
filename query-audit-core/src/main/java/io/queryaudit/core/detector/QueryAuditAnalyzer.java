package io.queryaudit.core.detector;

import io.queryaudit.core.baseline.Baseline;
import io.queryaudit.core.baseline.BaselineEntry;
import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.LifecyclePhase;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Runs configured detection rules against captured queries and produces a report. The analyzer
 * applies baseline filtering and severity overrides, then classifies findings as confirmed,
 * informational, or acknowledged.
 *
 * @author haroya
 * @since 0.2.0
 */
public class QueryAuditAnalyzer {

  private final List<DetectionRule> rules;
  private final boolean ruleInputsComplete;
  private final QueryAuditConfig config;
  private final List<BaselineEntry> baseline;

  public QueryAuditAnalyzer(QueryAuditConfig config) {
    this(config, (Path) null);
  }

  /**
   * Creates an analyzer with a specific baseline file path.
   *
   * @param config query-audit configuration
   * @param baselinePath path to the baseline file, or {@code null} to use the default ({@code
   *     .query-audit-baseline} in the working directory)
   */
  public QueryAuditAnalyzer(QueryAuditConfig config, Path baselinePath) {
    this.config = config;
    DetectionRuleRegistry.RuleSet registered =
        new DetectionRuleRegistry(config).createRuleSet(null);
    this.rules = registered.rules();
    this.ruleInputsComplete = registered.inputsComplete();

    // Load baseline
    if (baselinePath != null) {
      this.baseline = Baseline.load(baselinePath);
    } else {
      this.baseline = Baseline.load(Paths.get(Baseline.DEFAULT_FILE_NAME));
    }
  }

  /**
   * Creates an analyzer with a pre-loaded baseline list.
   *
   * @param config query-audit configuration
   * @param baseline pre-loaded baseline entries
   */
  public QueryAuditAnalyzer(QueryAuditConfig config, List<BaselineEntry> baseline) {
    this.config = config;
    DetectionRuleRegistry.RuleSet registered =
        new DetectionRuleRegistry(config).createRuleSet(null);
    this.rules = registered.rules();
    this.ruleInputsComplete = registered.inputsComplete();
    this.baseline = baseline != null ? baseline : List.of();
  }

  /**
   * Creates an analyzer with a specific baseline file path and additional custom detection rules.
   * The additional rules are appended after the built-in and ServiceLoader-discovered rules.
   *
   * @param config query-audit configuration
   * @param baselinePath path to the baseline file, or {@code null} to use the default
   * @param additionalRules extra detection rules to append, or {@code null} to skip
   */
  public QueryAuditAnalyzer(
      QueryAuditConfig config, Path baselinePath, List<DetectionRule> additionalRules) {
    this.config = config;
    DetectionRuleRegistry.RuleSet registered =
        new DetectionRuleRegistry(config).createRuleSet(additionalRules);
    this.rules = registered.rules();
    this.ruleInputsComplete = registered.inputsComplete();

    if (baselinePath != null) {
      this.baseline = Baseline.load(baselinePath);
    } else {
      this.baseline = Baseline.load(Paths.get(Baseline.DEFAULT_FILE_NAME));
    }
  }

  /**
   * Creates an analyzer with a pre-loaded baseline list and additional custom detection rules. The
   * additional rules are appended after the built-in and ServiceLoader-discovered rules.
   *
   * @param config query-audit configuration
   * @param baseline pre-loaded baseline entries
   * @param additionalRules extra detection rules to append, or {@code null} to skip
   */
  public QueryAuditAnalyzer(
      QueryAuditConfig config, List<BaselineEntry> baseline, List<DetectionRule> additionalRules) {
    this.config = config;
    DetectionRuleRegistry.RuleSet registered =
        new DetectionRuleRegistry(config).createRuleSet(additionalRules);
    this.rules = registered.rules();
    this.ruleInputsComplete = registered.inputsComplete();
    this.baseline = baseline != null ? baseline : List.of();
  }

  public QueryAuditAnalyzer() {
    this(QueryAuditConfig.defaults());
  }

  public QueryAuditReport analyze(
      String testClass, String testName, List<QueryRecord> queries, IndexMetadata indexMetadata) {
    if (!config.isEnabled() || queries == null || queries.isEmpty()) {
      return new QueryAuditReport(
          testClass,
          testName,
          List.of(),
          List.of(),
          List.of(),
          queries != null ? queries : List.of(),
          0,
          0,
          0L);
    }

    QueryAuditReport report = analyze(testName, queries, indexMetadata);
    return new QueryAuditReport(
        testClass,
        report.getTestName(),
        report.getConfirmedIssues(),
        report.getInfoIssues(),
        report.getAcknowledgedIssues(),
        report.getAllQueries(),
        report.getUniquePatternCount(),
        report.getTotalQueryCount(),
        report.getTotalExecutionTimeNanos());
  }

  public QueryAuditReport analyze(
      String testName, List<QueryRecord> queries, IndexMetadata indexMetadata) {
    if (!config.isEnabled() || queries == null || queries.isEmpty()) {
      return new QueryAuditReport(
          testName, List.of(), List.of(), queries != null ? queries : List.of(), 0, 0, 0L);
    }

    // Filter out suppressed queries (used for stats: total count, unique patterns, exec time)
    List<QueryRecord> filteredQueries =
        queries.stream().filter(q -> !config.isQuerySuppressed(q.sql())).toList();

    // For detection, further filter by lifecycle phase.
    // By default only TEST-phase queries are analyzed; setup/teardown queries are excluded
    // to prevent false positives from test infrastructure (e.g., deleteAll, repeated save).
    List<QueryRecord> detectableQueries =
        config.isIncludeSetupQueries()
            ? filteredQueries
            : filteredQueries.stream().filter(q -> q.phase() == LifecyclePhase.TEST).toList();

    // Collect all issues from all rules (only against detectable queries)
    List<Issue> allIssues = new ArrayList<>();
    for (DetectionRule rule : rules) {
      List<Issue> ruleIssues = rule.evaluate(detectableQueries, indexMetadata);
      allIssues.addAll(ruleIssues);
    }

    // Single-pass classification of issues into confirmed/info/acknowledged
    // buckets. Applies severity overrides from config before classification.
    List<Issue> confirmedIssues = new ArrayList<>();
    List<Issue> infoIssues = new ArrayList<>();
    List<Issue> acknowledgedIssues = new ArrayList<>();

    classifyIssues(allIssues, confirmedIssues, infoIssues, acknowledgedIssues);

    // Single-pass calculation of unique patterns and total execution time.
    // Replaces two separate stream passes over filteredQueries.
    Set<String> uniquePatterns = new HashSet<>();
    long totalExecutionTimeNanos = 0L;
    for (QueryRecord q : filteredQueries) {
      if (q.normalizedSql() != null) {
        uniquePatterns.add(q.normalizedSql());
      }
      totalExecutionTimeNanos += q.executionTimeNanos();
    }
    long uniquePatternCount = uniquePatterns.size();

    return new QueryAuditReport(
        null,
        testName,
        confirmedIssues,
        infoIssues,
        acknowledgedIssues,
        queries,
        (int) uniquePatternCount,
        filteredQueries.size(),
        totalExecutionTimeNanos);
  }

  /**
   * Applies the configured rule selection, suppressions, severity overrides, and baseline to issues
   * detected outside the normal SQL rule list, then merges them into an existing report.
   *
   * @param report report that already contains the SQL analysis result
   * @param detectedIssues additional issues to classify and merge
   * @return the merged report, or {@code report} when no additional issue remains after policy
   *     evaluation
   * @since 0.6.0
   */
  public QueryAuditReport mergeDetectedIssues(QueryAuditReport report, List<Issue> detectedIssues) {
    if (detectedIssues == null || detectedIssues.isEmpty()) {
      return report;
    }

    List<Issue> confirmedIssues = new ArrayList<>(report.getConfirmedIssues());
    List<Issue> infoIssues = new ArrayList<>(report.getInfoIssues());
    List<Issue> acknowledgedIssues = new ArrayList<>(report.getAcknowledgedIssues());
    int existingIssueCount = confirmedIssues.size() + infoIssues.size() + acknowledgedIssues.size();

    classifyIssues(detectedIssues, confirmedIssues, infoIssues, acknowledgedIssues);

    int mergedIssueCount = confirmedIssues.size() + infoIssues.size() + acknowledgedIssues.size();
    if (mergedIssueCount == existingIssueCount) {
      return report;
    }

    QueryAuditReport mergedReport =
        new QueryAuditReport(
            report.getTestClass(),
            report.getTestName(),
            confirmedIssues,
            infoIssues,
            acknowledgedIssues,
            report.getAllQueries(),
            report.getUniquePatternCount(),
            report.getTotalQueryCount(),
            report.getTotalExecutionTimeNanos());
    return mergedReport
        .withTestIdentity(report.getTestId(), report.getTestSelector())
        .withIndexMetadata(report.getIndexMetadata());
  }

  private void classifyIssues(
      List<Issue> issues,
      List<Issue> confirmedIssues,
      List<Issue> infoIssues,
      List<Issue> acknowledgedIssues) {
    for (Issue issue : issues) {
      // The exact issue code is the correctness net for detectors that do not declare a rule code.
      if (config.isRuleExcluded(issue.type().getCode())) {
        continue;
      }
      if (config.isSuppressed(issue.type().getCode(), issue.table(), issue.column())) {
        continue;
      }

      Issue effectiveIssue = applySeverityOverride(issue);
      if (Baseline.isAcknowledged(baseline, effectiveIssue)) {
        acknowledgedIssues.add(effectiveIssue);
      } else if (effectiveIssue.severity() == Severity.INFO) {
        infoIssues.add(effectiveIssue);
      } else {
        confirmedIssues.add(effectiveIssue);
      }
    }
  }

  private Issue applySeverityOverride(Issue issue) {
    Severity effectiveSeverity =
        config.getEffectiveSeverity(issue.type().getCode(), issue.severity());
    if (effectiveSeverity == issue.severity()) {
      return issue;
    }
    return new Issue(
        issue.type(),
        effectiveSeverity,
        issue.query(),
        issue.table(),
        issue.column(),
        issue.detail(),
        issue.suggestion(),
        issue.sourceLocation());
  }

  public QueryAuditConfig getConfig() {
    return config;
  }

  public List<DetectionRule> getRules() {
    return List.copyOf(rules);
  }

  /** Whether every active rule was constructed from the fingerprinted core configuration. */
  public boolean hasCompleteRuleInputs() {
    return ruleInputsComplete;
  }

  public List<BaselineEntry> getBaseline() {
    return List.copyOf(baseline);
  }
}
