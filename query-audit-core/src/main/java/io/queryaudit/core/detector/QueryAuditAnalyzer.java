package io.queryaudit.core.detector;

import io.queryaudit.core.baseline.Baseline;
import io.queryaudit.core.baseline.BaselineEntry;
import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.extension.AuditExtensions;
import io.queryaudit.core.extension.AuditRule;
import io.queryaudit.core.extension.RuleContext;
import io.queryaudit.core.extension.RuleDescriptor;
import io.queryaudit.core.extension.internal.AuditRuleRuntime;
import io.queryaudit.core.model.Finding;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.LifecyclePhase;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
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
  private final List<DetectionRuleRegistration> ruleRegistrations;
  private final boolean ruleInputsComplete;
  private final QueryAuditConfig config;
  private final List<BaselineEntry> baseline;
  private final AuditRuleRuntime auditRuleRuntime;
  private final FindingPolicy findingPolicy;

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
    this(config, loadBaseline(baselinePath), List.of(), List.of());
  }

  /**
   * Creates an analyzer with a pre-loaded baseline list.
   *
   * @param config query-audit configuration
   * @param baseline pre-loaded baseline entries
   */
  public QueryAuditAnalyzer(QueryAuditConfig config, List<BaselineEntry> baseline) {
    this(config, baseline, List.of(), List.of());
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
    this(config, loadBaseline(baselinePath), additionalRules, List.of());
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
    this(config, baseline, additionalRules, List.of());
  }

  private QueryAuditAnalyzer(
      QueryAuditConfig config,
      List<BaselineEntry> baseline,
      List<DetectionRule> additionalRules,
      List<AuditRule> auditRules) {
    this(config, baseline, additionalRules, auditRules, null);
  }

  private QueryAuditAnalyzer(
      QueryAuditConfig config,
      List<BaselineEntry> baseline,
      List<DetectionRule> additionalRules,
      List<AuditRule> auditRules,
      AuditExtensions extensions) {
    this.config = Objects.requireNonNull(config, "config");
    DetectionRuleRegistry registry = new DetectionRuleRegistry(config);
    DetectionRuleRegistry.RuleSet registered =
        extensions == null
            ? registry.createRuleSet(additionalRules)
            : registry.createRegisteredRuleSet(extensions.rulesById());
    this.rules = registered.rules();
    this.ruleRegistrations = registered.registrations();
    this.auditRuleRuntime =
        extensions == null
            ? new AuditRuleRuntime(config, auditRules, rules)
            : new AuditRuleRuntime(config, extensions.auditRulesById(), rules);
    this.ruleInputsComplete = registered.inputsComplete() && auditRuleRuntime.rules().isEmpty();
    this.baseline = baseline != null ? List.copyOf(baseline) : List.of();
    this.findingPolicy = new FindingPolicy(config, this.baseline);
  }

  /**
   * Assembles built-in, discovered, and explicit legacy/open rules under the same host policy.
   * Active open rules remain unverified comparison inputs: descriptor declarations cannot attest to
   * hidden implementation settings. This named factory avoids ambiguous constructor overloads.
   */
  public static QueryAuditAnalyzer withExtensions(
      QueryAuditConfig config, Path baselinePath, AuditExtensions extensions) {
    Objects.requireNonNull(extensions, "extensions");
    return new QueryAuditAnalyzer(
        config, loadBaseline(baselinePath), List.of(), List.of(), extensions);
  }

  private static List<BaselineEntry> loadBaseline(Path path) {
    return Baseline.load(path != null ? path : Paths.get(Baseline.DEFAULT_FILE_NAME));
  }

  public QueryAuditAnalyzer() {
    this(QueryAuditConfig.defaults());
  }

  public QueryAuditReport analyze(
      String testClass, String testName, List<QueryRecord> queries, IndexMetadata indexMetadata) {
    // Preserve virtual dispatch for integrations overriding the original three-argument method.
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
            report.getTotalExecutionTimeNanos())
        .withCustomFindings(
            report.getCustomConfirmedFindings(),
            report.getCustomInfoFindings(),
            report.getCustomAcknowledgedFindings());
  }

  public QueryAuditReport analyze(
      String testName, List<QueryRecord> queries, IndexMetadata indexMetadata) {
    queries = queries != null ? queries : List.of();
    if (!config.isEnabled()) {
      return new QueryAuditReport(testName, List.of(), List.of(), queries, 0, 0, 0L);
    }

    List<QueryRecord> filteredQueries =
        queries.stream().filter(q -> !config.isQuerySuppressed(q.sql())).toList();

    // By default only TEST-phase queries are analyzed; setup/teardown queries are excluded
    // to prevent false positives from test infrastructure (e.g., deleteAll, repeated save).
    List<QueryRecord> detectableQueries =
        config.isIncludeSetupQueries()
            ? filteredQueries
            : filteredQueries.stream().filter(q -> q.phase() == LifecyclePhase.TEST).toList();

    List<Issue> allIssues = new ArrayList<>();
    // Legacy detectors keep their no-captured-query behavior. Open rules may enforce absence.
    if (!queries.isEmpty()) {
      for (DetectionRuleRegistration registration : ruleRegistrations) {
        allIssues.addAll(registration.evaluate(detectableQueries, indexMetadata));
      }
    }

    List<Issue> confirmedIssues = new ArrayList<>();
    List<Issue> infoIssues = new ArrayList<>();
    List<Issue> acknowledgedIssues = new ArrayList<>();

    classifyIssues(allIssues, confirmedIssues, infoIssues, acknowledgedIssues);

    List<Finding> customConfirmed = new ArrayList<>();
    List<Finding> customInfo = new ArrayList<>();
    List<Finding> customAcknowledged = new ArrayList<>();
    if (!auditRuleRuntime.rules().isEmpty()) {
      List<Finding> openFindings =
          auditRuleRuntime.evaluate(new RuleContext(detectableQueries, indexMetadata));
      for (FindingPolicy.Classified classified :
          OpenFindingClassifier.classify(openFindings, findingPolicy)) {
        Finding finding = classified.finding();
        if (finding.kindId().isBuiltin()) {
          // Legacy adapters preserve the Issue API as well as the original policy code.
          Issue issue = finding.toIssue().orElseThrow();
          switch (classified.bucket()) {
            case ACKNOWLEDGED -> acknowledgedIssues.add(issue);
            case INFO -> infoIssues.add(issue);
            case CONFIRMED -> confirmedIssues.add(issue);
          }
        } else {
          switch (classified.bucket()) {
            case ACKNOWLEDGED -> customAcknowledged.add(finding);
            case INFO -> customInfo.add(finding);
            case CONFIRMED -> customConfirmed.add(finding);
          }
        }
      }
    }

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
            totalExecutionTimeNanos)
        .withCustomFindings(customConfirmed, customInfo, customAcknowledged);
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
        .withIndexMetadata(report.getIndexMetadata())
        .withCustomFindings(
            report.getCustomConfirmedFindings(),
            report.getCustomInfoFindings(),
            report.getCustomAcknowledgedFindings());
  }

  private void classifyIssues(
      List<Issue> issues,
      List<Issue> confirmedIssues,
      List<Issue> infoIssues,
      List<Issue> acknowledgedIssues) {
    for (Issue issue : issues) {
      FindingPolicy.Classified classified = findingPolicy.classify(Finding.fromIssue(issue));
      if (classified != null) {
        Issue effective =
            classified.finding().severity() == issue.severity()
                ? issue
                : classified.finding().toIssue().orElseThrow();
        switch (classified.bucket()) {
          case ACKNOWLEDGED -> acknowledgedIssues.add(effective);
          case INFO -> infoIssues.add(effective);
          case CONFIRMED -> confirmedIssues.add(effective);
        }
      }
    }
  }

  public QueryAuditConfig getConfig() {
    return config;
  }

  public List<DetectionRule> getRules() {
    return List.copyOf(rules);
  }

  public List<AuditRule> getAuditRules() {
    return auditRuleRuntime.rules();
  }

  /** Immutable descriptor snapshots for active open rules, in execution order. */
  public List<RuleDescriptor> getRuleDescriptors() {
    return auditRuleRuntime.descriptors();
  }

  /** Whether every active rule was constructed from the fingerprinted core configuration. */
  public boolean hasCompleteRuleInputs() {
    return ruleInputsComplete;
  }

  public List<BaselineEntry> getBaseline() {
    return List.copyOf(baseline);
  }
}
