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
import java.util.List;
import java.util.ServiceLoader;

/**
 * Central analyzer that runs all detection rules against captured queries and produces a report.
 * It orchestrates the execution of built-in, ServiceLoader-discovered, and user-provided custom
 * detection rules, applies baseline filtering and severity overrides, and classifies issues into
 * confirmed, informational, and acknowledged categories.
 *
 * @author haroya
 * @since 0.2.0
 */
public class QueryAuditAnalyzer {

  private final List<DetectionRule> rules;
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
    this.rules = createRules(config);

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
    this.rules = createRules(config);
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
    this.rules = createRules(config);
    if (additionalRules != null) {
      this.rules.addAll(additionalRules);
    }

    if (baselinePath != null) {
      this.baseline = Baseline.load(baselinePath);
    } else {
      this.baseline = Baseline.load(Paths.get(Baseline.DEFAULT_FILE_NAME));
    }
  }

  /**
   * Creates an analyzer with a pre-loaded baseline list and additional custom detection rules.
   * The additional rules are appended after the built-in and ServiceLoader-discovered rules.
   *
   * @param config query-audit configuration
   * @param baseline pre-loaded baseline entries
   * @param additionalRules extra detection rules to append, or {@code null} to skip
   */
  public QueryAuditAnalyzer(
      QueryAuditConfig config,
      List<BaselineEntry> baseline,
      List<DetectionRule> additionalRules) {
    this.config = config;
    this.rules = createRules(config);
    if (additionalRules != null) {
      this.rules.addAll(additionalRules);
    }
    this.baseline = baseline != null ? baseline : List.of();
  }

  /**
   * Creates the full list of detection rules based on the given configuration.
   *
   * @param config query-audit configuration used for threshold values
   * @return mutable list of all detection rules
   */
  private List<DetectionRule> createRules(QueryAuditConfig config) {
    List<DetectionRule> ruleList = new ArrayList<>();
    ruleList.add(new NPlusOneDetector(config.getNPlusOneThreshold()));
    ruleList.add(new SelectAllDetector());
    ruleList.add(new WhereFunctionDetector());
    ruleList.add(new OrAbuseDetector(config.getOrClauseThreshold()));
    ruleList.add(new OffsetPaginationDetector(config.getOffsetPaginationThreshold()));
    ruleList.add(new MissingIndexDetector());
    ruleList.add(new CompositeIndexDetector());
    ruleList.add(new LikeWildcardDetector());
    // DuplicateQueryDetector disabled: datasource-proxy provides SQL with '?' placeholders,
    // so we can't distinguish "same query, same params" from "same query, different params".
    // N+1 detector already covers repeated patterns. Re-enable when parameter tracking is added.
    // ruleList.add(new DuplicateQueryDetector(config.getNPlusOneThreshold()));
    ruleList.add(new CartesianJoinDetector());
    ruleList.add(new CorrelatedSubqueryDetector());
    ruleList.add(new ForUpdateWithoutIndexDetector());
    ruleList.add(new RedundantFilterDetector());
    ruleList.add(new SargabilityDetector());
    ruleList.add(new IndexRedundancyDetector());
    ruleList.add(new SlowQueryDetector(config.getSlowQueryWarningMs(), config.getSlowQueryErrorMs()));
    ruleList.add(new CountInsteadOfExistsDetector());
    ruleList.add(new UnboundedResultSetDetector());
    ruleList.add(new WriteAmplificationDetector(config.getWriteAmplificationThreshold()));
    ruleList.add(new ImplicitTypeConversionDetector());
    ruleList.add(new UnionWithoutAllDetector());
    ruleList.add(new CoveringIndexDetector());
    ruleList.add(new OrderByLimitWithoutIndexDetector());
    ruleList.add(new LargeInListDetector(config.getLargeInListThreshold()));
    ruleList.add(new DistinctMisuseDetector());
    ruleList.add(new NullComparisonDetector());
    ruleList.add(new HavingMisuseDetector());
    ruleList.add(new RangeLockDetector());
    ruleList.add(new UpdateWithoutWhereDetector());
    ruleList.add(new DmlWithoutIndexDetector());
    ruleList.add(new RepeatedSingleInsertDetector(config.getRepeatedInsertThreshold()));
    ruleList.add(new InsertSelectAllDetector());
    ruleList.add(new OrderByRandDetector());
    ruleList.add(new NotInSubqueryDetector());
    ruleList.add(new TooManyJoinsDetector(config.getTooManyJoinsThreshold()));
    ruleList.add(new ImplicitJoinDetector());
    ruleList.add(new StringConcatInWhereDetector());
    ruleList.add(new SelectCountStarWithoutWhereDetector());
    ruleList.add(new InsertOnDuplicateKeyDetector());
    ruleList.add(new GroupByFunctionDetector());
    ruleList.add(new ForUpdateNonUniqueIndexDetector());
    ruleList.add(new SubqueryInDmlDetector());
    ruleList.add(new InsertSelectLocksSourceDetector());
    ruleList.add(new CollectionManagementDetector());
    ruleList.add(new DerivedDeleteDetector());
    ruleList.add(new ExcessiveColumnFetchDetector(config.getExcessiveColumnThreshold()));
    ruleList.add(new ImplicitColumnsInsertDetector());
    ruleList.add(new RegexpInsteadOfLikeDetector());
    ruleList.add(new FindInSetDetector());
    ruleList.add(new UnusedJoinDetector());
    ruleList.add(new MergeableQueriesDetector());
    ruleList.add(new NonDeterministicPaginationDetector());

    // New detectors from Team 3 gap analysis
    ruleList.add(new LimitWithoutOrderByDetector());
    ruleList.add(new WindowFunctionWithoutPartitionDetector());
    ruleList.add(new ForUpdateWithoutTimeoutDetector());
    ruleList.add(new CaseInWhereDetector());
    ruleList.add(new ForceIndexHintDetector());

    // External detectors discovered via ServiceLoader (user-provided)
    ServiceLoader<DetectionRule> externalRules = ServiceLoader.load(DetectionRule.class);
    for (DetectionRule rule : externalRules) {
      ruleList.add(rule);
    }

    // Filter out disabled rules
    if (!config.getDisabledRules().isEmpty()) {
      ruleList.removeIf(rule -> isRuleDisabled(rule, config));
    }

    return ruleList;
  }

  /**
   * Checks if a rule should be disabled based on the config. Maps rule classes to their issue type
   * codes for filtering.
   */
  private boolean isRuleDisabled(DetectionRule rule, QueryAuditConfig config) {
    // Prefer the explicit rule code when available (exact match)
    String ruleCode = rule.getRuleCode();
    if (ruleCode != null) {
      return config.getDisabledRules().contains(ruleCode);
    }

    // Fallback: heuristic match based on class name for external/legacy rules
    String className = rule.getClass().getSimpleName();
    for (String disabledCode : config.getDisabledRules()) {
      if (matchesRuleCode(className, disabledCode)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Heuristic match between a detector class name and a rule code. Converts the code (e.g.,
   * "select-all") to a class name fragment (e.g., "SelectAll") and checks if the class name
   * contains it.
   */
  private boolean matchesRuleCode(String className, String code) {
    // Convert "select-all" -> "SelectAll", "n-plus-one" -> "NPlusOne"
    String[] parts = code.split("-");
    StringBuilder expected = new StringBuilder();
    for (String part : parts) {
      if (part.equals("n")) {
        expected.append("N");
      } else if (!part.isEmpty()) {
        expected.append(Character.toUpperCase(part.charAt(0)));
        if (part.length() > 1) {
          expected.append(part.substring(1));
        }
      }
    }
    String fragment = expected.toString();
    return className.contains(fragment);
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
            : filteredQueries.stream()
                .filter(q -> q.phase() == LifecyclePhase.TEST)
                .toList();

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

    for (Issue issue : allIssues) {
      if (config.isSuppressed(issue.type().getCode(), issue.table(), issue.column())) {
        continue;
      }

      // Apply severity override if configured
      Severity effectiveSeverity =
          config.getEffectiveSeverity(issue.type().getCode(), issue.severity());
      Issue effectiveIssue =
          effectiveSeverity != issue.severity()
              ? new Issue(
                  issue.type(),
                  effectiveSeverity,
                  issue.query(),
                  issue.table(),
                  issue.column(),
                  issue.detail(),
                  issue.suggestion(),
                  issue.sourceLocation())
              : issue;

      if (Baseline.isAcknowledged(baseline, effectiveIssue)) {
        acknowledgedIssues.add(effectiveIssue);
      } else if (effectiveIssue.severity() == Severity.INFO) {
        infoIssues.add(effectiveIssue);
      } else {
        confirmedIssues.add(effectiveIssue);
      }
    }

    // Single-pass calculation of unique patterns and total execution time.
    // Replaces two separate stream passes over filteredQueries.
    java.util.Set<String> uniquePatterns = new java.util.HashSet<>();
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

  public QueryAuditConfig getConfig() {
    return config;
  }

  public List<DetectionRule> getRules() {
    return List.copyOf(rules);
  }

  public List<BaselineEntry> getBaseline() {
    return List.copyOf(baseline);
  }
}
