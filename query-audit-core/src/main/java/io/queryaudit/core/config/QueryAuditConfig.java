package io.queryaudit.core.config;

import io.queryaudit.core.detector.RepeatedSingleInsertDetector;
import io.queryaudit.core.detector.RepeatedSingleUpdateDetector;
import io.queryaudit.core.detector.RepositoryReturnTypeResolver;
import io.queryaudit.core.interceptor.QueryInterceptor;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.parser.SqlParser;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * Immutable configuration for QueryAudit analysis. Controls which rules are enabled, their severity
 * overrides, detection thresholds (e.g., N+1, offset pagination, OR clause limits), suppression
 * patterns, baseline file path, and other behavioral settings. Instances are created via the {@link
 * Builder}.
 *
 * @author haroya
 * @since 0.2.0
 */
public class QueryAuditConfig {

  /**
   * Default directory for the suite HTML and JSON reports.
   *
   * @since 0.6.0
   */
  public static final String DEFAULT_REPORT_OUTPUT_DIR = "build/reports/query-audit";

  private final boolean enabled;
  private final boolean failOnDetection;
  private final int nPlusOneThreshold;
  private final int offsetPaginationThreshold;
  private final int orClauseThreshold;
  private final Set<String> suppressPatterns;
  private final Set<String> suppressQueries;
  private final boolean showInfo;
  private final ReportFormat reportFormat;
  private final String reportOutputDir;
  private final String baselinePath;
  private final boolean autoOpenReport;
  private final int maxQueries;
  private final Set<String> disabledRules;
  private final Map<String, Severity> severityOverrides;
  private final int largeInListThreshold;
  private final int tooManyJoinsThreshold;
  private final int excessiveColumnThreshold;
  private final int repeatedInsertThreshold;
  private final Set<String> repeatedInsertExcludeTables;
  private final int repeatedUpdateThreshold;
  private final Set<String> repeatedUpdateExcludeTables;
  private final int writeAmplificationThreshold;
  private final long slowQueryWarningMs;
  private final long slowQueryErrorMs;
  private final RepositoryReturnTypeResolver repositoryReturnTypeResolver;
  private final boolean includeSetupQueries;
  private final boolean countInsteadOfExistsEnabled;
  private final AuditMode auditMode;
  private final long connectionHeldIdleThresholdMs;
  private final RuleProfile ruleProfile;
  private final Set<String> enabledRules;

  private QueryAuditConfig(Builder builder) {
    this.enabled = builder.enabled;
    this.failOnDetection = builder.failOnDetection;
    this.nPlusOneThreshold = builder.nPlusOneThreshold;
    this.offsetPaginationThreshold = builder.offsetPaginationThreshold;
    this.orClauseThreshold = builder.orClauseThreshold;
    this.suppressPatterns = Collections.unmodifiableSet(new HashSet<>(builder.suppressPatterns));
    this.suppressQueries = Collections.unmodifiableSet(new HashSet<>(builder.suppressQueries));
    this.showInfo = builder.showInfo;
    this.reportFormat = builder.reportFormat;
    this.reportOutputDir = builder.reportOutputDir;
    this.baselinePath = builder.baselinePath;
    this.autoOpenReport = builder.autoOpenReport;
    this.maxQueries = builder.maxQueries;
    this.disabledRules = Collections.unmodifiableSet(new HashSet<>(builder.disabledRules));
    this.severityOverrides = Collections.unmodifiableMap(new HashMap<>(builder.severityOverrides));
    this.largeInListThreshold = builder.largeInListThreshold;
    this.tooManyJoinsThreshold = builder.tooManyJoinsThreshold;
    this.excessiveColumnThreshold = builder.excessiveColumnThreshold;
    this.repeatedInsertThreshold = builder.repeatedInsertThreshold;
    this.repeatedInsertExcludeTables =
        Collections.unmodifiableSet(new HashSet<>(builder.repeatedInsertExcludeTables));
    this.repeatedUpdateThreshold = builder.repeatedUpdateThreshold;
    this.repeatedUpdateExcludeTables =
        Collections.unmodifiableSet(new HashSet<>(builder.repeatedUpdateExcludeTables));
    this.writeAmplificationThreshold = builder.writeAmplificationThreshold;
    this.slowQueryWarningMs = builder.slowQueryWarningMs;
    this.slowQueryErrorMs = builder.slowQueryErrorMs;
    this.repositoryReturnTypeResolver = builder.repositoryReturnTypeResolver;
    this.includeSetupQueries = builder.includeSetupQueries;
    this.countInsteadOfExistsEnabled = builder.countInsteadOfExistsEnabled;
    this.auditMode = builder.auditMode;
    this.connectionHeldIdleThresholdMs = builder.connectionHeldIdleThresholdMs;
    this.ruleProfile = builder.ruleProfile;
    this.enabledRules = Collections.unmodifiableSet(new HashSet<>(builder.enabledRules));
  }

  public static Builder builder() {
    return new Builder();
  }

  public static QueryAuditConfig defaults() {
    return new Builder().build();
  }

  public boolean isEnabled() {
    return enabled;
  }

  public boolean isFailOnDetection() {
    return failOnDetection;
  }

  public int getNPlusOneThreshold() {
    return nPlusOneThreshold;
  }

  public int getOffsetPaginationThreshold() {
    return offsetPaginationThreshold;
  }

  public int getOrClauseThreshold() {
    return orClauseThreshold;
  }

  public Set<String> getSuppressPatterns() {
    return suppressPatterns;
  }

  public Set<String> getSuppressQueries() {
    return suppressQueries;
  }

  public boolean isShowInfo() {
    return showInfo;
  }

  /** Returns the selected suite report artifact. */
  public ReportFormat getReportFormat() {
    return reportFormat;
  }

  /**
   * Returns the directory where the suite HTML and JSON reports are written.
   *
   * @since 0.6.0
   */
  public String getReportOutputDir() {
    return reportOutputDir;
  }

  /**
   * Returns the configured baseline file path, or {@code null} to use the default ({@code
   * .query-audit-baseline} in the working directory).
   */
  public String getBaselinePath() {
    return baselinePath;
  }

  public boolean isAutoOpenReport() {
    return autoOpenReport;
  }

  /**
   * Returns the maximum number of queries to record per test. Default is {@value
   * QueryInterceptor#DEFAULT_MAX_QUERIES}.
   */
  public int getMaxQueries() {
    return maxQueries;
  }

  /**
   * Returns the set of disabled rule codes. Rules in this set will not be instantiated or executed.
   */
  public Set<String> getDisabledRules() {
    return disabledRules;
  }

  /**
   * Returns true if the given rule code is disabled via configuration.
   *
   * @param ruleCode the issue type code (e.g., "select-all", "n-plus-one")
   */
  /**
   * Returns whether the rule should not run, combining the profile with explicit overrides.
   * Precedence: {@code disabled-rules} wins over {@code enabled-rules}, which wins over the profile
   * tier.
   *
   * @since 0.5.0
   */
  public boolean isRuleExcluded(String issueCode) {
    if (disabledRules.contains(issueCode)) {
      return true;
    }
    if (enabledRules.contains(issueCode)) {
      return false;
    }
    return !ruleProfile.includes(issueCode);
  }

  public boolean isRuleDisabled(String ruleCode) {
    return disabledRules.contains(ruleCode);
  }

  /**
   * Returns the severity overrides map. Keys are issue type codes, values are the overridden
   * severity.
   */
  public Map<String, Severity> getSeverityOverrides() {
    return severityOverrides;
  }

  /**
   * Returns the effective severity for the given issue type code. If an override exists, returns
   * the override; otherwise returns the provided default.
   */
  public Severity getEffectiveSeverity(String issueCode, Severity defaultSeverity) {
    return severityOverrides.getOrDefault(issueCode, defaultSeverity);
  }

  public int getLargeInListThreshold() {
    return largeInListThreshold;
  }

  public int getTooManyJoinsThreshold() {
    return tooManyJoinsThreshold;
  }

  public int getExcessiveColumnThreshold() {
    return excessiveColumnThreshold;
  }

  public int getRepeatedInsertThreshold() {
    return repeatedInsertThreshold;
  }

  /**
   * Returns table-name globs (e.g. {@code temp_*}) that the repeated-single-insert detector should
   * skip. Defaults to {@link RepeatedSingleInsertDetector#DEFAULT_EXCLUDE_TABLES}.
   *
   * @since 0.4.0
   */
  public Set<String> getRepeatedInsertExcludeTables() {
    return repeatedInsertExcludeTables;
  }

  /**
   * Returns the repeated single-row UPDATE threshold.
   *
   * @since 0.6.0
   */
  public int getRepeatedUpdateThreshold() {
    return repeatedUpdateThreshold;
  }

  /**
   * Returns table-name globs excluded from repeated UPDATE detection.
   *
   * @since 0.6.0
   */
  public Set<String> getRepeatedUpdateExcludeTables() {
    return repeatedUpdateExcludeTables;
  }

  public int getWriteAmplificationThreshold() {
    return writeAmplificationThreshold;
  }

  public long getSlowQueryWarningMs() {
    return slowQueryWarningMs;
  }

  public long getSlowQueryErrorMs() {
    return slowQueryErrorMs;
  }

  /**
   * Returns the resolver for Spring Data repository return types, or {@code null} if not
   * configured. When {@code null}, the unbounded-result-set detector falls back to its default
   * behavior (all flagged queries are WARNING).
   *
   * @since 0.3.0
   */
  public RepositoryReturnTypeResolver getRepositoryReturnTypeResolver() {
    return repositoryReturnTypeResolver;
  }

  /**
   * Returns whether setup/teardown lifecycle phase queries should be included in analysis. Default
   * is {@code false} — only TEST-phase queries are analyzed.
   */
  public boolean isIncludeSetupQueries() {
    return includeSetupQueries;
  }

  /**
   * Returns whether {@link io.queryaudit.core.detector.CountInsteadOfExistsDetector} is enabled.
   * Off by default since 0.4.0 (issue #126) — the rule cannot tell aggregate counts from existence
   * checks from SQL alone, and was the loudest noise source on real reports.
   *
   * @since 0.4.0
   */
  public boolean isCountInsteadOfExistsEnabled() {
    return countInsteadOfExistsEnabled;
  }

  /**
   * Returns which tests the JUnit extension audits: {@link AuditMode#ANNOTATED} (opt-in, the
   * default) or {@link AuditMode#ALL} (opt-out via {@code @QueryAuditExclude}).
   *
   * @since 0.5.0
   */
  public AuditMode getAuditMode() {
    return auditMode;
  }

  /**
   * Minimum idle time (connection held minus database work, in milliseconds) before the
   * connection-held-idle rule fires. Conservative default: 200ms.
   *
   * @since 0.5.0
   */
  public long getConnectionHeldIdleThresholdMs() {
    return connectionHeldIdleThresholdMs;
  }

  /**
   * Returns the active rule profile tier. Defaults to {@link RuleProfile#RECOMMENDED} since 0.6.0.
   *
   * @since 0.5.0
   */
  public RuleProfile getRuleProfile() {
    return ruleProfile;
  }

  /**
   * Returns rule codes explicitly re-enabled on top of the profile tier.
   *
   * @since 0.5.0
   */
  public Set<String> getEnabledRules() {
    return enabledRules;
  }

  public boolean isSuppressed(String issueCode, String table, String column) {
    if (suppressPatterns.isEmpty()) {
      return false;
    }
    if (suppressPatterns.contains(issueCode)) {
      return true;
    }
    if (table != null && column != null) {
      String qualified = issueCode + ":" + table + "." + column;
      if (suppressPatterns.contains(qualified)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Cache of compiled word-boundary regexes for {@link #suppressQueries} patterns. The patterns are
   * user-supplied, immutable for the lifetime of this config, and small (one entry per suppression
   * rule); a plain {@link ConcurrentHashMap} is sufficient.
   */
  private static final Map<String, Pattern> SUPPRESS_PATTERN_CACHE = new ConcurrentHashMap<>();

  public boolean isQuerySuppressed(String sql) {
    if (suppressQueries.isEmpty() || sql == null) {
      return false;
    }
    // Mask string literals first so a suppression pattern can never match content inside a quoted
    // string (issue #124: "from users" used to spuriously match `description = 'from users …'`).
    String masked = SqlParser.replaceStringLiterals(sql).toLowerCase();
    for (String raw : suppressQueries) {
      String pattern = raw == null ? null : raw.trim();
      if (pattern == null || pattern.isEmpty()) {
        continue;
      }
      Pattern compiled =
          SUPPRESS_PATTERN_CACHE.computeIfAbsent(
              pattern.toLowerCase(),
              p ->
                  Pattern.compile(
                      "(?<![\\w])" + Pattern.quote(p) + "(?![\\w])", Pattern.CASE_INSENSITIVE));
      if (compiled.matcher(masked).find()) {
        return true;
      }
    }
    return false;
  }

  public static class Builder {

    private boolean enabled = true;
    private boolean failOnDetection = true;
    private int nPlusOneThreshold = 3;
    private int offsetPaginationThreshold = 1000;
    private int orClauseThreshold = 3;
    private Set<String> suppressPatterns = new HashSet<>();
    private Set<String> suppressQueries = new HashSet<>();
    private boolean showInfo = true;
    private ReportFormat reportFormat = ReportFormat.CONSOLE;
    private String reportOutputDir = DEFAULT_REPORT_OUTPUT_DIR;
    private String baselinePath = null;
    private boolean autoOpenReport = true;
    private int maxQueries = 10_000;
    private Set<String> disabledRules = new HashSet<>();
    private Map<String, Severity> severityOverrides = new HashMap<>();
    private int largeInListThreshold = 100;
    private int tooManyJoinsThreshold = 5;
    private int excessiveColumnThreshold = 15;
    private int repeatedInsertThreshold = 3;
    private Set<String> repeatedInsertExcludeTables =
        new HashSet<>(RepeatedSingleInsertDetector.DEFAULT_EXCLUDE_TABLES);
    private int repeatedUpdateThreshold = 3;
    private Set<String> repeatedUpdateExcludeTables =
        new HashSet<>(RepeatedSingleUpdateDetector.DEFAULT_EXCLUDE_TABLES);
    private int writeAmplificationThreshold = 6;
    private long slowQueryWarningMs = 500;
    private long slowQueryErrorMs = 3000;
    private RepositoryReturnTypeResolver repositoryReturnTypeResolver = null;
    private boolean includeSetupQueries = false;
    private boolean countInsteadOfExistsEnabled = false;
    private AuditMode auditMode = AuditMode.ANNOTATED;
    private long connectionHeldIdleThresholdMs = 200;
    private RuleProfile ruleProfile = RuleProfile.RECOMMENDED;
    private Set<String> enabledRules = new HashSet<>();

    /**
     * Creates a new builder pre-populated with all values from the given config. Useful for
     * layering overrides on top of an existing configuration (e.g., annotation overrides on top of
     * application.yml settings).
     */
    public static Builder from(QueryAuditConfig source) {
      Builder b = new Builder();
      b.enabled = source.enabled;
      b.failOnDetection = source.failOnDetection;
      b.nPlusOneThreshold = source.nPlusOneThreshold;
      b.offsetPaginationThreshold = source.offsetPaginationThreshold;
      b.orClauseThreshold = source.orClauseThreshold;
      b.suppressPatterns = new HashSet<>(source.suppressPatterns);
      b.suppressQueries = new HashSet<>(source.suppressQueries);
      b.showInfo = source.showInfo;
      b.reportFormat = source.reportFormat;
      b.reportOutputDir = source.reportOutputDir;
      b.baselinePath = source.baselinePath;
      b.autoOpenReport = source.autoOpenReport;
      b.maxQueries = source.maxQueries;
      b.disabledRules = new HashSet<>(source.disabledRules);
      b.severityOverrides = new HashMap<>(source.severityOverrides);
      b.largeInListThreshold = source.largeInListThreshold;
      b.tooManyJoinsThreshold = source.tooManyJoinsThreshold;
      b.excessiveColumnThreshold = source.excessiveColumnThreshold;
      b.repeatedInsertThreshold = source.repeatedInsertThreshold;
      b.repeatedInsertExcludeTables = new HashSet<>(source.repeatedInsertExcludeTables);
      b.repeatedUpdateThreshold = source.repeatedUpdateThreshold;
      b.repeatedUpdateExcludeTables = new HashSet<>(source.repeatedUpdateExcludeTables);
      b.writeAmplificationThreshold = source.writeAmplificationThreshold;
      b.slowQueryWarningMs = source.slowQueryWarningMs;
      b.slowQueryErrorMs = source.slowQueryErrorMs;
      b.repositoryReturnTypeResolver = source.repositoryReturnTypeResolver;
      b.includeSetupQueries = source.includeSetupQueries;
      b.countInsteadOfExistsEnabled = source.countInsteadOfExistsEnabled;
      b.auditMode = source.auditMode;
      b.connectionHeldIdleThresholdMs = source.connectionHeldIdleThresholdMs;
      b.ruleProfile = source.ruleProfile;
      b.enabledRules = new HashSet<>(source.enabledRules);
      return b;
    }

    public Builder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
    }

    public Builder failOnDetection(boolean failOnDetection) {
      this.failOnDetection = failOnDetection;
      return this;
    }

    public Builder nPlusOneThreshold(int nPlusOneThreshold) {
      this.nPlusOneThreshold = nPlusOneThreshold;
      return this;
    }

    public Builder offsetPaginationThreshold(int offsetPaginationThreshold) {
      this.offsetPaginationThreshold = offsetPaginationThreshold;
      return this;
    }

    public Builder orClauseThreshold(int orClauseThreshold) {
      this.orClauseThreshold = orClauseThreshold;
      return this;
    }

    public Builder suppressPatterns(Set<String> suppressPatterns) {
      this.suppressPatterns = suppressPatterns;
      return this;
    }

    public Builder addSuppressPattern(String pattern) {
      this.suppressPatterns.add(pattern);
      return this;
    }

    public Builder suppressQueries(Set<String> suppressQueries) {
      this.suppressQueries = suppressQueries;
      return this;
    }

    public Builder addSuppressQuery(String query) {
      this.suppressQueries.add(query);
      return this;
    }

    public Builder showInfo(boolean showInfo) {
      this.showInfo = showInfo;
      return this;
    }

    /** Selects the suite report artifact written after the test run. */
    public Builder reportFormat(ReportFormat reportFormat) {
      this.reportFormat = Objects.requireNonNull(reportFormat, "reportFormat");
      return this;
    }

    /**
     * Sets the directory where the suite HTML and JSON reports are written.
     *
     * @since 0.6.0
     */
    public Builder reportOutputDir(String reportOutputDir) {
      this.reportOutputDir = reportOutputDir;
      return this;
    }

    public Builder baselinePath(String baselinePath) {
      this.baselinePath = baselinePath;
      return this;
    }

    public Builder autoOpenReport(boolean autoOpenReport) {
      this.autoOpenReport = autoOpenReport;
      return this;
    }

    public Builder maxQueries(int maxQueries) {
      this.maxQueries = maxQueries;
      return this;
    }

    public Builder disabledRules(Set<String> disabledRules) {
      this.disabledRules = disabledRules;
      return this;
    }

    public Builder addDisabledRule(String ruleCode) {
      this.disabledRules.add(ruleCode);
      return this;
    }

    public Builder severityOverrides(Map<String, Severity> severityOverrides) {
      this.severityOverrides = severityOverrides;
      return this;
    }

    public Builder addSeverityOverride(String issueCode, Severity severity) {
      this.severityOverrides.put(issueCode, severity);
      return this;
    }

    public Builder largeInListThreshold(int largeInListThreshold) {
      this.largeInListThreshold = largeInListThreshold;
      return this;
    }

    public Builder tooManyJoinsThreshold(int tooManyJoinsThreshold) {
      this.tooManyJoinsThreshold = tooManyJoinsThreshold;
      return this;
    }

    public Builder excessiveColumnThreshold(int excessiveColumnThreshold) {
      this.excessiveColumnThreshold = excessiveColumnThreshold;
      return this;
    }

    public Builder repeatedInsertThreshold(int repeatedInsertThreshold) {
      this.repeatedInsertThreshold = repeatedInsertThreshold;
      return this;
    }

    /**
     * Replaces the table-name globs that {@link RepeatedSingleInsertDetector} treats as deliberate
     * staging tables. Each entry is a case-insensitive glob with {@code *} as the only wildcard
     * (e.g. {@code "etl_*"}). Pass an empty set to disable the exclusion entirely.
     *
     * @since 0.4.0
     */
    public Builder repeatedInsertExcludeTables(Set<String> patterns) {
      this.repeatedInsertExcludeTables = patterns;
      return this;
    }

    /**
     * Adds one extra table-name glob to the repeated-single-insert exclusion list.
     *
     * @since 0.4.0
     */
    public Builder addRepeatedInsertExcludeTable(String pattern) {
      this.repeatedInsertExcludeTables.add(pattern);
      return this;
    }

    /**
     * Sets the repeated single-row UPDATE threshold.
     *
     * @since 0.6.0
     */
    public Builder repeatedUpdateThreshold(int repeatedUpdateThreshold) {
      this.repeatedUpdateThreshold = repeatedUpdateThreshold;
      return this;
    }

    /**
     * Replaces the table-name globs that {@link RepeatedSingleUpdateDetector} treats as deliberate
     * staging tables. Each entry is a case-insensitive glob with {@code *} as the only wildcard
     * (e.g. {@code "etl_*"}). Pass an empty set to disable the exclusion entirely.
     *
     * @since 0.6.0
     */
    public Builder repeatedUpdateExcludeTables(Set<String> patterns) {
      this.repeatedUpdateExcludeTables = new HashSet<>(patterns);
      return this;
    }

    /**
     * Adds one extra table-name glob to the repeated-single-update exclusion list.
     *
     * @since 0.6.0
     */
    public Builder addRepeatedUpdateExcludeTable(String pattern) {
      this.repeatedUpdateExcludeTables.add(pattern);
      return this;
    }

    public Builder writeAmplificationThreshold(int writeAmplificationThreshold) {
      this.writeAmplificationThreshold = writeAmplificationThreshold;
      return this;
    }

    public Builder slowQueryWarningMs(long slowQueryWarningMs) {
      this.slowQueryWarningMs = slowQueryWarningMs;
      return this;
    }

    public Builder slowQueryErrorMs(long slowQueryErrorMs) {
      this.slowQueryErrorMs = slowQueryErrorMs;
      return this;
    }

    public Builder repositoryReturnTypeResolver(RepositoryReturnTypeResolver resolver) {
      this.repositoryReturnTypeResolver = resolver;
      return this;
    }

    public Builder includeSetupQueries(boolean includeSetupQueries) {
      this.includeSetupQueries = includeSetupQueries;
      return this;
    }

    /**
     * Toggles {@link io.queryaudit.core.detector.CountInsteadOfExistsDetector}. Off by default
     * since 0.4.0 (issue #126).
     *
     * @since 0.4.0
     */
    public Builder countInsteadOfExistsEnabled(boolean enabled) {
      this.countInsteadOfExistsEnabled = enabled;
      return this;
    }

    /**
     * Sets which tests the JUnit extension audits. {@code null} keeps the default ({@link
     * AuditMode#ANNOTATED}).
     *
     * @since 0.5.0
     */
    /**
     * Sets the rule profile tier. {@code null} leaves the current value unchanged.
     * The initial value is {@link RuleProfile#RECOMMENDED}.
     *
     * @since 0.5.0
     */
    public Builder ruleProfile(RuleProfile ruleProfile) {
      if (ruleProfile != null) {
        this.ruleProfile = ruleProfile;
      }
      return this;
    }

    /**
     * Rule codes to run even when the profile tier excludes them.
     *
     * @since 0.5.0
     */
    public Builder enabledRules(Set<String> enabledRules) {
      this.enabledRules = new HashSet<>(enabledRules);
      return this;
    }

    /** Adds a single rule code to run even when the profile tier excludes it. */
    public Builder addEnabledRule(String ruleCode) {
      this.enabledRules.add(ruleCode);
      return this;
    }

    /**
     * Sets the connection-held-idle threshold in milliseconds.
     *
     * @since 0.5.0
     */
    public Builder connectionHeldIdleThresholdMs(long thresholdMs) {
      this.connectionHeldIdleThresholdMs = thresholdMs;
      return this;
    }

    public Builder auditMode(AuditMode auditMode) {
      if (auditMode != null) {
        this.auditMode = auditMode;
      }
      return this;
    }

    public QueryAuditConfig build() {
      return new QueryAuditConfig(this);
    }
  }
}
