package io.queryaudit.core.config;

import io.queryaudit.core.detector.RepositoryReturnTypeResolver;
import io.queryaudit.core.interceptor.QueryInterceptor;
import io.queryaudit.core.model.Severity;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Immutable configuration for QueryAudit analysis. Controls which rules are enabled, their
 * severity overrides, detection thresholds (e.g., N+1, offset pagination, OR clause limits),
 * suppression patterns, baseline file path, and other behavioral settings. Instances are
 * created via the {@link Builder}.
 *
 * @author haroya
 * @since 0.2.0
 */
public class QueryAuditConfig {

  private final boolean enabled;
  private final boolean failOnDetection;
  private final int nPlusOneThreshold;
  private final int offsetPaginationThreshold;
  private final int orClauseThreshold;
  private final Set<String> suppressPatterns;
  private final Set<String> suppressQueries;
  private final boolean showInfo;
  private final String baselinePath;
  private final boolean autoOpenReport;
  private final int maxQueries;
  private final Set<String> disabledRules;
  private final Map<String, Severity> severityOverrides;
  private final int largeInListThreshold;
  private final int tooManyJoinsThreshold;
  private final int excessiveColumnThreshold;
  private final int repeatedInsertThreshold;
  private final int writeAmplificationThreshold;
  private final long slowQueryWarningMs;
  private final long slowQueryErrorMs;
  private final RepositoryReturnTypeResolver repositoryReturnTypeResolver;
  private final boolean includeSetupQueries;

  private QueryAuditConfig(Builder builder) {
    this.enabled = builder.enabled;
    this.failOnDetection = builder.failOnDetection;
    this.nPlusOneThreshold = builder.nPlusOneThreshold;
    this.offsetPaginationThreshold = builder.offsetPaginationThreshold;
    this.orClauseThreshold = builder.orClauseThreshold;
    this.suppressPatterns = Collections.unmodifiableSet(new HashSet<>(builder.suppressPatterns));
    this.suppressQueries = Collections.unmodifiableSet(new HashSet<>(builder.suppressQueries));
    this.showInfo = builder.showInfo;
    this.baselinePath = builder.baselinePath;
    this.autoOpenReport = builder.autoOpenReport;
    this.maxQueries = builder.maxQueries;
    this.disabledRules = Collections.unmodifiableSet(new HashSet<>(builder.disabledRules));
    this.severityOverrides = Collections.unmodifiableMap(new HashMap<>(builder.severityOverrides));
    this.largeInListThreshold = builder.largeInListThreshold;
    this.tooManyJoinsThreshold = builder.tooManyJoinsThreshold;
    this.excessiveColumnThreshold = builder.excessiveColumnThreshold;
    this.repeatedInsertThreshold = builder.repeatedInsertThreshold;
    this.writeAmplificationThreshold = builder.writeAmplificationThreshold;
    this.slowQueryWarningMs = builder.slowQueryWarningMs;
    this.slowQueryErrorMs = builder.slowQueryErrorMs;
    this.repositoryReturnTypeResolver = builder.repositoryReturnTypeResolver;
    this.includeSetupQueries = builder.includeSetupQueries;
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
   * Returns the effective severity for the given issue type code. If an override exists, returns the
   * override; otherwise returns the provided default.
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
   * Returns whether setup/teardown lifecycle phase queries should be included in analysis.
   * Default is {@code false} — only TEST-phase queries are analyzed.
   */
  public boolean isIncludeSetupQueries() {
    return includeSetupQueries;
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

  public boolean isQuerySuppressed(String sql) {
    if (suppressQueries.isEmpty() || sql == null) {
      return false;
    }
    String normalized = sql.trim().toLowerCase();
    for (String pattern : suppressQueries) {
      if (normalized.contains(pattern.trim().toLowerCase())) {
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
    private String baselinePath = null;
    private boolean autoOpenReport = true;
    private int maxQueries = 10_000;
    private Set<String> disabledRules = new HashSet<>();
    private Map<String, Severity> severityOverrides = new HashMap<>();
    private int largeInListThreshold = 100;
    private int tooManyJoinsThreshold = 5;
    private int excessiveColumnThreshold = 15;
    private int repeatedInsertThreshold = 3;
    private int writeAmplificationThreshold = 6;
    private long slowQueryWarningMs = 500;
    private long slowQueryErrorMs = 3000;
    private RepositoryReturnTypeResolver repositoryReturnTypeResolver = null;
    private boolean includeSetupQueries = false;

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
      b.baselinePath = source.baselinePath;
      b.autoOpenReport = source.autoOpenReport;
      b.maxQueries = source.maxQueries;
      b.disabledRules = new HashSet<>(source.disabledRules);
      b.severityOverrides = new HashMap<>(source.severityOverrides);
      b.largeInListThreshold = source.largeInListThreshold;
      b.tooManyJoinsThreshold = source.tooManyJoinsThreshold;
      b.excessiveColumnThreshold = source.excessiveColumnThreshold;
      b.repeatedInsertThreshold = source.repeatedInsertThreshold;
      b.writeAmplificationThreshold = source.writeAmplificationThreshold;
      b.slowQueryWarningMs = source.slowQueryWarningMs;
      b.slowQueryErrorMs = source.slowQueryErrorMs;
      b.repositoryReturnTypeResolver = source.repositoryReturnTypeResolver;
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

    public QueryAuditConfig build() {
      return new QueryAuditConfig(this);
    }
  }
}
