package io.queryaudit.spring;

import io.queryaudit.core.config.QueryAuditConfig;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for QueryAudit, bindable via {@code query-audit.*} in application.yml /
 * application.properties.
 *
 * @author haroya
 * @since 0.2.0
 */
@ConfigurationProperties(prefix = "query-audit")
public class QueryAuditProperties {

  private boolean enabled = true;
  private boolean failOnDetection = true;

  /**
   * Which tests the JUnit extension audits: {@code annotated} (opt-in via {@code @QueryAudit}, the
   * default) or {@code all} (every test, opt-out via {@code @QueryAuditExclude}). {@code all}
   * additionally requires JUnit extension autodetection so the extension is registered for
   * unannotated tests.
   */
  private String mode = "annotated";

  /**
   * Rule profile tier: {@code strict} (all rules, the default), {@code recommended} (opinionated
   * rules off), or {@code minimal} (safety-critical only). {@code disabled-rules} / {@code
   * enabled-rules} always win over the profile.
   */
  private String profile = "strict";

  /** Rule codes to run even when the profile tier excludes them. */
  private List<String> enabledRules = new ArrayList<>();

  private NPlusOne nPlusOne = new NPlusOne();
  private OffsetPagination offsetPagination = new OffsetPagination();
  private OrClause orClause = new OrClause();
  private List<String> suppressPatterns = new ArrayList<>();
  private List<String> suppressQueries = new ArrayList<>();
  private String baselinePath;
  private boolean autoOpenReport = true;
  private int maxQueries = 10_000;
  private Report report = new Report();
  private List<String> disabledRules = new ArrayList<>();
  private Map<String, String> severityOverrides = new HashMap<>();
  private LargeInList largeInList = new LargeInList();
  private TooManyJoins tooManyJoins = new TooManyJoins();
  private ExcessiveColumn excessiveColumn = new ExcessiveColumn();
  private RepeatedInsert repeatedInsert = new RepeatedInsert();
  private RepeatedUpdate repeatedUpdate = new RepeatedUpdate();
  private WriteAmplification writeAmplification = new WriteAmplification();
  private SlowQuery slowQuery = new SlowQuery();
  private CountInsteadOfExists countInsteadOfExists = new CountInsteadOfExists();
  private ConnectionHeldIdle connectionHeldIdle = new ConnectionHeldIdle();
  private WrapDataSource wrapDataSource = new WrapDataSource();

  // ── Top-level getters & setters ────────────────────────────────────

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public boolean isFailOnDetection() {
    return failOnDetection;
  }

  public void setFailOnDetection(boolean failOnDetection) {
    this.failOnDetection = failOnDetection;
  }

  public String getMode() {
    return mode;
  }

  public void setMode(String mode) {
    this.mode = mode;
  }

  public String getProfile() {
    return profile;
  }

  public void setProfile(String profile) {
    this.profile = profile;
  }

  public List<String> getEnabledRules() {
    return enabledRules;
  }

  public void setEnabledRules(List<String> enabledRules) {
    this.enabledRules = enabledRules;
  }

  public NPlusOne getNPlusOne() {
    return nPlusOne;
  }

  public void setNPlusOne(NPlusOne nPlusOne) {
    this.nPlusOne = nPlusOne;
  }

  public OffsetPagination getOffsetPagination() {
    return offsetPagination;
  }

  public void setOffsetPagination(OffsetPagination offsetPagination) {
    this.offsetPagination = offsetPagination;
  }

  public OrClause getOrClause() {
    return orClause;
  }

  public void setOrClause(OrClause orClause) {
    this.orClause = orClause;
  }

  public List<String> getSuppressPatterns() {
    return suppressPatterns;
  }

  public void setSuppressPatterns(List<String> suppressPatterns) {
    this.suppressPatterns = suppressPatterns;
  }

  public List<String> getSuppressQueries() {
    return suppressQueries;
  }

  public void setSuppressQueries(List<String> suppressQueries) {
    this.suppressQueries = suppressQueries;
  }

  public String getBaselinePath() {
    return baselinePath;
  }

  public void setBaselinePath(String baselinePath) {
    this.baselinePath = baselinePath;
  }

  public boolean isAutoOpenReport() {
    return autoOpenReport;
  }

  public void setAutoOpenReport(boolean autoOpenReport) {
    this.autoOpenReport = autoOpenReport;
  }

  public int getMaxQueries() {
    return maxQueries;
  }

  public void setMaxQueries(int maxQueries) {
    this.maxQueries = maxQueries;
  }

  public Report getReport() {
    return report;
  }

  public void setReport(Report report) {
    this.report = report;
  }

  public List<String> getDisabledRules() {
    return disabledRules;
  }

  public void setDisabledRules(List<String> disabledRules) {
    this.disabledRules = disabledRules;
  }

  public Map<String, String> getSeverityOverrides() {
    return severityOverrides;
  }

  public void setSeverityOverrides(Map<String, String> severityOverrides) {
    this.severityOverrides = severityOverrides;
  }

  public LargeInList getLargeInList() {
    return largeInList;
  }

  public void setLargeInList(LargeInList largeInList) {
    this.largeInList = largeInList;
  }

  public TooManyJoins getTooManyJoins() {
    return tooManyJoins;
  }

  public void setTooManyJoins(TooManyJoins tooManyJoins) {
    this.tooManyJoins = tooManyJoins;
  }

  public ExcessiveColumn getExcessiveColumn() {
    return excessiveColumn;
  }

  public void setExcessiveColumn(ExcessiveColumn excessiveColumn) {
    this.excessiveColumn = excessiveColumn;
  }

  public RepeatedInsert getRepeatedInsert() {
    return repeatedInsert;
  }

  public void setRepeatedInsert(RepeatedInsert repeatedInsert) {
    this.repeatedInsert = repeatedInsert;
  }

  public RepeatedUpdate getRepeatedUpdate() {
    return repeatedUpdate;
  }

  public void setRepeatedUpdate(RepeatedUpdate repeatedUpdate) {
    this.repeatedUpdate = repeatedUpdate;
  }

  public WriteAmplification getWriteAmplification() {
    return writeAmplification;
  }

  public void setWriteAmplification(WriteAmplification writeAmplification) {
    this.writeAmplification = writeAmplification;
  }

  public SlowQuery getSlowQuery() {
    return slowQuery;
  }

  public void setSlowQuery(SlowQuery slowQuery) {
    this.slowQuery = slowQuery;
  }

  public ConnectionHeldIdle getConnectionHeldIdle() {
    return connectionHeldIdle;
  }

  public void setConnectionHeldIdle(ConnectionHeldIdle connectionHeldIdle) {
    this.connectionHeldIdle = connectionHeldIdle;
  }

  public CountInsteadOfExists getCountInsteadOfExists() {
    return countInsteadOfExists;
  }

  public void setCountInsteadOfExists(CountInsteadOfExists countInsteadOfExists) {
    this.countInsteadOfExists = countInsteadOfExists;
  }

  public WrapDataSource getWrapDataSource() {
    return wrapDataSource;
  }

  public void setWrapDataSource(WrapDataSource wrapDataSource) {
    this.wrapDataSource = wrapDataSource;
  }

  // ── Nested configuration classes ───────────────────────────────────

  public static class NPlusOne {
    private int threshold = 3;

    public int getThreshold() {
      return threshold;
    }

    public void setThreshold(int threshold) {
      this.threshold = threshold;
    }
  }

  public static class OffsetPagination {
    private int threshold = 1000;

    public int getThreshold() {
      return threshold;
    }

    public void setThreshold(int threshold) {
      this.threshold = threshold;
    }
  }

  public static class OrClause {
    private int threshold = 3;

    public int getThreshold() {
      return threshold;
    }

    public void setThreshold(int threshold) {
      this.threshold = threshold;
    }
  }

  public static class Report {
    private String format = "console";
    private String redaction = "redacted";
    private String outputDir = QueryAuditConfig.DEFAULT_REPORT_OUTPUT_DIR;
    private boolean showInfo = true;

    public String getRedaction() {
      return redaction;
    }

    public void setRedaction(String redaction) {
      this.redaction = redaction;
    }

    public String getFormat() {
      return format;
    }

    public void setFormat(String format) {
      this.format = format;
    }

    public String getOutputDir() {
      return outputDir;
    }

    public void setOutputDir(String outputDir) {
      this.outputDir = outputDir;
    }

    public boolean isShowInfo() {
      return showInfo;
    }

    public void setShowInfo(boolean showInfo) {
      this.showInfo = showInfo;
    }
  }

  public static class LargeInList {
    private int threshold = 100;

    public int getThreshold() {
      return threshold;
    }

    public void setThreshold(int threshold) {
      this.threshold = threshold;
    }
  }

  public static class TooManyJoins {
    private int threshold = 5;

    public int getThreshold() {
      return threshold;
    }

    public void setThreshold(int threshold) {
      this.threshold = threshold;
    }
  }

  public static class ExcessiveColumn {
    private int threshold = 15;

    public int getThreshold() {
      return threshold;
    }

    public void setThreshold(int threshold) {
      this.threshold = threshold;
    }
  }

  public static class RepeatedInsert {
    private int threshold = 3;

    /**
     * Table-name globs (case-insensitive, {@code *} wildcard) the detector treats as deliberate
     * staging targets — repeated single-row inserts into these are not flagged. Defaults to common
     * temp/staging conventions; set to an empty list to disable the exclusion.
     *
     * @since 0.4.0
     */
    private List<String> excludeTables =
        new ArrayList<>(
            io.queryaudit.core.detector.RepeatedSingleInsertDetector.DEFAULT_EXCLUDE_TABLES);

    public int getThreshold() {
      return threshold;
    }

    public void setThreshold(int threshold) {
      this.threshold = threshold;
    }

    public List<String> getExcludeTables() {
      return excludeTables;
    }

    public void setExcludeTables(List<String> excludeTables) {
      this.excludeTables = excludeTables;
    }
  }

  /**
   * Spring properties for repeated single-row UPDATE detection.
   *
   * @since 0.6.0
   */
  public static class RepeatedUpdate {
    private int threshold = 3;

    /**
     * Table-name globs (case-insensitive, {@code *} wildcard) the detector treats as deliberate
     * staging targets — repeated single-row updates against these are not flagged. Defaults to
     * common temp/staging conventions; set to an empty list to disable the exclusion.
     */
    private List<String> excludeTables =
        new ArrayList<>(
            io.queryaudit.core.detector.RepeatedSingleUpdateDetector.DEFAULT_EXCLUDE_TABLES);

    public int getThreshold() {
      return threshold;
    }

    public void setThreshold(int threshold) {
      this.threshold = threshold;
    }

    public List<String> getExcludeTables() {
      return excludeTables;
    }

    public void setExcludeTables(List<String> excludeTables) {
      this.excludeTables = excludeTables;
    }
  }

  public static class WriteAmplification {
    private int threshold = 6;

    public int getThreshold() {
      return threshold;
    }

    public void setThreshold(int threshold) {
      this.threshold = threshold;
    }
  }

  /**
   * Controls the autoconfigured {@link org.springframework.beans.factory.config.BeanPostProcessor}
   * that wraps every {@code DataSource} bean. Set {@code query-audit.wrap-data-source.enabled:
   * false} as the documented escape hatch for issue #134 — when 3+ {@code @SpringBootTest} cache
   * signatures coexist, the wrap interacts badly with Spring TestContext caching and the underlying
   * {@code HikariDataSource} can be closed prematurely. Disabling this leaves {@link
   * io.queryaudit.core.config.QueryAuditConfig} and {@link
   * io.queryaudit.core.interceptor.QueryInterceptor} active so {@code @QueryAudit} per-test still
   * works.
   *
   * @since 0.4.0
   */
  public static class WrapDataSource {
    private boolean enabled = true;

    public boolean isEnabled() {
      return enabled;
    }

    public void setEnabled(boolean enabled) {
      this.enabled = enabled;
    }
  }

  /**
   * @since 0.4.0
   */
  public static class ConnectionHeldIdle {
    /** Minimum idle time (held minus database work, ms) before the rule fires. */
    private long thresholdMs = 200;

    public long getThresholdMs() {
      return thresholdMs;
    }

    public void setThresholdMs(long thresholdMs) {
      this.thresholdMs = thresholdMs;
    }
  }

  public static class CountInsteadOfExists {
    private boolean enabled = false;

    public boolean isEnabled() {
      return enabled;
    }

    public void setEnabled(boolean enabled) {
      this.enabled = enabled;
    }
  }

  public static class SlowQuery {
    private long warningMs = 500;
    private long errorMs = 3000;

    public long getWarningMs() {
      return warningMs;
    }

    public void setWarningMs(long warningMs) {
      this.warningMs = warningMs;
    }

    public long getErrorMs() {
      return errorMs;
    }

    public void setErrorMs(long errorMs) {
      this.errorMs = errorMs;
    }
  }
}
