package io.queryaudit.spring;

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
  private WriteAmplification writeAmplification = new WriteAmplification();
  private SlowQuery slowQuery = new SlowQuery();

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
    private String outputDir = "build/reports/query-audit";
    private boolean showInfo = true;

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

    public int getThreshold() {
      return threshold;
    }

    public void setThreshold(int threshold) {
      this.threshold = threshold;
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
