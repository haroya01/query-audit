package io.queryaudit.junit5;

import io.queryaudit.core.analyzer.ExplainAnalyzer;
import io.queryaudit.core.baseline.Baseline;
import io.queryaudit.core.baseline.BaselineEntry;
import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.detector.QueryAuditAnalyzer;
import io.queryaudit.core.interceptor.LazyLoadTracker;
import io.queryaudit.core.interceptor.QueryInterceptor;
import io.queryaudit.core.model.*;
import io.queryaudit.core.regression.QueryCountBaseline;
import io.queryaudit.core.regression.QueryCountRegressionDetector;
import io.queryaudit.core.regression.QueryCounts;
import io.queryaudit.core.reporter.ConsoleReporter;
import io.queryaudit.core.reporter.HtmlReportAggregator;
import io.queryaudit.core.reporter.JsonReporter;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;
import org.junit.jupiter.api.extension.*;

/**
 * JUnit 5 extension that intercepts SQL queries during test execution, analyzes them for
 * performance anti-patterns, and optionally fails the test when confirmed issues are detected.
 *
 * <p>This class acts as an orchestrator, delegating to:
 *
 * <ul>
 *   <li>{@link DataSourceResolver} — DataSource discovery and interceptor hooking
 *   <li>{@link IndexMetadataCollector} — index metadata from DB and JPA annotations
 *   <li>{@link HibernateIntegration} — Hibernate event listener registration and N+1 merging
 * </ul>
 *
 * @author haroya
 * @since 0.2.0
 */
public class QueryAuditExtension
    implements BeforeAllCallback, BeforeEachCallback, AfterEachCallback, AfterAllCallback {

  private static final ExtensionContext.Namespace NAMESPACE =
      ExtensionContext.Namespace.create(QueryAuditExtension.class);

  private static final String KEY_INTERCEPTOR = "interceptor";
  private static final String KEY_INDEX_METADATA = "indexMetadata";
  private static final String KEY_LAZY_LOAD_TRACKER = "lazyLoadTracker";
  private static final String KEY_COUNT_BASELINE = "countBaseline";
  private static final String KEY_CURRENT_COUNTS = "currentCounts";
  private static final String KEY_DATASOURCE = "dataSource";

  private static final QueryCountRegressionDetector REGRESSION_DETECTOR =
      new QueryCountRegressionDetector();

  private final DataSourceResolver dataSourceResolver = new DataSourceResolver();
  private final IndexMetadataCollector metadataCollector = new IndexMetadataCollector();
  private final HibernateIntegration hibernateIntegration = new HibernateIntegration();

  // ── BeforeAllCallback ──────────────────────────────────────────────

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    QueryInterceptor interceptor = new QueryInterceptor();

    // Apply memory configuration from @QueryAudit or defaults
    QueryAuditConfig earlyConfig = buildConfig(context);
    interceptor.setMaxQueries(earlyConfig.getMaxQueries());

    ExtensionContext.Store store = context.getStore(NAMESPACE);
    store.put(KEY_INTERCEPTOR, interceptor);

    DataSource dataSource = dataSourceResolver.resolve(context);
    if (dataSource != null) {
      store.put(KEY_DATASOURCE, dataSource);
      dataSourceResolver.hookInterceptor(dataSource, interceptor);

      IndexMetadata metadata = metadataCollector.collect(dataSource);
      if (metadata != null) {
        store.put(KEY_INDEX_METADATA, metadata);
      }
    }

    // Load query count baseline for regression detection
    Path countBaselinePath = resolveCountBaselinePath(context);
    Map<String, QueryCounts> countBaseline = QueryCountBaseline.load(countBaselinePath);
    store.put(KEY_COUNT_BASELINE, countBaseline);
    store.put(KEY_CURRENT_COUNTS, new ConcurrentHashMap<String, QueryCounts>());

    // Register Hibernate LazyLoadTracker if Hibernate is on the classpath
    LazyLoadTracker tracker = hibernateIntegration.registerTracker(context, NAMESPACE);
    if (tracker != null) {
      store.put(KEY_LAZY_LOAD_TRACKER, tracker);
    }
  }

  // ── BeforeEachCallback ─────────────────────────────────────────────

  @Override
  public void beforeEach(ExtensionContext context) {
    QueryInterceptor interceptor = getInterceptor(context);
    if (interceptor != null) {
      interceptor.start();
    }

    LazyLoadTracker tracker = getLazyLoadTracker(context);
    if (tracker != null) {
      tracker.start();
    }
  }

  // ── AfterEachCallback ──────────────────────────────────────────────

  @Override
  public void afterEach(ExtensionContext context) {
    QueryInterceptor interceptor = getInterceptor(context);
    if (interceptor == null) {
      return;
    }

    interceptor.stop();

    LazyLoadTracker tracker = getLazyLoadTracker(context);
    if (tracker != null) {
      tracker.stop();
    }

    List<QueryRecord> queries = interceptor.getRecordedQueries();
    if (queries.isEmpty() && (tracker == null || tracker.getRecords().isEmpty())) {
      return;
    }

    QueryAuditConfig config = buildConfig(context);
    IndexMetadata indexMetadata = getIndexMetadata(context);

    // Resolve baseline path
    java.nio.file.Path baselinePath =
        config.getBaselinePath() != null
            ? java.nio.file.Path.of(config.getBaselinePath())
            : java.nio.file.Path.of(Baseline.DEFAULT_FILE_NAME);

    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(config, baselinePath);
    // Use outermost class name (resolve @Nested inner classes to parent)
    Class<?> cls = context.getRequiredTestClass();
    while (cls.getEnclosingClass() != null) {
      cls = cls.getEnclosingClass();
    }
    String testClass = cls.getSimpleName();
    String testName = context.getDisplayName();
    QueryAuditReport report = analyzer.analyze(testClass, testName, queries, indexMetadata);

    // Merge Hibernate-level N+1 issues if tracker is available
    if (tracker != null && !tracker.getRecords().isEmpty()) {
      report = hibernateIntegration.mergeNPlusOneIssues(report, tracker, config);
    }

    // --- Query count regression detection ---
    report = detectQueryCountRegression(context, report, queries, testClass, testName);

    // --- EXPLAIN-based detection ---
    report = runExplainAnalysis(context, report, queries);

    List<BaselineEntry> baseline = analyzer.getBaseline();
    ConsoleReporter reporter =
        new ConsoleReporter(System.out, ConsoleReporter.detectColorSupport(), baseline);
    reporter.report(report);

    // Accumulate for HTML report
    HtmlReportAggregator.getInstance().addReport(report);

    // --- @ExpectMaxQueryCount ---
    checkMaxQueryCount(context, queries, testName);

    // --- @DetectNPlusOne ---
    checkDetectNPlusOne(context, report, testName);

    // --- @QueryAudit failOnDetection ---
    if (config.isFailOnDetection() && report.hasConfirmedIssues()) {
      List<Issue> failableIssues = filterFailableIssues(report, context);
      if (!failableIssues.isEmpty()) {
        throw new AssertionError(buildFailureMessage(testName, failableIssues));
      }
    }
  }

  // ── EXPLAIN-based analysis ───────────────────────────────────────

  private QueryAuditReport runExplainAnalysis(
      ExtensionContext context, QueryAuditReport report, List<QueryRecord> queries) {
    DataSource dataSource = getDataSource(context);
    if (dataSource == null || queries.isEmpty()) {
      return report;
    }

    try (Connection connection = dataSource.getConnection()) {
      String dbProduct = connection.getMetaData().getDatabaseProductName().toLowerCase();

      ServiceLoader<ExplainAnalyzer> loader = ServiceLoader.load(ExplainAnalyzer.class);
      for (ExplainAnalyzer explainAnalyzer : loader) {
        if (dbProduct.contains(explainAnalyzer.supportedDatabase())) {
          List<Issue> explainIssues = explainAnalyzer.analyze(connection, queries);
          if (!explainIssues.isEmpty()) {
            List<Issue> mergedInfo = new ArrayList<>(report.getInfoIssues());
            mergedInfo.addAll(explainIssues);

            report =
                new QueryAuditReport(
                    report.getTestClass(),
                    report.getTestName(),
                    report.getConfirmedIssues(),
                    mergedInfo,
                    report.getAcknowledgedIssues(),
                    report.getAllQueries(),
                    report.getUniquePatternCount(),
                    report.getTotalQueryCount(),
                    report.getTotalExecutionTimeNanos());
          }
          break;
        }
      }
    } catch (Exception e) {
      System.err.println("[QueryAudit] EXPLAIN analysis failed: " + e.getMessage());
    }

    return report;
  }

  // ── Query count regression detection ────────────────────────────────

  @SuppressWarnings("unchecked")
  private QueryAuditReport detectQueryCountRegression(
      ExtensionContext context,
      QueryAuditReport report,
      List<QueryRecord> queries,
      String testClass,
      String testName) {

    QueryCounts current = QueryCounts.from(queries);

    Map<String, QueryCounts> currentCounts = getCurrentCounts(context);
    if (currentCounts != null) {
      currentCounts.put(QueryCountBaseline.key(testClass, testName), current);
    }

    Map<String, QueryCounts> countBaseline = getCountBaseline(context);
    if (countBaseline == null || countBaseline.isEmpty()) {
      return report;
    }

    String key = QueryCountBaseline.key(testClass, testName);
    QueryCounts baselineCounts = countBaseline.get(key);

    List<Issue> regressionIssues =
        REGRESSION_DETECTOR.detect(testClass, testName, current, baselineCounts);
    if (regressionIssues.isEmpty()) {
      return report;
    }

    List<Issue> mergedConfirmed = new ArrayList<>(report.getConfirmedIssues());
    mergedConfirmed.addAll(regressionIssues);

    return new QueryAuditReport(
        report.getTestClass(),
        report.getTestName(),
        mergedConfirmed,
        report.getInfoIssues(),
        report.getAcknowledgedIssues(),
        report.getAllQueries(),
        report.getUniquePatternCount(),
        report.getTotalQueryCount(),
        report.getTotalExecutionTimeNanos());
  }

  // ── AfterAllCallback ──────────────────────────────────────────────

  @Override
  @SuppressWarnings("unchecked")
  public void afterAll(ExtensionContext context) {
    if (context.getRequiredTestClass().getEnclosingClass() != null) {
      return;
    }

    writeCountBaselineIfRequested(context);

    HtmlReportAggregator aggregator = HtmlReportAggregator.getInstance();
    if (aggregator.getReports().isEmpty()) {
      return;
    }

    try {
      java.nio.file.Path outputDir = java.nio.file.Path.of("build", "reports", "query-audit");
      aggregator.writeReport(outputDir);
      java.nio.file.Path reportPath = outputDir.toAbsolutePath().resolve("index.html");

      // Summary line — visible even without opening the report
      long totalErrors = aggregator.getReports().stream()
              .mapToLong(r -> r.getErrors().size()).sum();
      long totalWarnings = aggregator.getReports().stream()
              .mapToLong(r -> r.getWarnings().size()).sum();
      int totalQueries = aggregator.getReports().stream()
              .mapToInt(r -> r.getTotalQueryCount()).sum();
      int totalTests = aggregator.getReports().size();

      // Summary + clickable link on its own line (IDE auto-detects file:// URLs)
      String summary = "[QueryAudit] " + totalTests + " tests, " + totalQueries + " queries"
              + (totalErrors > 0 ? ", " + totalErrors + " ERROR" + (totalErrors > 1 ? "S" : "") : "")
              + (totalWarnings > 0 ? ", " + totalWarnings + " WARNING" + (totalWarnings > 1 ? "S" : "") : "")
              + (totalErrors == 0 && totalWarnings == 0 ? " — all clean" : "");
      System.out.println();
      System.out.println(summary);
      System.out.println("[QueryAudit] file://" + reportPath.toAbsolutePath());
      System.out.println();

      // Write JSON report alongside HTML
      writeJsonReport(aggregator.getReports(), outputDir);

      if (shouldAutoOpenReport(context)) {
        openReportInBrowser(reportPath);
      }
    } catch (Exception e) {
      System.err.println("[QueryAudit] Failed to write HTML report: " + e.getMessage());
    }
  }

  // ── Annotation-specific checks ─────────────────────────────────────

  private void checkMaxQueryCount(
      ExtensionContext context, List<QueryRecord> queries, String testName) {
    ExpectMaxQueryCount annotation =
        context.getRequiredTestMethod().getAnnotation(ExpectMaxQueryCount.class);
    if (annotation == null) return;

    int max = annotation.value();
    int actual = queries.size();
    if (actual > max) {
      throw new AssertionError(
          String.format(
              "QueryAudit: %s executed %d queries, expected at most %d.\n"
                  + "Tip: Check the Query Patterns section in the report above to identify which queries to optimize.",
              testName, actual, max));
    }
  }

  private void checkDetectNPlusOne(
      ExtensionContext context, QueryAuditReport report, String testName) {
    DetectNPlusOne methodAnnotation =
        context.getRequiredTestMethod().getAnnotation(DetectNPlusOne.class);
    DetectNPlusOne classAnnotation =
        context.getRequiredTestClass().getAnnotation(DetectNPlusOne.class);
    if (classAnnotation == null) {
      Class<?> enclosing = context.getRequiredTestClass().getEnclosingClass();
      while (enclosing != null && classAnnotation == null) {
        classAnnotation = enclosing.getAnnotation(DetectNPlusOne.class);
        enclosing = enclosing.getEnclosingClass();
      }
    }

    DetectNPlusOne annotation = methodAnnotation != null ? methodAnnotation : classAnnotation;
    if (annotation == null) return;

    List<Issue> nPlusOneIssues =
        report.getConfirmedIssues().stream().filter(i -> i.type() == IssueType.N_PLUS_ONE).toList();

    if (!nPlusOneIssues.isEmpty()) {
      StringBuilder sb = new StringBuilder();
      sb.append("QueryAudit: N+1 detected in ").append(testName).append("!\n\n");
      for (Issue issue : nPlusOneIssues) {
        sb.append("  ").append(issue.detail());
        if (issue.query() != null) {
          String sql = issue.query();
          sb.append("\n  Query: ").append(sql.length() > 100 ? sql.substring(0, 100) + "..." : sql);
        }
        if (issue.sourceLocation() != null) {
          sb.append("\n  Source: ").append(issue.sourceLocation());
        }
        sb.append("\n  Fix: ").append(issue.suggestion()).append("\n\n");
      }
      throw new AssertionError(sb.toString());
    }
  }

  // ── Config building ────────────────────────────────────────────────

  private QueryAuditConfig buildConfig(ExtensionContext context) {
    QueryAuditConfig.Builder builder = QueryAuditConfig.builder();

    if (hasEnableQueryInspector(context)) {
      builder.failOnDetection(false);
    }

    QueryAudit annotation = findAnnotation(context);
    if (annotation != null) {
      builder.failOnDetection(annotation.failOnDetection());

      if (annotation.nPlusOneThreshold() >= 0) {
        builder.nPlusOneThreshold(annotation.nPlusOneThreshold());
      }

      for (String suppress : annotation.suppress()) {
        builder.addSuppressPattern(suppress);
      }

      if (!annotation.baselinePath().isEmpty()) {
        builder.baselinePath(annotation.baselinePath());
      }
    }

    DetectNPlusOne detectNPlusOne = null;
    // getTestMethod() returns Optional.empty() in beforeAll (class-level context)
    java.util.Optional<java.lang.reflect.Method> method = context.getTestMethod();
    if (method.isPresent()) {
      detectNPlusOne = method.get().getAnnotation(DetectNPlusOne.class);
    }
    if (detectNPlusOne == null) {
      Class<?> clazz = context.getRequiredTestClass();
      while (clazz != null && detectNPlusOne == null) {
        detectNPlusOne = clazz.getAnnotation(DetectNPlusOne.class);
        clazz = clazz.getEnclosingClass();
      }
    }
    if (detectNPlusOne != null) {
      builder.nPlusOneThreshold(detectNPlusOne.threshold());
    }

    return builder.build();
  }

  private boolean hasEnableQueryInspector(ExtensionContext context) {
    Class<?> clazz = context.getRequiredTestClass();
    while (clazz != null) {
      if (clazz.isAnnotationPresent(EnableQueryInspector.class)) return true;
      clazz = clazz.getEnclosingClass();
    }
    return false;
  }

  private QueryAudit findAnnotation(ExtensionContext context) {
    // getTestMethod() returns Optional.empty() in afterAll (class-level context)
    java.util.Optional<java.lang.reflect.Method> testMethod = context.getTestMethod();
    if (testMethod.isPresent()) {
      QueryAudit annotation = testMethod.get().getAnnotation(QueryAudit.class);
      if (annotation != null) return annotation;
    }

    Class<?> clazz = context.getRequiredTestClass();
    while (clazz != null) {
      QueryAudit annotation = clazz.getAnnotation(QueryAudit.class);
      if (annotation != null) return annotation;
      clazz = clazz.getEnclosingClass();
    }

    return null;
  }

  // ── Issue filtering & failure message ──────────────────────────────

  private List<Issue> filterFailableIssues(QueryAuditReport report, ExtensionContext context) {
    List<Issue> confirmed = report.getConfirmedIssues();
    if (confirmed == null || confirmed.isEmpty()) {
      return List.of();
    }

    QueryAudit annotation = findAnnotation(context);

    if (annotation != null && annotation.failOn().length > 0) {
      Set<IssueType> failOnTypes = Set.of(annotation.failOn());
      return confirmed.stream().filter(issue -> failOnTypes.contains(issue.type())).toList();
    }

    return confirmed;
  }

  private String buildFailureMessage(String testName, List<Issue> issues) {
    StringBuilder sb = new StringBuilder();
    sb.append("QueryAudit detected ")
        .append(issues.size())
        .append(" issue(s) in ")
        .append(testName)
        .append(":\n\n");

    for (Issue issue : issues) {
      sb.append("  [").append(issue.severity()).append("] ").append(issue.type().getDescription());
      if (issue.table() != null) {
        sb.append(" (table: ").append(issue.table()).append(")");
      }
      if (issue.detail() != null) {
        sb.append("\n    Detail: ").append(issue.detail());
      }
      if (issue.suggestion() != null) {
        sb.append("\n    Suggestion: ").append(issue.suggestion());
      }
      sb.append("\n");
    }
    return sb.toString();
  }

  // ── JSON report ───────────────────────────────────────────────────

  private void writeJsonReport(
      List<io.queryaudit.core.model.QueryAuditReport> reports, java.nio.file.Path outputDir) {
    try {
      java.nio.file.Files.createDirectories(outputDir);
      StringBuilder sb = new StringBuilder();
      sb.append("[\n");
      for (int i = 0; i < reports.size(); i++) {
        sb.append(JsonReporter.toJson(reports.get(i)));
        if (i < reports.size() - 1) {
          sb.append(",");
        }
        sb.append("\n");
      }
      sb.append("]");
      java.nio.file.Path jsonPath = outputDir.resolve("report.json");
      java.nio.file.Files.writeString(jsonPath, sb.toString());
      System.out.println("[QueryAudit] JSON report: " + jsonPath.toAbsolutePath());
    } catch (Exception e) {
      System.err.println("[QueryAudit] Failed to write JSON report: " + e.getMessage());
    }
  }

  // ── Baseline & report helpers ──────────────────────────────────────

  private Path resolveCountBaselinePath(ExtensionContext context) {
    String sysProp = System.getProperty("queryGuard.countBaselinePath");
    if (sysProp != null && !sysProp.isEmpty()) {
      return Path.of(sysProp);
    }
    return Path.of(QueryCountBaseline.DEFAULT_FILE_NAME);
  }

  @SuppressWarnings("unchecked")
  private void writeCountBaselineIfRequested(ExtensionContext context) {
    boolean updateBaseline =
        Boolean.parseBoolean(System.getProperty("queryGuard.updateBaseline", "false"));
    if (!updateBaseline) {
      return;
    }

    Map<String, QueryCounts> currentCounts = getCurrentCounts(context);
    if (currentCounts == null || currentCounts.isEmpty()) {
      return;
    }

    try {
      Path countBaselinePath = resolveCountBaselinePath(context);

      Map<String, QueryCounts> merged =
          new LinkedHashMap<>(QueryCountBaseline.load(countBaselinePath));
      merged.putAll(currentCounts);

      QueryCountBaseline.save(countBaselinePath, merged);
      System.out.println(
          "[QueryAudit] Count baseline updated: "
              + countBaselinePath.toAbsolutePath()
              + " ("
              + currentCounts.size()
              + " test(s))");
    } catch (Exception e) {
      System.err.println("[QueryAudit] Failed to write count baseline: " + e.getMessage());
    }
  }

  private boolean shouldAutoOpenReport(ExtensionContext context) {
    String sysProp = System.getProperty("queryaudit.autoOpenReport");
    if (sysProp != null) {
      return Boolean.parseBoolean(sysProp);
    }

    String envVar = System.getenv("QUERYGUARD_AUTO_OPEN_REPORT");
    if (envVar != null) {
      return Boolean.parseBoolean(envVar);
    }

    if (System.getenv("CI") != null
        || System.getenv("JENKINS_HOME") != null
        || System.getenv("GITHUB_ACTIONS") != null
        || System.getenv("GITLAB_CI") != null) {
      return false;
    }

    QueryAudit annotation = findAnnotation(context);
    if (annotation != null) {
      return annotation.autoOpenReport();
    }

    // Default: auto-open when running locally (not in CI)
    return true;
  }

  private void openReportInBrowser(java.nio.file.Path reportPath) {
    try {
      java.io.File reportFile = reportPath.toFile();
      if (!reportFile.exists()) {
        return;
      }

      if (java.awt.Desktop.isDesktopSupported()) {
        java.awt.Desktop desktop = java.awt.Desktop.getDesktop();
        if (desktop.isSupported(java.awt.Desktop.Action.BROWSE)) {
          desktop.browse(reportFile.toURI());
          System.out.println("[QueryAudit] Report opened in browser.");
          return;
        }
      }

      String os = System.getProperty("os.name", "").toLowerCase();
      ProcessBuilder pb;
      if (os.contains("mac")) {
        pb = new ProcessBuilder("open", reportFile.getAbsolutePath());
      } else if (os.contains("win")) {
        pb = new ProcessBuilder("cmd", "/c", "start", "", reportFile.getAbsolutePath());
      } else {
        pb = new ProcessBuilder("xdg-open", reportFile.getAbsolutePath());
      }
      pb.redirectErrorStream(true);
      pb.start();
      System.out.println("[QueryAudit] Report opened in browser.");
    } catch (Exception e) {
      System.err.println("[QueryAudit] Could not open browser: " + e.getMessage());
      System.err.println("[QueryAudit] Open manually: " + reportPath.toAbsolutePath());
    }
  }

  // ── Store helpers ──────────────────────────────────────────────────

  private QueryInterceptor getInterceptor(ExtensionContext context) {
    ExtensionContext.Store store = context.getStore(NAMESPACE);
    QueryInterceptor interceptor = store.get(KEY_INTERCEPTOR, QueryInterceptor.class);
    if (interceptor == null) {
      ExtensionContext parent = context.getParent().orElse(null);
      if (parent != null) {
        interceptor = parent.getStore(NAMESPACE).get(KEY_INTERCEPTOR, QueryInterceptor.class);
      }
    }
    return interceptor;
  }

  private LazyLoadTracker getLazyLoadTracker(ExtensionContext context) {
    ExtensionContext.Store store = context.getStore(NAMESPACE);
    LazyLoadTracker tracker = store.get(KEY_LAZY_LOAD_TRACKER, LazyLoadTracker.class);
    if (tracker == null) {
      ExtensionContext parent = context.getParent().orElse(null);
      if (parent != null) {
        tracker = parent.getStore(NAMESPACE).get(KEY_LAZY_LOAD_TRACKER, LazyLoadTracker.class);
      }
    }
    return tracker;
  }

  private DataSource getDataSource(ExtensionContext context) {
    ExtensionContext.Store store = context.getStore(NAMESPACE);
    DataSource ds = store.get(KEY_DATASOURCE, DataSource.class);
    if (ds == null) {
      ExtensionContext parent = context.getParent().orElse(null);
      if (parent != null) {
        ds = parent.getStore(NAMESPACE).get(KEY_DATASOURCE, DataSource.class);
      }
    }
    return ds;
  }

  private IndexMetadata getIndexMetadata(ExtensionContext context) {
    ExtensionContext.Store store = context.getStore(NAMESPACE);
    IndexMetadata metadata = store.get(KEY_INDEX_METADATA, IndexMetadata.class);
    if (metadata == null) {
      ExtensionContext parent = context.getParent().orElse(null);
      if (parent != null) {
        metadata = parent.getStore(NAMESPACE).get(KEY_INDEX_METADATA, IndexMetadata.class);
      }
    }
    return metadata;
  }

  @SuppressWarnings("unchecked")
  private Map<String, QueryCounts> getCountBaseline(ExtensionContext context) {
    ExtensionContext.Store store = context.getStore(NAMESPACE);
    Object obj = store.get(KEY_COUNT_BASELINE);
    if (obj instanceof Map<?, ?> map) {
      return (Map<String, QueryCounts>) map;
    }
    ExtensionContext parent = context.getParent().orElse(null);
    if (parent != null) {
      obj = parent.getStore(NAMESPACE).get(KEY_COUNT_BASELINE);
      if (obj instanceof Map<?, ?> map) {
        return (Map<String, QueryCounts>) map;
      }
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  private Map<String, QueryCounts> getCurrentCounts(ExtensionContext context) {
    ExtensionContext.Store store = context.getStore(NAMESPACE);
    Object obj = store.get(KEY_CURRENT_COUNTS);
    if (obj instanceof Map<?, ?> map) {
      return (Map<String, QueryCounts>) map;
    }
    ExtensionContext parent = context.getParent().orElse(null);
    if (parent != null) {
      obj = parent.getStore(NAMESPACE).get(KEY_CURRENT_COUNTS);
      if (obj instanceof Map<?, ?> map) {
        return (Map<String, QueryCounts>) map;
      }
    }
    return null;
  }
}
