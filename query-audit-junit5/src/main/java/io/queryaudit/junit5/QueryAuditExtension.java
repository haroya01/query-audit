package io.queryaudit.junit5;

import io.queryaudit.core.analyzer.ExplainAnalyzer;
import io.queryaudit.core.baseline.Baseline;
import io.queryaudit.core.baseline.BaselineEntry;
import io.queryaudit.core.config.AuditMode;
import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.config.ReportFormat;
import io.queryaudit.core.config.RuleProfile;
import io.queryaudit.core.detector.QueryAuditAnalyzer;
import io.queryaudit.core.detector.RepositoryReturnTypeResolver;
import io.queryaudit.core.interceptor.ConnectionUsageTracker;
import io.queryaudit.core.interceptor.LazyLoadTracker;
import io.queryaudit.core.interceptor.QueryCaptureSnapshot;
import io.queryaudit.core.interceptor.QueryInterceptor;
import io.queryaudit.core.model.*;
import io.queryaudit.core.model.LifecyclePhase;
import io.queryaudit.core.parser.SqlParser;
import io.queryaudit.core.regression.QueryContracts;
import io.queryaudit.core.regression.QueryCountBaseline;
import io.queryaudit.core.regression.QueryCountRegressionDetector;
import io.queryaudit.core.regression.QueryCounts;
import io.queryaudit.core.reporter.ConsoleReporter;
import io.queryaudit.core.reporter.GitHubActionsReporter;
import io.queryaudit.core.reporter.HtmlReportAggregator;
import io.queryaudit.core.reporter.JsonReporter;
import java.awt.Desktop;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import javax.sql.DataSource;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.extension.*;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.platform.commons.support.AnnotationSupport;

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
    implements BeforeAllCallback,
        BeforeEachCallback,
        BeforeTestExecutionCallback,
        AfterTestExecutionCallback,
        AfterEachCallback,
        AfterAllCallback {

  private static final ExtensionContext.Namespace NAMESPACE =
      ExtensionContext.Namespace.create(QueryAuditExtension.class);

  private static final String KEY_INTERCEPTOR = "interceptor";
  private static final String KEY_INDEX_METADATA = "indexMetadata";
  private static final String KEY_LAZY_LOAD_TRACKER = "lazyLoadTracker";
  private static final String KEY_COUNT_BASELINE = "countBaseline";
  private static final String KEY_CURRENT_COUNTS = "currentCounts";
  private static final String KEY_DATASOURCE = "dataSource";
  private static final String KEY_RETURN_TYPE_RESOLVER = "returnTypeResolver";
  private static final String KEY_DATASOURCE_HOOK_CLEANUP = "dataSourceHookCleanup";
  private static final String KEY_METHOD_SCOPED_CLEANUP = "methodScopedCleanup";
  private static final String KEY_AUDIT_RESOURCE_OWNER = "auditResourceOwner";
  private static final String KEY_ACTIVE = "auditActive";
  private static final String KEY_METHOD_ACTIVE = "methodAuditActive";
  private static final String KEY_AFTER_EACH_DONE = "afterEachDone";
  private static final String KEY_CONCURRENT_EXECUTION_REJECTED = "concurrentExecutionRejected";
  private static final String KEY_INITIALIZATION_FAILURE_RECORDED = "initializationFailureRecorded";
  private static final String KEY_CONTRACTS = "queryContracts";
  private static final String KEY_RUN_STATE = AuditRunState.class.getName();
  private static final String KEY_LEGACY_IDENTITY_CLAIMS = "legacyIdentityClaims";

  private static final QueryCountRegressionDetector REGRESSION_DETECTOR =
      new QueryCountRegressionDetector();

  private enum InitializationScope {
    CLASS,
    METHOD
  }

  private final DataSourceResolver dataSourceResolver;
  private final IndexMetadataCollector metadataCollector;
  private final HibernateIntegration hibernateIntegration;

  public QueryAuditExtension() {
    this(new DataSourceResolver(), new IndexMetadataCollector(), new HibernateIntegration());
  }

  QueryAuditExtension(
      DataSourceResolver dataSourceResolver,
      IndexMetadataCollector metadataCollector,
      HibernateIntegration hibernateIntegration) {
    this.dataSourceResolver = Objects.requireNonNull(dataSourceResolver, "dataSourceResolver");
    this.metadataCollector = Objects.requireNonNull(metadataCollector, "metadataCollector");
    this.hibernateIntegration =
        Objects.requireNonNull(hibernateIntegration, "hibernateIntegration");
  }

  // ── BeforeAllCallback ──────────────────────────────────────────────

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    try {
      // With the ServiceLoader registration + JUnit extension autodetection, this callback runs for
      // every test class in the suite. The activation decision (audit mode + annotations +
      // @QueryAuditExclude) is made once per class and stored so the per-method callbacks can read
      // it without re-resolving the Spring context.
      ExtensionContext.Store activeStore = context.getStore(NAMESPACE);
      boolean active = computeActive(context);
      QueryAuditConfig auditConfig = active ? buildConfig(context) : null;
      if (auditConfig != null && !auditConfig.isEnabled()) {
        active = false;
      }
      activeStore.put(KEY_ACTIVE, active);
      if (!active) {
        return;
      }
      registerReportFinalizer(context, auditConfig);
      if (activeStore.get(KEY_INTERCEPTOR) != null) {
        // Extension registered twice for this class (autodetection + @ExtendWith/@QueryAudit) —
        // the second instance must not re-wrap the DataSource or double-hook listeners.
        return;
      }

      // A missing DataSource is reported from beforeEach, when method-level exclusions are known.
      // If a DataSource is already available, initialize once at class scope as before.
      initializeAudit(context, auditConfig, InitializationScope.CLASS);
    } catch (Exception | Error failure) {
      recordUnexpectedInitializationFailure(context, failure);
      throw failure;
    }
  }

  private void initializeAudit(
      ExtensionContext context, QueryAuditConfig auditConfig, InitializationScope scope)
      throws Exception {
    if (getInterceptor(context) != null) {
      return;
    }

    DataSourceResolver.ResolvedDataSource resolvedDataSource = dataSourceResolver.resolve(context);
    if (resolvedDataSource == null) {
      if (scope == InitializationScope.METHOD) {
        recordInitializationFailure(
            context,
            IncompleteReasonCode.DATASOURCE_UNAVAILABLE,
            "No DataSource was available for " + auditTarget(context));
        throw new ExtensionConfigurationException(
            "QueryAudit: DataSource unavailable for active audit of "
                + auditTarget(context)
                + ". Register a DataSource bean in the Spring ApplicationContext or expose a"
                + " non-null static DataSource field on the test class.");
      }
      return;
    }
    DataSource dataSource = resolvedDataSource.dataSource();

    Path countBaselinePath;
    Map<String, QueryCounts> countBaseline;
    Map<String, QueryCounts> contracts;
    try {
      countBaselinePath = resolveCountBaselinePath(context);
      countBaseline = QueryCountBaseline.load(countBaselinePath);
      contracts = QueryCountBaseline.load(resolveContractsPath());
    } catch (IllegalStateException | InvalidPathException e) {
      recordInitializationFailure(
          context, IncompleteReasonCode.CONTRACT_UNREADABLE, e.getMessage());
      throw e;
    }

    QueryInterceptor interceptor = new QueryInterceptor();
    try {
      interceptor.setMaxQueries(auditConfig.getMaxQueries());
    } catch (RuntimeException | Error failure) {
      recordInitializationFailure(
          context,
          IncompleteReasonCode.AUDIT_INITIALIZATION_FAILED,
          "Could not initialize query capture for "
              + auditTarget(context)
              + ": "
              + failureMessage(failure));
      throw failure;
    }

    ExtensionContext.Store store = context.getStore(NAMESPACE);
    Runnable hookCleanup;
    try {
      hookCleanup = dataSourceResolver.hookInterceptor(resolvedDataSource, interceptor);
    } catch (RuntimeException | Error e) {
      recordInitializationFailure(
          context,
          IncompleteReasonCode.AUDIT_INITIALIZATION_FAILED,
          "Could not initialize query capture for "
              + auditTarget(context)
              + ": "
              + failureMessage(e));
      throw e;
    }
    store.put(KEY_INTERCEPTOR, interceptor);
    store.put(KEY_DATASOURCE, dataSource);
    store.put(KEY_DATASOURCE_HOOK_CLEANUP, hookCleanup);
    store.put(KEY_AUDIT_RESOURCE_OWNER, contextIdentity(context));

    LazyLoadTracker tracker = null;
    try {
      IndexMetadata metadata = metadataCollector.collect(dataSource);
      if (metadata != null) {
        store.put(KEY_INDEX_METADATA, metadata);
      }

      // Build return type resolver from Spring Data repositories if available
      try {
        Object appContext = resolveApplicationContext(context);
        if (appContext != null) {
          store.put(KEY_RETURN_TYPE_RESOLVER, new SpringDataReturnTypeResolver(appContext));
        }
      } catch (Exception | NoClassDefFoundError e) {
        System.err.println(
            "[QueryAudit] Failed to initialize return type resolver: " + e.getMessage());
      }

      store.put(KEY_COUNT_BASELINE, countBaseline);
      store.put(KEY_CURRENT_COUNTS, new ConcurrentHashMap<String, QueryCounts>());
      store.put(KEY_CONTRACTS, contracts);

      // Register Hibernate LazyLoadTracker if Hibernate is on the classpath
      tracker = hibernateIntegration.registerTracker(context, NAMESPACE);
      if (tracker != null) {
        store.put(KEY_LAZY_LOAD_TRACKER, tracker);
      }
      if (scope == InitializationScope.METHOD) {
        LazyLoadTracker methodTracker = tracker;
        store.put(
            KEY_METHOD_SCOPED_CLEANUP,
            (ExtensionContext.Store.CloseableResource)
                () -> closeAuditScope(context, methodTracker, hookCleanup));
      }
    } catch (RuntimeException | Error failure) {
      rollbackInitialization(context, store, hookCleanup, tracker, failure);
      throw failure;
    }
  }

  private void rollbackInitialization(
      ExtensionContext context,
      ExtensionContext.Store store,
      Runnable hookCleanup,
      LazyLoadTracker tracker,
      Throwable failure) {
    if (tracker != null) {
      runCleanup(failure, () -> hibernateIntegration.unregisterTracker(context, tracker));
    }
    runCleanup(failure, hookCleanup);
    runCleanup(failure, QueryAuditDataSourceStore::clear);

    store.remove(KEY_INTERCEPTOR);
    store.remove(KEY_DATASOURCE);
    store.remove(KEY_DATASOURCE_HOOK_CLEANUP);
    store.remove(KEY_AUDIT_RESOURCE_OWNER);
    store.remove(KEY_INDEX_METADATA);
    store.remove(KEY_RETURN_TYPE_RESOLVER);
    store.remove(KEY_COUNT_BASELINE);
    store.remove(KEY_CURRENT_COUNTS);
    store.remove(KEY_CONTRACTS);
    store.remove(KEY_LAZY_LOAD_TRACKER);
    store.remove(KEY_METHOD_SCOPED_CLEANUP);
  }

  private static Throwable runCleanup(Throwable failure, Runnable cleanup) {
    try {
      cleanup.run();
      return failure;
    } catch (RuntimeException | Error cleanupFailure) {
      if (failure == null) {
        return cleanupFailure;
      }
      failure.addSuppressed(cleanupFailure);
      return failure;
    }
  }

  private void closeAuditScope(
      ExtensionContext context, LazyLoadTracker tracker, Runnable hookCleanup) {
    Throwable failure = null;
    if (tracker != null) {
      failure = runCleanup(failure, () -> hibernateIntegration.unregisterTracker(context, tracker));
    }
    if (hookCleanup != null) {
      failure = runCleanup(failure, hookCleanup);
    }
    failure = runCleanup(failure, QueryAuditDataSourceStore::clear);
    failure = runCleanup(failure, () -> writeCountBaselineIfRequested(context));
    failure = runCleanup(failure, () -> writeContractsIfRequested(context));

    if (failure == null) {
      return;
    }

    markIncomplete(
        context,
        IncompleteReasonCode.AUDIT_ANALYSIS_FAILED,
        "Could not clean up QueryAudit for "
            + auditTarget(context)
            + ": "
            + failureMessage(failure));
    if (failure instanceof RuntimeException runtimeFailure) {
      throw runtimeFailure;
    }
    throw (Error) failure;
  }

  private static String auditTarget(ExtensionContext context) {
    String target = context.getRequiredTestClass().getName();
    return context.getTestMethod().map(method -> target + "#" + method.getName()).orElse(target);
  }

  private static String contextIdentity(ExtensionContext context) {
    String uniqueId = context.getUniqueId();
    if (uniqueId != null && !uniqueId.isBlank()) {
      return uniqueId;
    }
    String className = context.getRequiredTestClass().getName();
    return context
        .getTestMethod()
        .map(method -> className + "#" + method.toGenericString())
        .orElse(className);
  }

  private static boolean ownsAuditResources(ExtensionContext context) {
    ExtensionContext.Store store = context.getStore(NAMESPACE);
    if (store == null) {
      return false;
    }
    String owner = store.get(KEY_AUDIT_RESOURCE_OWNER, String.class);
    return contextIdentity(context).equals(owner);
  }

  private static void requireSameThreadExecution(ExtensionContext context) {
    if (context.getExecutionMode() != ExecutionMode.CONCURRENT) {
      return;
    }

    context.getStore(NAMESPACE).put(KEY_CONCURRENT_EXECUTION_REJECTED, Boolean.TRUE);
    recordInitializationFailure(
        context,
        IncompleteReasonCode.AUDIT_INITIALIZATION_FAILED,
        "Concurrent execution prevented reliable query attribution for " + auditTarget(context));
    throw new ExtensionConfigurationException(
        "QueryAudit: cannot audit "
            + auditTarget(context)
            + " with concurrent execution. Query capture is shared within a test class, so"
            + " overlapping methods cannot be attributed reliably. Use @Execution(SAME_THREAD)"
            + " or set junit.jupiter.execution.parallel.mode.default=same_thread.");
  }

  private static boolean wasConcurrentExecutionRejected(ExtensionContext context) {
    return Boolean.TRUE.equals(
        context.getStore(NAMESPACE).get(KEY_CONCURRENT_EXECUTION_REJECTED, Boolean.class));
  }

  // ── BeforeEachCallback ─────────────────────────────────────────────

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    try {
      if (!isAuditActive(context)) {
        return;
      }
      QueryAuditConfig auditConfig = buildConfig(context);
      if (!auditConfig.isEnabled()) {
        context.getStore(NAMESPACE).put(KEY_ACTIVE, Boolean.FALSE);
        context.getStore(NAMESPACE).put(KEY_METHOD_ACTIVE, Boolean.FALSE);
        return;
      }
      registerReportFinalizer(context, auditConfig);
      rejectDynamicTestFactoryAudit(context);
      requireSameThreadExecution(context);
      initializeAudit(context, auditConfig, InitializationScope.METHOD);

      QueryInterceptor interceptor = getInterceptor(context);
      interceptor.start();
      interceptor.setPhase(LifecyclePhase.SETUP);

      LazyLoadTracker tracker = getLazyLoadTracker(context);
      if (tracker != null) {
        tracker.start();
      }
    } catch (Exception | Error failure) {
      recordUnexpectedInitializationFailure(context, failure);
      throw failure;
    }
  }

  private static void rejectDynamicTestFactoryAudit(ExtensionContext context) {
    Method method = context.getRequiredTestMethod();
    if (!AnnotationSupport.isAnnotated(method, TestFactory.class)) {
      return;
    }

    String target = auditTarget(context);
    String detail =
        "Dynamic-test audit rejected for "
            + target
            + ": JUnit lifecycle callbacks do not expose a separate audit boundary for each"
            + " DynamicTest child.";
    recordInitializationFailure(context, IncompleteReasonCode.AUDIT_INITIALIZATION_FAILED, detail);
    throw new ExtensionConfigurationException(
        "QueryAudit: cannot audit @TestFactory method "
            + target
            + ". JUnit lifecycle callbacks surround the factory method, but do not expose a"
            + " separate audit boundary for each DynamicTest child. Use ordinary @Test or"
            + " @ParameterizedTest methods for audited cases, or add @QueryAuditExclude to this"
            + " factory.");
  }

  // ── BeforeTestExecutionCallback ─────────────────────────────────────
  // Runs AFTER @BeforeEach methods, BEFORE the @Test method.

  @Override
  public void beforeTestExecution(ExtensionContext context) {
    if (wasConcurrentExecutionRejected(context)
        || wasInitializationFailureRecorded(context)
        || !isAuditActive(context)) {
      return;
    }
    QueryInterceptor interceptor = getInterceptor(context);
    if (interceptor != null) {
      interceptor.setPhase(LifecyclePhase.TEST);
    }
  }

  // ── AfterTestExecutionCallback ──────────────────────────────────────
  // Runs AFTER the @Test method, BEFORE @AfterEach methods.

  @Override
  public void afterTestExecution(ExtensionContext context) {
    if (wasConcurrentExecutionRejected(context)
        || wasInitializationFailureRecorded(context)
        || !isAuditActive(context)) {
      return;
    }
    QueryInterceptor interceptor = getInterceptor(context);
    if (interceptor != null) {
      interceptor.setPhase(LifecyclePhase.TEARDOWN);
    }
  }

  // ── AfterEachCallback ──────────────────────────────────────────────

  @Override
  public void afterEach(ExtensionContext context) {
    if (wasConcurrentExecutionRejected(context)
        || wasInitializationFailureRecorded(context)
        || !isAuditActive(context)) {
      return;
    }
    try {
      completeAudit(context);
    } catch (AuditCheckFailure failure) {
      // QueryAudit assertions record FAIL or INCONCLUSIVE at the point where they are created.
      throw failure;
    } catch (RuntimeException | Error failure) {
      markIncomplete(
          context,
          IncompleteReasonCode.AUDIT_ANALYSIS_FAILED,
          "Could not complete query analysis for "
              + auditTarget(context)
              + ": "
              + failureMessage(failure));
      throw failure;
    }
  }

  private void completeAudit(ExtensionContext context) {
    // Method-level store: guards against double analysis when the extension is registered twice
    // (autodetection + @ExtendWith/@QueryAudit).
    ExtensionContext.Store methodStore = context.getStore(NAMESPACE);
    if (methodStore.get(KEY_AFTER_EACH_DONE) != null) {
      return;
    }
    methodStore.put(KEY_AFTER_EACH_DONE, Boolean.TRUE);

    QueryInterceptor interceptor = getInterceptor(context);
    if (interceptor == null) {
      return;
    }

    interceptor.stop();

    LazyLoadTracker tracker = getLazyLoadTracker(context);
    if (tracker != null) {
      tracker.stop();
    }

    QueryCaptureSnapshot capture = interceptor.snapshot();
    boolean captureIncomplete = capture.truncated();
    if (captureIncomplete) {
      markIncomplete(
          context,
          IncompleteReasonCode.QUERY_LIMIT_REACHED,
          context.getDisplayName()
              + " retained "
              + capture.queries().size()
              + " queries and dropped "
              + capture.droppedCount());
    }

    List<QueryRecord> queries = capture.queries();
    // Empty executions still need contract enforcement, count recording, and report coverage.

    QueryAuditConfig config = buildConfig(context);
    IndexMetadata indexMetadata = getIndexMetadata(context);

    // Resolve baseline path
    Path baselinePath =
        config.getBaselinePath() != null
            ? Path.of(config.getBaselinePath())
            : Path.of(Baseline.DEFAULT_FILE_NAME);

    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(config, baselinePath);
    // Use outermost class name (resolve @Nested inner classes to parent)
    Class<?> cls = context.getRequiredTestClass();
    while (cls.getEnclosingClass() != null) {
      cls = cls.getEnclosingClass();
    }
    String testClass = cls.getSimpleName();
    String testName = context.getDisplayName();
    JUnitTestIdentity identity = JUnitTestIdentity.from(context);
    QueryAuditReport report = analyzer.analyze(testClass, testName, queries, indexMetadata);

    // Merge Hibernate-level N+1 issues if tracker is available
    if (tracker != null && !tracker.getRecords().isEmpty()) {
      report = hibernateIntegration.mergeNPlusOneIssues(report, tracker, analyzer);
    }

    // Merge findById-for-association issues if tracker recorded explicit loads
    if (tracker != null && !tracker.getExplicitLoads().isEmpty()) {
      report = hibernateIntegration.mergeFindByIdIssues(report, tracker, analyzer);
    }

    // A truncated capture cannot be compared with a complete count baseline.
    if (!captureIncomplete) {
      report =
          detectQueryCountRegression(
              context, report, queries, identity.testId(), testClass, testName, analyzer);
    }

    // --- EXPLAIN-based detection ---
    report = runExplainAnalysis(context, report, queries, analyzer);

    // --- Connection-held-idle detection (issue #168) ---
    report = mergeConnectionHeldIdleIssues(report, interceptor, analyzer);
    report = report.withTestIdentity(identity.testId(), identity.selector());

    report = report.withIndexMetadata(indexMetadata);
    QueryAuditReport outputReport = applyInfoVisibility(report, config.isShowInfo());

    List<BaselineEntry> baseline = analyzer.getBaseline();
    ConsoleReporter reporter =
        new ConsoleReporter(System.out, ConsoleReporter.detectColorSupport(), baseline);
    System.out.println(
        "[QueryAudit] Rule profile: " + config.getRuleProfile().name().toLowerCase(Locale.ROOT));
    reporter.report(outputReport);

    // GitHub Actions annotations + step summary (issue #85).
    if ("true".equals(System.getenv("GITHUB_ACTIONS"))) {
      new GitHubActionsReporter().report(outputReport);
    }

    // HTML and JSON are generated from this same accumulated view, keeping their visible findings
    // and summary counts aligned with the console.
    HtmlReportAggregator.getInstance().addReport(outputReport);

    if (captureIncomplete) {
      throw new AuditCheckFailure(
          buildTruncatedCaptureMessage(
              context.getDisplayName(), interceptor.getMaxQueries(), capture));
    }

    // Query budgets and snapshot contracts are direct test assertions, not findings. They
    // intentionally bypass rule selection, suppressions, severity overrides, and issue baselines.
    try {
      // --- @ExpectMaxQueryCount ---
      checkMaxQueryCount(context, queries, testName);

      // --- @ExpectQueries ---
      checkExpectQueries(context, queries, testName);

      // --- Query snapshot contracts (issue #166) ---
      checkQueryContracts(context, queries, identity.testId(), testClass, testName);

      // --- @DetectNPlusOne ---
      checkDetectNPlusOne(context, report, testName);

      // --- @QueryAudit failOnDetection ---
      if (config.isFailOnDetection() && report.hasConfirmedIssues()) {
        List<Issue> failableIssues = filterFailableIssues(report, context);
        if (!failableIssues.isEmpty()) {
          throw new AuditCheckFailure(buildFailureMessage(testName, failableIssues));
        }
      }
    } catch (AuditCheckFailure failure) {
      getOrCreateRunState(context).markPolicyFailed();
      throw failure;
    }
  }

  private static String buildTruncatedCaptureMessage(
      String testName, int maxQueries, QueryCaptureSnapshot capture) {
    return "QueryAudit: "
        + testName
        + " exceeded the query capture limit (maxQueries="
        + maxQueries
        + "). The audit is incomplete. Retained query count: "
        + capture.queries().size()
        + "; dropped query count: "
        + capture.droppedCount()
        + ". Increase query-audit.max-queries or reduce the test's query volume.";
  }

  // ── EXPLAIN-based analysis ───────────────────────────────────────

  QueryAuditReport runExplainAnalysis(
      ExtensionContext context,
      QueryAuditReport report,
      List<QueryRecord> queries,
      QueryAuditAnalyzer analyzer) {
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
          report = analyzer.mergeDetectedIssues(report, explainIssues);
          break;
        }
      }
    } catch (Exception e) {
      System.err.println("[QueryAudit] EXPLAIN analysis failed: " + e.getMessage());
    }

    return report;
  }

  // ── Query count regression detection ────────────────────────────────

  QueryAuditReport detectQueryCountRegression(
      ExtensionContext context,
      QueryAuditReport report,
      List<QueryRecord> queries,
      String testId,
      String testClass,
      String testName,
      QueryAuditAnalyzer analyzer) {

    QueryCounts current = QueryCounts.from(queries);

    Map<String, QueryCounts> currentCounts = getCurrentCounts(context);
    if (currentCounts != null) {
      currentCounts.put(QueryCountBaseline.key(testId), current);
    }

    Map<String, QueryCounts> countBaseline = getCountBaseline(context);
    if (countBaseline == null || countBaseline.isEmpty()) {
      return report;
    }

    trackLegacyIdentity(
        context, "count baseline", countBaseline, testId, testClass, testName, true);
    QueryCounts baselineCounts =
        QueryCountBaseline.find(countBaseline, testId, testClass, testName);

    List<Issue> regressionIssues =
        REGRESSION_DETECTOR.detect(testClass, testName, current, baselineCounts);
    return analyzer.mergeDetectedIssues(report, regressionIssues);
  }

  // ── AfterAllCallback ──────────────────────────────────────────────

  @Override
  public void afterAll(ExtensionContext context) {
    if (context.getRequiredTestClass().getEnclosingClass() != null
        && !ownsAuditResources(context)) {
      return;
    }

    // Release per-class resources so shared SessionFactory / reused worker threads don't leak
    // listeners or ThreadLocal holders across classes (issues #100, #101).
    LazyLoadTracker tracker = getLazyLoadTracker(context);
    Runnable hookCleanup =
        context.getStore(NAMESPACE).get(KEY_DATASOURCE_HOOK_CLEANUP, Runnable.class);
    closeAuditScope(context, tracker, hookCleanup);

    Object storedFinalizer =
        context.getRoot().getStore(NAMESPACE).get(ReportFinalizer.class.getName());
    if (storedFinalizer instanceof ReportFinalizer finalizer && shouldAutoOpenReport(context)) {
      finalizer.enableAutoOpen();
    }
  }

  void registerReportFinalizer(ExtensionContext context, QueryAuditConfig auditConfig) {
    AuditRunState runState = getOrCreateRunState(context);
    try {
      Path outputDirectory = resolveReportOutputDirectory(auditConfig);
      ReportFormat reportFormat = auditConfig.getReportFormat();
      ExtensionContext root = context.getRoot();
      ReportFinalizer finalizer =
          (ReportFinalizer)
              root.getStore(NAMESPACE)
                  .getOrComputeIfAbsent(
                      ReportFinalizer.class.getName(),
                      key -> new ReportFinalizer(this, outputDirectory, reportFormat, runState));
      finalizer.requireConfiguration(outputDirectory, reportFormat);
    } catch (RuntimeException | Error failure) {
      recordInitializationFailure(
          context,
          IncompleteReasonCode.AUDIT_INITIALIZATION_FAILED,
          "Could not configure suite reporting: " + failureMessage(failure));
      throw failure;
    }
  }

  private static AuditRunState getOrCreateRunState(ExtensionContext context) {
    return (AuditRunState)
        context
            .getRoot()
            .getStore(NAMESPACE)
            .getOrComputeIfAbsent(KEY_RUN_STATE, key -> new AuditRunState());
  }

  private static void markIncomplete(
      ExtensionContext context, IncompleteReasonCode code, String detail) {
    getOrCreateRunState(context).markIncomplete(new AuditIncompleteReason(code, detail));
  }

  private static void recordInitializationFailure(
      ExtensionContext context, IncompleteReasonCode code, String detail) {
    context.getStore(NAMESPACE).put(KEY_INITIALIZATION_FAILURE_RECORDED, Boolean.TRUE);
    markIncomplete(context, code, detail);
  }

  private static void recordUnexpectedInitializationFailure(
      ExtensionContext context, Throwable failure) {
    if (wasInitializationFailureRecorded(context)) {
      return;
    }
    recordInitializationFailure(
        context,
        IncompleteReasonCode.AUDIT_INITIALIZATION_FAILED,
        "Could not initialize QueryAudit for "
            + auditTarget(context)
            + ": "
            + failureMessage(failure));
  }

  private static boolean wasInitializationFailureRecorded(ExtensionContext context) {
    return Boolean.TRUE.equals(
        context.getStore(NAMESPACE).get(KEY_INITIALIZATION_FAILURE_RECORDED, Boolean.class));
  }

  private static String failureMessage(Throwable failure) {
    String message = failure.getMessage();
    return message == null || message.isBlank() ? failure.getClass().getSimpleName() : message;
  }

  private static Path resolveReportOutputDirectory(QueryAuditConfig config) {
    String configuredPath = config.getReportOutputDir();
    if (configuredPath == null || configuredPath.isBlank()) {
      throw new ExtensionConfigurationException(
          "QueryAudit: report output directory must not be blank. Configure"
              + " query-audit.report.output-dir with a valid directory.");
    }
    try {
      return Path.of(configuredPath).toAbsolutePath().normalize();
    } catch (InvalidPathException e) {
      throw new ExtensionConfigurationException(
          "QueryAudit: invalid report output directory '" + configuredPath + "'.", e);
    }
  }

  static final class AuditRunState {

    private final Set<AuditIncompleteReason> incompleteReasons = new LinkedHashSet<>();
    private boolean policyFailed;

    synchronized void markPolicyFailed() {
      policyFailed = true;
    }

    synchronized void markIncomplete(AuditIncompleteReason reason) {
      incompleteReasons.add(reason);
    }

    synchronized AuditRunResult result(List<QueryAuditReport> reports) {
      return AuditRunResult.determine(reports, policyFailed, incompleteReasons);
    }
  }

  private static final class AuditCheckFailure extends AssertionError {

    AuditCheckFailure(String message) {
      super(message);
    }
  }

  /**
   * Registered once in the root {@link ExtensionContext.Store} via {@code getOrComputeIfAbsent}.
   * JUnit calls {@link #close()} exactly once when the root context is torn down — after all test
   * classes have completed.
   */
  static final class ReportFinalizer implements ExtensionContext.Store.CloseableResource {

    private final QueryAuditExtension extension;
    private final Path outputDirectory;
    private final ReportFormat reportFormat;
    private final AuditRunState runState;
    private volatile boolean autoOpen;

    ReportFinalizer(
        QueryAuditExtension extension, Path outputDirectory, ReportFormat reportFormat) {
      this(extension, outputDirectory, reportFormat, new AuditRunState());
    }

    ReportFinalizer(
        QueryAuditExtension extension,
        Path outputDirectory,
        ReportFormat reportFormat,
        AuditRunState runState) {
      this.extension = extension;
      this.outputDirectory = outputDirectory.toAbsolutePath().normalize();
      this.reportFormat = reportFormat;
      this.runState = runState;
    }

    void requireConfiguration(Path requestedDirectory, ReportFormat requestedFormat) {
      Path normalizedRequest = requestedDirectory.toAbsolutePath().normalize();
      if (!outputDirectory.equals(normalizedRequest)) {
        throw new ExtensionConfigurationException(
            "QueryAudit: conflicting report output directories in the same test run: '"
                + outputDirectory
                + "' and '"
                + normalizedRequest
                + "'. Use one query-audit.report.output-dir value for all active test contexts.");
      }
      if (reportFormat != requestedFormat) {
        throw new ExtensionConfigurationException(
            "QueryAudit: conflicting report formats in the same test run: '"
                + reportFormat.name().toLowerCase(Locale.ROOT)
                + "' and '"
                + requestedFormat.name().toLowerCase(Locale.ROOT)
                + "'. Use one query-audit.report.format value for all active test contexts.");
      }
    }

    Path outputDirectory() {
      return outputDirectory;
    }

    ReportFormat reportFormat() {
      return reportFormat;
    }

    AuditRunResult result(List<QueryAuditReport> reports) {
      return runState.result(reports);
    }

    void enableAutoOpen() {
      this.autoOpen = true;
    }

    @Override
    public void close() {
      HtmlReportAggregator aggregator = HtmlReportAggregator.getInstance();
      AuditRunResult runResult = runState.result(aggregator.getReports());
      if (runResult.reports().isEmpty() && runResult.outcome() == AuditOutcome.PASS) {
        return;
      }

      Path reportPath = reportPath();
      try {
        switch (reportFormat) {
          case CONSOLE -> {
            // Per-test output and the suite summary are already on stdout.
          }
          case JSON -> extension.writeJsonReport(runResult, outputDirectory);
          case HTML -> {
            aggregator.writeReport(outputDirectory);
            System.out.println("[QueryAudit] file://" + reportPath.toAbsolutePath());
            if (autoOpen) {
              extension.openReportInBrowser(reportPath);
            }
          }
        }
      } catch (Exception e) {
        runState.markIncomplete(
            new AuditIncompleteReason(
                IncompleteReasonCode.REPORT_WRITE_FAILED,
                "Could not write the "
                    + reportFormat.name().toLowerCase(Locale.ROOT)
                    + " report to "
                    + reportPath.toAbsolutePath()));
        printSummary(runState.result(runResult.reports()));
        throw new ReportWriteException(reportFormat, reportPath, e);
      }
      printSummary(runResult);
    }

    private Path reportPath() {
      return switch (reportFormat) {
        case CONSOLE -> outputDirectory;
        case JSON -> outputDirectory.resolve("report.json");
        case HTML -> outputDirectory.resolve("index.html");
      };
    }

    private static void printSummary(AuditRunResult runResult) {
      List<QueryAuditReport> reports = runResult.reports();
      long totalErrors = reports.stream().mapToLong(report -> report.getErrors().size()).sum();
      long totalWarnings = reports.stream().mapToLong(report -> report.getWarnings().size()).sum();
      int totalQueries = reports.stream().mapToInt(QueryAuditReport::getTotalQueryCount).sum();

      String summary =
          "[QueryAudit] "
              + reports.size()
              + " tests, "
              + totalQueries
              + " queries"
              + (totalErrors > 0
                  ? ", " + totalErrors + " ERROR" + (totalErrors > 1 ? "S" : "")
                  : "")
              + (totalWarnings > 0
                  ? ", " + totalWarnings + " WARNING" + (totalWarnings > 1 ? "S" : "")
                  : "")
              + (totalErrors == 0 && totalWarnings == 0 && runResult.outcome() == AuditOutcome.PASS
                  ? " — all clean"
                  : "");
      System.out.println();
      System.out.println(summary);
      System.out.println("[QueryAudit] outcome: " + runResult.outcome());
      for (AuditIncompleteReason reason : runResult.incompleteReasons()) {
        String detail = reason.detail() == null ? "" : ": " + reason.detail();
        System.out.println("[QueryAudit] incomplete: " + reason.code() + detail);
      }
      System.out.println();
    }
  }

  // ── Connection-held-idle (issue #168) ──────────────────────────────

  /**
   * Flags connection checkouts whose held time exceeded their database-work time by more than the
   * configured threshold — the pool-exhaustion shape: a transaction holding its connection while
   * slow non-DB work (HTTP call, push send, file I/O) runs. Sessions never released by the end of
   * the test are the worst offenders and are flagged with their full held time.
   */
  QueryAuditReport mergeConnectionHeldIdleIssues(
      QueryAuditReport report, QueryInterceptor interceptor, QueryAuditAnalyzer analyzer) {
    QueryAuditConfig config = analyzer.getConfig();
    List<Issue> idleIssues = new ArrayList<>();
    for (ConnectionUsageTracker.ConnectionSession session :
        interceptor.getConnectionTracker().getCompletedSessions()) {
      long idleMillis = session.idleMillis();
      if (idleMillis < config.getConnectionHeldIdleThresholdMs()) {
        continue;
      }
      idleIssues.add(
          new Issue(
              IssueType.CONNECTION_HELD_IDLE,
              Severity.INFO,
              null,
              null,
              null,
              "Connection "
                  + session.connectionId()
                  + " held "
                  + session.heldMillis()
                  + "ms but executed database work for only "
                  + session.databaseWorkMillis()
                  + "ms ("
                  + idleMillis
                  + "ms idle"
                  + (session.released() ? "" : ", never released in the test window")
                  + ") — under load this shape exhausts the pool",
              "Release the connection before slow non-database work: move external calls (HTTP,"
                  + " push, file I/O) out of the transaction, or split the transaction around"
                  + " them",
              session.acquireCallSite()));
    }
    return analyzer.mergeDetectedIssues(report, idleIssues);
  }

  // ── Query snapshot contracts (issue #166) ─────────────────────────

  /**
   * Enforces the recorded query contract for this test, if one exists. Skipped in record mode (a
   * red suite must still be able to re-record) and when the method declares {@code @ExpectQueries}
   * — the inline budget is the more specific contract and wins.
   */
  private void checkQueryContracts(
      ExtensionContext context,
      List<QueryRecord> queries,
      String testId,
      String testClass,
      String testName) {
    if (isContractRecordMode()) {
      return;
    }
    Map<String, QueryCounts> contracts = getContracts(context);
    if (contracts == null || contracts.isEmpty()) {
      return;
    }
    Optional<Method> method = context.getTestMethod();
    boolean inlineContract =
        method.isPresent() && method.get().getAnnotation(ExpectQueries.class) != null;
    trackLegacyIdentity(
        context, "query contract", contracts, testId, testClass, testName, !inlineContract);
    if (inlineContract) {
      return;
    }
    String failure =
        QueryContracts.verify(
            testId, testClass, testName, QueryCounts.from(queries), contracts, queries);
    if (failure != null) {
      throw new AuditCheckFailure(failure);
    }
  }

  /** Merge-writes the accumulated per-test counts into the contracts file in record mode. */
  private void writeContractsIfRequested(ExtensionContext context) {
    if (!isContractRecordMode()) {
      return;
    }
    Map<String, QueryCounts> currentCounts = getCurrentCounts(context);
    if (currentCounts == null || currentCounts.isEmpty()) {
      return;
    }
    try {
      Path contractsPath = resolveContractsPath();
      Map<String, QueryCounts> merged = new LinkedHashMap<>(QueryCountBaseline.load(contractsPath));
      merged.putAll(currentCounts);
      QueryCountBaseline.save(contractsPath, merged, "QueryAudit Query Contracts");
      System.out.println(
          "[QueryAudit] Query contracts recorded: "
              + contractsPath.toAbsolutePath()
              + " ("
              + currentCounts.size()
              + " test(s))");
    } catch (Exception e) {
      System.err.println("[QueryAudit] Failed to write query contracts: " + e.getMessage());
    }
  }

  private static boolean isContractRecordMode() {
    return Boolean.parseBoolean(
        resolveSystemProperty(
            "queryAudit.contracts.record", "queryGuard.contracts.record", "false"));
  }

  private static Path resolveContractsPath() {
    String sysProp = resolveSystemProperty("queryAudit.contractsPath", "queryGuard.contractsPath");
    if (sysProp != null && !sysProp.isEmpty()) {
      return Path.of(sysProp);
    }
    return Path.of(QueryContracts.DEFAULT_FILE_NAME);
  }

  private static void trackLegacyIdentity(
      ExtensionContext context,
      String policy,
      Map<String, QueryCounts> entries,
      String testId,
      String testClass,
      String testName,
      boolean legacyFallbackEnabled) {
    if (!QueryCountBaseline.hasLegacyIdentity(entries, testClass, testName)) {
      return;
    }
    boolean usesLegacyIdentity =
        legacyFallbackEnabled
            && QueryCountBaseline.usesLegacyIdentity(entries, testId, testClass, testName);
    String warningKey = policy + "|" + testClass + "|" + testName;
    LegacyIdentityRegistry registry =
        claimLegacyIdentity(context, warningKey, testId, usesLegacyIdentity, testClass, testName);
    if (!usesLegacyIdentity) {
      return;
    }
    if (registry.markWarning(warningKey)) {
      System.err.println(
          "[QueryAudit] Legacy 0.5 "
              + policy
              + " entry matched "
              + testClass
              + "."
              + testName
              + " by display name. Re-record the file to migrate this test to its stable JUnit"
              + " ID; legacy entries cannot distinguish packages or duplicate display names.");
    }
  }

  static LegacyIdentityRegistry claimLegacyIdentity(
      ExtensionContext context,
      String claimKey,
      String testId,
      boolean usesLegacyIdentity,
      String testClass,
      String testName) {
    LegacyIdentityRegistry registry =
        context
            .getRoot()
            .getStore(NAMESPACE)
            .getOrComputeIfAbsent(
                KEY_LEGACY_IDENTITY_CLAIMS,
                ignored -> new LegacyIdentityRegistry(),
                LegacyIdentityRegistry.class);
    List<String> conflictingIds = registry.register(claimKey, testId, usesLegacyIdentity);
    if (!conflictingIds.isEmpty()) {
      throw new ExtensionConfigurationException(
          "QueryAudit: ambiguous 0.5 identity for "
              + testClass
              + "."
              + testName
              + ". Stable JUnit IDs "
              + String.join(", ", conflictingIds)
              + " match the same legacy entry while at least one test still depends on that"
              + " fallback. Re-record the policy file with QueryAudit"
              + " 0.6+.");
    }
    return registry;
  }

  static final class LegacyIdentityRegistry {

    private final Map<String, Map<String, Boolean>> claims = new HashMap<>();
    private final Set<String> warnings = new HashSet<>();

    synchronized List<String> register(String claimKey, String testId, boolean usesLegacyIdentity) {
      Map<String, Boolean> claimsForEntry =
          claims.computeIfAbsent(claimKey, ignored -> new LinkedHashMap<>());
      claimsForEntry.merge(testId, usesLegacyIdentity, Boolean::logicalOr);
      boolean fallbackIsAmbiguous =
          claimsForEntry.size() > 1 && claimsForEntry.containsValue(Boolean.TRUE);
      return fallbackIsAmbiguous ? List.copyOf(claimsForEntry.keySet()) : List.of();
    }

    synchronized boolean markWarning(String warningKey) {
      return warnings.add(warningKey);
    }
  }

  @SuppressWarnings("unchecked")
  private Map<String, QueryCounts> getContracts(ExtensionContext context) {
    ExtensionContext current = context;
    while (current != null) {
      Object obj = current.getStore(NAMESPACE).get(KEY_CONTRACTS);
      if (obj instanceof Map<?, ?> map) {
        return (Map<String, QueryCounts>) map;
      }
      current = current.getParent().orElse(null);
    }
    return null;
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
      throw new AuditCheckFailure(
          String.format(
              "QueryAudit: %s executed %d queries, expected at most %d.\n"
                  + "Tip: Check the Query Patterns section in the report above to identify which queries to optimize.",
              testName, actual, max));
    }
  }

  private void checkExpectQueries(
      ExtensionContext context, List<QueryRecord> queries, String testName) {
    ExpectQueries annotation = context.getRequiredTestMethod().getAnnotation(ExpectQueries.class);
    if (annotation == null) return;

    String failure = buildExpectQueriesFailureMessage(annotation, queries, testName);
    if (failure != null) {
      throw new AuditCheckFailure(failure);
    }
  }

  /**
   * Builds the failure message for {@link ExpectQueries}, or returns {@code null} when every
   * declared budget is respected. Package-private for testing.
   */
  static String buildExpectQueriesFailureMessage(
      ExpectQueries annotation, List<QueryRecord> queries, String testName) {
    StringBuilder violations = new StringBuilder();
    appendBudgetViolation(
        violations, "SELECT", annotation.select(), queries, SqlParser::isSelectQuery);
    appendBudgetViolation(
        violations, "INSERT", annotation.insert(), queries, SqlParser::isInsertQuery);
    appendBudgetViolation(
        violations, "UPDATE", annotation.update(), queries, SqlParser::isUpdateQuery);
    appendBudgetViolation(
        violations, "DELETE", annotation.delete(), queries, SqlParser::isDeleteQuery);

    if (violations.length() == 0) {
      return null;
    }
    return "QueryAudit: " + testName + " exceeded its query budget.\n" + violations;
  }

  /**
   * Appends one violation block when the given type exceeds its budget: a summary line followed by
   * every query of that type with its call site. A negative budget means "not verified".
   */
  private static void appendBudgetViolation(
      StringBuilder sb,
      String type,
      int max,
      List<QueryRecord> queries,
      Predicate<String> typeMatcher) {
    if (max < 0) {
      return;
    }
    List<QueryRecord> matched = queries.stream().filter(q -> typeMatcher.test(q.sql())).toList();
    if (matched.size() <= max) {
      return;
    }

    sb.append(String.format("%s: executed %d, expected at most %d.\n", type, matched.size(), max));
    for (QueryRecord query : matched) {
      String sql = query.sql();
      sb.append("  ").append(sql.length() > 100 ? sql.substring(0, 100) + "..." : sql);
      String callSite = firstStackFrame(query.stackTrace());
      if (callSite != null) {
        sb.append("\n    at ").append(callSite);
      }
      sb.append('\n');
    }
  }

  /** Returns the first (innermost application) frame of a recorded stack trace. */
  private static String firstStackFrame(String stackTrace) {
    if (stackTrace == null || stackTrace.isEmpty()) {
      return null;
    }
    int newline = stackTrace.indexOf('\n');
    return newline < 0 ? stackTrace : stackTrace.substring(0, newline);
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
      throw new AuditCheckFailure(sb.toString());
    }
  }

  // ── Audit activation (issue #163) ──────────────────────────────────

  /**
   * Decides whether this test class is audited. {@code @QueryAuditExclude} always wins. In {@link
   * AuditMode#ALL} every non-excluded class is audited; in {@link AuditMode#ANNOTATED} (the
   * default) only classes that opted in — via {@code @QueryAudit}, {@code @EnableQueryInspector},
   * one of the focused audit annotations, or a direct
   * {@code @ExtendWith(QueryAuditExtension.class)} — are. The annotated-mode check exists because
   * the ServiceLoader registration makes JUnit's extension autodetection register this extension
   * suite-wide; without it, merely enabling autodetection would audit everything.
   */
  private boolean computeActive(ExtensionContext context) {
    if (isClassExcluded(context.getRequiredTestClass())) {
      return false;
    }
    if (resolveAuditMode(context) == AuditMode.ALL) {
      return true;
    }
    return findAnnotation(context) != null
        || hasEnableQueryInspector(context)
        || hasFocusedAuditAnnotation(context)
        || hasDirectExtendWith(context);
  }

  /**
   * Per-callback gate: the class-level decision cached by {@code beforeAll}, plus method-level
   * {@code @QueryAuditExclude}. Falls back to computing when no cached flag exists — the case for
   * method-level {@code @QueryAudit}, where the extension is registered for the method only and
   * {@code beforeAll} never ran.
   */
  private boolean isAuditActive(ExtensionContext context) {
    Optional<Method> method = context.getTestMethod();
    if (method.isPresent() && method.get().isAnnotationPresent(QueryAuditExclude.class)) {
      return false;
    }
    if (isClassExcluded(context.getRequiredTestClass())) {
      return false;
    }

    // A method-level opt-in registers the extension too late for beforeAll. It must therefore opt
    // the method in even if suite-wide autodetection cached the otherwise plain class as false.
    if (method.isPresent() && isMethodLevelOptIn(method.get())) {
      ExtensionContext.Store methodStore = context.getStore(NAMESPACE);
      Boolean methodActive = methodStore.get(KEY_METHOD_ACTIVE, Boolean.class);
      if (methodActive != null) {
        return methodActive;
      }
      boolean active = buildConfig(context).isEnabled();
      methodStore.put(KEY_METHOD_ACTIVE, active);
      return active;
    }

    ExtensionContext current = context;
    while (current != null) {
      Boolean active = current.getStore(NAMESPACE).get(KEY_ACTIVE, Boolean.class);
      if (active != null) {
        return active;
      }
      current = current.getParent().orElse(null);
    }
    boolean active = computeActive(context);
    if (active) {
      active = buildConfig(context).isEnabled();
    }
    context.getStore(NAMESPACE).put(KEY_ACTIVE, active);
    return active;
  }

  private static boolean isClassExcluded(Class<?> testClass) {
    Class<?> clazz = testClass;
    while (clazz != null) {
      if (clazz.isAnnotationPresent(QueryAuditExclude.class)) {
        return true;
      }
      clazz = clazz.getEnclosingClass();
    }
    return false;
  }

  /**
   * Resolves the audit mode: the {@code queryAudit.mode} system property wins, then the Spring
   * {@code query-audit.mode} property (via the {@code QueryAuditConfig} bean), then the {@link
   * AuditMode#ANNOTATED} default.
   */
  private AuditMode resolveAuditMode(ExtensionContext context) {
    String sysProp = resolveSystemProperty("queryAudit.mode", "queryGuard.mode");
    if (sysProp != null && !sysProp.isBlank()) {
      return AuditMode.parse(sysProp);
    }
    QueryAuditConfig springConfig = resolveSpringConfig(context);
    if (springConfig != null) {
      return springConfig.getAuditMode();
    }
    return AuditMode.ANNOTATED;
  }

  private static boolean hasDirectExtendWith(ExtensionContext context) {
    Class<?> clazz = context.getRequiredTestClass();
    while (clazz != null) {
      for (ExtendWith extendWith : clazz.getAnnotationsByType(ExtendWith.class)) {
        for (Class<?> registered : extendWith.value()) {
          if (registered == QueryAuditExtension.class) {
            return true;
          }
        }
      }
      clazz = clazz.getEnclosingClass();
    }
    return false;
  }

  private static boolean hasFocusedAuditAnnotation(ExtensionContext context) {
    Optional<Method> method = context.getTestMethod();
    if (method.isPresent() && hasFocusedAuditAnnotation(method.get())) {
      return true;
    }

    Class<?> clazz = context.getRequiredTestClass();
    while (clazz != null) {
      if (clazz.isAnnotationPresent(DetectNPlusOne.class)) {
        return true;
      }
      clazz = clazz.getEnclosingClass();
    }
    return false;
  }

  private static boolean isMethodLevelOptIn(Method method) {
    return method.isAnnotationPresent(QueryAudit.class) || hasFocusedAuditAnnotation(method);
  }

  private static boolean hasFocusedAuditAnnotation(Method method) {
    return method.isAnnotationPresent(DetectNPlusOne.class)
        || method.isAnnotationPresent(ExpectQueries.class)
        || method.isAnnotationPresent(ExpectMaxQueryCount.class);
  }

  // ── Config building ────────────────────────────────────────────────

  private QueryAuditConfig buildConfig(ExtensionContext context) {
    // Layer 1: Start from Spring config (application.yml) if available, else hardcoded defaults
    QueryAuditConfig springConfig = resolveSpringConfig(context);
    QueryAuditConfig.Builder builder =
        springConfig != null
            ? QueryAuditConfig.Builder.from(springConfig)
            : QueryAuditConfig.builder();

    // Layer 2: @EnableQueryInspector override
    if (hasEnableQueryInspector(context)) {
      builder.failOnDetection(false);
    }

    // Layer 3: @QueryAudit annotation overrides (only explicitly specified values)
    QueryAudit annotation = findAnnotation(context);
    if (annotation != null) {
      // failOnDetection: only override when explicitly specified in the annotation
      if (annotation.failOnDetection().isSpecified()) {
        builder.failOnDetection(annotation.failOnDetection().toBoolean());
      }

      if (annotation.nPlusOneThreshold() >= 0) {
        builder.nPlusOneThreshold(annotation.nPlusOneThreshold());
      }

      for (String suppress : annotation.suppress()) {
        builder.addSuppressPattern(suppress);
      }

      if (!annotation.baselinePath().isEmpty()) {
        builder.baselinePath(annotation.baselinePath());
      }

      builder.includeSetupQueries(annotation.includeSetupQueries());
    }

    // Layer 4: @DetectNPlusOne override (highest priority for threshold)
    DetectNPlusOne detectNPlusOne = null;
    // getTestMethod() returns Optional.empty() in beforeAll (class-level context)
    Optional<Method> method = context.getTestMethod();
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

    String profile = System.getProperty("queryAudit.profile");
    if (profile != null) {
      builder.ruleProfile(RuleProfile.parse(profile));
    }

    String reportFormat =
        resolveSystemProperty("queryAudit.reportFormat", "queryGuard.reportFormat");
    if (reportFormat != null) {
      builder.reportFormat(ReportFormat.parse(reportFormat));
    }

    // Wire return type resolver if available
    RepositoryReturnTypeResolver resolver = getReturnTypeResolver(context);
    if (resolver != null) {
      builder.repositoryReturnTypeResolver(resolver);
    }

    return builder.build();
  }

  /**
   * Attempts to resolve a {@link QueryAuditConfig} bean from the Spring ApplicationContext via
   * reflection. Returns {@code null} if Spring is not on the classpath, the test does not use a
   * Spring context, or no {@code QueryAuditConfig} bean is registered.
   */
  private QueryAuditConfig resolveSpringConfig(ExtensionContext context) {
    try {
      Class<?> springExtensionClass =
          Class.forName("org.springframework.test.context.junit.jupiter.SpringExtension");
      Method getAppContext =
          springExtensionClass.getMethod("getApplicationContext", ExtensionContext.class);
      Object appContext = getAppContext.invoke(null, context);
      if (appContext != null) {
        Method getBean = appContext.getClass().getMethod("getBean", Class.class);
        Object bean = getBean.invoke(appContext, QueryAuditConfig.class);
        if (bean instanceof QueryAuditConfig config) {
          return config;
        }
      }
    } catch (Exception ignored) {
      // Spring not available, no context, or no QueryAuditConfig bean — fall back to defaults
    }
    return null;
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
    Optional<Method> testMethod = context.getTestMethod();
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

  /**
   * Returns the report view used by every generated format. Hiding INFO findings does not alter the
   * analysis result used for assertions, and the copy retains query statistics and index metadata.
   */
  static QueryAuditReport applyInfoVisibility(QueryAuditReport report, boolean showInfo) {
    List<Issue> infoIssues = report.getInfoIssues();
    if (showInfo || infoIssues == null || infoIssues.isEmpty()) {
      return report;
    }

    QueryAuditReport visibleReport =
        new QueryAuditReport(
            report.getTestClass(),
            report.getTestName(),
            report.getConfirmedIssues(),
            List.of(),
            report.getAcknowledgedIssues(),
            report.getAllQueries(),
            report.getUniquePatternCount(),
            report.getTotalQueryCount(),
            report.getTotalExecutionTimeNanos());
    return visibleReport
        .withTestIdentity(report.getTestId(), report.getTestSelector())
        .withIndexMetadata(report.getIndexMetadata());
  }

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

  void writeJsonReport(AuditRunResult runResult, Path outputDir) throws IOException {
    Path jsonPath = outputDir.resolve("report.json");
    Path temporaryPath = null;
    try {
      Files.createDirectories(outputDir);
      temporaryPath = Files.createTempFile(outputDir, ".report-", ".json.tmp");
      Files.writeString(
          temporaryPath, JsonReporter.toRunEnvelopeJson(runResult), StandardCharsets.UTF_8);
      moveJsonReportFile(temporaryPath, jsonPath);
      temporaryPath = null;
    } catch (IOException | RuntimeException failure) {
      deleteFailedReport(temporaryPath, failure);
      deleteFailedReport(jsonPath, failure);
      throw failure;
    }
    System.out.println("[QueryAudit] JSON report: " + jsonPath.toAbsolutePath());
  }

  void moveJsonReportFile(Path source, Path target) throws IOException {
    try {
      Files.move(
          source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    } catch (AtomicMoveNotSupportedException ignored) {
      Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
    }
  }

  private static void deleteFailedReport(Path path, Throwable failure) {
    if (path == null) {
      return;
    }
    try {
      Files.deleteIfExists(path);
    } catch (IOException | RuntimeException cleanupFailure) {
      failure.addSuppressed(cleanupFailure);
    }
  }

  // ── Baseline & report helpers ──────────────────────────────────────

  private Path resolveCountBaselinePath(ExtensionContext context) {
    String sysProp =
        resolveSystemProperty("queryAudit.countBaselinePath", "queryGuard.countBaselinePath");
    if (sysProp != null && !sysProp.isEmpty()) {
      return Path.of(sysProp);
    }
    return Path.of(QueryCountBaseline.DEFAULT_FILE_NAME);
  }

  @SuppressWarnings("unchecked")
  private void writeCountBaselineIfRequested(ExtensionContext context) {
    boolean updateBaseline =
        Boolean.parseBoolean(
            resolveSystemProperty(
                "queryAudit.updateBaseline", "queryGuard.updateBaseline", "false"));
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

    // Explicit annotation overrides CI detection
    QueryAudit annotation = findAnnotation(context);
    if (annotation != null && annotation.autoOpenReport().isSpecified()) {
      return annotation.autoOpenReport().toBoolean();
    }

    if (System.getenv("CI") != null
        || System.getenv("JENKINS_HOME") != null
        || System.getenv("GITHUB_ACTIONS") != null
        || System.getenv("GITLAB_CI") != null) {
      return false;
    }

    QueryAuditConfig springConfig = resolveSpringConfig(context);
    if (springConfig != null) {
      return springConfig.isAutoOpenReport();
    }

    // Default: auto-open when running locally (not in CI)
    return true;
  }

  private void openReportInBrowser(Path reportPath) {
    try {
      File reportFile = reportPath.toFile();
      if (!reportFile.exists()) {
        return;
      }

      if (Desktop.isDesktopSupported()) {
        Desktop desktop = Desktop.getDesktop();
        if (desktop.isSupported(Desktop.Action.BROWSE)) {
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

  // ── Spring context helper ──────────────────────────────────────────

  private Object resolveApplicationContext(ExtensionContext context) {
    try {
      Class<?> springExtensionClass =
          Class.forName("org.springframework.test.context.junit.jupiter.SpringExtension");
      Method getAppContext =
          springExtensionClass.getMethod("getApplicationContext", ExtensionContext.class);
      return getAppContext.invoke(null, context);
    } catch (Exception | NoClassDefFoundError ignored) {
      return null;
    }
  }

  // ── Store helpers ──────────────────────────────────────────────────

  private RepositoryReturnTypeResolver getReturnTypeResolver(ExtensionContext context) {
    ExtensionContext.Store store = context.getStore(NAMESPACE);
    Object resolver = store.get(KEY_RETURN_TYPE_RESOLVER);
    if (resolver instanceof RepositoryReturnTypeResolver r) {
      return r;
    }
    ExtensionContext parent = context.getParent().orElse(null);
    if (parent != null) {
      resolver = parent.getStore(NAMESPACE).get(KEY_RETURN_TYPE_RESOLVER);
      if (resolver instanceof RepositoryReturnTypeResolver r) {
        return r;
      }
    }
    return null;
  }

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

  /**
   * Returns the first non-null system property among the candidates, or {@code null} if none are
   * set. Used to honor both {@code queryAudit.*} (preferred) and the legacy {@code queryGuard.*}
   * prefix from the pre-rename days.
   */
  private static String resolveSystemProperty(String... keys) {
    for (String key : keys) {
      String v = System.getProperty(key);
      if (v != null) return v;
    }
    return null;
  }

  private static String resolveSystemProperty(String primary, String legacy, String defaultValue) {
    String v = resolveSystemProperty(primary, legacy);
    return v != null ? v : defaultValue;
  }
}
