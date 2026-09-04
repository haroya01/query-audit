package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.detector.QueryAuditAnalyzer;
import io.queryaudit.core.interceptor.QueryInterceptor;
import io.queryaudit.core.model.AuditOutcome;
import io.queryaudit.core.model.IncompleteReasonCode;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.provenance.AuditCapability;
import io.queryaudit.core.regression.QueryCounts;
import io.queryaudit.core.reporter.HtmlReportAggregator;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import javax.sql.DataSource;
import net.ttddyy.dsproxy.ExecutionInfo;
import net.ttddyy.dsproxy.QueryInfo;
import net.ttddyy.dsproxy.support.ProxyDataSource;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
class QueryAuditExtensionRunOutcomeTest {

  private static final ExtensionContext.Namespace NAMESPACE =
      ExtensionContext.Namespace.create(QueryAuditExtension.class);

  private String contractsPath;
  private String reportFormat;

  @BeforeEach
  void setUp() {
    contractsPath = System.getProperty("queryAudit.contractsPath");
    reportFormat = System.getProperty("queryAudit.reportFormat");
    System.clearProperty("queryAudit.contractsPath");
    System.clearProperty("queryAudit.reportFormat");
    HtmlReportAggregator.getInstance().reset();
    QueryAuditDataSourceStore.clear();
  }

  @AfterEach
  void tearDown() {
    restoreProperty("queryAudit.contractsPath", contractsPath);
    restoreProperty("queryAudit.reportFormat", reportFormat);
    HtmlReportAggregator.getInstance().reset();
    QueryAuditDataSourceStore.clear();
  }

  @Test
  void unavailableDataSourceMakesTheRootRunInconclusive() throws Exception {
    AuditContext fixture = contextFor(MissingDataSourceFixture.class, "audited", null);

    assertThatThrownBy(() -> new QueryAuditExtension().beforeEach(fixture.methodContext()))
        .isInstanceOf(ExtensionConfigurationException.class)
        .hasMessageContaining("DataSource unavailable");

    assertIncomplete(fixture, IncompleteReasonCode.DATASOURCE_UNAVAILABLE);
  }

  @Test
  void unreadableContractsMakeTheRootRunInconclusive(@TempDir Path tempDir) throws Exception {
    Path malformedContracts = tempDir.resolve("contracts");
    Files.writeString(malformedContracts, "OrderServiceTest | loadsOrders | invalid\n");
    System.setProperty("queryAudit.contractsPath", malformedContracts.toString());
    AuditContext fixture = contextFor(PolicyFixture.class, "audited", null);

    assertThatThrownBy(() -> new QueryAuditExtension().beforeEach(fixture.methodContext()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(malformedContracts.toAbsolutePath().toString());

    assertIncomplete(fixture, IncompleteReasonCode.CONTRACT_UNREADABLE);
  }

  @Test
  void dataSourceHookFailureMakesTheRootRunInconclusive() throws Exception {
    AuditContext fixture = contextFor(FinalDataSourceFixture.class, "audited", null);

    assertThatThrownBy(() -> new QueryAuditExtension().beforeEach(fixture.methodContext()))
        .isInstanceOf(ExtensionConfigurationException.class)
        .hasMessageContaining("is final");

    assertIncomplete(fixture, IncompleteReasonCode.AUDIT_INITIALIZATION_FAILED);
  }

  @Test
  void unexpectedConfigurationFailureMakesTheRootRunInconclusive() throws Exception {
    System.setProperty("queryAudit.reportFormat", "unsupported");
    AuditContext fixture = contextFor(PolicyFixture.class, "audited", null);

    assertThatThrownBy(() -> new QueryAuditExtension().beforeEach(fixture.methodContext()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unknown query-audit report format");

    assertIncomplete(fixture, IncompleteReasonCode.AUDIT_INITIALIZATION_FAILED);
  }

  @Test
  void postHookInitializationFailureRestoresTheDataSourceAndClearsState() throws Exception {
    DataSource originalDataSource = mock(DataSource.class);
    PostHookFailureFixture.DATA_SOURCE = originalDataSource;
    IndexMetadataCollector metadataCollector = mock(IndexMetadataCollector.class);
    when(metadataCollector.collectWithCapabilities(any(DataSource.class)))
        .thenThrow(new ServiceConfigurationError("broken metadata provider"));
    QueryAuditExtension extension =
        new QueryAuditExtension(
            new DataSourceResolver(), metadataCollector, new HibernateIntegration());
    AuditContext fixture = contextFor(PostHookFailureFixture.class, "audited", null);

    try {
      assertThatThrownBy(() -> extension.beforeEach(fixture.methodContext()))
          .isInstanceOf(ServiceConfigurationError.class)
          .hasMessageContaining("broken metadata provider");

      assertThat(PostHookFailureFixture.DATA_SOURCE).isSameAs(originalDataSource);
      assertThat(QueryAuditDataSourceStore.get()).isNull();
      assertThat(fixture.methodStore().get("interceptor")).isNull();
      assertThat(fixture.methodStore().get("dataSourceHookCleanup")).isNull();
      assertIncomplete(fixture, IncompleteReasonCode.AUDIT_INITIALIZATION_FAILED);
    } finally {
      PostHookFailureFixture.DATA_SOURCE = originalDataSource;
      QueryAuditDataSourceStore.clear();
    }
  }

  @Test
  void failedEnabledMetadataMakesAStandaloneRunInconclusive() throws Exception {
    JdbcDataSource dataSource = new JdbcDataSource();
    dataSource.setURL("jdbc:h2:mem:capability-failure");
    PostHookFailureFixture.DATA_SOURCE = dataSource;
    IndexMetadataCollector collector = mock(IndexMetadataCollector.class);
    when(collector.collectWithCapabilities(any(DataSource.class)))
        .thenReturn(
            new IndexMetadataCollector.Result(
                null, "h2", AuditCapability.failed("test-index-provider"), "SQLException"));
    QueryAuditExtension extension =
        new QueryAuditExtension(new DataSourceResolver(), collector, new HibernateIntegration());
    AuditContext fixture = contextFor(PostHookFailureFixture.class, "audited", null);
    try {
      extension.beforeEach(fixture.methodContext());
      assertIncomplete(fixture, IncompleteReasonCode.CAPABILITY_INITIALIZATION_FAILED);
    } finally {
      PostHookFailureFixture.DATA_SOURCE = dataSource;
    }
  }

  @Test
  void aCompleteAuditCapturesItsEffectiveInputsByStableTestId() throws Exception {
    JdbcDataSource dataSource = new JdbcDataSource();
    dataSource.setURL("jdbc:h2:mem:capability-success");
    PostHookFailureFixture.DATA_SOURCE = dataSource;
    QueryAuditExtension extension = new QueryAuditExtension();
    AuditContext fixture = contextFor(PostHookFailureFixture.class, "audited", null);
    try {
      extension.beforeEach(fixture.methodContext());
      extension.afterEach(fixture.methodContext());
      var result = runState(fixture).result(HtmlReportAggregator.getInstance().getReports());
      assertThat(result.outcome()).isEqualTo(AuditOutcome.PASS);
      assertThat(result.comparisonInputs()).containsKey(fixture.methodContext().getUniqueId());
      var inputs = result.comparisonInputs().get(fixture.methodContext().getUniqueId());
      assertThat(inputs.profile()).isEqualTo("recommended");
      assertThat(inputs.databaseDialect()).isEqualTo("h2");
      assertThat(inputs.parserName()).isEqualTo("JSqlParser");
      assertThat(inputs.capabilities().explain().state()).isEqualTo(AuditCapability.State.ABSENT);
    } finally {
      PostHookFailureFixture.DATA_SOURCE = dataSource;
    }
  }

  @Test
  void effectiveFailOnSelectionIsFingerprintableAndOrderIndependent() throws Exception {
    Map<String, String> fingerprints = new java.util.HashMap<>();
    for (String method :
        List.of("classPolicy", "reorderedPolicy", "restrictedPolicy", "allFindings")) {
      JdbcDataSource dataSource = new JdbcDataSource();
      dataSource.setURL("jdbc:h2:mem:fail-on-inputs");
      FailureSelectionFixture.DATA_SOURCE = dataSource;
      AuditContext fixture = contextFor(FailureSelectionFixture.class, method, null);
      QueryAuditExtension extension = new QueryAuditExtension();
      try {
        extension.beforeEach(fixture.methodContext());
        extension.afterEach(fixture.methodContext());
        fingerprints.put(
            method,
            runState(fixture)
                .result(List.of())
                .comparisonInputs()
                .get(fixture.methodContext().getUniqueId())
                .fingerprints()
                .queryContracts());
      } finally {
        FailureSelectionFixture.DATA_SOURCE = dataSource;
      }
    }
    assertThat(fingerprints.get("classPolicy")).isEqualTo(fingerprints.get("reorderedPolicy"));
    assertThat(fingerprints.get("restrictedPolicy")).isNotEqualTo(fingerprints.get("classPolicy"));
    assertThat(fingerprints.get("allFindings")).isNotEqualTo(fingerprints.get("classPolicy"));
  }

  @Test
  void failedExplainMakesAStandaloneRunInconclusiveWithoutLeakingItsCause() throws Exception {
    DataSource dataSource = mock(DataSource.class);
    Connection connection = mock(Connection.class);
    DatabaseMetaData metadata = mock(DatabaseMetaData.class);
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.getMetaData()).thenReturn(metadata);
    when(metadata.getDatabaseProductName()).thenReturn("Policy Test DB");
    AuditContext fixture = contextFor(PolicyFixture.class, "audited", null);
    fixture.classStore().put("dataSource", dataSource);
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of());
    List<QueryRecord> queries = List.of(new QueryRecord("SELECT private_marker", 0, 0, ""));
    QueryAuditReport report = analyzer.analyze("PolicyFixture", "audited", queries, null);

    new QueryAuditExtension()
        .runExplainAnalysis(fixture.methodContext(), report, queries, analyzer);

    assertIncomplete(fixture, IncompleteReasonCode.CAPABILITY_EXECUTION_FAILED);
    assertThat(runState(fixture).result(List.of()).incompleteReasons())
        .allSatisfy(reason -> assertThat(reason.detail()).doesNotContain("private_marker"));
  }

  @Test
  void concurrentExecutionMakesTheRootRunInconclusive() throws Exception {
    AuditContext fixture = contextFor(MissingDataSourceFixture.class, "audited", null);
    when(fixture.methodContext().getExecutionMode()).thenReturn(ExecutionMode.CONCURRENT);

    assertThatThrownBy(() -> new QueryAuditExtension().beforeEach(fixture.methodContext()))
        .isInstanceOf(ExtensionConfigurationException.class)
        .hasMessageContaining("concurrent execution");

    assertIncomplete(fixture, IncompleteReasonCode.AUDIT_INITIALIZATION_FAILED);
  }

  @Test
  void analysisFailureMakesTheRootRunInconclusive() throws Exception {
    QueryInterceptor interceptor = new QueryInterceptor();
    interceptor.start();
    AuditContext fixture = contextFor(InvalidBaselineFixture.class, "audited", interceptor);

    assertThatThrownBy(() -> new QueryAuditExtension().afterEach(fixture.methodContext()))
        .isInstanceOf(InvalidPathException.class);

    assertIncomplete(fixture, IncompleteReasonCode.AUDIT_ANALYSIS_FAILED);
  }

  @Test
  void cleanupFailureMakesTheRootRunInconclusive() throws Exception {
    AuditContext fixture = contextFor(PolicyFixture.class, "audited", null);
    fixture.classStore().put("auditResourceOwner", fixture.classContext().getUniqueId());
    fixture
        .classStore()
        .put(
            "dataSourceHookCleanup",
            (Runnable)
                () -> {
                  throw new IllegalStateException("broken cleanup");
                });

    assertThatThrownBy(() -> new QueryAuditExtension().afterAll(fixture.classContext()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("broken cleanup");

    assertIncomplete(fixture, IncompleteReasonCode.AUDIT_ANALYSIS_FAILED);
  }

  @Test
  void failOnDetectionMarksACompletedRunAsFailed() throws Exception {
    QueryInterceptor interceptor = new QueryInterceptor();
    interceptor.start();
    ExecutionInfo execution = new ExecutionInfo();
    execution.setElapsedTime(1L);
    interceptor.afterQuery(
        execution, List.of(new QueryInfo("UPDATE outcome_items SET name = 'updated'")));
    AuditContext fixture = contextFor(FailOnDetectionFixture.class, "audited", interceptor);

    assertThatThrownBy(() -> new QueryAuditExtension().afterEach(fixture.methodContext()))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("QueryAudit detected");

    assertThat(runState(fixture).result(HtmlReportAggregator.getInstance().getReports()).outcome())
        .isEqualTo(AuditOutcome.FAIL);
  }

  @Test
  void nonEnforcingFindingsRemainVisibleInAPassingRun() throws Exception {
    QueryInterceptor interceptor = new QueryInterceptor();
    interceptor.start();
    ExecutionInfo execution = new ExecutionInfo();
    execution.setElapsedTime(1L);
    interceptor.afterQuery(
        execution, List.of(new QueryInfo("UPDATE outcome_items SET name = 'updated'")));
    AuditContext fixture = contextFor(ReportOnlyFixture.class, "audited", interceptor);

    new QueryAuditExtension().afterEach(fixture.methodContext());

    assertThat(HtmlReportAggregator.getInstance().getReports())
        .singleElement()
        .satisfies(report -> assertThat(report.getConfirmedIssues()).isNotEmpty());
    assertThat(runState(fixture).result(HtmlReportAggregator.getInstance().getReports()).outcome())
        .isEqualTo(AuditOutcome.PASS);
  }

  private static void assertIncomplete(AuditContext fixture, IncompleteReasonCode expectedCode) {
    assertThat(runState(fixture).result(HtmlReportAggregator.getInstance().getReports()))
        .satisfies(
            result -> {
              assertThat(result.outcome()).isEqualTo(AuditOutcome.INCONCLUSIVE);
              assertThat(result.incompleteReasons())
                  .extracting(reason -> reason.code())
                  .containsExactly(expectedCode);
            });
  }

  private static QueryAuditExtension.AuditRunState runState(AuditContext fixture) {
    return (QueryAuditExtension.AuditRunState)
        fixture.rootStore().get(QueryAuditExtension.AuditRunState.class.getName());
  }

  private static AuditContext contextFor(
      Class<?> testClass, String methodName, QueryInterceptor interceptor) throws Exception {
    MapStore rootStore = new MapStore();
    rootStore.put(
        QueryAuditExtension.AuditRunState.class.getName(), new QueryAuditExtension.AuditRunState());
    MapStore classStore = new MapStore();
    MapStore methodStore = new MapStore();
    classStore.put("auditActive", Boolean.TRUE);
    if (interceptor != null) {
      classStore.put("interceptor", interceptor);
    }
    classStore.put("currentCounts", new ConcurrentHashMap<String, QueryCounts>());
    classStore.put("queryContracts", Map.of());

    ExtensionContext rootContext = mock(ExtensionContext.class);
    when(rootContext.getStore(any(ExtensionContext.Namespace.class))).thenReturn(rootStore);
    when(rootContext.getRoot()).thenReturn(rootContext);
    when(rootContext.getParent()).thenReturn(Optional.empty());

    ExtensionContext classContext = mock(ExtensionContext.class);
    when(classContext.getStore(any(ExtensionContext.Namespace.class))).thenReturn(classStore);
    when(classContext.getRoot()).thenReturn(rootContext);
    when(classContext.getParent()).thenReturn(Optional.of(rootContext));
    doReturn(testClass).when(classContext).getRequiredTestClass();
    when(classContext.getTestMethod()).thenReturn(Optional.empty());
    when(classContext.getUniqueId())
        .thenReturn("[engine:junit-jupiter]/[class:" + testClass.getName() + "]");

    Method method = testClass.getDeclaredMethod(methodName);
    ExtensionContext methodContext = mock(ExtensionContext.class);
    when(methodContext.getStore(any(ExtensionContext.Namespace.class))).thenReturn(methodStore);
    when(methodContext.getRoot()).thenReturn(rootContext);
    when(methodContext.getParent()).thenReturn(Optional.of(classContext));
    doReturn(testClass).when(methodContext).getRequiredTestClass();
    when(methodContext.getRequiredTestMethod()).thenReturn(method);
    when(methodContext.getTestMethod()).thenReturn(Optional.of(method));
    when(methodContext.getDisplayName()).thenReturn(methodName + "()");
    when(methodContext.getUniqueId())
        .thenReturn(
            "[engine:junit-jupiter]/[class:"
                + testClass.getName()
                + "]/[method:"
                + methodName
                + "()]");
    when(methodContext.getExecutionMode()).thenReturn(ExecutionMode.SAME_THREAD);
    return new AuditContext(classContext, methodContext, rootStore, classStore, methodStore);
  }

  private static void restoreProperty(String key, String value) {
    if (value == null) {
      System.clearProperty(key);
    } else {
      System.setProperty(key, value);
    }
  }

  @QueryAudit
  static class MissingDataSourceFixture {
    void audited() {}
  }

  @QueryAudit
  static class PolicyFixture {
    static final DataSource DATA_SOURCE = new ProxyDataSource(mock(DataSource.class));

    void audited() {}
  }

  @QueryAudit
  static class FinalDataSourceFixture {
    static final DataSource DATA_SOURCE = mock(DataSource.class);

    void audited() {}
  }

  @QueryAudit
  static class PostHookFailureFixture {
    static DataSource DATA_SOURCE;

    void audited() {}
  }

  @QueryAudit(failOn = {IssueType.N_PLUS_ONE, IssueType.UPDATE_WITHOUT_WHERE})
  static class FailureSelectionFixture {
    static DataSource DATA_SOURCE;

    void classPolicy() {}

    @QueryAudit(failOn = {IssueType.UPDATE_WITHOUT_WHERE, IssueType.N_PLUS_ONE})
    void reorderedPolicy() {}

    @QueryAudit(failOn = IssueType.N_PLUS_ONE)
    void restrictedPolicy() {}

    @QueryAudit
    void allFindings() {}
  }

  @QueryAudit(baselinePath = "\0")
  static class InvalidBaselineFixture {
    void audited() {}
  }

  @QueryAudit
  static class FailOnDetectionFixture {
    void audited() {}
  }

  @QueryAudit(failOnDetection = BooleanOverride.FALSE)
  static class ReportOnlyFixture {
    void audited() {}
  }

  private record AuditContext(
      ExtensionContext classContext,
      ExtensionContext methodContext,
      MapStore rootStore,
      MapStore classStore,
      MapStore methodStore) {}

  private static final class MapStore implements ExtensionContext.Store {
    private final Map<Object, Object> values = new ConcurrentHashMap<>();

    @Override
    public Object get(Object key) {
      return values.get(key);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V> V get(Object key, Class<V> requiredType) {
      return (V) values.get(key);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> Object getOrComputeIfAbsent(K key, Function<K, V> defaultCreator) {
      return values.computeIfAbsent(key, ignored -> defaultCreator.apply((K) key));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> V getOrComputeIfAbsent(
        K key, Function<K, V> defaultCreator, Class<V> requiredType) {
      return (V) values.computeIfAbsent(key, ignored -> defaultCreator.apply((K) key));
    }

    @Override
    public void put(Object key, Object value) {
      values.put(key, value);
    }

    @Override
    public Object remove(Object key) {
      return values.remove(key);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V> V remove(Object key, Class<V> requiredType) {
      return (V) values.remove(key);
    }
  }
}
