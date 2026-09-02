package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.queryaudit.core.interceptor.QueryInterceptor;
import io.queryaudit.core.model.AuditOutcome;
import io.queryaudit.core.model.IncompleteReasonCode;
import io.queryaudit.core.regression.QueryCounts;
import io.queryaudit.core.reporter.HtmlReportAggregator;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
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
    when(metadataCollector.collect(any(DataSource.class)))
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

    Method method = testClass.getDeclaredMethod(methodName);
    ExtensionContext methodContext = mock(ExtensionContext.class);
    when(methodContext.getStore(any(ExtensionContext.Namespace.class))).thenReturn(methodStore);
    when(methodContext.getRoot()).thenReturn(rootContext);
    when(methodContext.getParent()).thenReturn(Optional.of(classContext));
    doReturn(testClass).when(methodContext).getRequiredTestClass();
    when(methodContext.getRequiredTestMethod()).thenReturn(method);
    when(methodContext.getTestMethod()).thenReturn(Optional.of(method));
    when(methodContext.getDisplayName()).thenReturn(methodName + "()");
    when(methodContext.getExecutionMode()).thenReturn(ExecutionMode.SAME_THREAD);
    return new AuditContext(methodContext, rootStore, methodStore);
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
      ExtensionContext methodContext, MapStore rootStore, MapStore methodStore) {}

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
