package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.queryaudit.core.interceptor.QueryInterceptor;
import io.queryaudit.core.model.AuditOutcome;
import io.queryaudit.core.regression.QueryCountBaseline;
import io.queryaudit.core.regression.QueryCounts;
import io.queryaudit.core.reporter.HtmlReportAggregator;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

@DisplayName("QueryAuditExtension — zero-query contracts (issue #182)")
@ResourceLock(Resources.SYSTEM_PROPERTIES)
class QueryAuditExtensionZeroQueryContractTest {

  private static final String TEST_ID =
      "[engine:junit-jupiter]/[class:io.queryaudit.junit5.QueryAuditExtensionZeroQueryContractTest]/[method:auditedMethod()]";

  private static final ExtensionContext.Namespace NAMESPACE =
      ExtensionContext.Namespace.create(QueryAuditExtension.class);

  private String recordMode;
  private String legacyRecordMode;

  @BeforeEach
  void setUp() {
    recordMode = System.getProperty("queryAudit.contracts.record");
    legacyRecordMode = System.getProperty("queryGuard.contracts.record");
    System.clearProperty("queryAudit.contracts.record");
    System.clearProperty("queryGuard.contracts.record");
    HtmlReportAggregator.getInstance().reset();
  }

  @AfterEach
  void tearDown() {
    restoreProperty("queryAudit.contracts.record", recordMode);
    restoreProperty("queryGuard.contracts.record", legacyRecordMode);
    HtmlReportAggregator.getInstance().reset();
  }

  @Test
  @DisplayName("a recorded non-zero contract fails when the test executes no SQL")
  void nonZeroContractFailsForZeroQueries() throws Exception {
    QueryCounts expected = new QueryCounts(1, 0, 0, 0, 1);
    ContractContext fixture = contextWithContract(expected);

    assertThatThrownBy(() -> new QueryAuditExtension().afterEach(fixture.context()))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("deviates from its recorded query contract")
        .hasMessageContaining("SELECT: contract 1, executed 0");

    assertZeroQueryExecutionWasRecorded(fixture);
    assertThat(
            runState(fixture.context())
                .result(HtmlReportAggregator.getInstance().getReports())
                .outcome())
        .isEqualTo(AuditOutcome.FAIL);
  }

  @Test
  @DisplayName("a recorded zero-query contract still passes")
  @ResourceLock(Resources.SYSTEM_OUT)
  void zeroContractPassesForZeroQueries() throws Exception {
    QueryCounts expected = new QueryCounts(0, 0, 0, 0, 0);
    ContractContext fixture = contextWithContract(expected);

    ByteArrayOutputStream output = new ByteArrayOutputStream();
    PrintStream originalOut = System.out;
    try (PrintStream console = new PrintStream(output, true, StandardCharsets.UTF_8)) {
      System.setOut(console);
      assertThatCode(() -> new QueryAuditExtension().afterEach(fixture.context()))
          .doesNotThrowAnyException();
    } finally {
      System.setOut(originalOut);
    }
    assertThat(output.toString(StandardCharsets.UTF_8))
        .contains("[QueryAudit] Rule profile: recommended");

    assertZeroQueryExecutionWasRecorded(fixture);
    assertThat(
            runState(fixture.context())
                .result(HtmlReportAggregator.getInstance().getReports())
                .outcome())
        .isEqualTo(AuditOutcome.PASS);
  }

  private static void assertZeroQueryExecutionWasRecorded(ContractContext fixture) {
    assertThat(HtmlReportAggregator.getInstance().getReports())
        .singleElement()
        .satisfies(
            report -> {
              assertThat(report.getTestId()).isEqualTo(TEST_ID);
              assertThat(report.getTestSelector().type()).isEqualTo("junit-unique-id");
              assertThat(report.getTestSelector().value()).isEqualTo(TEST_ID);
              assertThat(report.getTestName()).isEqualTo("auditedMethod()");
              assertThat(report.getTotalQueryCount()).isZero();
            });
    assertThat(fixture.currentCounts())
        .containsEntry(contractKey(), new QueryCounts(0, 0, 0, 0, 0));
  }

  private static ContractContext contextWithContract(QueryCounts expected) throws Exception {
    MapStore methodStore = new MapStore();
    MapStore classStore = new MapStore();
    Map<String, QueryCounts> currentCounts = new ConcurrentHashMap<>();
    classStore.put("auditActive", Boolean.TRUE);
    classStore.put("interceptor", new QueryInterceptor());
    classStore.put("currentCounts", currentCounts);
    classStore.put("queryContracts", Map.of(contractKey(), expected));

    ExtensionContext classContext = mock(ExtensionContext.class);
    when(classContext.getStore(any(ExtensionContext.Namespace.class))).thenReturn(classStore);
    when(classContext.getParent()).thenReturn(Optional.empty());

    MapStore rootStore = new MapStore();
    rootStore.put(
        QueryAuditExtension.AuditRunState.class.getName(), new QueryAuditExtension.AuditRunState());
    ExtensionContext rootContext = mock(ExtensionContext.class);
    when(rootContext.getStore(any(ExtensionContext.Namespace.class))).thenReturn(rootStore);
    when(rootContext.getRoot()).thenReturn(rootContext);
    when(classContext.getRoot()).thenReturn(rootContext);

    Method testMethod =
        QueryAuditExtensionZeroQueryContractTest.class.getDeclaredMethod("auditedMethod");
    ExtensionContext methodContext = mock(ExtensionContext.class);
    when(methodContext.getStore(any(ExtensionContext.Namespace.class))).thenReturn(methodStore);
    when(methodContext.getParent()).thenReturn(Optional.of(classContext));
    when(methodContext.getRoot()).thenReturn(rootContext);
    doReturn(QueryAuditExtensionZeroQueryContractTest.class)
        .when(methodContext)
        .getRequiredTestClass();
    when(methodContext.getRequiredTestMethod()).thenReturn(testMethod);
    when(methodContext.getTestMethod()).thenReturn(Optional.of(testMethod));
    when(methodContext.getDisplayName()).thenReturn("auditedMethod()");
    when(methodContext.getUniqueId()).thenReturn(TEST_ID);
    return new ContractContext(methodContext, currentCounts);
  }

  private static String contractKey() {
    return QueryCountBaseline.key(TEST_ID);
  }

  private static QueryAuditExtension.AuditRunState runState(ExtensionContext context) {
    return (QueryAuditExtension.AuditRunState)
        context
            .getRoot()
            .getStore(NAMESPACE)
            .get(QueryAuditExtension.AuditRunState.class.getName());
  }

  private static void restoreProperty(String key, String value) {
    if (value == null) {
      System.clearProperty(key);
    } else {
      System.setProperty(key, value);
    }
  }

  @SuppressWarnings("unused")
  private void auditedMethod() {}

  private record ContractContext(
      ExtensionContext context, Map<String, QueryCounts> currentCounts) {}

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
