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
import io.queryaudit.core.model.IncompleteReasonCode;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.regression.QueryCountBaseline;
import io.queryaudit.core.regression.QueryCounts;
import io.queryaudit.core.reporter.HtmlReportAggregator;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import net.ttddyy.dsproxy.ExecutionInfo;
import net.ttddyy.dsproxy.QueryInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;

@DisplayName("QueryAuditExtension — truncated query capture (issue #186)")
class QueryAuditExtensionTruncatedCaptureTest {

  private static final ExtensionContext.Namespace NAMESPACE =
      ExtensionContext.Namespace.create(QueryAuditExtension.class);
  private static final String TEST_ID =
      "[engine:junit-jupiter]/[class:io.queryaudit.junit5.QueryAuditExtensionTruncatedCaptureTest]/[method:auditedMethod()]";

  @BeforeEach
  void setUp() {
    HtmlReportAggregator.getInstance().reset();
  }

  @AfterEach
  void tearDown() {
    HtmlReportAggregator.getInstance().reset();
  }

  @Test
  void truncatedCapturePreservesPartialFindingsAndSkipsCountContracts() throws Exception {
    AuditFixture fixture = auditFixture(1, "UPDATE orders SET status = 'cancelled'", "SELECT 2");

    assertThatThrownBy(() -> new QueryAuditExtension().afterEach(fixture.context()))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("auditedMethod()")
        .hasMessageContaining("maxQueries=1")
        .hasMessageContaining("audit is incomplete")
        .hasMessageContaining("Retained query count: 1")
        .hasMessageContaining("dropped query count: 1")
        .hasMessageContaining("query-audit.max-queries");

    assertThat(fixture.interceptor().isActive()).isFalse();
    assertThat(fixture.currentCounts()).isEmpty();
    assertThat(HtmlReportAggregator.getInstance().getReports())
        .singleElement()
        .satisfies(
            report -> {
              assertThat(report.getTotalQueryCount()).isEqualTo(1);
              assertThat(report.getConfirmedIssues())
                  .extracting(issue -> issue.type())
                  .contains(IssueType.UPDATE_WITHOUT_WHERE);
            });
    assertThat(runState(fixture.context()).result(HtmlReportAggregator.getInstance().getReports()))
        .satisfies(
            result -> {
              assertThat(result.outcome()).isEqualTo(AuditOutcome.INCONCLUSIVE);
              assertThat(result.incompleteReasons())
                  .extracting(reason -> reason.code())
                  .containsExactly(IncompleteReasonCode.QUERY_LIMIT_REACHED);
            });
  }

  @Test
  void captureAtTheExactLimitContinuesThroughTheAudit() throws Exception {
    AuditFixture fixture = auditFixture(1, "SELECT 1");

    assertThatCode(() -> new QueryAuditExtension().afterEach(fixture.context()))
        .doesNotThrowAnyException();

    assertThat(fixture.currentCounts())
        .containsEntry(contractKey(), new QueryCounts(1, 0, 0, 0, 1));
    assertThat(HtmlReportAggregator.getInstance().getReports())
        .singleElement()
        .satisfies(report -> assertThat(report.getTotalQueryCount()).isEqualTo(1));
  }

  private static AuditFixture auditFixture(int maxQueries, String... sqlStatements)
      throws Exception {
    QueryInterceptor interceptor = new QueryInterceptor();
    interceptor.setMaxQueries(maxQueries);
    interceptor.start();
    ExecutionInfo execution = new ExecutionInfo();
    execution.setElapsedTime(1L);
    interceptor.afterQuery(execution, Arrays.stream(sqlStatements).map(QueryInfo::new).toList());

    MapStore methodStore = new MapStore();
    MapStore classStore = new MapStore();
    Map<String, QueryCounts> currentCounts = new ConcurrentHashMap<>();
    classStore.put("auditActive", Boolean.TRUE);
    classStore.put("interceptor", interceptor);
    classStore.put("currentCounts", currentCounts);
    classStore.put("queryContracts", Map.of(contractKey(), new QueryCounts(1, 0, 0, 0, 1)));

    ExtensionContext classContext = mock(ExtensionContext.class);
    when(classContext.getStore(any(ExtensionContext.Namespace.class))).thenReturn(classStore);
    when(classContext.getParent()).thenReturn(Optional.empty());

    MapStore rootStore = new MapStore();
    ExtensionContext rootContext = mock(ExtensionContext.class);
    when(rootContext.getStore(any(ExtensionContext.Namespace.class))).thenReturn(rootStore);
    when(rootContext.getRoot()).thenReturn(rootContext);
    when(classContext.getRoot()).thenReturn(rootContext);

    Method testMethod =
        QueryAuditExtensionTruncatedCaptureTest.class.getDeclaredMethod("auditedMethod");
    ExtensionContext methodContext = mock(ExtensionContext.class);
    when(methodContext.getStore(any(ExtensionContext.Namespace.class))).thenReturn(methodStore);
    when(methodContext.getParent()).thenReturn(Optional.of(classContext));
    when(methodContext.getRoot()).thenReturn(rootContext);
    doReturn(QueryAuditExtensionTruncatedCaptureTest.class)
        .when(methodContext)
        .getRequiredTestClass();
    when(methodContext.getRequiredTestMethod()).thenReturn(testMethod);
    when(methodContext.getTestMethod()).thenReturn(Optional.of(testMethod));
    when(methodContext.getDisplayName()).thenReturn("auditedMethod()");
    when(methodContext.getUniqueId()).thenReturn(TEST_ID);
    return new AuditFixture(methodContext, interceptor, currentCounts);
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

  @QueryAudit(failOnDetection = BooleanOverride.FALSE)
  @SuppressWarnings("unused")
  private void auditedMethod() {}

  private record AuditFixture(
      ExtensionContext context,
      QueryInterceptor interceptor,
      Map<String, QueryCounts> currentCounts) {}

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
