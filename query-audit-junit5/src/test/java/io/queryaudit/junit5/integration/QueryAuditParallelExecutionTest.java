package io.queryaudit.junit5.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.queryaudit.core.interceptor.QueryInterceptor;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.regression.QueryCountBaseline;
import io.queryaudit.core.regression.QueryCounts;
import io.queryaudit.core.reporter.HtmlReportAggregator;
import io.queryaudit.junit5.BooleanOverride;
import io.queryaudit.junit5.QueryAudit;
import io.queryaudit.junit5.QueryAuditExtension;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import net.ttddyy.dsproxy.ExecutionInfo;
import net.ttddyy.dsproxy.QueryInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.parallel.ExecutionMode;

@DisplayName("QueryAudit parallel execution (issue #190)")
class QueryAuditParallelExecutionTest {

  private static final ExtensionContext.Namespace STORE =
      ExtensionContext.Namespace.create(QueryAuditExtension.class);

  @BeforeEach
  void setUp() {
    HtmlReportAggregator.getInstance().reset();
  }

  @AfterEach
  void tearDown() {
    HtmlReportAggregator.getInstance().reset();
  }

  @Test
  void concurrentMethodCannotInterruptAnActiveCapture() throws Exception {
    QueryAuditExtension extension = new QueryAuditExtension();
    QueryInterceptor interceptor = new QueryInterceptor();
    Map<String, QueryCounts> currentCounts = new ConcurrentHashMap<>();
    StubExtensionContext classContext = classContext(interceptor, currentCounts);
    MethodContext first = new MethodContext(classContext, "firstMethod", ExecutionMode.SAME_THREAD);
    MethodContext second =
        new MethodContext(classContext, "secondMethod", ExecutionMode.CONCURRENT);

    extension.beforeEach(first);
    record(interceptor, "SELECT 1");

    assertThatThrownBy(() -> extension.beforeEach(second))
        .isInstanceOf(ExtensionConfigurationException.class)
        .hasMessageContaining(AuditedFixture.class.getName() + "#secondMethod")
        .hasMessageContaining("@Execution(SAME_THREAD)")
        .hasMessageContaining("junit.jupiter.execution.parallel.mode.default=same_thread");
    assertThat(interceptor.isActive()).isTrue();
    assertThat(interceptor.snapshot().queries())
        .extracting(QueryRecord::sql)
        .containsExactly("SELECT 1");

    assertThatCode(() -> extension.afterEach(second)).doesNotThrowAnyException();
    record(interceptor, "SELECT 2");
    assertThat(interceptor.isActive()).isTrue();

    extension.afterEach(first);

    assertThat(interceptor.isActive()).isFalse();
    assertThat(currentCounts)
        .containsOnly(
            Map.entry(
                QueryCountBaseline.key(
                    QueryAuditParallelExecutionTest.class.getSimpleName(), "firstMethod()"),
                new QueryCounts(2, 0, 0, 0, 2)));
    assertThat(HtmlReportAggregator.getInstance().getReports())
        .singleElement()
        .satisfies(
            report -> {
              assertThat(report.getTestName()).isEqualTo("firstMethod()");
              assertThat(report.getAllQueries())
                  .extracting(QueryRecord::sql)
                  .containsExactly("SELECT 1", "SELECT 2");
            });
  }

  private static StubExtensionContext classContext(
      QueryInterceptor interceptor, Map<String, QueryCounts> currentCounts) {
    StubExtensionContext context = new StubExtensionContext(AuditedFixture.class);
    ExtensionContext.Store store = context.getStore(STORE);
    store.put("auditActive", Boolean.TRUE);
    store.put("interceptor", interceptor);
    store.put("currentCounts", currentCounts);
    store.put("countBaseline", Map.of());
    store.put("queryContracts", Map.of());
    return context;
  }

  private static void record(QueryInterceptor interceptor, String sql) {
    ExecutionInfo execution = new ExecutionInfo();
    execution.setElapsedTime(1L);
    interceptor.afterQuery(execution, List.of(new QueryInfo(sql)));
  }

  @QueryAudit(failOnDetection = BooleanOverride.FALSE)
  private static final class AuditedFixture {
    void firstMethod() {}

    void secondMethod() {}
  }

  private static final class MethodContext extends StubExtensionContext {
    private final StubExtensionContext parent;
    private final Method method;
    private final ExecutionMode executionMode;

    MethodContext(StubExtensionContext parent, String methodName, ExecutionMode executionMode)
        throws NoSuchMethodException {
      super(AuditedFixture.class);
      this.parent = parent;
      this.method = AuditedFixture.class.getDeclaredMethod(methodName);
      this.executionMode = executionMode;
    }

    @Override
    public Optional<Method> getTestMethod() {
      return Optional.of(method);
    }

    @Override
    public Optional<ExtensionContext> getParent() {
      return Optional.of(parent);
    }

    @Override
    public ExtensionContext getRoot() {
      return parent;
    }

    @Override
    public String getUniqueId() {
      return parent.getUniqueId() + "/" + method.getName();
    }

    @Override
    public String getDisplayName() {
      return method.getName() + "()";
    }

    @Override
    public ExecutionMode getExecutionMode() {
      return executionMode;
    }
  }
}
