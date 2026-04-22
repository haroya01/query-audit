package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.reporter.HtmlReportAggregator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;

@DisplayName("QueryAuditExtension — afterAll report finalization (issue #41)")
class QueryAuditExtensionAfterAllTest {

  private static final ExtensionContext.Namespace NAMESPACE =
      ExtensionContext.Namespace.create(QueryAuditExtension.class);

  @BeforeEach
  void setUp() {
    HtmlReportAggregator.getInstance().reset();
  }

  // ── Helpers ──────────────────────────────────────────────────────

  private static QueryAuditReport dummyReport(String testClass, String testName) {
    Issue issue =
        new Issue(
            IssueType.N_PLUS_ONE,
            Severity.ERROR,
            "SELECT * FROM orders WHERE user_id = ?",
            "orders",
            null,
            "Repeated query detected",
            "Use JOIN FETCH");
    return new QueryAuditReport(
        testClass, testName, List.of(issue), List.of(), List.of(), List.of(), 1, 5, 100_000L);
  }

  /**
   * Creates a mock ExtensionContext for a top-level test class. Uses a real Class object to avoid
   * Mockito's inability to mock Class.
   */
  @SuppressWarnings("unchecked")
  private static ExtensionContext mockContext(
      Class<?> testClass, ExtensionContext root, ExtensionContext.Store rootStore) {
    ExtensionContext ctx = mock(ExtensionContext.class);

    when(ctx.getRequiredTestClass()).thenReturn((Class) testClass);
    when(ctx.getRoot()).thenReturn(root);
    when(ctx.getTestMethod()).thenReturn(Optional.empty());
    when(ctx.getParent()).thenReturn(Optional.of(root));

    ExtensionContext.Store classStore = mock(ExtensionContext.Store.class);
    when(ctx.getStore(NAMESPACE)).thenReturn(classStore);

    return ctx;
  }

  /**
   * Creates a store backed by a real ConcurrentHashMap so that getOrComputeIfAbsent behaves
   * correctly across multiple calls.
   */
  private static ExtensionContext.Store createRootStore() {
    Map<Object, Object> backingMap = new ConcurrentHashMap<>();

    ExtensionContext.Store store = mock(ExtensionContext.Store.class);

    when(store.getOrComputeIfAbsent(anyString(), any()))
        .thenAnswer(
            invocation -> {
              String key = invocation.getArgument(0);
              Function<Object, Object> factory = invocation.getArgument(1);
              return backingMap.computeIfAbsent(key, factory);
            });

    when(store.get(anyString())).thenAnswer(inv -> backingMap.get(inv.getArgument(0)));

    return store;
  }

  // ── Tests ────────────────────────────────────────────────────────

  @Nested
  @DisplayName("ReportFinalizer is registered once via getOrComputeIfAbsent")
  class FinalizerRegistration {

    @Test
    @DisplayName("multiple afterAll calls register only one ReportFinalizer in root store")
    void multipleAfterAllCalls_registerOneFinalizer() {
      ExtensionContext.Store rootStore = createRootStore();
      ExtensionContext root = mock(ExtensionContext.class);
      when(root.getStore(NAMESPACE)).thenReturn(rootStore);

      QueryAuditExtension extension = new QueryAuditExtension();

      // Use top-level JDK classes (getEnclosingClass() == null) to pass the nested-class guard
      Class<?>[] fakeClasses = {String.class, Integer.class, Long.class};
      String[] classNames = {"String", "Integer", "Long"};

      for (int i = 0; i < fakeClasses.length; i++) {
        ExtensionContext ctx = mockContext(fakeClasses[i], root, rootStore);
        HtmlReportAggregator.getInstance().addReport(dummyReport(classNames[i], "test1"));
        extension.afterAll(ctx);
      }

      // getOrComputeIfAbsent called 3 times (once per afterAll)
      verify(rootStore, times(3))
          .getOrComputeIfAbsent(eq(QueryAuditExtension.ReportFinalizer.class.getName()), any());

      // But only one ReportFinalizer instance exists
      Object finalizer = rootStore.get(QueryAuditExtension.ReportFinalizer.class.getName());
      assertThat(finalizer)
          .as("Only one ReportFinalizer should be registered")
          .isInstanceOf(QueryAuditExtension.ReportFinalizer.class);
    }
  }

  @Nested
  @DisplayName("ReportFinalizer.close() writes report exactly once")
  class FinalizerClose {

    @Test
    @DisplayName("close() writes complete report with all accumulated data")
    void closeWritesCompleteReport() throws Exception {
      HtmlReportAggregator aggregator = HtmlReportAggregator.getInstance();
      aggregator.addReport(dummyReport("ClassA", "test1"));
      aggregator.addReport(dummyReport("ClassB", "test2"));
      aggregator.addReport(dummyReport("ClassC", "test3"));

      QueryAuditExtension extension = new QueryAuditExtension();
      QueryAuditExtension.ReportFinalizer finalizer =
          new QueryAuditExtension.ReportFinalizer(extension);

      finalizer.close();

      assertThat(aggregator.getReports()).hasSize(3);
    }

    @Test
    @DisplayName("close() does nothing when no reports accumulated")
    void closeWithNoReports_doesNothing() throws Exception {
      QueryAuditExtension extension = new QueryAuditExtension();
      QueryAuditExtension.ReportFinalizer finalizer =
          new QueryAuditExtension.ReportFinalizer(extension);

      finalizer.close();

      assertThat(HtmlReportAggregator.getInstance().getReports()).isEmpty();
    }
  }

  @Nested
  @DisplayName("Nested test classes skip afterAll report logic")
  class NestedClassHandling {

    // A real inner class (has enclosing class)
    class InnerTestClass {}

    @Test
    @SuppressWarnings("unchecked")
    @DisplayName("afterAll returns early for @Nested inner classes")
    void nestedClassSkipsReportFinalization() {
      ExtensionContext.Store rootStore = createRootStore();
      ExtensionContext root = mock(ExtensionContext.class);
      when(root.getStore(NAMESPACE)).thenReturn(rootStore);

      QueryAuditExtension extension = new QueryAuditExtension();

      ExtensionContext ctx = mock(ExtensionContext.class);
      when(ctx.getRequiredTestClass()).thenReturn((Class) InnerTestClass.class);

      HtmlReportAggregator.getInstance().addReport(dummyReport("Outer", "test1"));

      extension.afterAll(ctx);

      // Root store should NOT have been accessed — no finalizer registered
      verify(ctx, never()).getRoot();
    }
  }
}
