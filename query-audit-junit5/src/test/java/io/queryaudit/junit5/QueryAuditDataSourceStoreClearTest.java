package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.queryaudit.core.interceptor.QueryInterceptor;
import java.util.Optional;
import javax.sql.DataSource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Regression for issue #100 — {@link QueryAuditDataSourceStore#clear()} must be invoked in
 * {@link QueryAuditExtension#afterAll(ExtensionContext)} so the ThreadLocal holder is released
 * instead of living for the lifetime of the Gradle test worker thread.
 */
@DisplayName("QueryAuditDataSourceStore.clear() is called on afterAll (issue #100)")
class QueryAuditDataSourceStoreClearTest {

  @Test
  @DisplayName("afterAll removes the ThreadLocal holder regardless of whether one was set")
  void afterAllClearsThreadLocalHolder() {
    // Pre-populate the ThreadLocal as beforeAll/hookInterceptor would.
    QueryAuditDataSourceStore.set(
        mock(DataSource.class), mock(DataSource.class), new QueryInterceptor());
    assertThat(QueryAuditDataSourceStore.get()).as("precondition: holder is present").isNotNull();

    QueryAuditExtension extension = new QueryAuditExtension();
    extension.afterAll(topLevelContext());

    assertThat(QueryAuditDataSourceStore.get())
        .as("afterAll must clear the ThreadLocal holder")
        .isNull();
  }

  @Test
  @DisplayName("afterAll on a @Nested inner class does not clear the outer-class holder")
  void nestedAfterAllLeavesOuterHolderIntact() {
    QueryAuditDataSourceStore.set(
        mock(DataSource.class), mock(DataSource.class), new QueryInterceptor());

    QueryAuditExtension extension = new QueryAuditExtension();
    extension.afterAll(nestedInnerContext());

    // The outer test class's afterAll is what clears; nested inner classes must not touch it.
    assertThat(QueryAuditDataSourceStore.get()).isNotNull();
    QueryAuditDataSourceStore.clear();
  }

  @SuppressWarnings("unchecked")
  private static ExtensionContext topLevelContext() {
    ExtensionContext ctx = mock(ExtensionContext.class);
    when(ctx.getRequiredTestClass()).thenReturn((Class) String.class); // no enclosing class
    ExtensionContext root = mock(ExtensionContext.class);
    when(ctx.getRoot()).thenReturn(root);
    when(ctx.getTestMethod()).thenReturn(Optional.empty());
    ExtensionContext.Store store = mock(ExtensionContext.Store.class);
    when(ctx.getStore(org.mockito.ArgumentMatchers.any(ExtensionContext.Namespace.class)))
        .thenReturn(store);
    when(root.getStore(org.mockito.ArgumentMatchers.any(ExtensionContext.Namespace.class)))
        .thenReturn(store);
    when(store.getOrComputeIfAbsent(
            org.mockito.ArgumentMatchers.anyString(),
            org.mockito.ArgumentMatchers.any()))
        .thenAnswer(inv -> mock(QueryAuditExtension.ReportFinalizer.class));
    return ctx;
  }

  @SuppressWarnings("unchecked")
  private static ExtensionContext nestedInnerContext() {
    ExtensionContext ctx = mock(ExtensionContext.class);
    // InnerClass has enclosing (this test class) — triggers the early-return guard.
    when(ctx.getRequiredTestClass()).thenReturn((Class) InnerClass.class);
    return ctx;
  }

  class InnerClass {}
}
