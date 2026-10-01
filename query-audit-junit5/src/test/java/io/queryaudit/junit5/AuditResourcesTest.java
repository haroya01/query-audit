package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import io.queryaudit.core.interceptor.LazyLoadTracker;
import io.queryaudit.core.interceptor.QueryInterceptor;
import io.queryaudit.core.model.LifecyclePhase;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;

class AuditResourcesTest {
  @Test
  void cleanupIsOwnedOrderedAndIdempotentWithoutClosingTheApplicationDataSource() {
    List<String> released = new ArrayList<>();
    DataSource dataSource = mock(DataSource.class);
    AuditResources resources =
        new AuditResources(
            "class:one",
            new QueryInterceptor(),
            dataSource,
            () -> released.add("hook"),
            () -> released.add("holder"));
    resources.attachTracker(new LazyLoadTracker(), () -> released.add("tracker"));

    assertThat(resources.isOwnedBy("class:one")).isTrue();
    assertThat(resources.isOwnedBy("class:two")).isFalse();
    assertThat(resources.dataSource()).isSameAs(dataSource);
    assertThat(resources.close(null)).isNull();
    assertThat(resources.close(null)).isNull();

    assertThat(released).containsExactly("tracker", "hook", "holder");
    verifyNoInteractions(dataSource);
  }

  @Test
  void everyCleanupRunsAndFailuresAreSuppressedInReleaseOrder() {
    RuntimeException trackerFailure = new IllegalStateException("tracker");
    Error hookFailure = new AssertionError("hook");
    RuntimeException holderFailure = new IllegalStateException("holder");
    AuditResources resources =
        new AuditResources(
            "class:one",
            null,
            null,
            () -> {
              throw hookFailure;
            },
            () -> {
              throw holderFailure;
            });
    resources.attachTracker(
        new LazyLoadTracker(),
        () -> {
          throw trackerFailure;
        });

    assertThat(resources.close(null)).isSameAs(trackerFailure);
    assertThat(trackerFailure.getSuppressed()).containsExactly(hookFailure, holderFailure);
    assertThat(resources.isClosed()).isTrue();
  }

  @Test
  void partialInitializationRetainsTheOriginalFailureAndCanCloseWithoutATracker() {
    RuntimeException initializationFailure = new IllegalArgumentException("initialization");
    RuntimeException cleanupFailure = new IllegalStateException("hook");
    List<String> released = new ArrayList<>();
    AuditResources resources =
        new AuditResources(
            "method:one",
            null,
            null,
            () -> {
              throw cleanupFailure;
            },
            () -> released.add("holder"));

    assertThat(resources.close(initializationFailure)).isSameAs(initializationFailure);
    assertThat(initializationFailure.getSuppressed()).containsExactly(cleanupFailure);
    assertThat(released).containsExactly("holder");
  }

  @Test
  void cleanupDoesNotSuppressAFailureOntoItself() {
    RuntimeException original = new IllegalStateException("shared");
    assertThat(
            AuditResources.runCleanup(
                original,
                () -> {
                  throw original;
                }))
        .isSameAs(original);
    assertThat(original.getSuppressed()).isEmpty();
  }

  @Test
  void captureLifecycleMovesTheInterceptorAndTrackerTogether() {
    QueryInterceptor interceptor = mock(QueryInterceptor.class);
    LazyLoadTracker tracker = mock(LazyLoadTracker.class);
    AuditResources resources = new AuditResources("method:one", interceptor, null, null, () -> {});
    resources.attachTracker(tracker, () -> {});

    resources.startCapture();
    resources.stopCapture();

    var order = inOrder(interceptor, tracker);
    order.verify(interceptor).start();
    order.verify(interceptor).setPhase(LifecyclePhase.SETUP);
    order.verify(tracker).start();
    order.verify(interceptor).stop();
    order.verify(tracker).stop();
    order.verify(interceptor).snapshot();
  }
}
