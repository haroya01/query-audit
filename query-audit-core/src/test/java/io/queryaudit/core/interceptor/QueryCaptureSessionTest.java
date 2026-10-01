package io.queryaudit.core.interceptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.queryaudit.core.model.LifecyclePhase;
import io.queryaudit.core.model.QueryRecord;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import net.ttddyy.dsproxy.ConnectionInfo;
import net.ttddyy.dsproxy.ExecutionInfo;
import net.ttddyy.dsproxy.QueryInfo;
import net.ttddyy.dsproxy.listener.MethodExecutionContext;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class QueryCaptureSessionTest {
  private final QueryInterceptor router = new QueryInterceptor();
  private final LazyLoadTracker lazyRouter = new LazyLoadTracker();

  @Test
  void concurrentInvocationsHaveIndependentQueriesPhasesLimitsAndLazyEvents() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CyclicBarrier barrier = new CyclicBarrier(2);
    try {
      Future<QueryCaptureSession> first = executor.submit(() -> capture("first", barrier));
      Future<QueryCaptureSession> second = executor.submit(() -> capture("second", barrier));
      for (QueryCaptureSession session :
          List.of(first.get(10, TimeUnit.SECONDS), second.get(10, TimeUnit.SECONDS))) {
        assertThat(session.stop().queries())
            .extracting(QueryRecord::sql)
            .containsExactly(
                "select '" + session.testId() + " setup'",
                "select '" + session.testId() + " test'");
        assertThat(session.stop().queries())
            .extracting(QueryRecord::phase)
            .containsExactly(LifecyclePhase.SETUP, LifecyclePhase.TEST);
        assertThat(session.stop().droppedCount()).isEqualTo(1);
        assertThat(session.tracker().getRecords())
            .extracting(LazyLoadTracker.LazyLoadRecord::ownerIdString)
            .containsExactly(session.testId());
        assertThat(session.tracker().getExplicitLoads())
            .extracting(LazyLoadTracker.ExplicitLoadRecord::idString)
            .containsExactly(session.testId());
        assertThat(session.incompleteReasons()).isEmpty();
      }
    } finally {
      executor.shutdownNow();
    }
  }

  private QueryCaptureSession capture(String id, CyclicBarrier barrier) throws Exception {
    try (QueryCaptureSession session = QueryCaptureSession.open(router, lazyRouter, id, 2)) {
      barrier.await(5, TimeUnit.SECONDS);
      session.setPhase(LifecyclePhase.SETUP);
      fire(router, "select '" + id + " setup'");
      lazyRouter.recordCollectionInitialized("Entity.children", "Entity", id);
      session.setPhase(LifecyclePhase.TEST);
      fire(router, "select '" + id + " test'");
      lazyRouter.recordExplicitLoad("Entity", id, "callsite");
      session.setPhase(LifecyclePhase.TEARDOWN);
      fire(router, "select '" + id + " teardown'");
      barrier.await(5, TimeUnit.SECONDS);
      return session;
    }
  }

  @Test
  void sharedDatasourceListenersRecordOnlyTheInnermostOwnerOnce() {
    QueryInterceptor otherRouter = new QueryInterceptor();
    LazyLoadTracker otherLazyRouter = new LazyLoadTracker();
    try (QueryCaptureSession outer = QueryCaptureSession.open(router, lazyRouter, "outer", 10)) {
      fireBoth(router, otherRouter, "select 'outer before'");
      try (QueryCaptureSession inner =
          QueryCaptureSession.open(otherRouter, otherLazyRouter, "inner", 10)) {
        fireBoth(router, otherRouter, "select 'inner'");
        lazyRouter.recordProxyResolved("Entity", "not-outer");
        otherLazyRouter.recordProxyResolved("Entity", "inner");
        assertThat(inner.stop().queries())
            .extracting(QueryRecord::sql)
            .containsExactly("select 'inner'");
        assertThat(inner.tracker().getRecords()).hasSize(1);
      }
      fireBoth(router, otherRouter, "select 'outer after'");
      assertThat(outer.stop().queries())
          .extracting(QueryRecord::sql)
          .containsExactly("select 'outer before'", "select 'outer after'");
      assertThat(outer.tracker().getRecords()).isEmpty();
      assertThat(outer.incompleteReasons()).isEmpty();
    }
  }

  @Test
  void nestedCaptureOnTheSameRouterRestoresItsParent() {
    try (QueryCaptureSession outer = QueryCaptureSession.open(router, null, "outer", 10)) {
      fire(router, "select 1");
      try (QueryCaptureSession inner = QueryCaptureSession.open(router, null, "inner", 10)) {
        fire(router, "select 2");
        assertThat(inner.stop().queries()).extracting(QueryRecord::sql).containsExactly("select 2");
      }
      fire(router, "select 3");
      assertThat(outer.stop().queries())
          .extracting(QueryRecord::sql)
          .containsExactly("select 1", "select 3");
    }
  }

  @Test
  void queryOwnerAndPhaseArePinnedBeforeExecution() {
    ExecutionInfo execution = new ExecutionInfo();
    List<QueryInfo> query = List.of(new QueryInfo("select pinned"));
    try (QueryCaptureSession outer = QueryCaptureSession.open(router, null, "outer", 10)) {
      outer.setPhase(LifecyclePhase.SETUP);
      router.beforeQuery(execution, query);
      outer.setPhase(LifecyclePhase.TEST);
      try (QueryCaptureSession inner = QueryCaptureSession.open(router, null, "inner", 10)) {
        router.afterQuery(execution, query);
        assertThat(inner.stop().queries()).isEmpty();
      }
      assertThat(outer.stop().queries())
          .extracting(QueryRecord::phase)
          .containsExactly(LifecyclePhase.SETUP);
    }
  }

  @Test
  void queryBeginningOutsideCaptureIsNotAssignedToANewerCapture() {
    ExecutionInfo execution = new ExecutionInfo();
    List<QueryInfo> query = List.of(new QueryInfo("select earlier"));
    router.beforeQuery(execution, query);
    try (QueryCaptureSession session = QueryCaptureSession.open(router, null, "new", 10)) {
      router.afterQuery(execution, query);
      assertThat(session.stop().queries()).isEmpty();
    }
  }

  @Test
  void anInFlightQueryCannotBeReassignedAfterItsOwnerStops() {
    ExecutionInfo execution = new ExecutionInfo();
    List<QueryInfo> query = List.of(new QueryInfo("select private_marker"));
    try (QueryCaptureSession first = QueryCaptureSession.open(router, null, "first", 10)) {
      router.beforeQuery(execution, query);
      first.stop();
      try (QueryCaptureSession second = QueryCaptureSession.open(router, null, "second", 10)) {
        router.afterQuery(execution, query);
        assertThat(second.stop().queries()).isEmpty();
        assertThat(second.incompleteReasons()).isEmpty();
      }
      assertThat(first.incompleteReasons())
          .containsExactly("QUERY_COMPLETED_AFTER_CAPTURE", "QUERY_STILL_RUNNING");
      assertThatThrownBy(() -> first.incompleteReasons().clear())
          .isInstanceOf(UnsupportedOperationException.class);
    }
  }

  @Test
  void lazyLoadsBeyondTheCaptureLimitMakeTheCaptureIncomplete() {
    try (QueryCaptureSession session = QueryCaptureSession.open(router, lazyRouter, "owner", 2)) {
      for (int id = 0; id < 3; id++) {
        lazyRouter.recordProxyResolved("Entity", id);
      }
      session.stop();
      assertThat(session.tracker().getRecords()).hasSize(2);
      assertThat(session.incompleteReasons()).containsExactly("LAZY_LOAD_LIMIT_REACHED");
    }
  }

  @Test
  void unboundWorkersAreIncompleteInsteadOfSilentlyPassing() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try (QueryCaptureSession session = QueryCaptureSession.open(router, lazyRouter, "owner", 10)) {
      executor
          .submit(
              () -> {
                fire(router, "select private_marker");
                lazyRouter.recordProxyResolved("PrivateEntity", "private_marker");
              })
          .get(5, TimeUnit.SECONDS);
      assertThat(session.stop().queries()).isEmpty();
      assertThat(session.tracker().getRecords()).isEmpty();
      assertThat(session.incompleteReasons())
          .containsExactly("UNATTRIBUTED_LAZY_LOAD", "UNATTRIBUTED_QUERY");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void aSoleAdoptingSessionCountsUnboundWorkers() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try (QueryCaptureSession session = QueryCaptureSession.open(router, lazyRouter, "owner", 10)) {
      session.adoptUnboundWork();
      executor
          .submit(
              () -> {
                fire(router, "select background");
                lazyRouter.recordProxyResolved("Entity", 1);
              })
          .get(5, TimeUnit.SECONDS);
      assertThat(session.stop().queries())
          .extracting(QueryRecord::sql)
          .containsExactly("select background");
      assertThat(session.tracker().getRecords()).hasSize(1);
      assertThat(session.incompleteReasons()).isEmpty();
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void unboundWorkStaysUnattributedWhileAnotherSessionIsActive() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try (QueryCaptureSession adopting = QueryCaptureSession.open(router, null, "adopting", 10)) {
      adopting.adoptUnboundWork();
      try (QueryCaptureSession other = QueryCaptureSession.open(router, null, "other", 10)) {
        executor.submit(() -> fire(router, "select background")).get(5, TimeUnit.SECONDS);
        assertThat(other.stop().queries()).isEmpty();
        assertThat(other.incompleteReasons()).containsExactly("UNATTRIBUTED_QUERY");
      }
      assertThat(adopting.stop().queries()).isEmpty();
      assertThat(adopting.incompleteReasons()).containsExactly("UNATTRIBUTED_QUERY");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void runnableAndCallablePropagationRestoreTheReusedWorker() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try (QueryCaptureSession session = QueryCaptureSession.open(router, lazyRouter, "owner", 10)) {
      executor
          .submit(
              QueryCaptureSession.wrap(
                  (Runnable)
                      () -> {
                        fire(router, "select async");
                        lazyRouter.recordProxyResolved("Entity", 1);
                      }))
          .get(5, TimeUnit.SECONDS);
      assertThat(
              executor
                  .submit(QueryCaptureSession.wrap((Callable<String>) () -> "answer"))
                  .get(5, TimeUnit.SECONDS))
          .isEqualTo("answer");
      executor.submit(() -> fire(router, "select unbound")).get(5, TimeUnit.SECONDS);
      assertThat(session.stop().queries())
          .extracting(QueryRecord::sql)
          .containsExactly("select async");
      assertThat(session.tracker().getRecords()).hasSize(1);
      assertThat(session.incompleteReasons()).containsExactly("UNATTRIBUTED_QUERY");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void propagatedTaskAfterCloseOnlyMarksItsOriginalOwner() {
    QueryCaptureSession first = QueryCaptureSession.open(router, null, "first", 10);
    Runnable late = QueryCaptureSession.wrap((Runnable) () -> fire(router, "select late"));
    first.close();
    try (QueryCaptureSession second = QueryCaptureSession.open(router, null, "second", 10)) {
      late.run();
      fire(router, "select second");
      assertThat(first.incompleteReasons())
          .containsExactly("ASYNC_WORK_NOT_COMPLETED", "WORK_AFTER_CAPTURE");
      assertThat(second.incompleteReasons()).isEmpty();
      assertThat(second.stop().queries())
          .extracting(QueryRecord::sql)
          .containsExactly("select second");
    }
  }

  @Test
  void aWrappedTaskStartingAfterCloseIsDiagnosedWithoutAListenerCallback() {
    QueryCaptureSession session = QueryCaptureSession.open(router, null, "owner", 10);
    Runnable late = QueryCaptureSession.wrap((Runnable) () -> {});
    session.close();
    late.run();
    assertThat(session.incompleteReasons())
        .containsExactly("ASYNC_WORK_NOT_COMPLETED", "WORK_AFTER_CAPTURE");
  }

  @Test
  void queuedOrRejectedWorkIsAlreadyIncompleteBeforeItCanStart() {
    try (QueryCaptureSession session = QueryCaptureSession.open(router, null, "owner", 10)) {
      Runnable pending = QueryCaptureSession.wrap((Runnable) () -> fire(router, "select queued"));
      session.stop();
      assertThat(session.incompleteReasons()).containsExactly("ASYNC_WORK_NOT_COMPLETED");
      pending.run();
      assertThat(session.stop().queries()).isEmpty();
      assertThat(session.incompleteReasons())
          .containsExactly("ASYNC_WORK_NOT_COMPLETED", "WORK_AFTER_CAPTURE");
    }
  }

  @Test
  void capturedTasksAreSingleUseAndRejectedReuseCannotDuplicateQueries() throws Exception {
    try (QueryCaptureSession session = QueryCaptureSession.open(router, null, "owner", 10)) {
      Runnable task = QueryCaptureSession.wrap((Runnable) () -> fire(router, "select once"));
      task.run();
      assertThatThrownBy(task::run)
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("A captured task may only be executed once");
      Callable<Integer> callable = QueryCaptureSession.wrap((Callable<Integer>) () -> 42);
      assertThat(callable.call()).isEqualTo(42);
      assertThatThrownBy(callable::call).isInstanceOf(IllegalStateException.class);
      assertThat(session.stop().queries())
          .extracting(QueryRecord::sql)
          .containsExactly("select once");
      assertThat(session.incompleteReasons()).containsExactly("ASYNC_TASK_REUSED");
    }
  }

  @Test
  void suppressionClosedOnAnotherThreadIsPrunedAndRestoresItsOuterCapture() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try (QueryCaptureSession outer = QueryCaptureSession.open(router, null, "outer", 10)) {
      QueryCaptureSession.Scope suppression = QueryCaptureSession.suppress();
      fire(router, "select excluded");
      executor.submit(suppression::close).get(5, TimeUnit.SECONDS);
      fire(router, "select restored");
      assertThat(outer.stop().queries())
          .extracting(QueryRecord::sql)
          .containsExactly("select restored");
      assertThat(outer.incompleteReasons()).isEmpty();
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void aCaptureNestedInsideSuppressionRestoresTheExcludedInvocation() {
    try (QueryCaptureSession outer = QueryCaptureSession.open(router, null, "outer", 10)) {
      try (QueryCaptureSession.Scope suppression = QueryCaptureSession.suppress()) {
        try (QueryCaptureSession inner = QueryCaptureSession.open(router, null, "inner", 10)) {
          fire(router, "select inner");
          assertThat(inner.stop().queries())
              .extracting(QueryRecord::sql)
              .containsExactly("select inner");
        }
        fire(router, "select excluded");
      }
      fire(router, "select outer");
      assertThat(outer.stop().queries())
          .extracting(QueryRecord::sql)
          .containsExactly("select outer");
    }
  }

  @Test
  void explicitStandaloneRecordingWorksAlongsideADifferentScopedRouter() {
    QueryInterceptor standalone = new QueryInterceptor();
    standalone.start();
    try (QueryCaptureSession session = QueryCaptureSession.open(router, null, "owner", 10)) {
      fireBoth(router, standalone, "select both");
      try (QueryCaptureSession.Scope suppression = QueryCaptureSession.suppress()) {
        fireBoth(router, standalone, "select framework_internal");
      }
      assertThat(session.stop().queries())
          .extracting(QueryRecord::sql)
          .containsExactly("select both");
      assertThat(standalone.snapshot().queries())
          .extracting(QueryRecord::sql)
          .containsExactly("select both");
    } finally {
      standalone.stop();
    }
  }

  @Test
  void explicitStandaloneStateOnTheSameRouterRemainsIndependentOfItsSession() {
    router.start();
    fire(router, "select before");
    try (QueryCaptureSession session = QueryCaptureSession.open(router, null, "owner", 10)) {
      fire(router, "select during");
      assertThat(session.stop().queries())
          .extracting(QueryRecord::sql)
          .containsExactly("select during");
    }
    fire(router, "select after");
    router.stop();
    assertThat(router.snapshot().queries())
        .extracting(QueryRecord::sql)
        .containsExactly("select before", "select during", "select after");
  }

  @Test
  void stoppingBeforeJoinedAsyncWorkFinishesIsIncompleteEvenBetweenQueries() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    try (QueryCaptureSession session = QueryCaptureSession.open(router, null, "owner", 10)) {
      Future<Void> work =
          executor.submit(
              QueryCaptureSession.wrap(
                  (Callable<Void>)
                      () -> {
                        started.countDown();
                        assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
                        return null;
                      }));
      assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
      session.stop();
      assertThat(session.incompleteReasons()).containsExactly("ASYNC_WORK_STILL_RUNNING");
      release.countDown();
      work.get(5, TimeUnit.SECONDS);
    } finally {
      release.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  void suppressionNestsAndRestoresEvenWhenWorkThrows() {
    try (QueryCaptureSession session = QueryCaptureSession.open(router, lazyRouter, "owner", 10)) {
      assertThatThrownBy(
              () -> {
                try (QueryCaptureSession.Scope first = QueryCaptureSession.suppress()) {
                  fire(router, "select metadata");
                  try (QueryCaptureSession.Scope second = QueryCaptureSession.suppress()) {
                    fire(router, "explain select private_marker");
                    lazyRouter.recordProxyResolved("Entity", 1);
                  }
                  throw new IllegalStateException("expected");
                }
              })
          .isInstanceOf(IllegalStateException.class);
      fire(router, "select application");
      assertThat(session.stop().queries())
          .extracting(QueryRecord::sql)
          .containsExactly("select application");
      assertThat(session.tracker().getRecords()).isEmpty();
      assertThat(session.incompleteReasons()).isEmpty();
    }
  }

  @Test
  void stopKeepsTheBindingForLateCallbacksAndCloseIsIdempotent() {
    QueryCaptureSession session = QueryCaptureSession.open(router, null, "owner", 10);
    fire(router, "select before");
    QueryCaptureSnapshot snapshot = session.stop();
    fire(router, "select after");
    assertThat(session.stop()).isSameAs(snapshot);
    assertThat(session.incompleteReasons()).containsExactly("WORK_AFTER_CAPTURE");
    session.close();
    session.close();
    try (QueryCaptureSession next = QueryCaptureSession.open(router, null, "next", 10)) {
      fire(router, "select next");
      assertThat(next.stop().queries()).extracting(QueryRecord::sql).containsExactly("select next");
    }
  }

  @Test
  void crossThreadCloseIsExplicitAndOldWorkerBindingIsPruned() throws Exception {
    QueryCaptureSession session = QueryCaptureSession.open(router, null, "first", 10);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      executor.submit(session::close).get(5, TimeUnit.SECONDS);
      assertThat(session.incompleteReasons()).containsExactly("CAPTURE_THREAD_CHANGED");
      try (QueryCaptureSession next = QueryCaptureSession.open(router, null, "next", 10)) {
        fire(router, "select next");
        assertThat(next.stop().queries()).hasSize(1);
      }
      router.start();
      fire(router, "select standalone");
      assertThat(router.snapshot().queries())
          .extracting(QueryRecord::sql)
          .containsExactly("select standalone");
      router.stop();
    } finally {
      executor.shutdownNow();
      session.close();
    }
  }

  @Test
  void connectionSessionsAreIndependentUnderParallelCapture() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CyclicBarrier barrier = new CyclicBarrier(2);
    try {
      Callable<QueryCaptureSession> task =
          () -> {
            String id = Thread.currentThread().getName();
            try (QueryCaptureSession session = QueryCaptureSession.open(router, null, id, 10)) {
              barrier.await(5, TimeUnit.SECONDS);
              connection("getConnection", id, 0);
              connection("executeQuery", id, 12);
              barrier.await(5, TimeUnit.SECONDS);
              connection("close", id, 0);
              return session;
            }
          };
      Future<QueryCaptureSession> first = executor.submit(task);
      Future<QueryCaptureSession> second = executor.submit(task);
      for (QueryCaptureSession session :
          List.of(first.get(10, TimeUnit.SECONDS), second.get(10, TimeUnit.SECONDS))) {
        assertThat(session.interceptor().getConnectionTracker().getCompletedSessions())
            .singleElement()
            .satisfies(
                connection -> {
                  assertThat(connection.connectionId()).isEqualTo(session.testId());
                  assertThat(connection.databaseWorkMillis()).isEqualTo(12);
                  assertThat(connection.released()).isTrue();
                });
      }
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void preCaptureCheckoutIsImportedOnlyOnItsOwningThreadAndNotAfterPhysicalClose()
      throws Exception {
    connection("getConnection", "spring-transaction", 0);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      QueryCaptureSession other =
          executor
              .submit(
                  () -> {
                    try (QueryCaptureSession session =
                        QueryCaptureSession.open(router, null, "other", 10)) {
                      return session;
                    }
                  })
              .get(5, TimeUnit.SECONDS);
      assertThat(other.interceptor().getConnectionTracker().getCompletedSessions()).isEmpty();
      try (QueryCaptureSession session = QueryCaptureSession.open(router, null, "owner", 10)) {
        connection("executeQuery", "spring-transaction", 15);
        connection("close", "spring-transaction", 0);
        session.stop();
        assertThat(session.interceptor().getConnectionTracker().getCompletedSessions())
            .singleElement()
            .satisfies(
                connection -> {
                  assertThat(connection.connectionId()).isEqualTo("spring-transaction");
                  assertThat(connection.databaseWorkMillis()).isEqualTo(15);
                  assertThat(connection.released()).isTrue();
                });
      }
      try (QueryCaptureSession next = QueryCaptureSession.open(router, null, "next", 10)) {
        next.stop();
        assertThat(next.interceptor().getConnectionTracker().getCompletedSessions()).isEmpty();
      }
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void invalidOpenDoesNotReplaceTheCurrentCapture() {
    try (QueryCaptureSession session = QueryCaptureSession.open(router, null, "owner", 10)) {
      assertThatThrownBy(() -> QueryCaptureSession.open(router, null, "invalid", 0))
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> QueryCaptureSession.open(router, null, " ", 10))
          .isInstanceOf(IllegalArgumentException.class);
      fire(router, "select still_owned");
      assertThat(session.stop().queries())
          .extracting(QueryRecord::sql)
          .containsExactly("select still_owned");
    }
  }

  @Test
  void transactionCleanupAfterCaptureIsNotMistakenForLateSql() throws Exception {
    try (QueryCaptureSession session = QueryCaptureSession.open(router, null, "owner", 10)) {
      connection("getConnection", "transaction", 0);
      session.stop();
      connection("rollback", "transaction", 2);
      connection("close", "transaction", 0);
      assertThat(session.incompleteReasons()).isEmpty();
      assertThat(session.interceptor().getConnectionTracker().getCompletedSessions())
          .singleElement()
          .satisfies(connection -> assertThat(connection.released()).isFalse());
    }
    try (QueryCaptureSession next = QueryCaptureSession.open(router, null, "next", 10)) {
      next.stop();
      assertThat(next.interceptor().getConnectionTracker().getCompletedSessions()).isEmpty();
    }
  }

  @Test
  void lateSqlStillProducesEvidenceEvenWhenItHasConnectionCallbacks() throws Exception {
    try (QueryCaptureSession session = QueryCaptureSession.open(router, null, "owner", 10)) {
      session.stop();
      connection("getConnection", "late", 0);
      connection("executeQuery", "late", 1);
      fire(router, "select late_private_marker");
      connection("close", "late", 0);
      assertThat(session.stop().queries()).isEmpty();
      assertThat(session.incompleteReasons()).containsExactly("WORK_AFTER_CAPTURE");
    }
  }

  private void connection(String name, String id, long elapsed) throws Exception {
    Class<?> type =
        name.equals("getConnection")
            ? DataSource.class
            : name.equals("close") || name.equals("rollback") || name.equals("commit")
                ? Connection.class
                : Statement.class;
    Method method =
        type.getMethod(
            name, name.equals("executeQuery") ? new Class<?>[] {String.class} : new Class<?>[0]);
    MethodExecutionContext context = new MethodExecutionContext();
    context.setMethod(method);
    context.setTarget(Mockito.mock(type));
    context.setElapsedTime(elapsed);
    ConnectionInfo info = new ConnectionInfo();
    info.setConnectionId(id);
    context.setConnectionInfo(info);
    router.getConnectionTracker().beforeMethod(context);
    router.getConnectionTracker().afterMethod(context);
  }

  private static void fire(QueryInterceptor interceptor, String sql) {
    ExecutionInfo execution = new ExecutionInfo();
    List<QueryInfo> query = List.of(new QueryInfo(sql));
    interceptor.beforeQuery(execution, query);
    interceptor.afterQuery(execution, query);
  }

  private static void fireBoth(QueryInterceptor first, QueryInterceptor second, String sql) {
    ExecutionInfo execution = new ExecutionInfo();
    List<QueryInfo> query = List.of(new QueryInfo(sql));
    first.beforeQuery(execution, query);
    second.beforeQuery(execution, query);
    first.afterQuery(execution, query);
    second.afterQuery(execution, query);
  }
}
