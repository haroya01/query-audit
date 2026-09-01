package io.queryaudit.core.interceptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.queryaudit.core.model.QueryRecord;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import net.ttddyy.dsproxy.ExecutionInfo;
import net.ttddyy.dsproxy.QueryInfo;
import org.junit.jupiter.api.Test;

class QueryInterceptorCapacityTest {

  @Test
  void reachingTheLimitWithoutDroppingQueriesIsComplete() {
    QueryInterceptor interceptor = new QueryInterceptor();
    interceptor.setMaxQueries(2);
    interceptor.start();

    fireQueries(interceptor, "SELECT 1", "SELECT 2");

    QueryCaptureSnapshot snapshot = interceptor.snapshot();
    assertThat(snapshot.queries())
        .extracting(QueryRecord::sql)
        .containsExactly("SELECT 1", "SELECT 2");
    assertThat(snapshot.droppedCount()).isZero();
    assertThat(snapshot.truncated()).isFalse();
    assertThatThrownBy(() -> snapshot.queries().clear())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void countsEveryNonBlankQueryDroppedFromOneCallback() {
    QueryInterceptor interceptor = new QueryInterceptor();
    interceptor.setMaxQueries(1);
    interceptor.start();

    fireQueries(interceptor, "SELECT 1", "SELECT 2", "", null, "SELECT 3");

    QueryCaptureSnapshot snapshot = interceptor.snapshot();
    assertThat(snapshot.queries()).extracting(QueryRecord::sql).containsExactly("SELECT 1");
    assertThat(snapshot.droppedCount()).isEqualTo(2);
    assertThat(snapshot.truncated()).isTrue();
  }

  @Test
  void startAndClearResetTruncationState() {
    QueryInterceptor interceptor = new QueryInterceptor();
    interceptor.setMaxQueries(1);
    interceptor.start();
    fireQueries(interceptor, "SELECT 1", "SELECT 2");
    QueryCaptureSnapshot truncated = interceptor.snapshot();

    interceptor.start();
    fireQueries(interceptor, "SELECT 3");
    QueryCaptureSnapshot restarted = interceptor.snapshot();

    fireQueries(interceptor, "SELECT 4");
    interceptor.clear();
    QueryCaptureSnapshot cleared = interceptor.snapshot();

    assertThat(truncated.truncated()).isTrue();
    assertThat(truncated.queries()).extracting(QueryRecord::sql).containsExactly("SELECT 1");
    assertThat(restarted.queries()).extracting(QueryRecord::sql).containsExactly("SELECT 3");
    assertThat(restarted.truncated()).isFalse();
    assertThat(cleared.queries()).isEmpty();
    assertThat(cleared.droppedCount()).isZero();
    assertThat(cleared.truncated()).isFalse();
  }

  @Test
  void concurrentCallbacksRespectCapacityAndDroppedCount() throws InterruptedException {
    int maxQueries = 40;
    int threads = 8;
    int queriesPerThread = 30;
    int attemptedQueries = threads * queriesPerThread;
    QueryInterceptor interceptor = new QueryInterceptor();
    interceptor.setMaxQueries(maxQueries);
    interceptor.start();

    ExecutorService executor = Executors.newFixedThreadPool(threads);
    CountDownLatch start = new CountDownLatch(1);
    for (int thread = 0; thread < threads; thread++) {
      executor.submit(
          () -> {
            try {
              start.await();
              for (int query = 0; query < queriesPerThread; query++) {
                fireQueries(interceptor, "SELECT " + query);
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          });
    }

    start.countDown();
    executor.shutdown();
    assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
    interceptor.stop();

    QueryCaptureSnapshot snapshot = interceptor.snapshot();
    assertThat(snapshot.queries()).hasSize(maxQueries);
    assertThat(snapshot.droppedCount()).isEqualTo(attemptedQueries - maxQueries);
    assertThat(snapshot.queries().size() + snapshot.droppedCount()).isEqualTo(attemptedQueries);
  }

  private static void fireQueries(QueryInterceptor interceptor, String... sqlStatements) {
    ExecutionInfo execution = new ExecutionInfo();
    execution.setElapsedTime(1L);
    List<QueryInfo> queries = Arrays.stream(sqlStatements).map(QueryInfo::new).toList();
    interceptor.afterQuery(execution, queries);
  }
}
