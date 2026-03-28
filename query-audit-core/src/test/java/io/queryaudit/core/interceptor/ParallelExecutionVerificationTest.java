package io.queryaudit.core.interceptor;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.QueryRecord;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;
import net.ttddyy.dsproxy.ExecutionInfo;
import net.ttddyy.dsproxy.QueryInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Proves that a single shared QueryInterceptor instance is unsafe for parallel test execution.
 *
 * <p>When two tests share the same interceptor (class-level field), calling {@code start()} in one
 * test invokes {@code clear()}, which wipes queries already recorded by another concurrently
 * running test.
 */
class ParallelExecutionVerificationTest {

  /**
   * Simulates two tests sharing one QueryInterceptor. Thread A starts recording and adds queries.
   * Thread B then calls start(), which clears Thread A's queries via the internal clear() call.
   */
  @Test
  @DisplayName("start() on shared interceptor wipes another thread's recorded queries")
  void startClearsOtherThreadsQueries() throws Exception {
    // Single shared instance — simulates class-level @RegisterExtension or bean
    QueryInterceptor shared = new QueryInterceptor();

    AtomicReference<List<QueryRecord>> threadAQueriesAfterBStart = new AtomicReference<>();
    CyclicBarrier barrier = new CyclicBarrier(2);

    // -- Thread A: "Test A" starts recording and adds queries ----
    Thread threadA =
        new Thread(
            () -> {
              try {
                shared.start();
                addFakeQueries(shared, "SELECT * FROM orders", 3);

                // Signal Thread B that queries are recorded, then wait for B to call start()
                barrier.await();
                barrier.await();

                // After Thread B called start(), capture what Thread A sees
                threadAQueriesAfterBStart.set(shared.getRecordedQueries());
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });

    // -- Thread B: "Test B" calls start(), which clears the shared list ----
    Thread threadB =
        new Thread(
            () -> {
              try {
                // Wait until Thread A has recorded its queries
                barrier.await();

                // This start() calls clear() internally — wipes Thread A's data
                shared.start();

                // Signal Thread A that start() has been called
                barrier.await();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });

    threadA.start();
    threadB.start();
    threadA.join(5_000);
    threadB.join(5_000);

    List<QueryRecord> survivingQueries = threadAQueriesAfterBStart.get();
    assertThat(survivingQueries)
        .as(
            "Thread A recorded 3 queries, but Thread B's start() wiped them. "
                + "This proves the shared-interceptor race condition.")
        .isEmpty(); // <-- The bug: Thread A's queries are GONE
  }

  /**
   * Confirms that sequential start() calls on the same instance also lose earlier queries. This is
   * the simplest demonstration: no threads needed.
   */
  @Test
  @DisplayName("Sequential start() proves clear-on-start semantics erase prior data")
  void sequentialStartClearsPriorQueries() {
    QueryInterceptor interceptor = new QueryInterceptor();

    interceptor.start();
    addFakeQueries(interceptor, "SELECT 1 FROM dual", 5);
    assertThat(interceptor.getRecordedQueries()).hasSize(5);

    // Second start() — as if another test begins on the same shared instance
    interceptor.start();

    assertThat(interceptor.getRecordedQueries())
        .as("Second start() cleared the 5 queries recorded earlier")
        .isEmpty();
  }

  // -- helper -----------------------------------------------------------

  /**
   * Adds fake query records by calling afterQuery() with minimal datasource-proxy objects.
   */
  private static void addFakeQueries(QueryInterceptor interceptor, String sql, int count) {
    ExecutionInfo execInfo = new ExecutionInfo();
    for (int i = 0; i < count; i++) {
      QueryInfo qi = new QueryInfo();
      qi.setQuery(sql);
      interceptor.afterQuery(execInfo, List.of(qi));
    }
  }
}
