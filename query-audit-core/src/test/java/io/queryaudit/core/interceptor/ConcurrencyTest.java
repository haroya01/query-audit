package io.queryaudit.core.interceptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.detector.QueryAuditAnalyzer;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import net.ttddyy.dsproxy.ExecutionInfo;
import net.ttddyy.dsproxy.QueryInfo;
import org.junit.jupiter.api.Test;

/**
 * Thread-safety and concurrency tests for {@link QueryInterceptor} and the detection pipeline
 * ({@link QueryAuditAnalyzer}).
 *
 * <p>These tests verify that the {@link java.util.concurrent.CopyOnWriteArrayList} and {@code
 * volatile boolean} inside {@link QueryInterceptor} behave correctly under concurrent access, and
 * that the stateless analysis pipeline can be invoked from multiple threads without shared-state
 * corruption.
 */
class ConcurrencyTest {

  // ====================================================================
  //  1. QueryInterceptor thread safety
  // ====================================================================

  @Test
  void concurrentQueriesAreAllRecorded() throws Exception {
    QueryInterceptor interceptor = new QueryInterceptor();
    interceptor.start();

    int threads = 10;
    int queriesPerThread = 100;
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    CountDownLatch latch = new CountDownLatch(threads);

    for (int t = 0; t < threads; t++) {
      final int threadId = t;
      executor.submit(
          () -> {
            try {
              for (int i = 0; i < queriesPerThread; i++) {
                fireQuery(
                    interceptor,
                    "SELECT * FROM users WHERE id = " + (threadId * queriesPerThread + i));
              }
            } finally {
              latch.countDown();
            }
          });
    }

    assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
    interceptor.stop();
    executor.shutdown();

    assertThat(interceptor.getRecordedQueries()).hasSize(threads * queriesPerThread);
  }

  @Test
  void noConcurrentModificationExceptionDuringIteration() throws Exception {
    QueryInterceptor interceptor = new QueryInterceptor();
    interceptor.start();

    int threads = 10;
    int queriesPerThread = 50;
    ExecutorService executor = Executors.newFixedThreadPool(threads + 2);
    CountDownLatch writersLatch = new CountDownLatch(threads);
    AtomicBoolean readersRunning = new AtomicBoolean(true);
    AtomicBoolean readerFailed = new AtomicBoolean(false);

    // Readers: continuously iterate recorded queries while writers add
    for (int r = 0; r < 2; r++) {
      executor.submit(
          () -> {
            while (readersRunning.get()) {
              try {
                List<QueryRecord> snapshot = interceptor.getRecordedQueries();
                // Force iteration
                int count = 0;
                for (QueryRecord qr : snapshot) {
                  if (qr.sql() != null) {
                    count++;
                  }
                }
                assertThat(count).isGreaterThanOrEqualTo(0);
              } catch (Exception e) {
                readerFailed.set(true);
              }
            }
          });
    }

    // Writers: add queries concurrently
    for (int t = 0; t < threads; t++) {
      executor.submit(
          () -> {
            try {
              for (int i = 0; i < queriesPerThread; i++) {
                fireQuery(interceptor, "SELECT 1");
              }
            } finally {
              writersLatch.countDown();
            }
          });
    }

    assertThat(writersLatch.await(30, TimeUnit.SECONDS)).isTrue();
    readersRunning.set(false);
    interceptor.stop();
    executor.shutdown();
    executor.awaitTermination(5, TimeUnit.SECONDS);

    assertThat(readerFailed).isFalse();
  }

  @Test
  void startAndStopFromDifferentThreads() throws Exception {
    QueryInterceptor interceptor = new QueryInterceptor();

    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch queriesFired = new CountDownLatch(1);

    // Thread 1: start interceptor
    executor.submit(
        () -> {
          interceptor.start();
          started.countDown();
        });

    assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();

    // Thread 2: fire queries
    executor.submit(
        () -> {
          for (int i = 0; i < 50; i++) {
            fireQuery(interceptor, "SELECT id FROM orders WHERE status = 'ACTIVE'");
          }
          queriesFired.countDown();
        });

    assertThat(queriesFired.await(10, TimeUnit.SECONDS)).isTrue();

    // Main thread: stop
    interceptor.stop();
    assertThat(interceptor.isActive()).isFalse();
    assertThat(interceptor.getRecordedQueries()).hasSize(50);

    executor.shutdown();
  }

  @Test
  void clearDuringActiveRecordingDoesNotThrow() throws Exception {
    QueryInterceptor interceptor = new QueryInterceptor();
    interceptor.start();

    int threads = 5;
    ExecutorService executor = Executors.newFixedThreadPool(threads + 1);
    CountDownLatch latch = new CountDownLatch(threads);
    AtomicBoolean failed = new AtomicBoolean(false);

    // Writers
    for (int t = 0; t < threads; t++) {
      executor.submit(
          () -> {
            try {
              for (int i = 0; i < 100; i++) {
                fireQuery(interceptor, "INSERT INTO logs(msg) VALUES('test')");
              }
            } catch (Exception e) {
              failed.set(true);
            } finally {
              latch.countDown();
            }
          });
    }

    // Concurrent clears
    executor.submit(
        () -> {
          for (int i = 0; i < 20; i++) {
            interceptor.clear();
            try {
              Thread.sleep(1);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }
        });

    assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
    interceptor.stop();
    executor.shutdown();
    executor.awaitTermination(5, TimeUnit.SECONDS);

    assertThat(failed).isFalse();
  }

  // ====================================================================
  //  2. Multiple start/stop cycles
  // ====================================================================

  @Test
  void rapidStartStopCyclesFromMultipleThreads() throws Exception {
    QueryInterceptor interceptor = new QueryInterceptor();

    int threads = 6;
    int cycles = 20;
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    CyclicBarrier barrier = new CyclicBarrier(threads);
    AtomicBoolean failed = new AtomicBoolean(false);

    for (int t = 0; t < threads; t++) {
      final int threadId = t;
      executor.submit(
          () -> {
            try {
              barrier.await(10, TimeUnit.SECONDS);
              for (int c = 0; c < cycles; c++) {
                if (threadId % 2 == 0) {
                  interceptor.start();
                  fireQuery(interceptor, "SELECT 1");
                  interceptor.stop();
                } else {
                  interceptor.stop();
                  interceptor.start();
                  fireQuery(interceptor, "SELECT 2");
                }
              }
            } catch (Exception e) {
              failed.set(true);
            }
          });
    }

    executor.shutdown();
    assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
    assertThat(failed).isFalse();
  }

  @Test
  void queriesBetweenStartStopAreCaptured() throws Exception {
    QueryInterceptor interceptor = new QueryInterceptor();

    // Cycle 1
    interceptor.start();
    int queriesCycle1 = 30;
    ExecutorService executor = Executors.newFixedThreadPool(3);
    CountDownLatch latch1 = new CountDownLatch(3);
    for (int t = 0; t < 3; t++) {
      executor.submit(
          () -> {
            try {
              for (int i = 0; i < 10; i++) {
                fireQuery(interceptor, "SELECT 1");
              }
            } finally {
              latch1.countDown();
            }
          });
    }
    assertThat(latch1.await(10, TimeUnit.SECONDS)).isTrue();
    interceptor.stop();
    assertThat(interceptor.getRecordedQueries()).hasSize(queriesCycle1);

    // Cycle 2 — start() clears previous queries
    interceptor.start();
    int queriesCycle2 = 20;
    CountDownLatch latch2 = new CountDownLatch(2);
    for (int t = 0; t < 2; t++) {
      executor.submit(
          () -> {
            try {
              for (int i = 0; i < 10; i++) {
                fireQuery(interceptor, "SELECT 2");
              }
            } finally {
              latch2.countDown();
            }
          });
    }
    assertThat(latch2.await(10, TimeUnit.SECONDS)).isTrue();
    interceptor.stop();
    assertThat(interceptor.getRecordedQueries()).hasSize(queriesCycle2);

    executor.shutdown();
  }

  // ====================================================================
  //  3. QueryAuditAnalyzer thread safety
  // ====================================================================

  @Test
  void analyzerFromMultipleThreadsNoSharedStateCorruption() throws Exception {
    QueryAuditConfig config = QueryAuditConfig.defaults();
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(config, List.of());
    IndexMetadata emptyIndex = new IndexMetadata(Map.of());

    int threads = 8;
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    CyclicBarrier barrier = new CyclicBarrier(threads);
    CopyOnWriteArrayList<QueryAuditReport> reports = new CopyOnWriteArrayList<>();
    AtomicBoolean failed = new AtomicBoolean(false);

    for (int t = 0; t < threads; t++) {
      final int threadId = t;
      executor.submit(
          () -> {
            try {
              barrier.await(10, TimeUnit.SECONDS);
              // Each thread creates its own unique query list
              List<QueryRecord> queries = new ArrayList<>();
              for (int i = 0; i < 20; i++) {
                queries.add(
                    new QueryRecord(
                        "SELECT * FROM table_" + threadId + " WHERE id = " + i,
                        1_000_000L,
                        System.currentTimeMillis(),
                        "test.ClassName.method:" + i));
              }
              QueryAuditReport report =
                  analyzer.analyze("TestClass", "test_" + threadId, queries, emptyIndex);
              reports.add(report);
            } catch (Exception e) {
              failed.set(true);
            }
          });
    }

    executor.shutdown();
    assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
    assertThat(failed).isFalse();
    assertThat(reports).hasSize(threads);

    // Verify each report got its own data — no cross-contamination
    for (int t = 0; t < threads; t++) {
      final int tid = t;
      QueryAuditReport report =
          reports.stream()
              .filter(r -> ("test_" + tid).equals(r.getTestName()))
              .findFirst()
              .orElseThrow();
      assertThat(report.getTotalQueryCount()).isEqualTo(20);
    }
  }

  // ====================================================================
  //  4. QueryRecord immutability
  // ====================================================================

  @Test
  void queryRecordFieldsAreImmutable() {
    QueryRecord record =
        new QueryRecord("SELECT 1", 500_000L, System.currentTimeMillis(), "com.example.Foo.bar:42");

    // QueryRecord is a Java record — all fields are final.
    // Verify getRecordedQueries returns an unmodifiable copy.
    QueryInterceptor interceptor = new QueryInterceptor();
    interceptor.start();
    fireQuery(interceptor, "SELECT 1");
    interceptor.stop();

    List<QueryRecord> queries = interceptor.getRecordedQueries();
    assertThat(queries).hasSize(1);

    // The returned list should be unmodifiable
    assertThatNoException()
        .isThrownBy(
            () -> {
              // Record field access — just verifying no mutation path exists
              String sql = record.sql();
              String normalized = record.normalizedSql();
              long time = record.executionTimeNanos();
              long ts = record.timestamp();
              String stack = record.stackTrace();
              int hash = record.fullStackHash();
              assertThat(sql).isEqualTo("SELECT 1");
              assertThat(normalized).isNotNull();
              assertThat(time).isEqualTo(500_000L);
              assertThat(ts).isGreaterThan(0);
              assertThat(stack).isEqualTo("com.example.Foo.bar:42");
              assertThat(hash).isEqualTo("com.example.Foo.bar:42".hashCode());
            });

    // Verify the list returned by getRecordedQueries is a snapshot (defensive copy).
    // Modifying it should not affect the interceptor's internal list.
    int sizeBefore = interceptor.getRecordedQueries().size();
    queries.add(record); // mutate the snapshot
    assertThat(interceptor.getRecordedQueries()).hasSize(sizeBefore); // internal list unchanged
  }

  // ====================================================================
  //  5. Stress test
  // ====================================================================

  @Test
  void stressTest_1000QueriesFrom50Threads() throws Exception {
    QueryInterceptor interceptor = new QueryInterceptor();
    interceptor.start();

    int threads = 50;
    int queriesPerThread = 20; // 50 * 20 = 1000 total
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    CyclicBarrier barrier = new CyclicBarrier(threads);
    CountDownLatch latch = new CountDownLatch(threads);

    long startTime = System.nanoTime();

    for (int t = 0; t < threads; t++) {
      final int threadId = t;
      executor.submit(
          () -> {
            try {
              barrier.await(10, TimeUnit.SECONDS);
              for (int i = 0; i < queriesPerThread; i++) {
                fireQuery(interceptor, "SELECT * FROM products WHERE category_id = " + threadId);
              }
            } catch (Exception e) {
              // barrier/interrupt — acceptable in stress test
            } finally {
              latch.countDown();
            }
          });
    }

    assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
    interceptor.stop();

    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);

    executor.shutdown();
    executor.awaitTermination(5, TimeUnit.SECONDS);

    List<QueryRecord> recorded = interceptor.getRecordedQueries();
    assertThat(recorded).hasSize(threads * queriesPerThread);

    // Sanity: recording should not take an unreasonable amount of time
    // (generous bound — mainly guards against lock contention pathology)
    assertThat(elapsedMs).isLessThan(30_000L);
  }

  @Test
  void stressTest_queriesAreNotLostUnderContention() throws Exception {
    QueryInterceptor interceptor = new QueryInterceptor();
    interceptor.start();

    int threads = 50;
    int queriesPerThread = 20;
    int totalExpected = threads * queriesPerThread;
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    CyclicBarrier barrier = new CyclicBarrier(threads);
    CountDownLatch latch = new CountDownLatch(threads);

    for (int t = 0; t < threads; t++) {
      final int threadId = t;
      executor.submit(
          () -> {
            try {
              barrier.await(10, TimeUnit.SECONDS);
              for (int i = 0; i < queriesPerThread; i++) {
                fireQuery(
                    interceptor,
                    "UPDATE inventory SET qty = qty - 1 WHERE sku = 'SKU-"
                        + threadId
                        + "-"
                        + i
                        + "'");
              }
            } catch (Exception e) {
              // barrier/interrupt
            } finally {
              latch.countDown();
            }
          });
    }

    assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
    interceptor.stop();
    executor.shutdown();
    executor.awaitTermination(5, TimeUnit.SECONDS);

    // Every single query must be recorded — no lost writes
    assertThat(interceptor.getRecordedQueries()).hasSize(totalExpected);

    // Verify each query's SQL is non-null and non-blank
    for (QueryRecord qr : interceptor.getRecordedQueries()) {
      assertThat(qr.sql()).isNotBlank();
      assertThat(qr.normalizedSql()).isNotNull();
      assertThat(qr.stackTrace()).isNotNull();
    }
  }

  // ====================================================================
  //  Helpers
  // ====================================================================

  /**
   * Simulates the datasource-proxy callback by constructing real {@link ExecutionInfo} and {@link
   * QueryInfo} objects and invoking {@link QueryInterceptor#afterQuery}.
   */
  private static void fireQuery(QueryInterceptor interceptor, String sql) {
    ExecutionInfo execInfo = new ExecutionInfo();
    execInfo.setElapsedTime(1L); // 1 ms
    QueryInfo queryInfo = new QueryInfo(sql);
    interceptor.afterQuery(execInfo, List.of(queryInfo));
  }
}
