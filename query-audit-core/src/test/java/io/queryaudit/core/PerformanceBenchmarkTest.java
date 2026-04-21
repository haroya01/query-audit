package io.queryaudit.core;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.detector.QueryAuditAnalyzer;
import io.queryaudit.core.model.IndexInfo;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Performance benchmark that verifies the detection pipeline stays lightweight when processing a
 * large number of queries. This test ensures we do not regress on RAM or CPU usage -- the library
 * runs during tests and must not be a bottleneck.
 */
class PerformanceBenchmarkTest {

  private static final int QUERY_COUNT = 10_000;

  @Test
  void fullPipeline_10kQueries_runsWithinBudget() {
    // -- Generate realistic SQL queries --
    List<QueryRecord> queries = generateQueries(QUERY_COUNT);

    // Minimal index metadata so detectors have something to work with
    IndexMetadata indexMetadata =
        new IndexMetadata(
            Map.of(
                "users",
                    List.of(
                        new IndexInfo("users", "idx_users_pk", "id", 1, false, 1000),
                        new IndexInfo("users", "idx_users_email", "email", 1, true, 1000)),
                "orders",
                    List.of(
                        new IndexInfo("orders", "idx_orders_pk", "id", 1, false, 5000),
                        new IndexInfo("orders", "idx_orders_user", "user_id", 1, true, 5000)),
                "products",
                    List.of(new IndexInfo("products", "idx_products_pk", "id", 1, false, 2000))));

    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of());

    // -- Warm up JIT --
    analyzer.analyze("warmup", queries.subList(0, 100), indexMetadata);

    // -- Force GC and measure baseline memory --
    System.gc();
    try {
      Thread.sleep(100);
    } catch (InterruptedException ignored) {
    }
    long memBefore = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

    // -- Run the full pipeline --
    long startNanos = System.nanoTime();
    QueryAuditReport report = analyzer.analyze("benchmarkTest", queries, indexMetadata);
    long elapsedNanos = System.nanoTime() - startNanos;

    // -- Measure memory after --
    long memAfter = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    long memDeltaMB = (memAfter - memBefore) / (1024 * 1024);

    double elapsedSeconds = elapsedNanos / 1_000_000_000.0;
    double queriesPerSecond = QUERY_COUNT / elapsedSeconds;

    // -- Print results --
    System.out.printf("=== Performance Benchmark ===%n");
    System.out.printf("Queries processed : %,d%n", QUERY_COUNT);
    System.out.printf("Elapsed time      : %.3f s%n", elapsedSeconds);
    System.out.printf("Throughput         : %,.0f queries/sec%n", queriesPerSecond);
    System.out.printf("Memory delta       : %d MB%n", memDeltaMB);
    System.out.printf(
        "Issues found       : %d confirmed, %d info%n",
        report.getConfirmedIssues().size(), report.getInfoIssues().size());
    System.out.printf("Unique patterns    : %d%n", report.getUniquePatternCount());

    // Budget: 30s / 200MB for 10k queries — headroom for slower CI runners. JSqlParser's
    // AST path is ~2x the time and ~3x the memory of the regex path; pays for #54 / #102
    // / #103 correctness.
    assertThat(elapsedSeconds)
        .as("Full pipeline for %d queries should complete in under 30 seconds", QUERY_COUNT)
        .isLessThan(30.0);

    assertThat(memDeltaMB)
        .as("Peak memory delta should stay under 200 MB for %d queries", QUERY_COUNT)
        .isLessThan(200L);
  }

  /**
   * Generates a mix of realistic SQL query patterns including SELECTs, INSERTs, UPDATEs, JOINs,
   * subqueries, and various anti-patterns that the detectors look for.
   */
  private static List<QueryRecord> generateQueries(int count) {
    String[] templates = {
      // Basic SELECT with WHERE
      "SELECT id, name, email FROM users WHERE id = %d",
      "SELECT * FROM users WHERE email = 'user%d@example.com'",
      // JOINs
      "SELECT u.id, u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id WHERE u.id = %d",
      "SELECT u.name, p.title FROM users u, products p WHERE u.id = %d",
      // Aggregation
      "SELECT COUNT(*) FROM orders WHERE user_id = %d",
      "SELECT user_id, SUM(total) FROM orders GROUP BY user_id HAVING SUM(total) > %d",
      // INSERT
      "INSERT INTO orders (user_id, total, created_at) VALUES (%d, 99.99, '2024-01-01')",
      // UPDATE
      "UPDATE users SET name = 'updated' WHERE id = %d",
      "UPDATE orders SET status = 'shipped' WHERE id = %d",
      // Subquery
      "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > %d)",
      // LIKE with leading wildcard (anti-pattern)
      "SELECT id, name FROM users WHERE name LIKE '%%user%d%%'",
      // OFFSET pagination (anti-pattern)
      "SELECT * FROM products ORDER BY created_at LIMIT 20 OFFSET %d",
      // OR conditions
      "SELECT * FROM users WHERE id = %d OR email = 'test@example.com' OR name = 'test'",
      // DISTINCT
      "SELECT DISTINCT u.name FROM users u JOIN orders o ON u.id = o.user_id WHERE o.total > %d",
      // DELETE without WHERE (anti-pattern for small fraction)
      "DELETE FROM orders WHERE id = %d",
      // COUNT instead of EXISTS
      "SELECT COUNT(*) FROM users WHERE id = %d AND status = 'active'",
      // Multiple JOINs
      "SELECT u.name, o.total, p.title FROM users u JOIN orders o ON u.id = o.user_id JOIN products p ON o.product_id = p.id WHERE u.id = %d",
      // UNION
      "SELECT id, name FROM users WHERE status = 'active' UNION SELECT id, name FROM users WHERE id = %d",
      // FOR UPDATE
      "SELECT id, balance FROM users WHERE id = %d FOR UPDATE",
      // ORDER BY with LIMIT
      "SELECT * FROM orders ORDER BY created_at DESC LIMIT %d",
    };

    List<QueryRecord> queries = new ArrayList<>(count);
    long now = System.currentTimeMillis();

    for (int i = 0; i < count; i++) {
      String template = templates[i % templates.length];
      String sql = String.format(template, i);
      // Synthetic stack trace for N+1 grouping
      String stack =
          "com.example.Service.method" + (i % 50) + ":42\n" + "com.example.Controller.handle:100";
      queries.add(new QueryRecord(sql, 1_000_000L, now + i, stack));
    }

    return queries;
  }
}
