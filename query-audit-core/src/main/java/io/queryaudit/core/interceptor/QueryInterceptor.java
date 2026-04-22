package io.queryaudit.core.interceptor;

import io.queryaudit.core.model.LifecyclePhase;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.parser.SqlParser;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import net.ttddyy.dsproxy.ExecutionInfo;
import net.ttddyy.dsproxy.QueryInfo;
import net.ttddyy.dsproxy.listener.QueryExecutionListener;

/**
 * Intercepts SQL queries via datasource-proxy and records all statements (SELECT, INSERT, UPDATE,
 * DELETE) for analysis by query-audit detectors.
 *
 * <p>Captures up to 10 non-framework application frames from the call stack as a string for N+1
 * grouping. Both SQL strings and stack traces are pooled via {@link ConcurrentHashMap} so that
 * identical values (common in N+1 scenarios) share a single String instance, saving significant
 * memory without any user-facing configuration.
 *
 * <h3>Memory controls</h3>
 *
 * <ul>
 *   <li><b>Max queries</b> — stops recording after {@link #getMaxQueries()} queries per test
 *       (default {@value #DEFAULT_MAX_QUERIES}). This prevents OOM when a test generates an
 *       unexpectedly large number of queries.
 *   <li><b>String interning</b> — stack traces and SQL strings from the same call site or query
 *       pattern are automatically deduplicated in memory.
 * </ul>
 *
 * @author haroya
 * @since 0.2.0
 */
public class QueryInterceptor implements QueryExecutionListener {

  /** Default maximum number of queries recorded per test before recording stops. */
  public static final int DEFAULT_MAX_QUERIES = 10_000;

  private static final Set<String> SKIP_PREFIXES =
      Set.of(
          "java.lang.Thread",
          "sun.",
          "jdk.internal.",
          "io.queryaudit.core.interceptor.",
          "org.springframework.",
          "org.hibernate.",
          "org.junit.",
          "org.gradle.",
          "net.ttddyy.",
          "com.zaxxer.",
          "org.apache.",
          "net.bytebuddy.",
          "com.github.gavlyukovskiy.");

  private static final int MAX_FRAMES = 10;

  // ArrayList + synchronization replaces CopyOnWriteArrayList.
  // CopyOnWriteArrayList copies the entire backing array on every add(),
  // which is O(n) per write and O(n^2) total for n queries — unacceptable
  // when capturing thousands of queries per test.
  private final List<QueryRecord> recordedQueries =
      Collections.synchronizedList(new ArrayList<>(256));
  private volatile boolean active = false;
  private volatile int maxQueries = DEFAULT_MAX_QUERIES;
  private volatile boolean capacityWarningLogged = false;
  private volatile LifecyclePhase currentPhase = LifecyclePhase.TEST;

  // SQL string pool: identical SQL strings share the same object reference,
  // saving memory when the same query pattern appears many times (e.g., N+1).
  private final Map<String, String> sqlPool = new ConcurrentHashMap<>();

  // Stack trace string pool: N+1 queries originate from the same call site,
  // so their stack traces are identical. Pooling avoids redundant String objects.
  private final Map<String, String> stackTracePool = new ConcurrentHashMap<>();

  @Override
  public void beforeQuery(ExecutionInfo execInfo, List<QueryInfo> queryInfoList) {
    // no-op
  }

  @Override
  public void afterQuery(ExecutionInfo execInfo, List<QueryInfo> queryInfoList) {
    if (!active) {
      return;
    }

    for (QueryInfo queryInfo : queryInfoList) {
      String sql = queryInfo.getQuery();
      if (sql != null && !sql.isBlank()) {
        // Check capacity before recording to prevent unbounded memory growth.
        if (recordedQueries.size() >= maxQueries) {
          if (!capacityWarningLogged) {
            capacityWarningLogged = true;
            System.err.println(
                "[QueryAudit] WARNING: Max query capacity ("
                    + maxQueries
                    + ") reached. "
                    + "Further queries will not be recorded. "
                    + "Increase the limit via QueryInterceptor.setMaxQueries() or "
                    + "query-audit.max-queries in application.yml.");
          }
          return;
        }
        String pooledSql = poolString(sqlPool, sql);
        String stackTrace = poolString(stackTracePool, captureStackTrace());
        String normalized = SqlParser.normalize(pooledSql);
        int stackHash = stackTrace == null ? 0 : stackTrace.hashCode();
        recordedQueries.add(
            new QueryRecord(
                pooledSql,
                normalized,
                execInfo.getElapsedTime() * 1_000_000L,
                System.currentTimeMillis(),
                stackTrace,
                stackHash,
                currentPhase));
      }
    }
  }

  /**
   * Sets the current lifecycle phase. Queries recorded after this call will be tagged with the
   * given phase.
   *
   * @param phase the lifecycle phase (SETUP, TEST, or TEARDOWN)
   */
  public void setPhase(LifecyclePhase phase) {
    this.currentPhase = phase;
  }

  public void start() {
    recordedQueries.clear();
    sqlPool.clear();
    stackTracePool.clear();
    capacityWarningLogged = false;
    currentPhase = LifecyclePhase.TEST;
    active = true;
  }

  public void stop() {
    active = false;
  }

  public List<QueryRecord> getRecordedQueries() {
    // Defensive copy into a mutable ArrayList. Cheaper than List.copyOf()
    // which iterates and checks each element for null. The caller gets
    // an independent snapshot that won't affect the internal list.
    synchronized (recordedQueries) {
      return new ArrayList<>(recordedQueries);
    }
  }

  public void clear() {
    recordedQueries.clear();
    sqlPool.clear();
    stackTracePool.clear();
  }

  public boolean isActive() {
    return active;
  }

  /**
   * Returns the maximum number of queries that will be recorded per test. When this limit is
   * reached, further queries are silently dropped (with a single warning to stderr).
   *
   * @return the current max queries limit
   */
  public int getMaxQueries() {
    return maxQueries;
  }

  /**
   * Sets the maximum number of queries to record per test. Use this to control memory usage in
   * tests that generate a very large number of SQL statements.
   *
   * @param maxQueries the maximum number of queries (must be positive)
   * @throws IllegalArgumentException if maxQueries is not positive
   */
  public void setMaxQueries(int maxQueries) {
    if (maxQueries <= 0) {
      throw new IllegalArgumentException("maxQueries must be positive, got: " + maxQueries);
    }
    this.maxQueries = maxQueries;
  }

  // -------------------------------------------------------------------------
  // Internal helpers
  // -------------------------------------------------------------------------

  /**
   * Deduplicates strings so that identical values share a single String instance in memory. This is
   * especially effective for N+1 patterns where the same SQL and stack trace appear many times.
   */
  private static String poolString(Map<String, String> pool, String value) {
    if (value == null) return null;
    return pool.computeIfAbsent(value, k -> k);
  }

  /**
   * Captures up to {@value MAX_FRAMES} non-framework application frames from the current call
   * stack. Each frame is formatted as {@code className.methodName:lineNumber} and joined with
   * newlines. The result is interned so that identical stack traces (common in N+1 scenarios) share
   * a single String instance.
   */
  private static String captureStackTrace() {
    StackTraceElement[] elements = Thread.currentThread().getStackTrace();
    StringBuilder sb = new StringBuilder(512);
    int count = 0;

    for (StackTraceElement element : elements) {
      if (count >= MAX_FRAMES) {
        break;
      }
      String className = element.getClassName();
      if (shouldSkip(className)) {
        continue;
      }
      if (count > 0) {
        sb.append('\n');
      }
      sb.append(className)
          .append('.')
          .append(element.getMethodName())
          .append(':')
          .append(element.getLineNumber());
      count++;
    }

    return sb.toString();
  }

  private static boolean shouldSkip(String className) {
    for (String prefix : SKIP_PREFIXES) {
      if (className.startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }
}
