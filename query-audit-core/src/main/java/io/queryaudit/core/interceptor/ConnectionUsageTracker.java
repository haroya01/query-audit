package io.queryaudit.core.interceptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import javax.sql.DataSource;
import net.ttddyy.dsproxy.ConnectionInfo;
import net.ttddyy.dsproxy.listener.MethodExecutionContext;
import net.ttddyy.dsproxy.listener.MethodExecutionListener;

/**
 * Tracks how long each JDBC connection is held versus how long it actually executes SQL, so the
 * connection-held-idle rule can flag transactions that keep a pooled connection while slow non-DB
 * work runs (issue #168).
 *
 * <p>Per-statement rules structurally cannot see this failure mode: the SQL is all fine, the
 * transaction boundary is wrong, and under load the pool drains and takes down unrelated endpoints.
 * The tracker observes the {@code getConnection}/{@code close} lifecycle and the elapsed time of
 * every {@code execute*}/{@code commit}/{@code rollback} call in between — the gap is time the
 * connection was held doing something other than database work.
 *
 * <p>Wall-clock caveat, stated up front: the measurement needs the non-DB work to actually take
 * time in the test (Testcontainers, WireMock with latency). Zero-latency mocks never reproduce the
 * gap. The clock is injectable for deterministic unit tests.
 *
 * @author haroya
 * @since 0.5.0
 */
public class ConnectionUsageTracker implements MethodExecutionListener {

  /** JDBC calls whose elapsed time counts as database work rather than idle time. */
  private static final Set<String> DB_WORK_METHODS =
      Set.of(
          "execute",
          "executeQuery",
          "executeUpdate",
          "executeBatch",
          "executeLargeUpdate",
          "executeLargeBatch",
          "commit",
          "rollback");

  private static final Set<String> SKIP_PREFIXES =
      Set.of(
          "java.lang.Thread",
          "java.base/",
          "sun.",
          "jdk.",
          "io.queryaudit.",
          "org.springframework.",
          "org.hibernate.",
          "org.junit.",
          "org.gradle.",
          "net.ttddyy.",
          "com.zaxxer.",
          "org.apache.",
          "net.bytebuddy.",
          "com.sun.");

  /** One completed connection checkout: how long it was held and how much of that was SQL. */
  public record ConnectionSession(
      String connectionId,
      long heldMillis,
      long databaseWorkMillis,
      boolean released,
      String acquireCallSite) {

    public long idleMillis() {
      return Math.max(0, heldMillis - databaseWorkMillis);
    }
  }

  private static final class OpenSession {
    volatile long acquiredNanos;
    final String acquireCallSite;
    final AtomicLong dbWorkMillis = new AtomicLong();

    OpenSession(long acquiredNanos, String acquireCallSite) {
      this.acquiredNanos = acquiredNanos;
      this.acquireCallSite = acquireCallSite;
    }

    /** Re-anchors a connection acquired before the window so only in-window time is measured. */
    void rebase(long windowStartNanos) {
      this.acquiredNanos = windowStartNanos;
      this.dbWorkMillis.set(0);
    }
  }

  private final LongSupplier nanoClock;
  private final Map<String, OpenSession> openSessions = new ConcurrentHashMap<>();
  private final List<ConnectionSession> completedSessions =
      Collections.synchronizedList(new ArrayList<>());
  private volatile boolean active = false;

  public ConnectionUsageTracker() {
    this(System::nanoTime);
  }

  /** Clock-injectable constructor for deterministic tests. */
  public ConnectionUsageTracker(LongSupplier nanoClock) {
    this.nanoClock = nanoClock;
  }

  /**
   * Starts a fresh tracking window (called per test). Connection lifecycle bookkeeping runs
   * continuously — Spring's transaction manager acquires the test's connection before this
   * extension's own callbacks run — so connections already open are re-anchored to the window start
   * instead of being lost.
   */
  public void start() {
    long now = nanoClock.getAsLong();
    openSessions.values().forEach(open -> open.rebase(now));
    completedSessions.clear();
    active = true;
  }

  /**
   * Stops tracking. Connections still open at the end of the window are recorded as unreleased
   * sessions — a connection held to the end of the test is the worst offender, not an exemption.
   */
  public void stop() {
    active = false;
    long now = nanoClock.getAsLong();
    // Still-open connections stay in the map (they are physically open — the next window
    // re-anchors them); they are reported against this window as unreleased holds.
    openSessions.forEach(
        (id, open) ->
            completedSessions.add(
                new ConnectionSession(
                    id,
                    toMillis(now - open.acquiredNanos),
                    open.dbWorkMillis.get(),
                    false,
                    open.acquireCallSite)));
  }

  /** Sessions completed during the current window, releases and end-of-window holds alike. */
  public List<ConnectionSession> getCompletedSessions() {
    return List.copyOf(completedSessions);
  }

  @Override
  public void beforeMethod(MethodExecutionContext context) {
    // all bookkeeping happens after the call, when elapsed time and results exist
  }

  @Override
  public void afterMethod(MethodExecutionContext context) {
    if (context.getThrown() != null) {
      return;
    }
    ConnectionInfo info = context.getConnectionInfo();
    if (info == null || info.getConnectionId() == null) {
      return;
    }
    String methodName = context.getMethod().getName();
    String connectionId = info.getConnectionId();

    if ("getConnection".equals(methodName) && context.getTarget() instanceof DataSource) {
      openSessions.put(
          connectionId, new OpenSession(nanoClock.getAsLong(), captureAcquireCallSite()));
      return;
    }
    if (DB_WORK_METHODS.contains(methodName)) {
      OpenSession open = openSessions.get(connectionId);
      if (open != null) {
        open.dbWorkMillis.addAndGet(Math.max(0, context.getElapsedTime()));
      }
      return;
    }
    if ("close".equals(methodName) && context.getTarget() instanceof java.sql.Connection) {
      OpenSession open = openSessions.remove(connectionId);
      if (open != null && active) {
        completedSessions.add(
            new ConnectionSession(
                connectionId,
                toMillis(nanoClock.getAsLong() - open.acquiredNanos),
                open.dbWorkMillis.get(),
                true,
                open.acquireCallSite));
      }
    }
  }

  private static long toMillis(long nanos) {
    return nanos / 1_000_000L;
  }

  private static String captureAcquireCallSite() {
    for (StackTraceElement frame : Thread.currentThread().getStackTrace()) {
      String cls = frame.getClassName();
      if (SKIP_PREFIXES.stream().noneMatch(cls::startsWith) && !cls.contains("$Proxy")) {
        return frame.toString();
      }
    }
    return null;
  }
}
