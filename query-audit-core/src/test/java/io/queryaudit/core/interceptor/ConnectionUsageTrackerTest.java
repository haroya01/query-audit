package io.queryaudit.core.interceptor;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import javax.sql.DataSource;
import net.ttddyy.dsproxy.ConnectionInfo;
import net.ttddyy.dsproxy.listener.MethodExecutionContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("ConnectionUsageTracker (issue #168)")
class ConnectionUsageTrackerTest {

  private final AtomicLong nanos = new AtomicLong();
  private final ConnectionUsageTracker tracker = new ConnectionUsageTracker(nanos::get);

  private static final DataSource DATA_SOURCE =
      (DataSource)
          Proxy.newProxyInstance(
              ConnectionUsageTrackerTest.class.getClassLoader(),
              new Class<?>[] {DataSource.class},
              (proxy, method, args) -> null);

  private static final Connection CONNECTION =
      (Connection)
          Proxy.newProxyInstance(
              ConnectionUsageTrackerTest.class.getClassLoader(),
              new Class<?>[] {Connection.class},
              (proxy, method, args) -> null);

  private static final java.sql.Statement STATEMENT =
      (java.sql.Statement)
          Proxy.newProxyInstance(
              ConnectionUsageTrackerTest.class.getClassLoader(),
              new Class<?>[] {java.sql.Statement.class},
              (proxy, method, args) -> null);

  private void fire(String methodName, Object target, String connectionId, long elapsedMs)
      throws Exception {
    MethodExecutionContext context = new MethodExecutionContext();
    Method method = findMethod(target, methodName);
    context.setMethod(method);
    context.setTarget(target);
    context.setElapsedTime(elapsedMs);
    ConnectionInfo info = new ConnectionInfo();
    info.setConnectionId(connectionId);
    context.setConnectionInfo(info);
    tracker.afterMethod(context);
  }

  private static Method findMethod(Object target, String name) throws Exception {
    for (Method m : target.getClass().getMethods()) {
      if (m.getName().equals(name)) {
        return m;
      }
    }
    throw new NoSuchMethodException(name);
  }

  private void advanceMillis(long ms) {
    nanos.addAndGet(ms * 1_000_000L);
  }

  @Test
  @DisplayName("held vs database-work time: the idle gap is the finding")
  void tracksHeldVersusDbWork() throws Exception {
    tracker.start();

    fire("getConnection", DATA_SOURCE, "1", 0);
    advanceMillis(20);
    fire("executeQuery", STATEMENT, "1", 20);
    advanceMillis(300); // slow non-DB work while holding the connection
    fire("close", CONNECTION, "1", 0);

    List<ConnectionUsageTracker.ConnectionSession> sessions = tracker.getCompletedSessions();
    assertThat(sessions).hasSize(1);
    ConnectionUsageTracker.ConnectionSession session = sessions.get(0);
    assertThat(session.heldMillis()).isEqualTo(320);
    assertThat(session.databaseWorkMillis()).isEqualTo(20);
    assertThat(session.idleMillis()).isEqualTo(300);
    assertThat(session.released()).isTrue();
  }

  @Test
  @DisplayName("commit time counts as database work, not idle time")
  void commitCountsAsDbWork() throws Exception {
    tracker.start();

    fire("getConnection", DATA_SOURCE, "1", 0);
    advanceMillis(150);
    fire("executeUpdate", STATEMENT, "1", 100);
    fire("commit", CONNECTION, "1", 50);
    fire("close", CONNECTION, "1", 0);

    ConnectionUsageTracker.ConnectionSession session = tracker.getCompletedSessions().get(0);
    assertThat(session.databaseWorkMillis()).isEqualTo(150);
    assertThat(session.idleMillis()).isZero();
  }

  @Test
  @DisplayName("a connection never closed in the window is recorded as unreleased at stop()")
  void unreleasedConnectionRecordedAtStop() throws Exception {
    tracker.start();

    fire("getConnection", DATA_SOURCE, "1", 0);
    advanceMillis(500);
    tracker.stop();

    ConnectionUsageTracker.ConnectionSession session = tracker.getCompletedSessions().get(0);
    assertThat(session.heldMillis()).isEqualTo(500);
    assertThat(session.released()).isFalse();
  }

  @Test
  @DisplayName("concurrent checkouts are tracked independently by connection id")
  void independentSessions() throws Exception {
    tracker.start();

    fire("getConnection", DATA_SOURCE, "1", 0);
    advanceMillis(50);
    fire("getConnection", DATA_SOURCE, "2", 0);
    advanceMillis(50);
    fire("close", CONNECTION, "1", 0); // held 100
    advanceMillis(200);
    fire("close", CONNECTION, "2", 0); // held 250

    List<ConnectionUsageTracker.ConnectionSession> sessions = tracker.getCompletedSessions();
    assertThat(sessions).hasSize(2);
    assertThat(sessions.get(0).heldMillis()).isEqualTo(100);
    assertThat(sessions.get(1).heldMillis()).isEqualTo(250);
  }

  @Test
  @DisplayName("inactive tracker records nothing; start() clears the previous window")
  void inactiveAndReset() throws Exception {
    fire("getConnection", DATA_SOURCE, "1", 0);
    fire("close", CONNECTION, "1", 0);
    assertThat(tracker.getCompletedSessions()).isEmpty();

    tracker.start();
    fire("getConnection", DATA_SOURCE, "1", 0);
    fire("close", CONNECTION, "1", 0);
    assertThat(tracker.getCompletedSessions()).hasSize(1);

    tracker.start();
    assertThat(tracker.getCompletedSessions()).isEmpty();
  }
}
