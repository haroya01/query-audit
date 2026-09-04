package io.queryaudit.junit5;

import java.util.ArrayDeque;
import java.util.Deque;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.TestPlan;

/**
 * Reconciles an explicit audit manifest with platform events, including tests that never enter a
 * Jupiter extension callback. Registered through the JUnit Platform service loader.
 */
public final class AuditCoverageListener implements TestExecutionListener {

  private static final ThreadLocal<Deque<Execution>> EXECUTIONS =
      ThreadLocal.withInitial(ArrayDeque::new);

  private AuditCoverageSession session;
  private SessionHandle handle;

  private record Execution(SessionHandle handle, String testId) {}

  private static final class SessionHandle {
    private volatile AuditCoverageSession session;
    private volatile boolean active = true;

    SessionHandle(AuditCoverageSession session) {
      this.session = session;
    }

    void close() {
      session = null;
      active = false;
    }
  }

  static AuditCoverageSession currentSession() {
    Execution execution = currentExecution();
    return execution == null ? null : execution.handle().session;
  }

  static AuditCoverageSession currentSession(ExtensionContext context) {
    Execution execution = currentExecution();
    if (execution == null || !execution.testId().equals(context.getUniqueId())) {
      return null;
    }
    AuditCoverageSession current = execution.handle().session;
    return current != null && current.bindRoot(context.getRoot()) ? current : null;
  }

  private static Execution currentExecution() {
    Deque<Execution> executions = EXECUTIONS.get();
    while (!executions.isEmpty() && !executions.peek().handle().active) {
      executions.pop();
    }
    if (executions.isEmpty()) {
      EXECUTIONS.remove();
      return null;
    }
    return executions.peek();
  }

  @Override
  public void testPlanExecutionStarted(TestPlan testPlan) {
    session = AuditCoverageSession.open(testPlan);
    handle = new SessionHandle(session);
  }

  @Override
  public void dynamicTestRegistered(TestIdentifier testIdentifier) {
    if (session != null) {
      session.discovered(testIdentifier);
    }
  }

  @Override
  public void executionStarted(TestIdentifier testIdentifier) {
    // An inactive nested launcher must also hide its parent's session during extension callbacks.
    EXECUTIONS.get().push(new Execution(handle, testIdentifier.getUniqueId()));
    if (session != null) {
      session.started(testIdentifier);
    }
  }

  @Override
  public void executionSkipped(TestIdentifier testIdentifier, String reason) {
    if (session != null) {
      session.skipped(testIdentifier);
    }
  }

  @Override
  public void executionFinished(
      TestIdentifier testIdentifier, TestExecutionResult testExecutionResult) {
    try {
      if (session != null) {
        session.finished(testIdentifier, testExecutionResult);
      }
    } finally {
      Deque<Execution> executions = EXECUTIONS.get();
      if (!executions.isEmpty()
          && executions.peek().handle() == handle
          && executions.peek().testId().equals(testIdentifier.getUniqueId())) {
        executions.pop();
      }
      if (executions.isEmpty()) {
        EXECUTIONS.remove();
      }
    }
  }

  @Override
  public void testPlanExecutionFinished(TestPlan testPlan) {
    try {
      if (session != null) {
        session.finishWithoutExtension();
      }
    } finally {
      handle.close();
      session = null;
      currentSession();
    }
  }
}
