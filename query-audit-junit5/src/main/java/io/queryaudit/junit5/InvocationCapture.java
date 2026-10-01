package io.queryaudit.junit5;

import io.queryaudit.core.interceptor.QueryCaptureSession;
import io.queryaudit.core.interceptor.QueryCaptureSnapshot;
import io.queryaudit.core.model.IncompleteReasonCode;
import org.junit.jupiter.api.extension.ExtensionContext;

/** One invocation owns its evidence; the method Store closes it after all lifecycle callbacks. */
final class InvocationCapture implements ExtensionContext.Store.CloseableResource {
  private final AuditScope scope;
  private final AuditResources owner;
  private final QueryCaptureSession session;
  private Runnable backgroundWork;

  InvocationCapture(AuditScope scope, AuditResources owner, QueryCaptureSession session) {
    this.scope = scope;
    this.owner = owner;
    this.session = session;
  }

  boolean belongsTo(String identity) {
    return scope.identity().equals(identity);
  }

  QueryCaptureSession session() {
    return session;
  }

  void includeBackgroundWork(Runnable awaitIdle) {
    backgroundWork = awaitIdle;
    session.adoptUnboundWork();
  }

  void awaitBackgroundWork() {
    if (backgroundWork != null) backgroundWork.run();
  }

  QueryCaptureSnapshot stop() {
    QueryCaptureSnapshot snapshot = session.stop();
    recordProblems();
    return snapshot;
  }

  private void recordProblems() {
    for (String reason : session.incompleteReasons()) {
      AuditDiagnostics.incomplete(
          scope,
          IncompleteReasonCode.AUDIT_ANALYSIS_FAILED,
          "Query capture incomplete [" + reason + "] for " + scope.identity());
    }
  }

  @Override
  public void close() {
    try {
      session.close();
    } finally {
      try {
        recordProblems();
      } finally {
        owner.releaseCapture(this);
      }
    }
  }
}
