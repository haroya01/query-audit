package io.queryaudit.junit5;

import io.queryaudit.core.interceptor.QueryCaptureSession;
import org.junit.jupiter.api.extension.ExtensionContext;

/** Marks a known, non-audited JUnit invocation so it is not mistaken for unknown async work. */
final class InvocationSuppression implements ExtensionContext.Store.CloseableResource {
  private final String identity;
  private final QueryCaptureSession.Scope binding = QueryCaptureSession.suppress();

  InvocationSuppression(String identity) {
    this.identity = identity;
  }

  boolean belongsTo(String candidate) {
    return identity.equals(candidate);
  }

  @Override
  public void close() {
    binding.close();
  }
}
