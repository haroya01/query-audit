package io.queryaudit.junit5;

import io.queryaudit.core.interceptor.LazyLoadTracker;
import io.queryaudit.core.interceptor.QueryCaptureSession;
import io.queryaudit.core.interceptor.QueryCaptureSnapshot;
import io.queryaudit.core.interceptor.QueryInterceptor;
import io.queryaudit.core.model.LifecyclePhase;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;

/**
 * Resources owned by one initialized JUnit audit scope, not by the shared extension catalog. Only
 * installed hooks and holders are released: the application DataSource and extension beans remain
 * owned by their creator. Cleanup is idempotent and attempts every action after a failure.
 */
final class AuditResources {
  private final String owner;
  private final QueryInterceptor interceptor;
  private final DataSource dataSource;
  private final Runnable hookCleanup;
  private final Runnable holderCleanup;
  private LazyLoadTracker tracker;
  private Runnable trackerCleanup;
  private boolean closed;
  private final Set<InvocationCapture> captures = ConcurrentHashMap.newKeySet();

  AuditResources(
      String owner, QueryInterceptor interceptor, DataSource dataSource, Runnable hookCleanup) {
    this(owner, interceptor, dataSource, hookCleanup, QueryAuditDataSourceStore::clear);
  }

  AuditResources(
      String owner,
      QueryInterceptor interceptor,
      DataSource dataSource,
      Runnable hookCleanup,
      Runnable holderCleanup) {
    this.owner = Objects.requireNonNull(owner, "owner");
    this.interceptor = interceptor;
    this.dataSource = dataSource;
    this.hookCleanup = hookCleanup;
    this.holderCleanup = Objects.requireNonNull(holderCleanup, "holderCleanup");
  }

  boolean isOwnedBy(String candidate) {
    return owner.equals(candidate);
  }

  QueryInterceptor interceptor() {
    return interceptor;
  }

  DataSource dataSource() {
    return dataSource;
  }

  LazyLoadTracker tracker() {
    return tracker;
  }

  synchronized void attachTracker(LazyLoadTracker tracker, Runnable cleanup) {
    if (closed || this.tracker != null) {
      throw new IllegalStateException("Audit tracker has already been attached or closed");
    }
    this.tracker = Objects.requireNonNull(tracker, "tracker");
    this.trackerCleanup = Objects.requireNonNull(cleanup, "cleanup");
  }

  void startCapture() {
    interceptor.start();
    interceptor.setPhase(LifecyclePhase.SETUP);
    if (tracker != null) {
      tracker.start();
    }
  }

  synchronized InvocationCapture openCapture(AuditScope scope, int maxQueries) {
    if (closed) throw new IllegalStateException("Audit resources have already closed");
    QueryCaptureSession session =
        QueryCaptureSession.open(interceptor, tracker, scope.identity(), maxQueries);
    session.setPhase(LifecyclePhase.SETUP);
    InvocationCapture capture = new InvocationCapture(scope, this, session);
    captures.add(capture);
    return capture;
  }

  void releaseCapture(InvocationCapture capture) {
    captures.remove(capture);
  }

  QueryCaptureSnapshot stopCapture() {
    interceptor.stop();
    if (tracker != null) {
      tracker.stop();
    }
    return interceptor.snapshot();
  }

  synchronized boolean isClosed() {
    return closed;
  }

  synchronized Throwable close(Throwable failure) {
    if (closed) {
      return failure;
    }
    closed = true;
    for (InvocationCapture capture : captures) {
      failure = runCleanup(failure, capture::close);
    }
    failure = runCleanup(failure, trackerCleanup);
    failure = runCleanup(failure, hookCleanup);
    return runCleanup(failure, holderCleanup);
  }

  static Throwable runCleanup(Throwable failure, Runnable cleanup) {
    if (cleanup == null) {
      return failure;
    }
    try {
      cleanup.run();
    } catch (RuntimeException | Error cleanupFailure) {
      if (failure == null) {
        return cleanupFailure;
      }
      if (failure != cleanupFailure) {
        failure.addSuppressed(cleanupFailure);
      }
    }
    return failure;
  }
}
