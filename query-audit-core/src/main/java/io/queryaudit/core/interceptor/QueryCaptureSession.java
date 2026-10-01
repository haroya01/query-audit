package io.queryaudit.core.interceptor;

import io.queryaudit.core.model.LifecyclePhase;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import net.ttddyy.dsproxy.ExecutionInfo;
import net.ttddyy.dsproxy.QueryInfo;
import net.ttddyy.dsproxy.listener.MethodExecutionContext;

/**
 * One invocation's capture state, independent of the shared DataSource listener. Context is local
 * to the current thread; use {@link #wrap(Runnable)} or {@link #wrap(Callable)} when submitting
 * work to an executor, and join that work before stopping the session.
 */
public final class QueryCaptureSession implements AutoCloseable {
  /** A lexical context binding with no checked exception on close. */
  public interface Scope extends AutoCloseable {
    @Override
    void close();
  }

  private static final ThreadLocal<Binding> CURRENT = new ThreadLocal<>();
  private static final ConcurrentHashMap<QueryInterceptor, Set<QueryCaptureSession>> ACTIVE =
      new ConcurrentHashMap<>();
  private static final ConcurrentHashMap<LazyLoadTracker, Set<QueryCaptureSession>> LAZY_ACTIVE =
      new ConcurrentHashMap<>();
  private static final AtomicLong ROUTER_IDS = new AtomicLong();
  private static final AtomicInteger UNBOUND_WORK_CLAIMS = new AtomicInteger();
  private static final Origin IGNORED = new Origin(null, LifecyclePhase.TEST, false);
  private static final Origin LEGACY = new Origin(null, LifecyclePhase.TEST, false);

  private final QueryInterceptor router;
  private final LazyLoadTracker lazyRouter;
  private final QueryInterceptor interceptor = new QueryInterceptor(true);
  private final LazyLoadTracker tracker;
  private final String testId;
  private final Set<String> incompleteReasons = new ConcurrentSkipListSet<>();
  private final BindingScope binding;
  private volatile LifecyclePhase phase = LifecyclePhase.TEST;
  private boolean stopped;
  private volatile boolean closed;
  private volatile boolean adoptsUnboundWork;
  private int inFlightQueries;
  private int runningTasks;
  private int pendingTasks;
  private QueryCaptureSnapshot snapshot;

  private QueryCaptureSession(
      QueryInterceptor router, LazyLoadTracker lazyRouter, String testId, int maxQueries) {
    this.router = Objects.requireNonNull(router, "router");
    this.lazyRouter = lazyRouter;
    this.testId = Objects.requireNonNull(testId, "testId");
    if (testId.isBlank()) throw new IllegalArgumentException("testId must not be blank");
    interceptor.setMaxQueries(maxQueries);
    interceptor.start();
    router.getConnectionTracker().copyOpenConnectionsTo(interceptor.getConnectionTracker());
    tracker = lazyRouter == null ? null : new LazyLoadTracker(true);
    if (tracker != null) tracker.start();
    add(ACTIVE, router, this);
    if (lazyRouter != null) {
      add(LAZY_ACTIVE, lazyRouter, this);
    }
    binding = bind(new Binding(this, false, false));
  }

  /** Opens and binds a fresh capture; nested sessions temporarily replace the outer binding. */
  public static QueryCaptureSession open(
      QueryInterceptor router, LazyLoadTracker lazyRouter, String testId, int maxQueries) {
    return new QueryCaptureSession(router, lazyRouter, testId, maxQueries);
  }

  public QueryInterceptor interceptor() {
    return interceptor;
  }

  /** Returns the invocation's Hibernate event collector, or null when no router was supplied. */
  public LazyLoadTracker tracker() {
    return tracker;
  }

  public String testId() {
    return testId;
  }

  public void setPhase(LifecyclePhase phase) {
    this.phase = Objects.requireNonNull(phase, "phase");
    interceptor.setPhase(phase);
  }

  /** Stops this invocation, not whichever invocation happens to be bound to the caller. */
  public synchronized QueryCaptureSnapshot stop() {
    if (!stopped) {
      stopped = true;
      if (inFlightQueries > 0) incompleteReasons.add("QUERY_STILL_RUNNING");
      if (runningTasks > 0) incompleteReasons.add("ASYNC_WORK_STILL_RUNNING");
      if (pendingTasks > 0) incompleteReasons.add("ASYNC_WORK_NOT_COMPLETED");
      interceptor.stop();
      if (tracker != null) tracker.stop();
      snapshot = interceptor.snapshot();
      remove(ACTIVE, router, this);
      if (lazyRouter != null) remove(LAZY_ACTIVE, lazyRouter, this);
    }
    return snapshot;
  }

  /**
   * Counts SQL and lazy loads from threads without a capture binding toward this session while it
   * is the only active capture on its router. With several active captures that work stays
   * unattributed, because it cannot be assigned to one of them.
   */
  public void adoptUnboundWork() {
    adoptsUnboundWork = true;
  }

  /** Returns immutable diagnostic codes; SQL, identifiers, and exception messages are excluded. */
  public List<String> incompleteReasons() {
    return List.copyOf(incompleteReasons);
  }

  @Override
  public synchronized void close() {
    if (closed) return;
    stop();
    closed = true;
    binding.close();
  }

  /**
   * Reserves one task in the current capture and restores the worker's binding after execution. The
   * returned task is single-use and must complete before the capture stops. Queued, rejected,
   * cancelled, or never-submitted tasks make that capture incomplete.
   */
  public static Runnable wrap(Runnable task) {
    Objects.requireNonNull(task, "task");
    Binding captured = forPropagation(current());
    TaskReservation reservation = new TaskReservation(captured);
    return () -> {
      try (Scope ignored = bind(captured);
          Scope work = reservation.start()) {
        task.run();
      }
    };
  }

  /** Callable counterpart of {@link #wrap(Runnable)}. */
  public static <T> Callable<T> wrap(Callable<T> task) {
    Objects.requireNonNull(task, "task");
    Binding captured = forPropagation(current());
    TaskReservation reservation = new TaskReservation(captured);
    return () -> {
      try (Scope ignored = bind(captured);
          Scope work = reservation.start()) {
        return task.call();
      }
    };
  }

  public static Scope claimUnboundWork() {
    UNBOUND_WORK_CLAIMS.incrementAndGet();
    AtomicBoolean released = new AtomicBoolean();
    return () -> {
      if (released.compareAndSet(false, true)) UNBOUND_WORK_CLAIMS.decrementAndGet();
    };
  }

  /** Excludes framework-owned metadata/EXPLAIN work from all invocation captures on this thread. */
  public static Scope suppress() {
    return bind(new Binding(null, true, false));
  }

  static String newRouterKey() {
    return QueryCaptureSession.class.getName() + ":" + ROUTER_IDS.incrementAndGet();
  }

  static void beforeQuery(QueryInterceptor router, ExecutionInfo execution) {
    Binding current = current();
    Origin origin = origin(router, current, true);
    execution.addCustomValue(router.captureKey(), origin == null ? LEGACY : origin);
  }

  static boolean afterQuery(
      QueryInterceptor router, ExecutionInfo execution, List<QueryInfo> queries) {
    Origin origin = execution.getCustomValue(router.captureKey(), Origin.class);
    if (origin == null) origin = origin(router, current(), false);
    if (origin == null || origin == LEGACY) return false;
    if (origin.session() != null) {
      origin.session().finishQuery(execution, queries, origin);
      return !router.isActive();
    }
    return true;
  }

  private static Origin origin(QueryInterceptor router, Binding binding, boolean started) {
    if (binding != null) {
      if (binding.suppressed()) return IGNORED;
      if (binding.session().router != router) return router.isActive() ? LEGACY : IGNORED;
      Origin selected = binding.session().beginQuery(started);
      return selected == IGNORED && router.isActive() ? LEGACY : selected;
    }
    Set<QueryCaptureSession> sessions = ACTIVE.get(router);
    if (sessions == null || sessions.isEmpty()) return null;
    QueryCaptureSession adopter = soleAdopter(sessions);
    if (adopter != null) {
      Origin selected = adopter.beginQuery(started);
      return selected == IGNORED && router.isActive() ? LEGACY : selected;
    }
    if (UNBOUND_WORK_CLAIMS.get() == 0) {
      sessions.forEach(session -> session.incompleteReasons.add("UNATTRIBUTED_QUERY"));
    }
    return router.isActive() ? LEGACY : IGNORED;
  }

  private static QueryCaptureSession soleAdopter(Set<QueryCaptureSession> sessions) {
    Iterator<QueryCaptureSession> active = sessions.iterator();
    if (!active.hasNext()) return null;
    QueryCaptureSession only = active.next();
    return !active.hasNext() && only.adoptsUnboundWork ? only : null;
  }

  private synchronized Origin beginQuery(boolean started) {
    if (stopped) {
      incompleteReasons.add("WORK_AFTER_CAPTURE");
      return IGNORED;
    }
    if (started) inFlightQueries++;
    return new Origin(this, phase, started);
  }

  private synchronized void finishQuery(
      ExecutionInfo execution, List<QueryInfo> queries, Origin origin) {
    if (origin.started()) inFlightQueries--;
    if (stopped) {
      incompleteReasons.add("QUERY_COMPLETED_AFTER_CAPTURE");
      return;
    }
    interceptor.recordQuery(execution, queries, origin.phase());
  }

  static boolean routeLazy(LazyLoadTracker router, Consumer<LazyLoadTracker> record) {
    Binding binding = current();
    if (binding != null) {
      if (!binding.suppressed() && binding.session().lazyRouter == router) {
        binding.session().recordLazy(record);
      }
      return true;
    }
    Set<QueryCaptureSession> sessions = LAZY_ACTIVE.get(router);
    if (sessions == null || sessions.isEmpty()) return false;
    QueryCaptureSession adopter = soleAdopter(sessions);
    if (adopter != null) {
      adopter.recordLazy(record);
      return true;
    }
    if (UNBOUND_WORK_CLAIMS.get() == 0) {
      sessions.forEach(session -> session.incompleteReasons.add("UNATTRIBUTED_LAZY_LOAD"));
    }
    return true;
  }

  static boolean lazyRouterActive(LazyLoadTracker router) {
    Binding binding = current();
    if (binding != null) return !binding.suppressed() && binding.session().lazyRouter == router;
    Set<QueryCaptureSession> sessions = LAZY_ACTIVE.get(router);
    return sessions != null && !sessions.isEmpty();
  }

  private synchronized void recordLazy(Consumer<LazyLoadTracker> record) {
    if (stopped) incompleteReasons.add("WORK_AFTER_CAPTURE");
    else record.accept(tracker);
  }

  static void beforeConnection(QueryInterceptor router, MethodExecutionContext context) {
    Binding binding = current();
    context.addCustomValue(router.captureKey(), binding == null ? IGNORED : binding);
  }

  static boolean afterConnection(QueryInterceptor router, MethodExecutionContext context) {
    Object pinned = context.getCustomValue(router.captureKey(), Object.class);
    Binding binding = pinned instanceof Binding value ? value : current();
    if (pinned == IGNORED) binding = null;
    if (binding != null) {
      if (!binding.suppressed() && binding.session().router == router) {
        binding.session().recordConnection(context);
      }
      return binding.suppressed() || !router.isActive();
    }
    // Keep pre-window connection bookkeeping for Spring's transaction callback ordering. Only
    // same-thread checkouts are imported into a newly opened session.
    return false;
  }

  private synchronized void recordConnection(MethodExecutionContext context) {
    // Transaction managers can release/roll back after the audit's afterEach callback. Physical
    // bookkeeping continues in the router; actual late SQL is diagnosed by the query listener.
    if (!stopped) interceptor.getConnectionTracker().recordAfterMethod(context);
  }

  private static <T> void remove(
      ConcurrentHashMap<T, Set<QueryCaptureSession>> sessionsByRouter,
      T router,
      QueryCaptureSession session) {
    sessionsByRouter.computeIfPresent(
        router,
        (ignored, sessions) -> {
          sessions.remove(session);
          return sessions.isEmpty() ? null : sessions;
        });
  }

  private static <T> void add(
      ConcurrentHashMap<T, Set<QueryCaptureSession>> sessionsByRouter,
      T router,
      QueryCaptureSession session) {
    sessionsByRouter.compute(
        router,
        (ignored, sessions) -> {
          if (sessions == null) sessions = ConcurrentHashMap.newKeySet();
          sessions.add(session);
          return sessions;
        });
  }

  private static Binding forPropagation(Binding binding) {
    return binding == null ? null : new Binding(binding.session(), binding.suppressed(), true);
  }

  private static Binding current() {
    Binding binding = CURRENT.get();
    while (binding != null
        && (binding.closed
            || (!binding.suppressed() && !binding.propagated() && binding.session().closed))) {
      binding = binding.previous;
    }
    if (binding == null) CURRENT.remove();
    else CURRENT.set(binding);
    return binding;
  }

  private static BindingScope bind(Binding binding) {
    Binding previous = current();
    Binding installed =
        binding == null
            ? null
            : new Binding(binding.session(), binding.suppressed(), binding.propagated(), previous);
    BindingScope scope = new BindingScope(previous, installed);
    if (installed == null) CURRENT.remove();
    else CURRENT.set(installed);
    return scope;
  }

  private static final class Binding {
    private final QueryCaptureSession session;
    private final boolean suppressed;
    private final boolean propagated;
    private final Binding previous;
    private volatile boolean closed;

    Binding(QueryCaptureSession session, boolean suppressed, boolean propagated) {
      this(session, suppressed, propagated, null);
    }

    Binding(QueryCaptureSession session, boolean suppressed, boolean propagated, Binding previous) {
      this.session = session;
      this.suppressed = suppressed;
      this.propagated = propagated;
      this.previous = previous;
    }

    QueryCaptureSession session() {
      return session;
    }

    boolean suppressed() {
      return suppressed;
    }

    boolean propagated() {
      return propagated;
    }
  }

  private static final class TaskReservation {
    private final QueryCaptureSession session;
    private final AtomicBoolean started = new AtomicBoolean();

    TaskReservation(Binding binding) {
      session = binding == null || binding.suppressed() ? null : binding.session();
      if (session != null) {
        synchronized (session) {
          session.pendingTasks++;
          if (session.stopped) session.incompleteReasons.add("WORK_AFTER_CAPTURE");
        }
      }
    }

    Scope start() {
      if (!started.compareAndSet(false, true)) {
        if (session != null) session.incompleteReasons.add("ASYNC_TASK_REUSED");
        throw new IllegalStateException("A captured task may only be executed once");
      }
      if (session == null) return () -> {};
      synchronized (session) {
        session.pendingTasks--;
        session.runningTasks++;
        if (session.stopped) session.incompleteReasons.add("WORK_AFTER_CAPTURE");
      }
      return () -> {
        synchronized (session) {
          session.runningTasks--;
        }
      };
    }
  }

  private record Origin(QueryCaptureSession session, LifecyclePhase phase, boolean started) {}

  private static final class BindingScope implements Scope {
    private final Thread owner = Thread.currentThread();
    private final Binding previous;
    private final Binding installed;
    private boolean closed;

    private BindingScope(Binding previous, Binding installed) {
      this.previous = previous;
      this.installed = installed;
    }

    @Override
    public synchronized void close() {
      if (closed) return;
      closed = true;
      if (installed != null) installed.closed = true;
      if (Thread.currentThread() != owner) {
        if (installed != null && installed.session() != null) {
          installed.session().incompleteReasons.add("CAPTURE_THREAD_CHANGED");
        }
        return;
      }
      if (CURRENT.get() != installed) {
        if (installed != null && installed.session() != null) {
          installed.session().incompleteReasons.add("CAPTURE_SCOPE_MISMATCH");
        }
        return;
      }
      if (previous == null) CURRENT.remove();
      else CURRENT.set(previous);
    }
  }
}
