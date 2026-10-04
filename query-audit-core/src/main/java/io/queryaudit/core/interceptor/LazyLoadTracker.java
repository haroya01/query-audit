package io.queryaudit.core.interceptor;

import io.queryaudit.core.model.LifecyclePhase;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Tracks lazy loading events for N+1 detection and explicit PK loads for findById-for-association
 * detection.
 *
 * <p>This class carries no compile-time dependency on Hibernate (issue #248) so that plain JDBC
 * auditing works without Hibernate on the runtime classpath. It is fed by a Hibernate-specific
 * adapter — {@code io.queryaudit.junit5.HibernateLazyLoadListener} — which implements Hibernate's
 * {@code InitializeCollectionEventListener} / {@code PostLoadEventListener} SPIs and translates
 * those events into calls to {@link #recordCollectionInitialized}, {@link #recordProxyResolved},
 * and {@link #recordExplicitLoad}. That adapter is only ever instantiated once Hibernate has been
 * confirmed present on the classpath.
 *
 * <p>Two kinds of events are tracked:
 *
 * <ul>
 *   <li>Collection initialization -- fires when a lazy {@code @OneToMany} / {@code @ManyToMany}
 *       collection is initialized (e.g., {@code team.getMembers()}).
 *   <li>Post-load -- fires after an entity is loaded. Combined with stack-trace inspection, this
 *       detects {@code @ManyToOne} / {@code @OneToOne} lazy proxy resolution (e.g., {@code
 *       room.getOwner().getName()}) and explicit loads via {@code findById()}.
 * </ul>
 *
 * <p>For proxy resolution, the tracker examines the call stack to distinguish proxy-triggered loads
 * from explicit loads (e.g., {@code findById}). Proxy-triggered loads are recorded as {@link
 * LazyLoadRecord}s, while explicit loads with {@code findById} in the stack trace are recorded as
 * {@link ExplicitLoadRecord}s for findById-for-association analysis.
 *
 * <p>This is the same approach used by:
 *
 * <ul>
 *   <li>nplusone (Python/SQLAlchemy) - tracks lazy_load events
 *   <li>Bullet (Ruby/ActiveRecord) - tracks association loading
 *   <li>jplusone (Java/Hibernate) - tracks implicit operations via stack inspection
 * </ul>
 *
 * @author haroya
 * @since 0.2.0
 */
public class LazyLoadTracker {

  /** Prefix used for proxy-resolution records to distinguish from collection records. */
  public static final String PROXY_ROLE_PREFIX = "proxy:";

  /** Record of a lazy load event (collection initialization or proxy resolution). */
  public record LazyLoadRecord(
      String collectionRole, // e.g., "com.example.Order.items" or "proxy:com.example.User"
      String ownerEntity, // e.g., "com.example.Order" or "com.example.User"
      String ownerIdString, // e.g., "42"
      long timestamp,
      LifecyclePhase phase) {

    public LazyLoadRecord(
        String collectionRole, String ownerEntity, String ownerIdString, long timestamp) {
      this(collectionRole, ownerEntity, ownerIdString, timestamp, LifecyclePhase.TEST);
    }
  }

  /** Record of an explicit entity load via findById (non-proxy PostLoadEvent). */
  public record ExplicitLoadRecord(
      String entityType, // e.g., "com.example.User"
      String idString, // e.g., "42"
      long timestamp,
      String stackTrace,
      LifecyclePhase phase) {

    public ExplicitLoadRecord(String entityType, String idString, long timestamp, String stackTrace) {
      this(entityType, idString, timestamp, stackTrace, LifecyclePhase.TEST);
    }
  }

  private static final int DEFAULT_MAX_EVENTS = 10_000;

  private final Object lock = new Object();
  private final List<LazyLoadRecord> records = new ArrayList<>();
  private final List<ExplicitLoadRecord> explicitLoads = new ArrayList<>();
  private volatile boolean active = false;
  private volatile LifecyclePhase phase = LifecyclePhase.TEST;
  private final boolean captureLeaf;
  private final int maxEvents;
  private int droppedEvents;

  public LazyLoadTracker() {
    this(false, DEFAULT_MAX_EVENTS);
  }

  LazyLoadTracker(boolean captureLeaf, int maxEvents) {
    this.captureLeaf = captureLeaf;
    this.maxEvents = maxEvents;
  }

  // ── Collection initialization (recorded via HibernateLazyLoadListener) ────

  /** Records a lazy collection initialization event (e.g., a {@code @OneToMany} access). */
  public void recordCollectionInitialized(
      String collectionRole, String ownerEntity, Object ownerId) {
    if (!captureLeaf
        && QueryCaptureSession.routeLazy(
            this,
            tracker -> tracker.recordCollectionInitialized(collectionRole, ownerEntity, ownerId)))
      return;
    if (!active) return;

    append(
        records,
        new LazyLoadRecord(
            collectionRole != null ? collectionRole : "unknown",
            ownerEntity,
            ownerId != null ? ownerId.toString() : "null",
            System.currentTimeMillis(),
            phase));
  }

  // ── Post-load (proxy resolution / explicit load) ───────────────────────

  /** Records a lazy proxy resolution (e.g., accessing a {@code @ManyToOne} association). */
  public void recordProxyResolved(String entityName, Object id) {
    if (!captureLeaf
        && QueryCaptureSession.routeLazy(
            this, tracker -> tracker.recordProxyResolved(entityName, id))) return;
    if (!active) return;

    String deproxied = deproxyClassName(entityName);
    append(
        records,
        new LazyLoadRecord(
            PROXY_ROLE_PREFIX + deproxied,
            deproxied,
            id != null ? id.toString() : "null",
            System.currentTimeMillis(),
            phase));
  }

  /**
   * Records an explicit entity load via {@code findById()}, for findById-for-association analysis.
   */
  public void recordExplicitLoad(String entityName, Object id, String stackTrace) {
    if (!captureLeaf
        && QueryCaptureSession.routeLazy(
            this, tracker -> tracker.recordExplicitLoad(entityName, id, stackTrace))) return;
    if (!active) return;

    append(
        explicitLoads,
        new ExplicitLoadRecord(
            deproxyClassName(entityName),
            id != null ? id.toString() : "null",
            System.currentTimeMillis(),
            stackTrace,
            phase));
  }

  /**
   * Checks whether the current call stack contains a {@code findById} invocation, indicating an
   * explicit entity load via Spring Data's {@code CrudRepository.findById()} or similar.
   *
   * @return true if {@code findById} is found in the call stack
   */
  public static boolean hasFindByIdInStack() {
    StackTraceElement[] stack = Thread.currentThread().getStackTrace();
    for (StackTraceElement frame : stack) {
      if ("findById".equals(frame.getMethodName())) return true;
    }
    return false;
  }

  /**
   * Captures a compact stack trace of application frames (up to 10), filtering out framework
   * classes (Spring, Hibernate, java.*, javax.*, proxy classes).
   */
  public static String captureApplicationStack() {
    StackTraceElement[] stack = Thread.currentThread().getStackTrace();
    StringBuilder sb = new StringBuilder();
    int count = 0;
    for (StackTraceElement frame : stack) {
      String cls = frame.getClassName();
      if (cls.startsWith("java.") || cls.startsWith("javax.") || cls.startsWith("jdk.")) continue;
      if (cls.startsWith("org.hibernate.") || cls.startsWith("org.springframework.")) continue;
      if (CallSites.isGeneratedSubclass(cls)) continue;
      if (cls.startsWith("io.queryaudit.")) continue;
      if (cls.startsWith("sun.") || cls.startsWith("com.sun.")) continue;

      if (count > 0) sb.append('\n');
      sb.append(cls).append('.').append(frame.getMethodName());
      sb.append('(')
          .append(frame.getFileName())
          .append(':')
          .append(frame.getLineNumber())
          .append(')');
      if (++count >= 10) break;
    }
    return sb.toString();
  }

  /**
   * Checks whether the current load was triggered by Hibernate proxy resolution by inspecting the
   * call stack for proxy interceptor classes.
   *
   * <p>Hibernate 6 uses ByteBuddy to generate proxy classes. When a lazy proxy is accessed (e.g.,
   * {@code room.getOwner().getName()}), the call goes through ByteBuddy's interceptor which
   * triggers the actual entity load. Explicit loads via {@code EntityManager.find()} or Spring
   * Data's {@code findById()} do NOT pass through these interceptor classes.
   *
   * @return true if the call stack indicates proxy resolution
   */
  public static boolean isProxyResolution() {
    StackTraceElement[] stack = Thread.currentThread().getStackTrace();
    for (StackTraceElement frame : stack) {
      String cls = frame.getClassName();

      // ByteBuddy interceptor (Hibernate 6 default proxy mechanism)
      if (cls.contains("$HibernateProxy$") || cls.endsWith("$HibernateProxy")) return true;
      if (cls.contains("$ByteBuddy$")) return true;

      // Legacy CGLIB proxy (older Hibernate / Spring proxies)
      if (cls.contains("$$EnhancerByCGLIB$$")) return true;
      if (cls.contains("$$_jvst")) return true;

      // Hibernate's lazy initializer
      String method = frame.getMethodName();
      if (cls.contains("ByteBuddyInterceptor") && "intercept".equals(method)) return true;
      if (cls.contains("AbstractLazyInitializer") && "initialize".equals(method)) return true;
    }
    return false;
  }

  /**
   * Strips Hibernate/ByteBuddy proxy suffixes from the class name.
   *
   * <p>Example: {@code "com.example.User$HibernateProxy$abc123"} becomes {@code
   * "com.example.User"}.
   */
  static String deproxyClassName(String className) {
    int idx = className.indexOf("$HibernateProxy$");
    if (idx > 0) return className.substring(0, idx);
    if (className.endsWith("$HibernateProxy")) {
      return className.substring(0, className.length() - "$HibernateProxy".length());
    }
    idx = className.indexOf("$ByteBuddy$");
    if (idx > 0) return className.substring(0, idx);
    idx = className.indexOf("$$EnhancerByCGLIB$$");
    if (idx > 0) return className.substring(0, idx);
    return className;
  }

  // ── Lifecycle ────────────────────────────────────────────────────

  private <T> void append(List<T> events, T event) {
    synchronized (lock) {
      if (records.size() + explicitLoads.size() >= maxEvents) {
        droppedEvents++;
        return;
      }
      events.add(event);
    }
  }

  public void start() {
    clear();
    phase = LifecyclePhase.TEST;
    active = true;
  }

  public void stop() {
    active = false;
  }

  /**
   * Sets the lifecycle phase that events recorded after this call carry.
   *
   * @since 0.8.0
   */
  public void setPhase(LifecyclePhase phase) {
    this.phase = Objects.requireNonNull(phase, "phase");
  }

  public void clear() {
    synchronized (lock) {
      records.clear();
      explicitLoads.clear();
      droppedEvents = 0;
    }
  }

  public List<LazyLoadRecord> getRecords() {
    synchronized (lock) {
      return List.copyOf(records);
    }
  }

  public List<ExplicitLoadRecord> getExplicitLoads() {
    synchronized (lock) {
      return List.copyOf(explicitLoads);
    }
  }

  /**
   * Returns the lazy-load events of the test body, or of every phase when {@code includeSetup} is
   * true.
   *
   * @since 0.8.0
   */
  public List<LazyLoadRecord> getRecords(boolean includeSetup) {
    return getRecords().stream()
        .filter(record -> includeSetup || record.phase() == LifecyclePhase.TEST)
        .toList();
  }

  /**
   * Returns the explicit loads of the test body, or of every phase when {@code includeSetup} is
   * true.
   *
   * @since 0.8.0
   */
  public List<ExplicitLoadRecord> getExplicitLoads(boolean includeSetup) {
    return getExplicitLoads().stream()
        .filter(record -> includeSetup || record.phase() == LifecyclePhase.TEST)
        .toList();
  }

  /** Returns how many events were not recorded because the capture limit was reached. */
  public int getDroppedEventCount() {
    synchronized (lock) {
      return droppedEvents;
    }
  }

  public boolean isActive() {
    return active || (!captureLeaf && QueryCaptureSession.lazyRouterActive(this));
  }
}
