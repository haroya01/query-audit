package io.queryaudit.core.interceptor;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.hibernate.event.spi.InitializeCollectionEvent;
import org.hibernate.event.spi.InitializeCollectionEventListener;
import org.hibernate.event.spi.PostLoadEvent;
import org.hibernate.event.spi.PostLoadEventListener;

/**
 * Tracks Hibernate lazy loading events for N+1 detection.
 *
 * <p>Listens for two types of Hibernate events:
 *
 * <ul>
 *   <li>{@link InitializeCollectionEvent} -- fires when a lazy {@code @OneToMany} /
 *       {@code @ManyToMany} collection is initialized (e.g., {@code team.getMembers()}).
 *   <li>{@link PostLoadEvent} -- fires after an entity is loaded. Combined with stack-trace
 *       inspection, this detects {@code @ManyToOne} / {@code @OneToOne} lazy proxy resolution
 *       (e.g., {@code room.getOwner().getName()}).
 * </ul>
 *
 * <p>For proxy resolution, the tracker examines the call stack to distinguish proxy-triggered loads
 * from explicit loads (e.g., {@code findById}). Only loads triggered through Hibernate's proxy
 * interceptor are recorded.
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
public class LazyLoadTracker implements InitializeCollectionEventListener, PostLoadEventListener {

  /** Prefix used for proxy-resolution records to distinguish from collection records. */
  public static final String PROXY_ROLE_PREFIX = "proxy:";

  /** Record of a lazy load event (collection initialization or proxy resolution). */
  public record LazyLoadRecord(
      String collectionRole, // e.g., "com.example.Order.items" or "proxy:com.example.User"
      String ownerEntity, // e.g., "com.example.Order" or "com.example.User"
      String ownerIdString, // e.g., "42"
      long timestamp) {}

  private final CopyOnWriteArrayList<LazyLoadRecord> records = new CopyOnWriteArrayList<>();
  private volatile boolean active = false;

  // ── InitializeCollectionEventListener (collections) ──────────────

  @Override
  public void onInitializeCollection(InitializeCollectionEvent event) {
    if (!active) return;

    String role = event.getCollection() != null ? event.getCollection().getRole() : "unknown";
    String ownerEntity = event.getAffectedOwnerEntityName();
    Object ownerId = event.getAffectedOwnerIdOrNull();

    records.add(
        new LazyLoadRecord(
            role,
            ownerEntity,
            ownerId != null ? ownerId.toString() : "null",
            System.currentTimeMillis()));
  }

  // ── PostLoadEventListener (proxy resolution) ─────────────────────

  @Override
  public void onPostLoad(PostLoadEvent event) {
    if (!active) return;

    // Only record loads triggered by proxy resolution, not explicit findById calls
    if (!isProxyResolution()) return;

    String entityName = event.getEntity().getClass().getName();
    entityName = deproxyClassName(entityName);

    Object id = event.getId();

    records.add(
        new LazyLoadRecord(
            PROXY_ROLE_PREFIX + entityName,
            entityName,
            id != null ? id.toString() : "null",
            System.currentTimeMillis()));
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
  static boolean isProxyResolution() {
    StackTraceElement[] stack = Thread.currentThread().getStackTrace();
    for (StackTraceElement frame : stack) {
      String cls = frame.getClassName();

      // ByteBuddy interceptor (Hibernate 6 default proxy mechanism)
      if (cls.contains("$HibernateProxy$")) return true;
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
    idx = className.indexOf("$ByteBuddy$");
    if (idx > 0) return className.substring(0, idx);
    idx = className.indexOf("$$EnhancerByCGLIB$$");
    if (idx > 0) return className.substring(0, idx);
    return className;
  }

  // ── Lifecycle ────────────────────────────────────────────────────

  public void start() {
    records.clear();
    active = true;
  }

  public void stop() {
    active = false;
  }

  public void clear() {
    records.clear();
  }

  public List<LazyLoadRecord> getRecords() {
    return List.copyOf(records);
  }

  public boolean isActive() {
    return active;
  }
}
