package io.queryaudit.junit5;

import io.queryaudit.core.interceptor.LazyLoadTracker;
import io.queryaudit.core.interceptor.QueryInterceptor;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.extension.ExtensionContext;

/** Read-only bridge for hosts and callback fixtures that still seed individual Store keys. */
final class LegacyAuditResourceStore {
  private static final String INTERCEPTOR = "interceptor";
  private static final String DATA_SOURCE = "dataSource";
  private static final String TRACKER = "lazyLoadTracker";
  private static final String HOOK_CLEANUP = "dataSourceHookCleanup";
  private static final String OWNER = "auditResourceOwner";

  private LegacyAuditResourceStore() {}

  static QueryInterceptor interceptor(ExtensionContext context) {
    return inherited(context, INTERCEPTOR, QueryInterceptor.class);
  }

  static DataSource dataSource(ExtensionContext context) {
    return inherited(context, DATA_SOURCE, DataSource.class);
  }

  static LazyLoadTracker tracker(ExtensionContext context) {
    return inherited(context, TRACKER, LazyLoadTracker.class);
  }

  static boolean isOwnedBy(ExtensionContext context, String identity) {
    return identity.equals(inherited(context, OWNER, String.class));
  }

  static AuditResources capture(ExtensionContext context, String identity) {
    // A legacy capture view does not own hook removal. Cleanup is materialized separately at the
    // owner's close boundary, where the host's HibernateIntegration instance is available.
    AuditResources resources =
        new AuditResources(identity, interceptor(context), dataSource(context), null, () -> {});
    LazyLoadTracker tracker = tracker(context);
    if (tracker != null) resources.attachTracker(tracker, () -> {});
    return resources;
  }

  static AuditResources cleanup(
      ExtensionContext context, String identity, HibernateIntegration hibernate) {
    AuditResources resources =
        new AuditResources(
            identity,
            interceptor(context),
            dataSource(context),
            inherited(context, HOOK_CLEANUP, Runnable.class));
    LazyLoadTracker tracker = tracker(context);
    if (tracker != null) {
      resources.attachTracker(tracker, () -> hibernate.unregisterTracker(context, tracker));
    }
    return resources;
  }

  static void clear(ExtensionContext.Store store) {
    for (String key : List.of(INTERCEPTOR, DATA_SOURCE, TRACKER, HOOK_CLEANUP, OWNER)) {
      store.remove(key);
    }
  }

  private static <T> T inherited(ExtensionContext context, String key, Class<T> type) {
    for (ExtensionContext current = context;
        current != null;
        current = current.getParent().orElse(null)) {
      ExtensionContext.Store store = current.getStore(AuditScope.NAMESPACE);
      T value = store == null ? null : store.get(key, type);
      if (value != null) return value;
    }
    return null;
  }
}
