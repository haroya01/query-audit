package io.queryaudit.junit5;

import io.queryaudit.core.interceptor.LazyLoadTracker;
import org.hibernate.event.spi.InitializeCollectionEvent;
import org.hibernate.event.spi.InitializeCollectionEventListener;
import org.hibernate.event.spi.PostLoadEvent;
import org.hibernate.event.spi.PostLoadEventListener;

/**
 * Adapts Hibernate's {@link InitializeCollectionEventListener} and {@link PostLoadEventListener}
 * SPIs to a Hibernate-free {@link LazyLoadTracker}.
 *
 * <p>This class — not {@link LazyLoadTracker} itself — carries the compile-time dependency on
 * Hibernate's event types. It is only ever constructed by {@link HibernateIntegration} after
 * confirming Hibernate is on the classpath, so referencing it never triggers class loading for a
 * plain JDBC audit (issue #248).
 *
 * @since 0.6.0
 */
class HibernateLazyLoadListener
    implements InitializeCollectionEventListener, PostLoadEventListener {

  private final LazyLoadTracker tracker;

  HibernateLazyLoadListener(LazyLoadTracker tracker) {
    this.tracker = tracker;
  }

  @Override
  public void onInitializeCollection(InitializeCollectionEvent event) {
    if (!tracker.isActive()) return;

    String role = event.getCollection() != null ? event.getCollection().getRole() : "unknown";
    if (event.getCollection() != null && collectionBatchLoadable(event, role)) return;
    tracker.recordCollectionInitialized(
        role, event.getAffectedOwnerEntityName(), event.getAffectedOwnerIdOrNull());
  }

  @Override
  public void onPostLoad(PostLoadEvent event) {
    if (!tracker.isActive()) return;

    String entityName = event.getEntity().getClass().getName();
    Object id = event.getId();

    if (LazyLoadTracker.isProxyResolution()) {
      if (!entityBatchLoadable(event)) tracker.recordProxyResolved(entityName, id);
    } else if (LazyLoadTracker.hasFindByIdInStack()) {
      tracker.recordExplicitLoad(entityName, id, LazyLoadTracker.captureApplicationStack());
    }
  }

  // AbstractEvent.getFactory() keeps one signature from Hibernate 6.6 to 7.x; getSession() does
  // not. A metamodel that cannot answer leaves the event recorded, as before batch awareness.
  private static boolean collectionBatchLoadable(InitializeCollectionEvent event, String role) {
    try {
      return event.getFactory().getMappingMetamodel().getCollectionDescriptor(role).isBatchLoadable();
    } catch (RuntimeException | LinkageError unavailable) {
      return false;
    }
  }

  private static boolean entityBatchLoadable(PostLoadEvent event) {
    try {
      return event.getPersister() != null && event.getPersister().isBatchLoadable();
    } catch (RuntimeException | LinkageError unavailable) {
      return false;
    }
  }
}
