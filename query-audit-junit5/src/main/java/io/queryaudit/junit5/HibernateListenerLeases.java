package io.queryaudit.junit5;

import io.queryaudit.core.interceptor.LazyLoadTracker;
import io.queryaudit.core.provenance.AuditCapability;
import io.queryaudit.core.provenance.AuditRuntimeIdentity;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/** Shares one Hibernate adapter per registry without changing Hibernate's duplication policies. */
final class HibernateListenerLeases {
  private static final Map<Object, SharedRegistration> REGISTRIES = new IdentityHashMap<>();
  private final Map<LazyLoadTracker, OwnedRegistration> owned = new IdentityHashMap<>();

  HibernateIntegration.Registration acquire(Object registry) throws Exception {
    synchronized (REGISTRIES) {
      SharedRegistration shared = REGISTRIES.get(registry);
      if (shared != null && shared.users == 0) {
        // A failed cleanup remains known. Retry it before installing another adapter; otherwise
        // Hibernate would reject a duplicate or an old listener could retain dead capture state.
        requireCleanup(shared.removeListeners());
        REGISTRIES.remove(registry);
        shared = null;
      }
      if (shared == null) {
        shared = new SharedRegistration(registry);
        try {
          shared.appendListeners();
        } catch (Exception | LinkageError failure) {
          Throwable cleanup = shared.removeListeners();
          if (cleanup != null) {
            REGISTRIES.put(registry, shared);
            IllegalStateException rollbackFailure =
                new IllegalStateException(
                    "QueryAudit could not roll back its Hibernate event listeners", failure);
            rollbackFailure.addSuppressed(cleanup);
            throw rollbackFailure;
          }
          throw failure;
        }
        REGISTRIES.put(registry, shared);
      }
      shared.users++;
      OwnedRegistration ownership = owned.get(shared.tracker);
      if (ownership == null) owned.put(shared.tracker, new OwnedRegistration(shared));
      else ownership.claims++;
      return new HibernateIntegration.Registration(shared.tracker, shared.capability, null);
    }
  }

  void release(LazyLoadTracker tracker) {
    if (tracker == null) return;
    synchronized (REGISTRIES) {
      OwnedRegistration ownership = owned.get(tracker);
      if (ownership == null) return;
      if (--ownership.claims == 0) owned.remove(tracker);
      SharedRegistration shared = ownership.shared;
      if (--shared.users > 0) return;
      Throwable failure = shared.removeListeners();
      if (failure == null) REGISTRIES.remove(shared.registry);
      requireCleanup(failure);
    }
  }

  private static void requireCleanup(Throwable failure) {
    if (failure != null) {
      throw new IllegalStateException(
          "QueryAudit could not release its Hibernate event listeners", failure);
    }
  }

  private static final class OwnedRegistration {
    final SharedRegistration shared;
    int claims = 1;

    OwnedRegistration(SharedRegistration shared) {
      this.shared = shared;
    }
  }

  private static final class SharedRegistration {
    private static final String REGISTRY = "org.hibernate.event.service.spi.EventListenerRegistry";
    private static final List<Event> EVENTS =
        List.of(
            new Event(
                "INIT_COLLECTION", "org.hibernate.event.spi.InitializeCollectionEventListener"),
            new Event("POST_LOAD", "org.hibernate.event.spi.PostLoadEventListener"));

    final Object registry;
    final LazyLoadTracker tracker = new LazyLoadTracker();
    final HibernateLazyLoadListener listener = new HibernateLazyLoadListener(tracker);
    final AuditCapability capability;
    int users;

    SharedRegistration(Object registry) throws Exception {
      this.registry = registry;
      String version =
          (String)
              Class.forName("org.hibernate.Version").getMethod("getVersionString").invoke(null);
      if (version == null || version.isBlank() || version.equalsIgnoreCase("unknown")) {
        throw new IllegalStateException("Hibernate did not identify its runtime version");
      }
      capability =
          AuditCapability.available(
              "hibernate:"
                  + version
                  + ";"
                  + AuditRuntimeIdentity.implementation(listener.getClass()));
    }

    void appendListeners() throws Exception {
      synchronized (registry) {
        Class<?> eventTypeClass = Class.forName("org.hibernate.event.spi.EventType");
        Method append =
            Class.forName(REGISTRY).getMethod("appendListeners", eventTypeClass, Object[].class);
        for (Event event : EVENTS) {
          Object eventType = eventTypeClass.getField(event.name()).get(null);
          Object listeners = Array.newInstance(Class.forName(event.listenerType()), 1);
          Array.set(listeners, 0, listener);
          append.invoke(registry, eventType, listeners);
        }
      }
    }

    Throwable removeListeners() {
      synchronized (registry) {
        Throwable failure = null;
        for (Event event : EVENTS) {
          try {
            removeListener(event);
          } catch (Exception | LinkageError cleanupFailure) {
            if (failure == null) failure = cleanupFailure;
            else if (failure != cleanupFailure) failure.addSuppressed(cleanupFailure);
          }
        }
        return failure;
      }
    }

    private void removeListener(Event event) throws Exception {
      Class<?> eventTypeClass = Class.forName("org.hibernate.event.spi.EventType");
      Class<?> registryClass = Class.forName(REGISTRY);
      Object eventType = eventTypeClass.getField(event.name()).get(null);
      Object group =
          registryClass
              .getMethod("getEventListenerGroup", eventTypeClass)
              .invoke(registry, eventType);
      if (group == null) return;
      Iterable<?> current =
          (Iterable<?>)
              Class.forName("org.hibernate.event.service.spi.EventListenerGroup")
                  .getMethod("listeners")
                  .invoke(group);
      List<Object> retained = new ArrayList<>();
      boolean found = false;
      for (Object registered : current) {
        if (registered == listener) found = true;
        else retained.add(registered);
      }
      if (!found) return;
      Object listeners = Array.newInstance(Class.forName(event.listenerType()), retained.size());
      for (int index = 0; index < retained.size(); index++)
        Array.set(listeners, index, retained.get(index));
      registryClass
          .getMethod("setListeners", eventTypeClass, Object[].class)
          .invoke(registry, eventType, listeners);
    }

    private record Event(String name, String listenerType) {}
  }
}
