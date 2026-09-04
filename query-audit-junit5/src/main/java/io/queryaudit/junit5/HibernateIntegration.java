package io.queryaudit.junit5;

import io.queryaudit.core.detector.FindByIdForAssociationDetector;
import io.queryaudit.core.detector.LazyLoadNPlusOneDetector;
import io.queryaudit.core.detector.QueryAuditAnalyzer;
import io.queryaudit.core.interceptor.LazyLoadTracker;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.provenance.AuditCapability;
import io.queryaudit.core.provenance.AuditRuntimeIdentity;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Handles Hibernate-specific integration: registering a {@link HibernateLazyLoadListener} as a
 * Hibernate event listener and merging Hibernate-level N+1 issues into the report.
 *
 * <p>Only this class and {@link HibernateLazyLoadListener} carry compile-time references to
 * Hibernate types; both are reached exclusively through the {@code Class.forName} guard below, so a
 * plain JDBC audit never forces those classes to load (issue #248). {@link LazyLoadTracker} itself
 * — the object returned to and stored by {@link QueryAuditExtension} — is Hibernate-free.
 *
 * @author haroya
 * @since 0.2.0
 */
class HibernateIntegration {

  private static final String INIT_COLLECTION_LISTENER_CLASS =
      "org.hibernate.event.spi.InitializeCollectionEventListener";
  private static final String POST_LOAD_LISTENER_CLASS =
      "org.hibernate.event.spi.PostLoadEventListener";

  // Retain the adapter instance so partial registration and normal cleanup remove the same
  // listener.
  private final Map<LazyLoadTracker, HibernateLazyLoadListener> registeredListeners =
      new ConcurrentHashMap<>();

  record Registration(LazyLoadTracker tracker, AuditCapability capability, String failure) {}

  LazyLoadTracker registerTracker(ExtensionContext context, ExtensionContext.Namespace namespace) {
    return registerWithCapabilities(context).tracker();
  }

  Registration registerWithCapabilities(ExtensionContext context) {
    try {
      Object emf = resolveEntityManagerFactory(context);
      if (emf == null) {
        return new Registration(null, AuditCapability.absent(), null);
      }
      return registerWithCapabilitiesForEmf(emf);
    } catch (RuntimeException | LinkageError failure) {
      return new Registration(
          null, AuditCapability.failed("hibernate-discovery"), failure.getClass().getSimpleName());
    }
  }

  /** Removes the tracker from the Hibernate event listener registry (issue #101). */
  void unregisterTracker(ExtensionContext context, LazyLoadTracker tracker) {
    if (tracker == null) return;
    Object emf = resolveEntityManagerFactory(context);
    if (emf == null) return;
    unregisterTrackerForEmf(emf, tracker);
  }

  LazyLoadTracker registerTrackerForEmf(Object emf) {
    return registerWithCapabilitiesForEmf(emf).tracker();
  }

  Registration registerWithCapabilitiesForEmf(Object emf) {
    LazyLoadTracker tracker = null;
    try {
      Class.forName(INIT_COLLECTION_LISTENER_CLASS);

      Object eventListenerRegistry = resolveEventListenerRegistry(emf);
      if (eventListenerRegistry == null) {
        throw new IllegalStateException("Hibernate event listener registry is unavailable");
      }

      tracker = new LazyLoadTracker();
      HibernateLazyLoadListener listener = new HibernateLazyLoadListener(tracker);
      registeredListeners.put(tracker, listener);

      Class<?> eventTypeClass = Class.forName("org.hibernate.event.spi.EventType");
      Class<?> registryClass =
          Class.forName("org.hibernate.event.service.spi.EventListenerRegistry");
      Method appendListenersMethod =
          registryClass.getMethod("appendListeners", eventTypeClass, Object[].class);

      appendListener(
          eventListenerRegistry,
          eventTypeClass,
          "INIT_COLLECTION",
          Class.forName(INIT_COLLECTION_LISTENER_CLASS),
          appendListenersMethod,
          listener);
      appendListener(
          eventListenerRegistry,
          eventTypeClass,
          "POST_LOAD",
          Class.forName(POST_LOAD_LISTENER_CLASS),
          appendListenersMethod,
          listener);

      String version =
          (String)
              Class.forName("org.hibernate.Version").getMethod("getVersionString").invoke(null);
      if (version == null || version.isBlank() || version.equalsIgnoreCase("unknown")) {
        throw new IllegalStateException("Hibernate did not identify its runtime version");
      }
      return new Registration(
          tracker,
          AuditCapability.available(
              "hibernate:"
                  + version
                  + ";"
                  + AuditRuntimeIdentity.implementation(listener.getClass())),
          null);
    } catch (ClassNotFoundException failure) {
      if (tracker == null && INIT_COLLECTION_LISTENER_CLASS.equals(failure.getMessage())) {
        return new Registration(null, AuditCapability.absent(), null);
      }
      unregisterTrackerForEmf(emf, tracker);
      return new Registration(
          null, AuditCapability.failed("hibernate-events"), failure.getClass().getSimpleName());
    } catch (Exception | LinkageError failure) {
      unregisterTrackerForEmf(emf, tracker);
      return new Registration(
          null, AuditCapability.failed("hibernate-events"), failure.getClass().getSimpleName());
    }
  }

  void unregisterTrackerForEmf(Object emf, LazyLoadTracker tracker) {
    if (tracker == null) return;
    HibernateLazyLoadListener listener = registeredListeners.remove(tracker);
    if (listener == null) return;
    try {
      Class.forName(INIT_COLLECTION_LISTENER_CLASS);

      Object eventListenerRegistry = resolveEventListenerRegistry(emf);
      if (eventListenerRegistry == null) return;

      Class<?> eventTypeClass = Class.forName("org.hibernate.event.spi.EventType");
      removeListener(
          eventListenerRegistry,
          eventTypeClass,
          "INIT_COLLECTION",
          Class.forName(INIT_COLLECTION_LISTENER_CLASS),
          listener);
      removeListener(
          eventListenerRegistry,
          eventTypeClass,
          "POST_LOAD",
          Class.forName(POST_LOAD_LISTENER_CLASS),
          listener);
    } catch (ClassNotFoundException ignored) {
      // Hibernate not on classpath, nothing to do
    } catch (Exception e) {
      System.err.println(
          "[QueryAudit] Failed to unregister Hibernate LazyLoadTracker: " + e.getMessage());
    }
  }

  private static void appendListener(
      Object eventListenerRegistry,
      Class<?> eventTypeClass,
      String eventTypeFieldName,
      Class<?> listenerInterface,
      Method appendListenersMethod,
      HibernateLazyLoadListener listener)
      throws Exception {
    Object eventType = eventTypeClass.getField(eventTypeFieldName).get(null);
    Object listenersArray = Array.newInstance(listenerInterface, 1);
    Array.set(listenersArray, 0, listener);
    appendListenersMethod.invoke(eventListenerRegistry, eventType, listenersArray);
  }

  private static void removeListener(
      Object eventListenerRegistry,
      Class<?> eventTypeClass,
      String eventTypeFieldName,
      Class<?> listenerInterface,
      HibernateLazyLoadListener listener)
      throws Exception {
    Object eventType = eventTypeClass.getField(eventTypeFieldName).get(null);

    // Resolve via public SPI interfaces; internal impls block reflective access on JPMS.
    Class<?> registryClass = Class.forName("org.hibernate.event.service.spi.EventListenerRegistry");
    Class<?> groupClass = Class.forName("org.hibernate.event.service.spi.EventListenerGroup");

    Method getGroupMethod = registryClass.getMethod("getEventListenerGroup", eventTypeClass);
    Object group = getGroupMethod.invoke(eventListenerRegistry, eventType);
    if (group == null) return;

    Iterable<?> currentListeners = (Iterable<?>) groupClass.getMethod("listeners").invoke(group);

    List<Object> retained = new ArrayList<>();
    for (Object registered : currentListeners) {
      if (registered != listener) {
        retained.add(registered);
      }
    }

    Object retainedArray = Array.newInstance(listenerInterface, retained.size());
    for (int i = 0; i < retained.size(); i++) {
      Array.set(retainedArray, i, retained.get(i));
    }

    Method setListenersMethod =
        registryClass.getMethod("setListeners", eventTypeClass, Object[].class);
    setListenersMethod.invoke(eventListenerRegistry, eventType, retainedArray);
  }

  /** Resolves the Hibernate {@code EventListenerRegistry} from the given EMF, or null. */
  private Object resolveEventListenerRegistry(Object emf) throws Exception {
    Class<?> sfiClass = Class.forName("org.hibernate.engine.spi.SessionFactoryImplementor");
    Method unwrapMethod = emf.getClass().getMethod("unwrap", Class.class);
    Object sfi = unwrapMethod.invoke(emf, sfiClass);

    Class<?> registryClass = Class.forName("org.hibernate.event.service.spi.EventListenerRegistry");
    Object serviceRegistry = sfi.getClass().getMethod("getServiceRegistry").invoke(sfi);
    Method getServiceMethod = serviceRegistry.getClass().getMethod("getService", Class.class);
    return getServiceMethod.invoke(serviceRegistry, registryClass);
  }

  /** Detects Hibernate-level N+1 issues and merges them through the analyzer's policy pipeline. */
  QueryAuditReport mergeNPlusOneIssues(
      QueryAuditReport report, LazyLoadTracker tracker, QueryAuditAnalyzer analyzer) {

    LazyLoadNPlusOneDetector hibernateDetector =
        new LazyLoadNPlusOneDetector(analyzer.getConfig().getNPlusOneThreshold());
    List<Issue> hibernateIssues = hibernateDetector.evaluate(tracker.getRecords());
    return analyzer.mergeDetectedIssues(report, hibernateIssues);
  }

  /**
   * Merges findById-for-association issues into the report. These are INFO-level issues suggesting
   * {@code getReferenceById()} when {@code findById()} is used only for FK assignment.
   */
  QueryAuditReport mergeFindByIdIssues(
      QueryAuditReport report, LazyLoadTracker tracker, QueryAuditAnalyzer analyzer) {

    FindByIdForAssociationDetector detector = new FindByIdForAssociationDetector();
    List<Issue> findByIdIssues =
        detector.evaluate(tracker.getExplicitLoads(), tracker.getRecords(), report.getAllQueries());
    return analyzer.mergeDetectedIssues(report, findByIdIssues);
  }

  /** Resolves the EntityManagerFactory from Spring context via reflection. */
  private Object resolveEntityManagerFactory(ExtensionContext context) {
    Object applicationContext;
    try {
      Class<?> springExtensionClass =
          Class.forName("org.springframework.test.context.junit.jupiter.SpringExtension");
      Method getAppContext =
          springExtensionClass.getMethod("getApplicationContext", ExtensionContext.class);
      applicationContext = getAppContext.invoke(null, context);
    } catch (ReflectiveOperationException unavailableContext) {
      return null;
    }
    return entityManagerFactoryBean(applicationContext);
  }

  static Object entityManagerFactoryBean(Object applicationContext) {
    if (applicationContext == null) {
      return null;
    }
    try {
      Class<?> emfClass = Class.forName("jakarta.persistence.EntityManagerFactory");
      Method getBean = applicationContext.getClass().getMethod("getBean", Class.class);
      return getBean.invoke(applicationContext, emfClass);
    } catch (ClassNotFoundException absentJpa) {
      return null;
    } catch (InvocationTargetException failure) {
      Throwable cause = failure.getCause();
      if (cause != null
          && cause
              .getClass()
              .getName()
              .equals("org.springframework.beans.factory.NoSuchBeanDefinitionException")) {
        return null;
      }
      throw new IllegalStateException("Could not initialize the EntityManagerFactory", cause);
    } catch (ReflectiveOperationException failure) {
      throw new IllegalStateException("Could not discover the EntityManagerFactory", failure);
    }
  }
}
