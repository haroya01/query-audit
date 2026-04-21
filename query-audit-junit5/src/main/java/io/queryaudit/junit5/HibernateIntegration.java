package io.queryaudit.junit5;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.detector.FindByIdForAssociationDetector;
import io.queryaudit.core.detector.LazyLoadNPlusOneDetector;
import io.queryaudit.core.interceptor.LazyLoadTracker;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.QueryAuditReport;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Handles Hibernate-specific integration: registering a {@link LazyLoadTracker} as a Hibernate
 * event listener and merging Hibernate-level N+1 issues into the report.
 *
 * @author haroya
 * @since 0.2.0
 */
class HibernateIntegration {

  private static final String INIT_COLLECTION_LISTENER_CLASS =
      "org.hibernate.event.spi.InitializeCollectionEventListener";
  private static final String POST_LOAD_LISTENER_CLASS =
      "org.hibernate.event.spi.PostLoadEventListener";

  /**
   * Registers a LazyLoadTracker as a Hibernate event listener. Returns the tracker if successful,
   * or null if Hibernate is not on the classpath or registration fails.
   */
  LazyLoadTracker registerTracker(ExtensionContext context, ExtensionContext.Namespace namespace) {
    Object emf = resolveEntityManagerFactory(context);
    if (emf == null) return null;
    return registerTrackerForEmf(emf);
  }

  /**
   * Removes the tracker from the Hibernate event listener registry. Without this, every test class
   * accumulates a fresh listener on a shared {@link
   * org.hibernate.event.service.spi.EventListenerRegistry}, leaking memory and fanning out events
   * to dead trackers (issue #101).
   */
  void unregisterTracker(ExtensionContext context, LazyLoadTracker tracker) {
    if (tracker == null) return;
    Object emf = resolveEntityManagerFactory(context);
    if (emf == null) return;
    unregisterTrackerForEmf(emf, tracker);
  }

  // Package-private entry points that operate directly on an EntityManagerFactory. Kept separate
  // from the ExtensionContext-based API so that listener lifecycle can be exercised without a
  // JUnit ExtensionContext stub, which would need to impersonate SpringExtension internals.

  LazyLoadTracker registerTrackerForEmf(Object emf) {
    try {
      Class.forName(INIT_COLLECTION_LISTENER_CLASS);

      Object eventListenerRegistry = resolveEventListenerRegistry(emf);
      if (eventListenerRegistry == null) return null;

      LazyLoadTracker tracker = new LazyLoadTracker();

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
          tracker);
      appendListener(
          eventListenerRegistry,
          eventTypeClass,
          "POST_LOAD",
          Class.forName(POST_LOAD_LISTENER_CLASS),
          appendListenersMethod,
          tracker);

      return tracker;
    } catch (ClassNotFoundException ignored) {
      // Hibernate not on classpath, skip
    } catch (Exception e) {
      System.err.println(
          "[QueryAudit] Failed to register Hibernate LazyLoadTracker: " + e.getMessage());
    }
    return null;
  }

  void unregisterTrackerForEmf(Object emf, LazyLoadTracker tracker) {
    if (tracker == null) return;
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
          tracker);
      removeListener(
          eventListenerRegistry,
          eventTypeClass,
          "POST_LOAD",
          Class.forName(POST_LOAD_LISTENER_CLASS),
          tracker);
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
      LazyLoadTracker tracker)
      throws Exception {
    Object eventType = eventTypeClass.getField(eventTypeFieldName).get(null);
    Object listenersArray = Array.newInstance(listenerInterface, 1);
    Array.set(listenersArray, 0, tracker);
    appendListenersMethod.invoke(eventListenerRegistry, eventType, listenersArray);
  }

  private static void removeListener(
      Object eventListenerRegistry,
      Class<?> eventTypeClass,
      String eventTypeFieldName,
      Class<?> listenerInterface,
      LazyLoadTracker tracker)
      throws Exception {
    Object eventType = eventTypeClass.getField(eventTypeFieldName).get(null);

    // Resolve methods from the public SPI interfaces — the concrete impls
    // (EventListenerGroupImpl) are in internal packages and reflective calls against them
    // fail with IllegalAccessException on the module boundary.
    Class<?> registryClass = Class.forName("org.hibernate.event.service.spi.EventListenerRegistry");
    Class<?> groupClass = Class.forName("org.hibernate.event.service.spi.EventListenerGroup");

    Method getGroupMethod = registryClass.getMethod("getEventListenerGroup", eventTypeClass);
    Object group = getGroupMethod.invoke(eventListenerRegistry, eventType);
    if (group == null) return;

    Iterable<?> currentListeners = (Iterable<?>) groupClass.getMethod("listeners").invoke(group);

    List<Object> retained = new ArrayList<>();
    for (Object listener : currentListeners) {
      if (listener != tracker) {
        retained.add(listener);
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

  /**
   * Merges Hibernate-level N+1 issues into the report. Hibernate-level issues are authoritative
   * (ERROR severity).
   */
  QueryAuditReport mergeNPlusOneIssues(
      QueryAuditReport report, LazyLoadTracker tracker, QueryAuditConfig config) {

    LazyLoadNPlusOneDetector hibernateDetector =
        new LazyLoadNPlusOneDetector(config.getNPlusOneThreshold());
    List<Issue> hibernateIssues = hibernateDetector.evaluate(tracker.getRecords());

    if (hibernateIssues.isEmpty()) {
      return report;
    }

    // Add Hibernate-level N+1 issues to the confirmed list
    List<Issue> mergedConfirmed = new ArrayList<>(report.getConfirmedIssues());
    mergedConfirmed.addAll(hibernateIssues);

    return new QueryAuditReport(
        report.getTestClass(),
        report.getTestName(),
        mergedConfirmed,
        report.getInfoIssues(),
        report.getAcknowledgedIssues(),
        report.getAllQueries(),
        report.getUniquePatternCount(),
        report.getTotalQueryCount(),
        report.getTotalExecutionTimeNanos());
  }

  /**
   * Merges findById-for-association issues into the report. These are INFO-level issues suggesting
   * {@code getReferenceById()} when {@code findById()} is used only for FK assignment.
   */
  QueryAuditReport mergeFindByIdIssues(
      QueryAuditReport report, LazyLoadTracker tracker, QueryAuditConfig config) {

    if (config.getDisabledRules().contains("find-by-id-for-association")) {
      return report;
    }

    FindByIdForAssociationDetector detector = new FindByIdForAssociationDetector();
    List<Issue> findByIdIssues =
        detector.evaluate(tracker.getExplicitLoads(), tracker.getRecords(), report.getAllQueries());

    if (findByIdIssues.isEmpty()) {
      return report;
    }

    // findById issues are INFO severity → add to infoIssues
    List<Issue> mergedInfo = new ArrayList<>(report.getInfoIssues());
    mergedInfo.addAll(findByIdIssues);

    return new QueryAuditReport(
        report.getTestClass(),
        report.getTestName(),
        report.getConfirmedIssues(),
        mergedInfo,
        report.getAcknowledgedIssues(),
        report.getAllQueries(),
        report.getUniquePatternCount(),
        report.getTotalQueryCount(),
        report.getTotalExecutionTimeNanos());
  }

  /** Resolves the EntityManagerFactory from Spring context via reflection. */
  private Object resolveEntityManagerFactory(ExtensionContext context) {
    try {
      Class<?> springExtensionClass =
          Class.forName("org.springframework.test.context.junit.jupiter.SpringExtension");
      Method getAppContext =
          springExtensionClass.getMethod("getApplicationContext", ExtensionContext.class);
      Object appContext = getAppContext.invoke(null, context);
      if (appContext != null) {
        Class<?> emfClass = Class.forName("jakarta.persistence.EntityManagerFactory");
        Method getBean = appContext.getClass().getMethod("getBean", Class.class);
        return getBean.invoke(appContext, emfClass);
      }
    } catch (Exception ignored) {
    }
    return null;
  }
}
