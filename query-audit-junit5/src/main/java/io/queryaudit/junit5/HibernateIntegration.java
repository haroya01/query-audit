package io.queryaudit.junit5;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.detector.LazyLoadNPlusOneDetector;
import io.queryaudit.core.interceptor.LazyLoadTracker;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.QueryAuditReport;
import java.lang.reflect.Field;
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

  /**
   * Registers a LazyLoadTracker as a Hibernate event listener. Returns the tracker if successful,
   * or null if Hibernate is not on the classpath or registration fails.
   */
  LazyLoadTracker registerTracker(ExtensionContext context, ExtensionContext.Namespace namespace) {
    try {
      // Check if Hibernate is on the classpath
      Class.forName("org.hibernate.event.spi.InitializeCollectionEventListener");

      Object emf = resolveEntityManagerFactory(context);
      if (emf == null) return null;

      LazyLoadTracker tracker = new LazyLoadTracker();

      // Get SessionFactoryImplementor from EntityManagerFactory
      Class<?> sfiClass = Class.forName("org.hibernate.engine.spi.SessionFactoryImplementor");
      Method unwrapMethod = emf.getClass().getMethod("unwrap", Class.class);
      Object sfi = unwrapMethod.invoke(emf, sfiClass);

      // Get EventEngine from SessionFactoryImplementor
      Class<?> eventEngineClass =
          Class.forName("org.hibernate.event.service.spi.EventListenerRegistry");

      Method getServiceRegistryMethod = sfi.getClass().getMethod("getServiceRegistry");
      Object serviceRegistry = getServiceRegistryMethod.invoke(sfi);
      Method getServiceMethod = serviceRegistry.getClass().getMethod("getService", Class.class);
      Object eventListenerRegistry = getServiceMethod.invoke(serviceRegistry, eventEngineClass);

      // Get EventType constants
      Class<?> eventTypeClass = Class.forName("org.hibernate.event.spi.EventType");
      Method appendListenersMethod =
          eventListenerRegistry
              .getClass()
              .getMethod("appendListeners", eventTypeClass, Object[].class);

      // Register for INIT_COLLECTION (collection lazy loading)
      Field initCollectionField = eventTypeClass.getField("INIT_COLLECTION");
      Object initCollectionEventType = initCollectionField.get(null);
      Object initCollListenersArray =
          java.lang.reflect.Array.newInstance(
              Class.forName("org.hibernate.event.spi.InitializeCollectionEventListener"), 1);
      java.lang.reflect.Array.set(initCollListenersArray, 0, tracker);
      appendListenersMethod.invoke(
          eventListenerRegistry, initCollectionEventType, initCollListenersArray);

      // Register for POST_LOAD (@ManyToOne/@OneToOne proxy resolution)
      Field postLoadField = eventTypeClass.getField("POST_LOAD");
      Object postLoadEventType = postLoadField.get(null);
      Object postLoadListenersArray =
          java.lang.reflect.Array.newInstance(
              Class.forName("org.hibernate.event.spi.PostLoadEventListener"), 1);
      java.lang.reflect.Array.set(postLoadListenersArray, 0, tracker);
      appendListenersMethod.invoke(
          eventListenerRegistry, postLoadEventType, postLoadListenersArray);

      return tracker;
    } catch (ClassNotFoundException ignored) {
      // Hibernate not on classpath, skip
    } catch (Exception e) {
      System.err.println(
          "[QueryAudit] Failed to register Hibernate LazyLoadTracker: " + e.getMessage());
    }
    return null;
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
