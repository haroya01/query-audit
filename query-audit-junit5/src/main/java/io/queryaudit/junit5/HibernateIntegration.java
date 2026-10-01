package io.queryaudit.junit5;

import io.queryaudit.core.detector.FindByIdForAssociationDetector;
import io.queryaudit.core.detector.LazyLoadNPlusOneDetector;
import io.queryaudit.core.detector.QueryAuditAnalyzer;
import io.queryaudit.core.interceptor.LazyLoadTracker;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.provenance.AuditCapability;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
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
  private final HibernateListenerLeases listeners = new HibernateListenerLeases();

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
    unregisterTrackerForEmf(null, tracker);
  }

  LazyLoadTracker registerTrackerForEmf(Object emf) {
    return registerWithCapabilitiesForEmf(emf).tracker();
  }

  Registration registerWithCapabilitiesForEmf(Object emf) {
    try {
      Class.forName(INIT_COLLECTION_LISTENER_CLASS);

      Object eventListenerRegistry = resolveEventListenerRegistry(emf);
      if (eventListenerRegistry == null) {
        throw new IllegalStateException("Hibernate event listener registry is unavailable");
      }

      return listeners.acquire(eventListenerRegistry);
    } catch (ClassNotFoundException failure) {
      if (INIT_COLLECTION_LISTENER_CLASS.equals(failure.getMessage())) {
        return new Registration(null, AuditCapability.absent(), null);
      }
      return new Registration(
          null, AuditCapability.failed("hibernate-events"), failure.getClass().getSimpleName());
    } catch (Exception | LinkageError failure) {
      return new Registration(
          null, AuditCapability.failed("hibernate-events"), failure.getClass().getSimpleName());
    }
  }

  void unregisterTrackerForEmf(Object emf, LazyLoadTracker tracker) {
    listeners.release(tracker);
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
    List<Issue> hibernateIssues =
        hibernateDetector.evaluate(tracker.getRecords()).stream()
            .map(
                issue ->
                    new Issue(
                        issue.type(),
                        Severity.INFO,
                        issue.query(),
                        issue.table(),
                        issue.column(),
                        issue.detail(),
                        issue.suggestion(),
                        issue.sourceLocation()))
            .toList();
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
