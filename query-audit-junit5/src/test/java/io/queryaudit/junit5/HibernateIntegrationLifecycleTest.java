package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.interceptor.LazyLoadTracker;
import io.queryaudit.junit5.integration.TestApplication;
import jakarta.persistence.EntityManagerFactory;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.event.service.spi.EventListenerGroup;
import org.hibernate.event.service.spi.EventListenerRegistry;
import org.hibernate.event.spi.EventType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Regression for issue #101 — verifies {@link HibernateIntegration#unregisterTrackerForEmf} removes
 * the {@link LazyLoadTracker} it previously attached to the Hibernate {@link
 * EventListenerRegistry}, so that repeated test classes against a shared {@code SessionFactory}
 * don't accumulate dead listeners.
 */
@SpringBootTest(classes = TestApplication.class)
@DisplayName("HibernateIntegration — tracker register/unregister lifecycle (issue #101)")
class HibernateIntegrationLifecycleTest {

  @Autowired EntityManagerFactory emf;

  @Test
  @DisplayName("unregister removes both INIT_COLLECTION and POST_LOAD listeners it added")
  void unregisterRemovesTrackerFromBothEventTypes() {
    HibernateIntegration integration = new HibernateIntegration();

    int initBefore = countListeners(EventType.INIT_COLLECTION);
    int postLoadBefore = countListeners(EventType.POST_LOAD);

    LazyLoadTracker tracker = integration.registerTrackerForEmf(emf);
    assertThat(tracker).as("register should succeed in a Hibernate context").isNotNull();

    assertThat(countListeners(EventType.INIT_COLLECTION)).isEqualTo(initBefore + 1);
    assertThat(countListeners(EventType.POST_LOAD)).isEqualTo(postLoadBefore + 1);
    assertThat(listenerInstances(EventType.INIT_COLLECTION)).contains(tracker);
    assertThat(listenerInstances(EventType.POST_LOAD)).contains(tracker);

    integration.unregisterTrackerForEmf(emf, tracker);

    assertThat(countListeners(EventType.INIT_COLLECTION)).isEqualTo(initBefore);
    assertThat(countListeners(EventType.POST_LOAD)).isEqualTo(postLoadBefore);
    assertThat(listenerInstances(EventType.INIT_COLLECTION)).doesNotContain(tracker);
    assertThat(listenerInstances(EventType.POST_LOAD)).doesNotContain(tracker);
  }

  @Test
  @DisplayName("repeated register/unregister cycles do not accumulate listeners")
  void repeatedCyclesDoNotAccumulate() {
    HibernateIntegration integration = new HibernateIntegration();

    int initBaseline = countListeners(EventType.INIT_COLLECTION);
    int postLoadBaseline = countListeners(EventType.POST_LOAD);

    for (int i = 0; i < 5; i++) {
      LazyLoadTracker tracker = integration.registerTrackerForEmf(emf);
      assertThat(tracker).isNotNull();
      integration.unregisterTrackerForEmf(emf, tracker);
    }

    assertThat(countListeners(EventType.INIT_COLLECTION)).isEqualTo(initBaseline);
    assertThat(countListeners(EventType.POST_LOAD)).isEqualTo(postLoadBaseline);
  }

  @Test
  @DisplayName("unregister on a null tracker is a no-op")
  void unregisterNullIsNoOp() {
    HibernateIntegration integration = new HibernateIntegration();

    int initBefore = countListeners(EventType.INIT_COLLECTION);
    int postLoadBefore = countListeners(EventType.POST_LOAD);

    integration.unregisterTrackerForEmf(emf, null);

    assertThat(countListeners(EventType.INIT_COLLECTION)).isEqualTo(initBefore);
    assertThat(countListeners(EventType.POST_LOAD)).isEqualTo(postLoadBefore);
  }

  private int countListeners(EventType<?> eventType) {
    return listenerGroup(eventType).count();
  }

  private java.util.List<Object> listenerInstances(EventType<?> eventType) {
    java.util.List<Object> out = new java.util.ArrayList<>();
    for (Object listener : listenerGroup(eventType).listeners()) {
      out.add(listener);
    }
    return out;
  }

  private EventListenerGroup<?> listenerGroup(EventType<?> eventType) {
    EventListenerRegistry registry =
        emf.unwrap(SessionFactoryImplementor.class)
            .getServiceRegistry()
            .getService(EventListenerRegistry.class);
    return registry.getEventListenerGroup(eventType);
  }
}
