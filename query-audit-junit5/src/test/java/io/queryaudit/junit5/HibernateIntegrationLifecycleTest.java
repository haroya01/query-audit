package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.queryaudit.core.interceptor.LazyLoadTracker;
import io.queryaudit.junit5.integration.TestApplication;
import jakarta.persistence.EntityManagerFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.event.service.spi.EventListenerGroup;
import org.hibernate.event.service.spi.EventListenerRegistry;
import org.hibernate.event.spi.EventType;
import org.hibernate.event.spi.InitializeCollectionEventListener;
import org.hibernate.event.spi.PostLoadEventListener;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Regression for issue #101 — verifies {@link HibernateIntegration#unregisterTrackerForEmf} removes
 * the listener it previously attached to the Hibernate {@link EventListenerRegistry} on behalf of a
 * {@link LazyLoadTracker}, so that repeated test classes against a shared {@code SessionFactory}
 * don't accumulate dead listeners.
 *
 * <p>The registered listener is a Hibernate-typed adapter distinct from the returned {@link
 * LazyLoadTracker} (issue #248 keeps the tracker itself Hibernate-free), so these assertions go by
 * listener count rather than by identity against the returned tracker.
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

    integration.unregisterTrackerForEmf(emf, tracker);

    assertThat(countListeners(EventType.INIT_COLLECTION)).isEqualTo(initBefore);
    assertThat(countListeners(EventType.POST_LOAD)).isEqualTo(postLoadBefore);
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

  @Test
  void parallelOwnersShareOneAdapterUntilTheLastOwnerReleasesIt() throws Exception {
    int initBefore = countListeners(EventType.INIT_COLLECTION);
    int postBefore = countListeners(EventType.POST_LOAD);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CyclicBarrier start = new CyclicBarrier(2);
    HibernateIntegration first = new HibernateIntegration();
    HibernateIntegration second = new HibernateIntegration();
    LazyLoadTracker one = null;
    LazyLoadTracker two = null;
    try {
      Future<LazyLoadTracker> firstResult =
          executor.submit(
              () -> {
                start.await(5, TimeUnit.SECONDS);
                return first.registerTrackerForEmf(emf);
              });
      Future<LazyLoadTracker> secondResult =
          executor.submit(
              () -> {
                start.await(5, TimeUnit.SECONDS);
                return second.registerTrackerForEmf(emf);
              });
      one = firstResult.get(10, TimeUnit.SECONDS);
      two = secondResult.get(10, TimeUnit.SECONDS);
      assertThat(one).isNotNull().isSameAs(two);
      assertThat(countListeners(EventType.INIT_COLLECTION)).isEqualTo(initBefore + 1);
      assertThat(countListeners(EventType.POST_LOAD)).isEqualTo(postBefore + 1);
      first.unregisterTrackerForEmf(null, one);
      assertThat(countListeners(EventType.INIT_COLLECTION)).isEqualTo(initBefore + 1);
      assertThat(countListeners(EventType.POST_LOAD)).isEqualTo(postBefore + 1);
      second.unregisterTrackerForEmf(null, two);
      assertThat(countListeners(EventType.INIT_COLLECTION)).isEqualTo(initBefore);
      assertThat(countListeners(EventType.POST_LOAD)).isEqualTo(postBefore);
    } finally {
      first.unregisterTrackerForEmf(null, one);
      second.unregisterTrackerForEmf(null, two);
      executor.shutdownNow();
    }
  }

  @Test
  void repeatedClaimsByTheSameIntegrationAreReferenceCountedAndForeignReleaseIsIgnored() {
    int baseline = countListeners(EventType.POST_LOAD);
    HibernateIntegration owner = new HibernateIntegration();
    LazyLoadTracker first = owner.registerTrackerForEmf(emf);
    LazyLoadTracker second = owner.registerTrackerForEmf(emf);
    try {
      assertThat(first).isNotNull().isSameAs(second);
      new HibernateIntegration().unregisterTrackerForEmf(emf, first);
      assertThat(countListeners(EventType.POST_LOAD)).isEqualTo(baseline + 1);
      owner.unregisterTrackerForEmf(emf, first);
      assertThat(countListeners(EventType.POST_LOAD)).isEqualTo(baseline + 1);
      owner.unregisterTrackerForEmf(emf, second);
      assertThat(countListeners(EventType.POST_LOAD)).isEqualTo(baseline);
      owner.unregisterTrackerForEmf(emf, second);
      assertThat(countListeners(EventType.POST_LOAD)).isEqualTo(baseline);
    } finally {
      owner.unregisterTrackerForEmf(emf, first);
      owner.unregisterTrackerForEmf(emf, second);
    }
  }

  @Test
  void aPartialRegistrationRollsBackWithoutRemovingForeignListeners() {
    RegistryFixture fixture = new RegistryFixture();
    fixture.failPostAppend.set(true);
    HibernateListenerLeases leases = new HibernateListenerLeases();
    assertThatThrownBy(() -> leases.acquire(fixture.registry)).isInstanceOf(Exception.class);
    assertThat(fixture.init).containsExactly(fixture.foreignInit);
    assertThat(fixture.post).containsExactly(fixture.foreignPost);
  }

  @Test
  void lastReleaseAttemptsBothEventTypesAndFailedCleanupIsRetriedBeforeAnotherRegistration()
      throws Exception {
    RegistryFixture fixture = new RegistryFixture();
    HibernateListenerLeases leases = new HibernateListenerLeases();
    LazyLoadTracker first = leases.acquire(fixture.registry).tracker();
    fixture.failInitRemoval.set(true);
    assertThatThrownBy(() -> leases.release(first))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("QueryAudit could not release its Hibernate event listeners");
    assertThat(fixture.init).hasSize(2);
    assertThat(fixture.post).containsExactly(fixture.foreignPost);
    fixture.failInitRemoval.set(false);
    LazyLoadTracker retried = leases.acquire(fixture.registry).tracker();
    try {
      assertThat(retried).isNotSameAs(first);
      assertThat(fixture.init).hasSize(2);
      assertThat(fixture.post).hasSize(2);
    } finally {
      leases.release(retried);
    }
    assertThat(fixture.init).containsExactly(fixture.foreignInit);
    assertThat(fixture.post).containsExactly(fixture.foreignPost);
  }

  @Test
  void aRollbackFailureKeepsItsCauseAndIsRetriedWithoutInstallingDuplicates() throws Exception {
    RegistryFixture fixture = new RegistryFixture();
    fixture.failPostAppend.set(true);
    fixture.failInitRemoval.set(true);
    HibernateListenerLeases leases = new HibernateListenerLeases();
    assertThatThrownBy(() -> leases.acquire(fixture.registry))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("QueryAudit could not roll back its Hibernate event listeners")
        .satisfies(
            failure -> {
              assertThat(failure.getCause()).isNotNull();
              assertThat(failure.getSuppressed()).hasSize(1);
            });
    fixture.failInitRemoval.set(false);
    fixture.failPostAppend.set(false);
    LazyLoadTracker tracker = leases.acquire(fixture.registry).tracker();
    assertThat(fixture.init).hasSize(2);
    assertThat(fixture.post).hasSize(2);
    leases.release(tracker);
    assertThat(fixture.init).containsExactly(fixture.foreignInit);
    assertThat(fixture.post).containsExactly(fixture.foreignPost);
  }

  private static final class RegistryFixture {
    final Object foreignInit = Mockito.mock(InitializeCollectionEventListener.class);
    final Object foreignPost = Mockito.mock(PostLoadEventListener.class);
    final List<Object> init = new ArrayList<>(List.of(foreignInit));
    final List<Object> post = new ArrayList<>(List.of(foreignPost));
    final AtomicBoolean failPostAppend = new AtomicBoolean();
    final AtomicBoolean failInitRemoval = new AtomicBoolean();
    final EventListenerRegistry registry;

    RegistryFixture() {
      Map<Object, List<Object>> listeners =
          Map.of(EventType.INIT_COLLECTION, init, EventType.POST_LOAD, post);
      Map<Object, EventListenerGroup<?>> groups = new HashMap<>();
      listeners.forEach(
          (type, values) ->
              groups.put(
                  type,
                  Mockito.mock(
                      EventListenerGroup.class,
                      invocation ->
                          switch (invocation.getMethod().getName()) {
                            case "listeners" -> List.copyOf(values);
                            case "count" -> values.size();
                            default -> Answers.RETURNS_DEFAULTS.answer(invocation);
                          })));
      registry =
          Mockito.mock(
              EventListenerRegistry.class,
              invocation -> {
                Object[] arguments = invocation.getRawArguments();
                String method = invocation.getMethod().getName();
                if (method.equals("getEventListenerGroup")) return groups.get(arguments[0]);
                if (method.equals("appendListeners") || method.equals("setListeners")) {
                  Object type = arguments[0];
                  if (method.equals("appendListeners")
                      && type == EventType.POST_LOAD
                      && failPostAppend.get()) {
                    throw new IllegalStateException("private append failure");
                  }
                  if (method.equals("setListeners")
                      && type == EventType.INIT_COLLECTION
                      && failInitRemoval.get()) {
                    throw new IllegalStateException("private cleanup failure");
                  }
                  List<Object> target = listeners.get(type);
                  if (method.equals("setListeners")) target.clear();
                  target.addAll(List.of((Object[]) arguments[1]));
                  return null;
                }
                return Answers.RETURNS_DEFAULTS.answer(invocation);
              });
    }
  }

  private int countListeners(EventType<?> eventType) {
    return listenerGroup(eventType).count();
  }

  private EventListenerGroup<?> listenerGroup(EventType<?> eventType) {
    EventListenerRegistry registry =
        emf.unwrap(SessionFactoryImplementor.class)
            .getServiceRegistry()
            .getService(EventListenerRegistry.class);
    return registry.getEventListenerGroup(eventType);
  }
}
