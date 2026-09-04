package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.queryaudit.core.interceptor.LazyLoadTracker;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.event.spi.InitializeCollectionEvent;
import org.hibernate.event.spi.PostLoadEvent;
import org.junit.jupiter.api.Test;

class HibernateLazyLoadListenerTest {

  @Test
  void stoppedTrackerDoesNotInspectCollectionEvents() {
    LazyLoadTracker tracker = stoppedTracker();
    InitializeCollectionEvent event = mock(InitializeCollectionEvent.class);

    new HibernateLazyLoadListener(tracker).onInitializeCollection(event);

    verifyNoInteractions(event);
    assertThat(tracker.getRecords()).isEmpty();
  }

  @Test
  void stoppedTrackerDoesNotInspectPostLoadEvents() {
    LazyLoadTracker tracker = stoppedTracker();
    PostLoadEvent event = mock(PostLoadEvent.class);
    when(event.getEntity()).thenReturn(new Object());
    when(event.getId()).thenReturn(42L);

    new HibernateLazyLoadListener(tracker).onPostLoad(event);

    verifyNoInteractions(event);
    assertThat(tracker.getRecords()).isEmpty();
    assertThat(tracker.getExplicitLoads()).isEmpty();
  }

  @Test
  void activeTrackerRecordsCollectionEvents() {
    LazyLoadTracker tracker = new LazyLoadTracker();
    tracker.start();
    PersistentCollection<?> collection = mock(PersistentCollection.class);
    when(collection.getRole()).thenReturn("example.Team.members");
    InitializeCollectionEvent event = mock(InitializeCollectionEvent.class);
    doReturn(collection).when(event).getCollection();
    when(event.getAffectedOwnerEntityName()).thenReturn("example.Team");
    when(event.getAffectedOwnerIdOrNull()).thenReturn(42L);

    new HibernateLazyLoadListener(tracker).onInitializeCollection(event);

    assertThat(tracker.getRecords())
        .singleElement()
        .satisfies(
            record -> {
              assertThat(record.collectionRole()).isEqualTo("example.Team.members");
              assertThat(record.ownerEntity()).isEqualTo("example.Team");
              assertThat(record.ownerIdString()).isEqualTo("42");
            });
  }

  private static LazyLoadTracker stoppedTracker() {
    LazyLoadTracker tracker = new LazyLoadTracker();
    tracker.start();
    tracker.stop();
    return tracker;
  }
}
