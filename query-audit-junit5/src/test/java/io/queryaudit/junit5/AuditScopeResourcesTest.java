package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.queryaudit.core.interceptor.LazyLoadTracker;
import io.queryaudit.core.interceptor.QueryInterceptor;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class AuditScopeResourcesTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void oneTypedOwnerSuppliesCaptureAndCleanupAcrossNestedContexts(boolean inheritedStore) {
    Context owner = context("class:outer", null, inheritedStore);
    Context nested = context("class:outer/nested", owner, inheritedStore);
    Context method = context("class:outer/nested/method", nested, inheritedStore);
    QueryInterceptor interceptor = mock(QueryInterceptor.class);
    DataSource dataSource = mock(DataSource.class);
    LazyLoadTracker tracker = mock(LazyLoadTracker.class);
    AtomicInteger hookRemoved = new AtomicInteger();
    AtomicInteger trackerRemoved = new AtomicInteger();
    AuditResources resources =
        new AuditResources(
            owner.scope().identity(),
            interceptor,
            dataSource,
            hookRemoved::incrementAndGet,
            () -> {});

    owner.scope().installCapture(resources);
    resources.attachTracker(tracker, trackerRemoved::incrementAndGet);

    assertThat(owner.store().local).containsOnlyKeys(AuditResources.class.getName());
    assertThat(method.scope().interceptor()).isSameAs(interceptor);
    assertThat(method.scope().dataSource()).isSameAs(dataSource);
    assertThat(method.scope().tracker()).isSameAs(tracker);
    assertThat(method.scope().captureResources()).isSameAs(resources);
    assertThat(nested.scope().captureResources()).isSameAs(resources);
    assertThat(owner.scope().ownsResources()).isTrue();
    assertThat(nested.scope().ownsResources()).isFalse();
    assertThat(method.scope().ownsResources()).isFalse();

    method.scope().captureResources().startCapture();
    method.scope().captureResources().stopCapture();
    HibernateIntegration hibernate = mock(HibernateIntegration.class);
    AuditResources cleanup = owner.scope().resourcesForCleanup(hibernate);
    assertThat(cleanup).isSameAs(resources);
    assertThat(cleanup.close(null)).isNull();
    assertThat(owner.scope().resourcesForCleanup(hibernate).close(null)).isNull();

    verify(interceptor).start();
    verify(interceptor).stop();
    verify(tracker).start();
    verify(tracker).stop();
    assertThat(hookRemoved).hasValue(1);
    assertThat(trackerRemoved).hasValue(1);
    verifyNoInteractions(dataSource, hibernate);
  }

  @Test
  void staleLegacyFieldsCannotOverrideAnInstalledTypedOwnerOrFillItsAbsentTracker() {
    Context owner = context("class:owner", null, false);
    QueryInterceptor interceptor = new QueryInterceptor();
    DataSource dataSource = mock(DataSource.class);
    AuditResources resources =
        new AuditResources(owner.scope().identity(), interceptor, dataSource, null, () -> {});
    owner.scope().installCapture(resources);
    owner.store().put("interceptor", new QueryInterceptor());
    owner.store().put("dataSource", mock(DataSource.class));
    owner.store().put("lazyLoadTracker", new LazyLoadTracker());
    owner.store().put("auditResourceOwner", "stale:owner");

    assertThat(owner.scope().interceptor()).isSameAs(interceptor);
    assertThat(owner.scope().dataSource()).isSameAs(dataSource);
    assertThat(owner.scope().tracker()).isNull();
    assertThat(owner.scope().ownsResources()).isTrue();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void nestedOwnerCleanupAndRollbackLeaveTheParentResourceIntact(boolean inheritedStore) {
    Context parent = context("class:parent", null, inheritedStore);
    Context nested = context("class:parent/nested", parent, inheritedStore);
    Context method = context("class:parent/nested/method", nested, inheritedStore);
    AtomicInteger parentCleanup = new AtomicInteger();
    AtomicInteger nestedCleanup = new AtomicInteger();
    AuditResources parentResources =
        new AuditResources(
            parent.scope().identity(),
            new QueryInterceptor(),
            null,
            parentCleanup::incrementAndGet,
            () -> {});
    AuditResources nestedResources =
        new AuditResources(
            nested.scope().identity(),
            new QueryInterceptor(),
            null,
            nestedCleanup::incrementAndGet,
            () -> {});
    parent.scope().installCapture(parentResources);
    nested.scope().installCapture(nestedResources);
    nested.scope().countPolicies(Map.of(), Map.of());
    nested.scope().flag(AuditScope.Flag.INITIALIZATION_FAILED, true);
    assertThat(nested.scope().ownsResources()).isTrue();
    assertThat(method.scope().captureResources()).isSameAs(nestedResources);

    RuntimeException initializationFailure = new IllegalStateException("initialization");
    assertThat(nestedResources.close(initializationFailure)).isSameAs(initializationFailure);
    nested.scope().clearInitialization();

    assertThat(nested.store().local).containsOnlyKeys("initializationFailureRecorded");
    assertThat(nested.scope().flag(AuditScope.Flag.INITIALIZATION_FAILED)).isTrue();
    assertThat(nested.scope().ownsResources()).isFalse();
    assertThat(method.scope().captureResources()).isSameAs(parentResources);
    assertThat(parentResources.isClosed()).isFalse();
    assertThat(parentCleanup).hasValue(0);
    assertThat(nestedCleanup).hasValue(1);
    assertThat(nestedResources.close(null)).isNull();
    assertThat(nestedCleanup).hasValue(1);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void legacyStoreCaptureRemainsReadableAndOwnerCleanupIsCachedOnce(boolean inheritedStore) {
    Context owner = context("class:legacy", null, inheritedStore);
    Context nested = context("class:legacy/nested", owner, inheritedStore);
    Context method = context("class:legacy/nested/method", nested, inheritedStore);
    QueryInterceptor interceptor = mock(QueryInterceptor.class);
    LazyLoadTracker tracker = mock(LazyLoadTracker.class);
    DataSource dataSource = mock(DataSource.class);
    AtomicInteger hookRemoved = new AtomicInteger();
    owner.store().put("interceptor", interceptor);
    owner.store().put("lazyLoadTracker", tracker);
    owner.store().put("dataSource", dataSource);
    owner.store().put("auditResourceOwner", owner.scope().identity());
    owner.store().put("dataSourceHookCleanup", (Runnable) hookRemoved::incrementAndGet);

    assertThat(method.scope().interceptor()).isSameAs(interceptor);
    assertThat(method.scope().tracker()).isSameAs(tracker);
    assertThat(method.scope().dataSource()).isSameAs(dataSource);
    assertThat(owner.scope().ownsResources()).isTrue();
    assertThat(nested.scope().ownsResources()).isFalse();
    method.scope().captureResources().startCapture();
    method.scope().captureResources().stopCapture();
    assertThat(method.store().local).isEmpty();
    assertThat(owner.store().local).doesNotContainKey(AuditResources.class.getName());
    assertThat(hookRemoved).hasValue(0);

    HibernateIntegration hibernate = mock(HibernateIntegration.class);
    AuditResources cleanup = owner.scope().resourcesForCleanup(hibernate);
    assertThat(cleanup.close(null)).isNull();
    assertThat(owner.scope().resourcesForCleanup(hibernate)).isSameAs(cleanup);
    assertThat(cleanup.close(null)).isNull();

    verify(interceptor).start();
    verify(interceptor).stop();
    verify(hibernate, times(1)).unregisterTracker(owner.junit(), tracker);
    assertThat(hookRemoved).hasValue(1);
    verifyNoInteractions(dataSource);
    owner.scope().clearInitialization();
    assertThat(owner.store().local).isEmpty();
  }

  private static Context context(String id, Context parent, boolean inheritedStore) {
    MapStore store = new MapStore(inheritedStore && parent != null ? parent.store() : null);
    ExtensionContext context = mock(ExtensionContext.class);
    when(context.getUniqueId()).thenReturn(id);
    when(context.getStore(any(ExtensionContext.Namespace.class))).thenReturn(store);
    when(context.getParent()).thenReturn(Optional.ofNullable(parent).map(Context::junit));
    ExtensionContext root = parent == null ? context : parent.junit().getRoot();
    when(context.getRoot()).thenReturn(root);
    doReturn(AuditScopeResourcesTest.class).when(context).getRequiredTestClass();
    return new Context(context, store);
  }

  private record Context(ExtensionContext junit, MapStore store) {
    AuditScope scope() {
      return AuditScope.of(junit);
    }
  }

  /** Models both JUnit's inherited Store and older non-inheriting callback fixtures. */
  private static final class MapStore implements ExtensionContext.Store {
    private final Map<Object, Object> local = new LinkedHashMap<>();
    private final MapStore parent;

    private MapStore(MapStore parent) {
      this.parent = parent;
    }

    @Override
    public Object get(Object key) {
      Object value = local.get(key);
      return value == null && parent != null ? parent.get(key) : value;
    }

    @Override
    public <V> V get(Object key, Class<V> requiredType) {
      return requiredType.cast(get(key));
    }

    @Override
    public <K, V> Object getOrComputeIfAbsent(K key, Function<K, V> defaultCreator) {
      Object value = get(key);
      if (value == null) {
        value = defaultCreator.apply(key);
        put(key, value);
      }
      return value;
    }

    @Override
    public <K, V> V getOrComputeIfAbsent(
        K key, Function<K, V> defaultCreator, Class<V> requiredType) {
      return requiredType.cast(getOrComputeIfAbsent(key, defaultCreator));
    }

    @Override
    public void put(Object key, Object value) {
      local.put(key, value);
    }

    @Override
    public Object remove(Object key) {
      return local.remove(key);
    }

    @Override
    public <V> V remove(Object key, Class<V> requiredType) {
      return requiredType.cast(remove(key));
    }
  }
}
