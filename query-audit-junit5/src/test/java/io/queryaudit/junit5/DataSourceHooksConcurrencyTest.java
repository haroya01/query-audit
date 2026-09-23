package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.queryaudit.core.interceptor.QueryInterceptor;
import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import net.ttddyy.dsproxy.listener.QueryExecutionListener;
import net.ttddyy.dsproxy.support.ProxyDataSource;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;

class DataSourceHooksConcurrencyTest {
  static DataSource shared;

  @Test
  void staleDiscoveryAndRepeatedCleanupPreserveTheRemainingFieldOwner() throws Exception {
    DataSource original = source();
    shared = original;
    Field field = getClass().getDeclaredField("shared");
    var stale = new DataSourceResolver.ResolvedDataSource(original, field);
    var first = new QueryInterceptor();
    var second = new QueryInterceptor();
    Runnable releaseFirst = DataSourceHooks.install(stale, first);
    DataSource replacement = shared;
    Runnable releaseSecond = DataSourceHooks.install(stale, second);
    try {
      releaseFirst.run();
      releaseFirst.run();
      assertThat(shared).isSameAs(replacement);
      second.start();
      query(shared);
      second.stop();
      assertThat(second.getRecordedQueries()).hasSize(1);
    } finally {
      releaseFirst.run();
      releaseSecond.run();
    }
    assertThat(shared).isSameAs(original);
    assertThat(QueryAuditDataSourceStore.get()).isNull();
  }

  @Test
  void externallyReplacedFieldIsNotOverwrittenDuringCleanup() throws Exception {
    shared = source();
    Runnable cleanup =
        DataSourceHooks.install(
            new DataSourceResolver.ResolvedDataSource(
                shared, getClass().getDeclaredField("shared")),
            new QueryInterceptor());
    DataSource externallyOwned = source();
    shared = externallyOwned;
    cleanup.run();
    assertThat(shared).isSameAs(externallyOwned);
    assertThat(QueryAuditDataSourceStore.get()).isNull();
  }

  @Test
  void listenerFailureStillRestoresFieldAndReleasesMethodListenerAndHolder() throws Exception {
    DataSource original = source();
    shared = original;
    var interceptor = new QueryInterceptor();
    Runnable cleanup =
        DataSourceHooks.install(
            new DataSourceResolver.ResolvedDataSource(
                shared, getClass().getDeclaredField("shared")),
            interceptor);
    ProxyDataSource proxy = DataSourceResolver.findProxyDataSource(shared);
    proxy
        .getProxyConfig()
        .getQueryListener()
        .setListeners(
            new CopyOnWriteArrayList<QueryExecutionListener>(List.of(interceptor)) {
              @Override
              public boolean remove(Object value) {
                super.remove(value);
                throw new IllegalStateException("synthetic detach failure");
              }
            });
    assertThatThrownBy(cleanup::run).isInstanceOf(IllegalStateException.class);
    assertThat(shared).isSameAs(original);
    assertThat(proxy.getProxyConfig().getMethodListener().getListeners())
        .doesNotContain(interceptor.getConnectionTracker());
    assertThat(QueryAuditDataSourceStore.get()).isNull();
    cleanup.run();
  }

  @Test
  void crossThreadCleanupClearsTheInstallingWorkersHolder() throws Exception {
    var executor = Executors.newSingleThreadExecutor();
    var cleanup = new AtomicReference<Runnable>();
    var barrier = new CyclicBarrier(2);
    try {
      var result =
          executor.submit(
              () -> {
                DataSource original = source();
                cleanup.set(
                    QueryAuditDataSourceStore.install(original, original, new QueryInterceptor()));
                barrier.await(10, TimeUnit.SECONDS);
                barrier.await(10, TimeUnit.SECONDS);
                return QueryAuditDataSourceStore.get();
              });
      barrier.await(10, TimeUnit.SECONDS);
      cleanup.get().run();
      barrier.await(10, TimeUnit.SECONDS);
      assertThat(result.get(10, TimeUnit.SECONDS)).isNull();
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void registrationsAndCallbacksOverlapWithoutLosingOrLeakingListeners() throws Exception {
    ProxyDataSource proxy = new ProxyDataSource(source());
    List<?> originalQueries = List.copyOf(proxy.getProxyConfig().getQueryListener().getListeners());
    List<?> originalMethods =
        List.copyOf(proxy.getProxyConfig().getMethodListener().getListeners());
    var executor = Executors.newFixedThreadPool(3);
    var barrier = new CyclicBarrier(3);
    try {
      var queries =
          executor.submit(
              () -> {
                barrier.await(10, TimeUnit.SECONDS);
                for (int i = 0; i < 300; i++) query(proxy);
                return null;
              });
      java.util.concurrent.Callable<Void> registrations =
          () -> {
            barrier.await(10, TimeUnit.SECONDS);
            for (int i = 0; i < 100; i++) {
              Runnable cleanup =
                  DataSourceHooks.install(
                      DataSourceResolver.ResolvedDataSource.fromSpring(proxy),
                      new QueryInterceptor());
              try {
                query(proxy);
              } finally {
                cleanup.run();
                cleanup.run();
              }
            }
            return null;
          };
      var first = executor.submit(registrations);
      var second = executor.submit(registrations);
      queries.get(20, TimeUnit.SECONDS);
      first.get(20, TimeUnit.SECONDS);
      second.get(20, TimeUnit.SECONDS);
    } finally {
      executor.shutdownNow();
    }
    assertThat(proxy.getProxyConfig().getQueryListener().getListeners()).isEqualTo(originalQueries);
    assertThat(proxy.getProxyConfig().getMethodListener().getListeners())
        .isEqualTo(originalMethods);
  }

  private static DataSource source() {
    var source = new JdbcDataSource();
    source.setURL("jdbc:h2:mem:hook-concurrency");
    return source;
  }

  private static void query(DataSource dataSource) throws Exception {
    try (var connection = dataSource.getConnection();
        var statement = connection.createStatement();
        var rows = statement.executeQuery("SELECT 1")) {
      assertThat(rows.next()).isTrue();
    }
  }
}
