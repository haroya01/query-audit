package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.interceptor.QueryInterceptor;
import javax.sql.DataSource;
import net.ttddyy.dsproxy.listener.ChainListener;
import net.ttddyy.dsproxy.listener.QueryExecutionListener;
import net.ttddyy.dsproxy.support.ProxyDataSource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Regression for the Strategy-1 ({@link ProxyDataSource} reuse) path in {@link
 * DataSourceResolver#hookInterceptor}. Each test class creates a fresh {@link QueryInterceptor},
 * and the previous one must be detached from the shared proxy when the test class finishes —
 * otherwise listeners accumulate across the JVM run, leaking memory and causing queries from a
 * later test class to be dispatched to all prior interceptors.
 */
class DataSourceResolverListenerLifecycleTest {

  @Test
  @DisplayName(
      "Strategy 1: hookInterceptor returns a cleanup that detaches the listener from the proxy")
  void hookInterceptor_returnsCleanupThatDetachesListener() {
    ProxyDataSource proxy = new ProxyDataSource(Mockito.mock(DataSource.class));
    DataSourceResolver resolver = new DataSourceResolver();
    QueryInterceptor interceptor = new QueryInterceptor();

    int before = listenerCount(proxy);
    Runnable cleanup =
        resolver.hookInterceptor(
            DataSourceResolver.ResolvedDataSource.fromSpring(proxy), interceptor);

    assertThat(cleanup).as("Strategy 1 must return a non-null cleanup").isNotNull();
    assertThat(listenerCount(proxy)).isEqualTo(before + 1);
    assertThat(listenerInstances(proxy)).contains(interceptor);

    cleanup.run();

    assertThat(listenerCount(proxy)).isEqualTo(before);
    assertThat(listenerInstances(proxy)).doesNotContain(interceptor);
  }

  @Test
  @DisplayName(
      "Strategy 1: repeated hook/release cycles do not accumulate listeners on a shared proxy")
  void repeatedHookAndReleaseCyclesDoNotAccumulate() {
    ProxyDataSource proxy = new ProxyDataSource(Mockito.mock(DataSource.class));
    DataSourceResolver resolver = new DataSourceResolver();

    int baseline = listenerCount(proxy);

    for (int i = 0; i < 5; i++) {
      QueryInterceptor interceptor = new QueryInterceptor();
      Runnable cleanup =
          resolver.hookInterceptor(
              DataSourceResolver.ResolvedDataSource.fromSpring(proxy), interceptor);
      assertThat(cleanup).isNotNull();
      cleanup.run();
    }

    assertThat(listenerCount(proxy))
        .as("After N register/release cycles, listener count must return to baseline")
        .isEqualTo(baseline);
  }

  private static int listenerCount(ProxyDataSource proxy) {
    return chain(proxy).getListeners().size();
  }

  private static java.util.List<QueryExecutionListener> listenerInstances(ProxyDataSource proxy) {
    return chain(proxy).getListeners();
  }

  private static ChainListener chain(ProxyDataSource proxy) {
    return proxy.getProxyConfig().getQueryListener();
  }
}
