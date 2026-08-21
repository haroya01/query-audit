package io.queryaudit.core.interceptor;

import javax.sql.DataSource;
import net.ttddyy.dsproxy.support.ProxyDataSourceBuilder;

/**
 * Utility for wrapping a {@link DataSource} with datasource-proxy so that all executed queries are
 * forwarded to a {@link QueryInterceptor}.
 *
 * @author haroya
 * @since 0.2.0
 */
public final class DataSourceProxyFactory {

  private DataSourceProxyFactory() {
    // utility class
  }

  /**
   * Wraps the given {@code DataSource} in a proxy that delegates query lifecycle events to the
   * supplied {@link QueryInterceptor}. The proxy is itself wrapped in a {@link
   * NonClosingDataSource} so Spring does not auto-infer a destroy method on the post-BPP instance —
   * see issue #153 for the cascade-close regression this prevents.
   *
   * @param original the real data source to wrap
   * @param interceptor the interceptor that will record queries
   * @return a non-{@code Closeable} wrapper around the query-recording proxy
   */
  public static DataSource wrap(DataSource original, QueryInterceptor interceptor) {
    DataSource proxy =
        ProxyDataSourceBuilder.create(original)
            .name("query-audit")
            .listener(interceptor)
            .methodListener(interceptor.getConnectionTracker())
            .build();
    return new NonClosingDataSource(proxy);
  }
}
