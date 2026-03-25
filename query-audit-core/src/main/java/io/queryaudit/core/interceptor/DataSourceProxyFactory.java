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
   * supplied {@link QueryInterceptor}.
   *
   * @param original the real data source to wrap
   * @param interceptor the interceptor that will record queries
   * @return a proxy data source
   */
  public static DataSource wrap(DataSource original, QueryInterceptor interceptor) {
    return ProxyDataSourceBuilder.create(original)
        .name("query-audit")
        .listener(interceptor)
        .build();
  }
}
