package io.queryaudit.junit5;

import io.queryaudit.core.interceptor.QueryInterceptor;
import javax.sql.DataSource;

/**
 * Thread-local holder for the proxied DataSource and its associated {@link QueryInterceptor}. Used
 * by the extension to manage the interceptor lifecycle within a test execution.
 *
 * @author haroya
 * @since 0.2.0
 */
public class QueryAuditDataSourceStore {

  private static final ThreadLocal<QueryInterceptorHolder> HOLDER = new ThreadLocal<>();

  public static void set(DataSource original, DataSource proxied, QueryInterceptor interceptor) {
    HOLDER.set(new QueryInterceptorHolder(original, proxied, interceptor));
  }

  public static QueryInterceptorHolder get() {
    return HOLDER.get();
  }

  public static void clear() {
    HOLDER.remove();
  }

  public record QueryInterceptorHolder(
      DataSource originalDataSource, DataSource proxiedDataSource, QueryInterceptor interceptor) {}
}
