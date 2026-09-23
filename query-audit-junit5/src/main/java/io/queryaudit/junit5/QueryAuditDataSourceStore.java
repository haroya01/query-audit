package io.queryaudit.junit5;

import io.queryaudit.core.interceptor.QueryInterceptor;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;

/**
 * Legacy thread-local view of an installed DataSource and its router. Invocation routing is owned
 * by {@link io.queryaudit.core.interceptor.QueryCaptureSession}, not by this compatibility holder.
 *
 * @author haroya
 * @since 0.2.0
 */
public class QueryAuditDataSourceStore {

  private static final ThreadLocal<AtomicReference<QueryInterceptorHolder>> HOLDER =
      new ThreadLocal<>();

  public static void set(DataSource original, DataSource proxied, QueryInterceptor interceptor) {
    HOLDER.set(new AtomicReference<>(new QueryInterceptorHolder(original, proxied, interceptor)));
  }

  public static QueryInterceptorHolder get() {
    AtomicReference<QueryInterceptorHolder> slot = HOLDER.get();
    if (slot == null) return null;
    QueryInterceptorHolder holder = slot.get();
    if (holder == null) HOLDER.remove();
    return holder;
  }

  static Runnable install(DataSource original, DataSource proxied, QueryInterceptor interceptor) {
    set(original, proxied, interceptor);
    AtomicReference<QueryInterceptorHolder> slot = HOLDER.get();
    return () -> {
      // JUnit may close a class on another worker: release the original worker's references too.
      slot.set(null);
      if (HOLDER.get() == slot) HOLDER.remove();
    };
  }

  public static void clear() {
    HOLDER.remove();
  }

  public record QueryInterceptorHolder(
      DataSource originalDataSource, DataSource proxiedDataSource, QueryInterceptor interceptor) {}
}
