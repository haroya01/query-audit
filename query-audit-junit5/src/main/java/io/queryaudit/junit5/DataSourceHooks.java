package io.queryaudit.junit5;

import io.queryaudit.core.interceptor.DataSourceProxyFactory;
import io.queryaudit.core.interceptor.QueryInterceptor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;
import net.ttddyy.dsproxy.listener.ChainListener;
import net.ttddyy.dsproxy.listener.CompositeMethodListener;
import net.ttddyy.dsproxy.support.ProxyDataSource;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;

/** Owns concurrent listener registration and reference-counted replacement of static fields. */
final class DataSourceHooks {
  private static final Map<Field, FieldLease> FIELDS = new HashMap<>();

  private DataSourceHooks() {}

  static Runnable install(
      DataSourceResolver.ResolvedDataSource resolved, QueryInterceptor interceptor) {
    if (resolved.staticField() != null) return installField(resolved.staticField(), interceptor);
    ProxyDataSource proxy = DataSourceResolver.findProxyDataSource(resolved.dataSource());
    if (proxy == null) {
      throw new ExtensionConfigurationException(
          "QueryAudit: the Spring DataSource is not query-aware. Add the QueryAudit Spring Boot"
              + " starter or register a datasource-proxy DataSource bean.");
    }
    return attach(proxy, interceptor);
  }

  private static synchronized Runnable installField(Field field, QueryInterceptor interceptor) {
    DataSource current = read(field);
    FieldLease lease = FIELDS.get(field);
    if (lease != null && current == lease.replacement) {
      Runnable detach = attach(lease.proxy, interceptor);
      lease.users++;
      return releaseOnce(field, lease, detach);
    }
    ProxyDataSource existing = DataSourceResolver.findProxyDataSource(current);
    if (existing != null) return attach(existing, interceptor);
    if (Modifier.isFinal(field.getModifiers())) throw unsupported(field, "is final");

    DataSource replacement = DataSourceProxyFactory.wrap(current, interceptor);
    if (!field.getType().isInstance(replacement)) {
      throw unsupported(
          field,
          "is declared as " + field.getType().getName() + " instead of javax.sql.DataSource");
    }
    ProxyDataSource proxy = DataSourceResolver.findProxyDataSource(replacement);
    synchronized (proxy) {
      stabilize(proxy);
    }
    write(field, replacement, "install the recording proxy in");
    Runnable holderCleanup = QueryAuditDataSourceStore.install(current, replacement, interceptor);
    FieldLease installed = new FieldLease(current, replacement, proxy, holderCleanup);
    FIELDS.put(field, installed);
    return releaseOnce(field, installed, () -> detach(proxy, interceptor));
  }

  private static Runnable releaseOnce(Field field, FieldLease lease, Runnable detach) {
    AtomicBoolean released = new AtomicBoolean();
    return () -> {
      if (!released.compareAndSet(false, true)) return;
      synchronized (DataSourceHooks.class) {
        Throwable failure = AuditResources.runCleanup(null, detach);
        if (--lease.users == 0) {
          failure =
              AuditResources.runCleanup(
                  failure,
                  () -> {
                    if (read(field) == lease.replacement) write(field, lease.original, "restore");
                  });
          FIELDS.remove(field, lease);
          failure = AuditResources.runCleanup(failure, lease.holderCleanup);
        }
        rethrow(failure);
      }
    };
  }

  private static Runnable attach(ProxyDataSource proxy, QueryInterceptor interceptor) {
    synchronized (proxy) {
      stabilize(proxy);
      ChainListener queries = proxy.getProxyConfig().getQueryListener();
      queries.addListener(interceptor);
      try {
        proxy.getProxyConfig().getMethodListener().addListener(interceptor.getConnectionTracker());
      } catch (RuntimeException | Error failure) {
        queries.getListeners().remove(interceptor);
        throw failure;
      }
    }
    AtomicBoolean detached = new AtomicBoolean();
    return () -> {
      if (detached.compareAndSet(false, true)) detach(proxy, interceptor);
    };
  }

  private static void detach(ProxyDataSource proxy, QueryInterceptor interceptor) {
    synchronized (proxy) {
      stabilize(proxy);
      Throwable failure =
          AuditResources.runCleanup(
              null,
              () -> proxy.getProxyConfig().getQueryListener().getListeners().remove(interceptor));
      failure =
          AuditResources.runCleanup(
              failure,
              () ->
                  proxy
                      .getProxyConfig()
                      .getMethodListener()
                      .getListeners()
                      .remove(interceptor.getConnectionTracker()));
      rethrow(failure);
    }
  }

  private static void rethrow(Throwable failure) {
    if (failure instanceof RuntimeException runtime) throw runtime;
    if (failure instanceof Error error) throw error;
  }

  private static void stabilize(ProxyDataSource proxy) {
    ChainListener queries = proxy.getProxyConfig().getQueryListener();
    CompositeMethodListener methods = proxy.getProxyConfig().getMethodListener();
    // In-flight callbacks keep their previous snapshot; no live ArrayList is modified. Subsequent
    // registrations mutate the same COW list rather than replacing another owner's registration.
    if (!(queries.getListeners() instanceof CopyOnWriteArrayList<?>)) {
      queries.setListeners(new CopyOnWriteArrayList<>(queries.getListeners()));
    }
    if (!(methods.getListeners() instanceof CopyOnWriteArrayList<?>)) {
      methods.setListeners(new CopyOnWriteArrayList<>(methods.getListeners()));
    }
  }

  private static DataSource read(Field field) {
    try {
      field.setAccessible(true);
      Object value = field.get(null);
      if (value instanceof DataSource dataSource) return dataSource;
      throw unsupported(field, "no longer contains a DataSource");
    } catch (IllegalAccessException failure) {
      throw new ExtensionConfigurationException(
          "QueryAudit: could not read static DataSource field " + field.getName(), failure);
    }
  }

  private static void write(Field field, DataSource value, String operation) {
    try {
      field.setAccessible(true);
      field.set(null, value);
    } catch (IllegalAccessException | RuntimeException failure) {
      throw new ExtensionConfigurationException(
          "QueryAudit: could not "
              + operation
              + " static DataSource field "
              + field.getDeclaringClass().getName()
              + "."
              + field.getName(),
          failure);
    }
  }

  private static ExtensionConfigurationException unsupported(Field field, String reason) {
    return new ExtensionConfigurationException(
        "QueryAudit: static DataSource field "
            + field.getDeclaringClass().getName()
            + "."
            + field.getName()
            + " "
            + reason
            + ". Declare it as a mutable javax.sql.DataSource field so QueryAudit can install its recording proxy.");
  }

  private static final class FieldLease {
    final DataSource original;
    final DataSource replacement;
    final ProxyDataSource proxy;
    final Runnable holderCleanup;
    int users = 1;

    FieldLease(
        DataSource original,
        DataSource replacement,
        ProxyDataSource proxy,
        Runnable holderCleanup) {
      this.original = original;
      this.replacement = replacement;
      this.proxy = proxy;
      this.holderCleanup = holderCleanup;
    }
  }
}
