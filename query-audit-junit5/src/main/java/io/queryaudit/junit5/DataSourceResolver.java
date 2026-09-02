package io.queryaudit.junit5;

import io.queryaudit.core.interceptor.DataSourceProxyFactory;
import io.queryaudit.core.interceptor.QueryInterceptor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.sql.DataSource;
import net.ttddyy.dsproxy.listener.ChainListener;
import net.ttddyy.dsproxy.listener.CompositeMethodListener;
import net.ttddyy.dsproxy.listener.MethodExecutionListener;
import net.ttddyy.dsproxy.listener.QueryExecutionListener;
import net.ttddyy.dsproxy.support.ProxyDataSource;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Resolves the {@link DataSource} from the test context and hooks the {@link QueryInterceptor} into
 * it. Supports Spring ApplicationContext-based resolution and static field-based resolution.
 *
 * @author haroya
 * @since 0.2.0
 */
class DataSourceResolver {

  /**
   * Resolves a DataSource from the given extension context. Tries Spring ApplicationContext first,
   * then falls back to static DataSource fields on the test class.
   */
  ResolvedDataSource resolve(ExtensionContext context) {
    // Strategy 1: Spring ApplicationContext
    try {
      DataSource ds = resolveFromSpringContext(context);
      if (ds != null) return ResolvedDataSource.fromSpring(ds);
    } catch (Exception | NoClassDefFoundError ignored) {
    }

    // Strategy 2: Static DataSource field on the test class
    Class<?> testClass = context.getRequiredTestClass();
    for (Field field : getAllFields(testClass)) {
      if (DataSource.class.isAssignableFrom(field.getType())
          && Modifier.isStatic(field.getModifiers())) {
        try {
          field.setAccessible(true);
          Object value = field.get(null);
          if (value instanceof DataSource ds) {
            return ResolvedDataSource.fromStaticField(ds, field);
          }
        } catch (IllegalAccessException ignored) {
        }
      }
    }

    return null;
  }

  /**
   * Hooks the QueryInterceptor into the DataSource and returns a cleanup callback that must be
   * invoked from {@code afterAll} so that listeners do not accumulate on a shared proxy across test
   * classes. If the DataSource is already a {@link ProxyDataSource}, the interceptor is added as a
   * listener (Strategy 1). Otherwise, a fresh proxy is created via {@link DataSourceProxyFactory}
   * and installed in the mutable static field used by a plain JUnit test (Strategy 2). Cleanup
   * restores the original field value.
   *
   * @return a cleanup callback that detaches the interceptor; never {@code null}
   */
  Runnable hookInterceptor(ResolvedDataSource resolved, QueryInterceptor interceptor) {
    DataSource dataSource = resolved.dataSource();

    // Strategy 1: DataSource is already a ProxyDataSource (e.g., gavlyukovskiy)
    ProxyDataSource proxy = findProxyDataSource(dataSource);
    if (proxy != null) {
      proxy.addListener(interceptor);
      ChainListener chain = proxy.getProxyConfig().getQueryListener();
      CompositeMethodListener methodChain = proxy.getProxyConfig().getMethodListener();
      methodChain.addListener(interceptor.getConnectionTracker());
      return () -> {
        detachListener(chain, interceptor);
        detachMethodListener(methodChain, interceptor.getConnectionTracker());
      };
    }

    // Strategy 2: Replace a plain JUnit static field with our own proxy.
    Field field = resolved.staticField();
    if (field == null) {
      throw new ExtensionConfigurationException(
          "QueryAudit: the Spring DataSource is not query-aware. Add the QueryAudit Spring Boot"
              + " starter or register a datasource-proxy DataSource bean.");
    }
    if (Modifier.isFinal(field.getModifiers())) {
      throw unsupportedStaticField(field, "is final");
    }

    DataSource proxied = DataSourceProxyFactory.wrap(dataSource, interceptor);
    if (!field.getType().isInstance(proxied)) {
      throw unsupportedStaticField(
          field,
          "is declared as " + field.getType().getName() + " instead of javax.sql.DataSource");
    }

    setStaticField(field, proxied);
    QueryAuditDataSourceStore.set(dataSource, proxied, interceptor);
    return () -> {
      try {
        restoreStaticField(field, dataSource, proxied);
      } finally {
        QueryAuditDataSourceStore.clear();
      }
    };
  }

  private static ExtensionConfigurationException unsupportedStaticField(
      Field field, String reason) {
    return new ExtensionConfigurationException(
        "QueryAudit: static DataSource field "
            + field.getDeclaringClass().getName()
            + "."
            + field.getName()
            + " "
            + reason
            + ". Declare it as a mutable javax.sql.DataSource field so QueryAudit can install its"
            + " recording proxy.");
  }

  private static void setStaticField(Field field, DataSource value) {
    try {
      field.setAccessible(true);
      field.set(null, value);
    } catch (IllegalAccessException | RuntimeException e) {
      throw new ExtensionConfigurationException(
          "QueryAudit: could not install the recording proxy in static DataSource field "
              + field.getDeclaringClass().getName()
              + "."
              + field.getName(),
          e);
    }
  }

  private static void restoreStaticField(Field field, DataSource original, DataSource proxied) {
    try {
      field.setAccessible(true);
      if (field.get(null) == proxied) {
        field.set(null, original);
      }
    } catch (IllegalAccessException | RuntimeException e) {
      throw new ExtensionConfigurationException(
          "QueryAudit: could not restore static DataSource field "
              + field.getDeclaringClass().getName()
              + "."
              + field.getName(),
          e);
    }
  }

  /**
   * Removes a previously-added listener from a {@link ChainListener}. {@code ChainListener} does
   * not expose a {@code removeListener} API directly, so we copy → filter → reinstall.
   */
  private static void detachMethodListener(
      CompositeMethodListener chain, MethodExecutionListener target) {
    List<MethodExecutionListener> remaining = new ArrayList<>(chain.getListeners());
    if (remaining.remove(target)) {
      chain.setListeners(remaining);
    }
  }

  private static void detachListener(ChainListener chain, QueryExecutionListener target) {
    List<QueryExecutionListener> remaining = new ArrayList<>(chain.getListeners());
    if (remaining.remove(target)) {
      chain.setListeners(remaining);
    }
  }

  private DataSource resolveFromSpringContext(ExtensionContext context) {
    try {
      Class<?> springExtensionClass =
          Class.forName("org.springframework.test.context.junit.jupiter.SpringExtension");
      Method getAppContext =
          springExtensionClass.getMethod("getApplicationContext", ExtensionContext.class);
      Object appContext = getAppContext.invoke(null, context);
      if (appContext != null) {
        Method getBean = appContext.getClass().getMethod("getBean", Class.class);
        Object ds = getBean.invoke(appContext, DataSource.class);
        if (ds instanceof DataSource dataSource) {
          return dataSource;
        }
      }
    } catch (Exception ignored) {
    }
    return null;
  }

  /**
   * Walks the DataSource chain to find a ProxyDataSource. Handles Spring's DelegatingDataSource
   * wrappers.
   */
  private ProxyDataSource findProxyDataSource(DataSource dataSource) {
    DataSource current = dataSource;
    Set<DataSource> visited = new HashSet<>();

    while (current != null && visited.add(current)) {
      if (current instanceof ProxyDataSource proxy) {
        return proxy;
      }

      // Try Spring's DelegatingDataSource via reflection (avoid hard dependency)
      try {
        Method getTarget = current.getClass().getMethod("getTargetDataSource");
        Object target = getTarget.invoke(current);
        if (target instanceof DataSource ds) {
          current = ds;
          continue;
        }
      } catch (Exception ignored) {
        // Not a DelegatingDataSource
      }

      // Try unwrap (JDBC standard)
      try {
        if (current.isWrapperFor(ProxyDataSource.class)) {
          return current.unwrap(ProxyDataSource.class);
        }
      } catch (Exception ignored) {
        // unwrap not supported
      }

      break;
    }
    return null;
  }

  static List<Field> getAllFields(Class<?> clazz) {
    List<Field> fields = new ArrayList<>();
    Class<?> current = clazz;
    while (current != null && current != Object.class) {
      Collections.addAll(fields, current.getDeclaredFields());
      current = current.getSuperclass();
    }
    return fields;
  }

  record ResolvedDataSource(DataSource dataSource, Field staticField) {

    static ResolvedDataSource fromSpring(DataSource dataSource) {
      return new ResolvedDataSource(dataSource, null);
    }

    static ResolvedDataSource fromStaticField(DataSource dataSource, Field field) {
      return new ResolvedDataSource(dataSource, field);
    }
  }
}
