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
import net.ttddyy.dsproxy.support.ProxyDataSource;
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
  DataSource resolve(ExtensionContext context) {
    // Strategy 1: Spring ApplicationContext
    try {
      DataSource ds = resolveFromSpringContext(context);
      if (ds != null) return ds;
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
            return ds;
          }
        } catch (IllegalAccessException ignored) {
        }
      }
    }

    return null;
  }

  /**
   * Hooks the QueryInterceptor into the DataSource. If the DataSource is already a ProxyDataSource,
   * adds the interceptor as a listener. Otherwise, wraps with a new ProxyDataSource and replaces
   * the original where possible.
   */
  void hookInterceptor(DataSource dataSource, QueryInterceptor interceptor) {
    // Strategy 1: DataSource is already a ProxyDataSource (e.g., gavlyukovskiy)
    ProxyDataSource proxy = findProxyDataSource(dataSource);
    if (proxy != null) {
      proxy.addListener(interceptor);
      return;
    }

    // Strategy 2: Wrap with our own proxy via DataSourceProxyFactory
    QueryAuditDataSourceStore.set(
        dataSource, DataSourceProxyFactory.wrap(dataSource, interceptor), interceptor);
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
}
