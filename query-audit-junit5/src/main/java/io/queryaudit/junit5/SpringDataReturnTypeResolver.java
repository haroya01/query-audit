package io.queryaudit.junit5;

import io.queryaudit.core.detector.RepositoryReturnType;
import io.queryaudit.core.detector.RepositoryReturnTypeResolver;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Resolves Spring Data repository method return types by inspecting the ApplicationContext.
 * Parses JDK proxy frames from the stack trace to extract the proxy class and method name,
 * then maps the proxy to its repository interface and reflects on the method's return type.
 *
 * <p>Keyed by "proxyClassName.methodName" to avoid method name collisions across different
 * repositories (e.g., {@code UserRepository.findByStatus} returning {@code Optional<User>}
 * vs {@code OrderRepository.findByStatus} returning {@code List<Order>}).
 *
 * <p>All Spring classes are accessed via reflection to avoid compile-time dependencies on
 * Spring Data in this module.
 *
 * @author haroya
 * @since 0.3.0
 */
class SpringDataReturnTypeResolver implements RepositoryReturnTypeResolver {

  /**
   * Matches JDK dynamic proxy frames: {@code jdk.proxy3.$Proxy296.findByRoomId:-1}
   * Group 1: full proxy class (e.g., {@code jdk.proxy3.$Proxy296})
   * Group 2: method name (e.g., {@code findByRoomId})
   */
  private static final Pattern PROXY_FRAME_PATTERN =
      Pattern.compile("(jdk\\.proxy\\d+\\.\\$Proxy\\d+)\\.(\\w+):-?\\d+");

  private final Map<String, RepositoryReturnType> cache = new ConcurrentHashMap<>();

  /**
   * Maps "proxyClassName.methodName" → return type. Built at construction time by scanning
   * the ApplicationContext for all Spring Data repository beans.
   */
  private final Map<String, RepositoryReturnType> proxyMethodReturnTypes;

  /**
   * Creates a resolver by scanning the ApplicationContext for Spring Data repository beans.
   *
   * @param applicationContext the Spring ApplicationContext (as Object to avoid compile-time dep)
   */
  SpringDataReturnTypeResolver(Object applicationContext) {
    this.proxyMethodReturnTypes = scanRepositories(applicationContext);
  }

  @Override
  public RepositoryReturnType resolve(String stackTrace) {
    if (stackTrace == null || stackTrace.isEmpty()) {
      return RepositoryReturnType.UNKNOWN;
    }

    String key = extractProxyKey(stackTrace);
    if (key == null) {
      return RepositoryReturnType.UNKNOWN;
    }

    return cache.computeIfAbsent(key, k -> {
      // Try exact match first (proxyClass.methodName)
      RepositoryReturnType type = proxyMethodReturnTypes.get(k);
      if (type != null) {
        return type;
      }
      // No fallback by method name alone — different repositories may have
      // methods with the same name but different return types. Returning
      // UNKNOWN keeps the detector's default WARNING, which is safer than
      // guessing the wrong type and silently suppressing a real issue.
      return RepositoryReturnType.UNKNOWN;
    });
  }

  /**
   * Extracts the proxy key "proxyClassName.methodName" from the stack trace.
   * Returns null if no proxy frame is found.
   */
  static String extractProxyKey(String stackTrace) {
    if (stackTrace == null) {
      return null;
    }
    Matcher matcher = PROXY_FRAME_PATTERN.matcher(stackTrace);
    if (matcher.find()) {
      return matcher.group(1) + "." + matcher.group(2);
    }
    return null;
  }

  /**
   * Extracts just the method name from the stack trace (for testing/backward compat).
   */
  static String extractProxyMethodName(String stackTrace) {
    String key = extractProxyKey(stackTrace);
    if (key == null) {
      return null;
    }
    return key.substring(key.lastIndexOf('.') + 1);
  }

  /**
   * Scans the ApplicationContext for all beans whose class implements a Spring Data Repository
   * interface, then inspects each method's return type (including inherited methods).
   */
  @SuppressWarnings("unchecked")
  private Map<String, RepositoryReturnType> scanRepositories(Object applicationContext) {
    Map<String, RepositoryReturnType> result = new ConcurrentHashMap<>();

    try {
      Class<?> repositoryClass =
          Class.forName("org.springframework.data.repository.Repository");

      Method getBeansOfType =
          applicationContext.getClass().getMethod("getBeansOfType", Class.class);
      Map<String, ?> beans =
          (Map<String, ?>) getBeansOfType.invoke(applicationContext, repositoryClass);

      for (Object bean : beans.values()) {
        scanBeanInterfaces(bean, repositoryClass, result);
      }
    } catch (Exception | NoClassDefFoundError ignored) {
      // Spring Data not on classpath or context not available — return empty
    }

    return result;
  }

  private void scanBeanInterfaces(
      Object bean, Class<?> repositoryMarker, Map<String, RepositoryReturnType> result) {
    Class<?> proxyClass = bean.getClass();
    String proxyClassName = proxyClass.getName();

    for (Class<?> iface : proxyClass.getInterfaces()) {
      if (!repositoryMarker.isAssignableFrom(iface)) {
        continue;
      }
      // Skip Spring's own interfaces (CrudRepository, JpaRepository, etc.)
      if (iface.getName().startsWith("org.springframework.data.")) {
        continue;
      }
      // getMethods() returns all public methods including inherited ones
      // (fixes issue where user-defined base repos declare methods)
      for (Method method : collectUserMethods(iface, repositoryMarker)) {
        RepositoryReturnType type = classifyReturnType(method);
        result.put(proxyClassName + "." + method.getName(), type);
      }
    }
  }

  /**
   * Collects all methods from the interface hierarchy, excluding methods declared on
   * Spring Data's own interfaces (CrudRepository, JpaRepository, etc.).
   */
  private List<Method> collectUserMethods(
      Class<?> iface, Class<?> repositoryMarker) {
    List<Method> methods = new ArrayList<>();

    // getMethods() walks the full interface hierarchy
    for (Method method : iface.getMethods()) {
      Class<?> declaringClass = method.getDeclaringClass();
      // Skip methods from Spring Data's own interfaces and Object
      if (declaringClass == Object.class) {
        continue;
      }
      if (declaringClass.getName().startsWith("org.springframework.data.")) {
        continue;
      }
      methods.add(method);
    }
    return methods;
  }

  static RepositoryReturnType classifyReturnType(Method method) {
    Class<?> rawType = method.getReturnType();
    Type genericType = method.getGenericReturnType();

    // Optional<T>
    if (Optional.class.isAssignableFrom(rawType)) {
      return RepositoryReturnType.OPTIONAL;
    }

    // Page<T> / Slice<T> — use isAssignableFrom to catch subtypes (e.g., PageImpl)
    try {
      Class<?> sliceClass =
          Class.forName("org.springframework.data.domain.Slice");
      if (sliceClass.isAssignableFrom(rawType)) {
        return RepositoryReturnType.PAGE_OR_SLICE;
      }
    } catch (ClassNotFoundException ignored) {
      // Spring Data not on classpath — fall through to name-based check
      String rawTypeName = rawType.getName();
      if (rawTypeName.equals("org.springframework.data.domain.Page")
          || rawTypeName.equals("org.springframework.data.domain.Slice")) {
        return RepositoryReturnType.PAGE_OR_SLICE;
      }
    }

    // Collection types: List, Set, Collection, Iterable, Stream
    if (Collection.class.isAssignableFrom(rawType)
        || Iterable.class.isAssignableFrom(rawType)
        || Stream.class.isAssignableFrom(rawType)) {
      return RepositoryReturnType.COLLECTION;
    }

    // Check generic parameterized types (e.g. CompletableFuture<List<T>>)
    if (genericType instanceof ParameterizedType pt) {
      for (Type arg : pt.getActualTypeArguments()) {
        if (arg instanceof Class<?> argClass) {
          if (Collection.class.isAssignableFrom(argClass)) {
            return RepositoryReturnType.COLLECTION;
          }
        }
      }
    }

    // Single entity (T) — not a collection, not Optional, not Page/Slice
    return RepositoryReturnType.SINGLE_ENTITY;
  }
}
