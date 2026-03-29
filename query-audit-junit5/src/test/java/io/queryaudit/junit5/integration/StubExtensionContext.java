package io.queryaudit.junit5.integration;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExecutableInvoker;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.extension.TestInstances;

/**
 * Minimal stub of {@link ExtensionContext} for unit-testing {@code buildConfig()}.
 * Only implements the methods that buildConfig() actually calls.
 */
class StubExtensionContext implements ExtensionContext {

  private final Class<?> testClass;
  private final Store store;

  StubExtensionContext(Class<?> testClass) {
    this.testClass = testClass;
    this.store = new MapStore();
  }

  @Override
  public Class<?> getRequiredTestClass() {
    return testClass;
  }

  @Override
  public Optional<Method> getTestMethod() {
    return Optional.empty();
  }

  @Override
  public Optional<Class<?>> getTestClass() {
    return Optional.of(testClass);
  }

  @Override
  public Store getStore(Namespace namespace) {
    return store;
  }

  // ── Remaining methods: no-op / default ──

  @Override
  public Optional<ExtensionContext> getParent() {
    return Optional.empty();
  }

  @Override
  public ExtensionContext getRoot() {
    return this;
  }

  @Override
  public String getUniqueId() {
    return "stub";
  }

  @Override
  public String getDisplayName() {
    return testClass.getSimpleName();
  }

  @Override
  public Set<String> getTags() {
    return Set.of();
  }

  @Override
  public Optional<AnnotatedElement> getElement() {
    return Optional.empty();
  }

  @Override
  public Optional<Object> getTestInstance() {
    return Optional.empty();
  }

  @Override
  public Optional<TestInstances> getTestInstances() {
    return Optional.empty();
  }

  @Override
  public Optional<Throwable> getExecutionException() {
    return Optional.empty();
  }

  @Override
  public ExecutionMode getExecutionMode() {
    return ExecutionMode.SAME_THREAD;
  }

  @Override
  public Optional<String> getConfigurationParameter(String key) {
    return Optional.empty();
  }

  @Override
  public <T> Optional<T> getConfigurationParameter(String key, Function<String, T> transformer) {
    return Optional.empty();
  }

  @Override
  public void publishReportEntry(Map<String, String> map) {}

  @Override
  public Optional<TestInstance.Lifecycle> getTestInstanceLifecycle() {
    return Optional.empty();
  }

  @Override
  public ExecutableInvoker getExecutableInvoker() {
    throw new UnsupportedOperationException();
  }

  // ── Simple in-memory Store ──

  private static class MapStore implements Store {
    private final Map<Object, Object> map = new ConcurrentHashMap<>();

    @Override
    public Object get(Object key) {
      return map.get(key);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V> V get(Object key, Class<V> requiredType) {
      return (V) map.get(key);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> Object getOrComputeIfAbsent(K key, Function<K, V> defaultCreator) {
      return map.computeIfAbsent(key, k -> defaultCreator.apply((K) k));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> V getOrComputeIfAbsent(K key, Function<K, V> defaultCreator, Class<V> requiredType) {
      return (V) map.computeIfAbsent(key, k -> defaultCreator.apply((K) k));
    }

    @Override
    public void put(Object key, Object value) {
      map.put(key, value);
    }

    @Override
    public Object remove(Object key) {
      return map.remove(key);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V> V remove(Object key, Class<V> requiredType) {
      return (V) map.remove(key);
    }
  }
}
