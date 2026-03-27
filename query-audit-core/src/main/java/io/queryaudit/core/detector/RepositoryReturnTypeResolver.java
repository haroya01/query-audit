package io.queryaudit.core.detector;

/**
 * Resolves the return type category of a Spring Data repository method from a query's
 * stack trace. Implementations typically inspect the stack trace for JDK proxy frames
 * (e.g., {@code jdk.proxy3.$Proxy296.findByRoomId:-1}), identify the corresponding
 * repository interface, and reflect on the method's return type.
 *
 * @author haroya
 * @since 0.3.0
 */
@FunctionalInterface
public interface RepositoryReturnTypeResolver {

  /**
   * Resolves the repository method return type from the given stack trace.
   *
   * @param stackTrace newline-separated stack frames in {@code className.methodName:line} format
   * @return the resolved return type category, or {@link RepositoryReturnType#UNKNOWN}
   *     if the stack trace does not contain an identifiable repository method
   */
  RepositoryReturnType resolve(String stackTrace);
}
