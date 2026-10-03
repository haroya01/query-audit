package io.queryaudit.spring;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskDecorator;

/**
 * Reads the {@link TaskDecorator} a {@link SimpleAsyncTaskExecutor} already carries, and composes
 * QueryAudit's tracking decorator around it.
 *
 * <p>Why one reflective read: Spring exposes {@code setTaskDecorator(TaskDecorator)} on {@link
 * SimpleAsyncTaskExecutor} but no matching getter, checked against Spring Framework 6.2.1 and
 * 7.0.7. Reading the installed value is the only way to chain onto an application-supplied
 * decorator instead of silently replacing it. This class is the only place in the module that
 * touches a Spring internal — the field {@code SimpleAsyncTaskExecutor#taskDecorator}. If a future
 * Spring version moves it, startup fails with a message naming the bean and the field rather than
 * quietly dropping the application's decorator.
 */
final class InstalledTaskDecorator {
  private static final String FIELD_NAME = "taskDecorator";

  private InstalledTaskDecorator() {}

  /**
   * Returns the decorator already installed on {@code executor}, or {@code null} when it has none.
   *
   * @throws IllegalStateException when the field cannot be read, so the failure is explicit instead
   *     of overwriting a decorator that may be in use
   */
  static TaskDecorator read(SimpleAsyncTaskExecutor executor, String beanName) {
    try {
      Field field = SimpleAsyncTaskExecutor.class.getDeclaredField(FIELD_NAME);
      field.setAccessible(true);
      return (TaskDecorator) field.get(executor);
    } catch (NoSuchFieldException | IllegalAccessException | RuntimeException failure) {
      throw new IllegalStateException(
          "query-audit.await-executors names the SimpleAsyncTaskExecutor bean '"
              + beanName
              + "', but its existing TaskDecorator could not be read from SimpleAsyncTaskExecutor."
              + FIELD_NAME
              + " ("
              + failure
              + "). QueryAudit does not replace an existing TaskDecorator because it cannot preserve"
              + " it. Remove '"
              + beanName
              + "' from query-audit.await-executors, or declare a ThreadPoolTaskExecutor instead.",
          failure);
    }
  }

  /**
   * Wraps {@code installed} so every task submitted to the executor is counted from submission
   * until it finishes.
   *
   * <p>Spring calls {@link TaskDecorator#decorate(Runnable)} at submission time, including for
   * tasks held back by a concurrency limit, so counting there — rather than when the task starts —
   * keeps a throttled executor honest: a task that has not begun yet is already outstanding.
   */
  static TaskDecorator tracking(TaskDecorator installed, AtomicInteger inFlight) {
    return task -> {
      inFlight.incrementAndGet();
      Runnable decorated;
      try {
        decorated = installed == null ? task : installed.decorate(task);
      } catch (RuntimeException | Error failure) {
        inFlight.decrementAndGet();
        throw failure;
      }
      return () -> {
        try {
          decorated.run();
        } finally {
          inFlight.decrementAndGet();
        }
      };
    };
  }
}
