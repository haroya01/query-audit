package io.queryaudit.spring;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskDecorator;

/**
 * Counts the tasks submitted to each awaited {@link SimpleAsyncTaskExecutor} bean so {@link
 * ExecutorIdleAwaiter} can wait for them the way it already waits for pool executors.
 *
 * <p>A {@code SimpleAsyncTaskExecutor} exposes no queue or pool state, so idle detection for it has
 * to come from somewhere else. The public {@link TaskDecorator} hook is the supported place: Spring
 * runs it around every submitted task, which turns the executor into something countable without
 * changing the executor's own behaviour.
 *
 * <p>Scope is deliberately narrow. Only executors already named in {@code
 * query-audit.await-executors} are touched, so this is not a global asynchronous-task tracker, and
 * executors created outside the application context are never seen. Pool executors are left
 * untouched: they already report active and queued counts, and {@code ThreadPoolTaskExecutor}
 * copies its decorator into the underlying pool during {@code initialize()}, so setting one
 * afterwards would silently have no effect.
 */
final class AwaitedTaskTracker implements BeanPostProcessor {
  private final Set<String> awaitedNames;
  private final ConcurrentMap<String, AtomicInteger> inFlight = new ConcurrentHashMap<>();

  AwaitedTaskTracker(Set<String> awaitedNames) {
    this.awaitedNames = Set.copyOf(awaitedNames);
  }

  @Override
  public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
    if (bean instanceof SimpleAsyncTaskExecutor executor && awaitedNames.contains(beanName)) {
      TaskDecorator installed = InstalledTaskDecorator.read(executor, beanName);
      AtomicInteger counter = new AtomicInteger();
      executor.setTaskDecorator(InstalledTaskDecorator.tracking(installed, counter));
      inFlight.put(beanName, counter);
    }
    return bean;
  }

  /** The submitted-and-unfinished count for {@code beanName}, empty when it is not tracked here. */
  Optional<AtomicInteger> counter(String beanName) {
    return Optional.ofNullable(inFlight.get(beanName));
  }
}
