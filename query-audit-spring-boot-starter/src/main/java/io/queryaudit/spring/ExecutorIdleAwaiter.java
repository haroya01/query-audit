package io.queryaudit.spring;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.locks.LockSupport;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

final class ExecutorIdleAwaiter implements Runnable {
  private static final Duration TIMEOUT = Duration.ofSeconds(30);
  private static final Duration QUIET_PERIOD = Duration.ofMillis(50);

  private final ListableBeanFactory beanFactory;
  private final List<String> names;

  ExecutorIdleAwaiter(ListableBeanFactory beanFactory, List<String> names) {
    this.beanFactory = beanFactory;
    this.names = List.copyOf(names);
  }

  @Override
  public void run() {
    List<Pool> pools = names.stream().map(this::pool).toList();
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    long idleSince = -1;
    while (true) {
      List<String> busy = new ArrayList<>();
      for (Pool pool : pools) {
        if (pool.active() > 0 || pool.queued() > 0) {
          busy.add(
              pool.name() + " has " + pool.active() + " active and " + pool.queued() + " queued");
        }
      }
      long now = System.nanoTime();
      if (!busy.isEmpty()) {
        idleSince = -1;
      } else if (idleSince < 0) {
        idleSince = now;
      } else if (now - idleSince >= QUIET_PERIOD.toNanos()) {
        return;
      }
      if (now >= deadline) {
        throw new AssertionError(
            "Background work did not finish within "
                + TIMEOUT.toSeconds()
                + "s: "
                + String.join(", ", busy));
      }
      LockSupport.parkNanos(Duration.ofMillis(5).toNanos());
      if (Thread.interrupted()) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted while waiting for background work");
      }
    }
  }

  private Pool pool(String name) {
    if (!beanFactory.containsBean(name)) {
      throw new IllegalStateException(
          "query-audit.await-executors names " + name + ", but no such bean exists");
    }
    Object bean = beanFactory.getBean(name);
    if (bean instanceof ThreadPoolTaskExecutor executor) {
      return new Pool(name, executor.getThreadPoolExecutor());
    }
    if (bean instanceof ThreadPoolExecutor executor) {
      return new Pool(name, executor);
    }
    throw new IllegalStateException(
        "query-audit.await-executors names "
            + name
            + ", which is not a ThreadPoolTaskExecutor or ThreadPoolExecutor");
  }

  private record Pool(String name, ThreadPoolExecutor executor) {
    int active() {
      return executor.getActiveCount();
    }

    int queued() {
      return executor.getQueue().size();
    }
  }
}
