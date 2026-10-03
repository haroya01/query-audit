package io.queryaudit.spring;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.IntSupplier;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

final class ExecutorIdleAwaiter implements Runnable {
  private static final Duration TIMEOUT = Duration.ofSeconds(30);
  private static final Duration QUIET_PERIOD = Duration.ofMillis(50);

  private final ListableBeanFactory beanFactory;
  private final List<String> names;
  private final AwaitedTaskTracker tracker;

  ExecutorIdleAwaiter(
      ListableBeanFactory beanFactory, List<String> names, AwaitedTaskTracker tracker) {
    this.beanFactory = beanFactory;
    this.names = List.copyOf(names);
    this.tracker = tracker;
  }

  @Override
  public void run() {
    List<Pool> pools = names.stream().map(this::pool).toList();
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    long idleSince = -1;
    while (true) {
      List<String> busy = new ArrayList<>();
      for (Pool pool : pools) {
        if (pool.busy()) {
          busy.add(pool.describe());
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
      return Pool.pool(name, executor.getThreadPoolExecutor());
    }
    if (bean instanceof ThreadPoolExecutor executor) {
      return Pool.pool(name, executor);
    }
    if (bean instanceof SimpleAsyncTaskExecutor) {
      // Only executors named in await-executors are instrumented, and only those this tracker saw
      // during bean initialization. An untracked one is an executor created outside Spring, which
      // this feature does not reach.
      AtomicInteger inFlight =
          tracker
              .counter(name)
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "query-audit.await-executors names the SimpleAsyncTaskExecutor "
                              + name
                              + ", but no tasks are being tracked for it. It was not seen as a bean"
                              + " when the context was initialized, so it is most likely created"
                              + " outside Spring. Declare it as a bean, or use a"
                              + " ThreadPoolTaskExecutor."));
      return Pool.tracked(name, inFlight);
    }
    throw new IllegalStateException(
        "query-audit.await-executors names "
            + name
            + ", which is not a ThreadPoolTaskExecutor, ThreadPoolExecutor, or"
            + " SimpleAsyncTaskExecutor");
  }

  /**
   * One awaited executor reduced to the two counts that decide whether it is busy.
   *
   * <p>Pool executors report live thread-pool state. A tracked {@code SimpleAsyncTaskExecutor} has
   * no queue to inspect, so its submitted-and-unfinished count stands in for both: it already
   * includes tasks that are accepted but not yet running.
   */
  private record Pool(String name, IntSupplier active, IntSupplier queued, boolean pooled) {
    static Pool pool(String name, ThreadPoolExecutor executor) {
      return new Pool(name, executor::getActiveCount, () -> executor.getQueue().size(), true);
    }

    static Pool tracked(String name, AtomicInteger inFlight) {
      return new Pool(name, inFlight::get, () -> 0, false);
    }

    boolean busy() {
      return active.getAsInt() > 0 || queued.getAsInt() > 0;
    }

    String describe() {
      return pooled
          ? name + " has " + active.getAsInt() + " active and " + queued.getAsInt() + " queued"
          : name + " has " + active.getAsInt() + " tasks in flight";
    }
  }
}
