package io.queryaudit.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskDecorator;

/**
 * Unit coverage for awaiting a {@link SimpleAsyncTaskExecutor}, which reports no queue or pool
 * state and is therefore tracked by counting submissions.
 */
class ExecutorIdleAwaiterTest {
  private static final String NAME = "work";

  private final StaticListableBeanFactory beans = new StaticListableBeanFactory();
  private final AwaitedTaskTracker tracker = new AwaitedTaskTracker(Set.of(NAME));

  private ExecutorIdleAwaiter awaiter() {
    return new ExecutorIdleAwaiter(beans, List.of(NAME), tracker);
  }

  /**
   * Registers {@code executor} as a bean and lets the tracker instrument it, as a context would.
   */
  private SimpleAsyncTaskExecutor tracked(SimpleAsyncTaskExecutor executor) {
    beans.addBean(NAME, executor);
    tracker.postProcessAfterInitialization(executor, NAME);
    return executor;
  }

  @Test
  void returnsImmediatelyWhenNoTaskIsRunning() {
    tracked(new SimpleAsyncTaskExecutor("idle-"));

    assertThatCode(awaiter()::run).doesNotThrowAnyException();
  }

  @Test
  void waitsUntilEveryInFlightTaskHasFinished() throws Exception {
    SimpleAsyncTaskExecutor executor = tracked(new SimpleAsyncTaskExecutor("multi-"));
    executor.setConcurrencyLimit(8);
    CountDownLatch finished = new CountDownLatch(3);

    for (int index = 0; index < 3; index++) {
      executor.execute(
          () -> {
            sleep(120);
            finished.countDown();
          });
    }

    awaiter().run();

    assertThat(finished.getCount()).as("every submitted task must have completed").isZero();
  }

  @Test
  void waitsForATaskSubmittedWhileAnotherIsStillRunning() throws Exception {
    SimpleAsyncTaskExecutor executor = tracked(new SimpleAsyncTaskExecutor("chain-"));
    AtomicBoolean lateTaskFinished = new AtomicBoolean();
    CountDownLatch lateTaskDone = new CountDownLatch(1);

    // The first task hands off to a second one as it finishes, after the awaiter's first look at
    // the executor. Counting at submission is what keeps that second task from being missed.
    executor.execute(
        () -> {
          sleep(80);
          executor.execute(
              () -> {
                sleep(80);
                lateTaskFinished.set(true);
                lateTaskDone.countDown();
              });
        });

    awaiter().run();

    assertThat(lateTaskDone.await(0, TimeUnit.MILLISECONDS))
        .as("the task submitted during the wait must also have finished")
        .isTrue();
    assertThat(lateTaskFinished).isTrue();
  }

  @Test
  void aTaskThatFinishesImmediatelyIsNotWaitedOnUnnecessarily() {
    SimpleAsyncTaskExecutor executor = tracked(new SimpleAsyncTaskExecutor("quick-"));
    executor.execute(() -> {});

    long start = System.nanoTime();
    awaiter().run();
    long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

    assertThat(elapsedMillis)
        .as("only the quiet period should be spent once the task is already done")
        .isLessThan(2_000);
  }

  @Test
  void aTaskThatThrowsStillReleasesItsCount() {
    AtomicInteger inFlight = new AtomicInteger();
    Runnable decorated =
        InstalledTaskDecorator.tracking(null, inFlight)
            .decorate(
                () -> {
                  throw new IllegalStateException("task failed");
                });

    assertThatThrownBy(decorated::run)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("task failed");
    assertThat(inFlight)
        .as("a failing task must not stay outstanding and stall the wait")
        .hasValue(0);
  }

  @Test
  void reportsAnUntrackedSimpleAsyncTaskExecutorClearly() {
    beans.addBean(NAME, new SimpleAsyncTaskExecutor("outside-"));

    assertThatThrownBy(() -> awaiter().run())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(NAME)
        .hasMessageContaining("most likely created outside Spring");
  }

  @Test
  void rejectsABeanThatIsNotASupportedExecutor() {
    beans.addBean(NAME, "not an executor");

    assertThatThrownBy(() -> awaiter().run())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("not a ThreadPoolTaskExecutor, ThreadPoolExecutor, or")
        .hasMessageContaining(NAME);
  }

  @Test
  void keepsAnAlreadyInstalledTaskDecoratorInTheChain() throws Exception {
    AtomicInteger decorated = new AtomicInteger();
    AtomicInteger decoratedInContext = new AtomicInteger();
    SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor("decorated-");
    executor.setTaskDecorator(
        runnable -> {
          decoratedInContext.incrementAndGet();
          return runnable;
        });

    SimpleAsyncTaskExecutor wrapped = tracked(executor);
    CountDownLatch done = new CountDownLatch(1);
    wrapped.execute(
        () -> {
          decorated.incrementAndGet();
          done.countDown();
        });

    assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
    awaiter().run();

    assertThat(decoratedInContext)
        .as("QueryAudit must not drop the application's own TaskDecorator")
        .hasValue(1);
    assertThat(decorated).as("the task still runs").hasValue(1);
  }

  @Test
  void trackingComposesAroundAnExistingDecorator() {
    AtomicInteger inFlight = new AtomicInteger();
    AtomicInteger decoratedByUser = new AtomicInteger();
    TaskDecorator tracking =
        InstalledTaskDecorator.tracking(
            runnable -> {
              decoratedByUser.incrementAndGet();
              return runnable;
            },
            inFlight);

    Runnable decorated = tracking.decorate(() -> {});

    assertThat(inFlight).as("counted at submission").hasValue(1);
    assertThat(decoratedByUser).hasValue(1);
    decorated.run();
    assertThat(inFlight).as("released once the task finishes").hasValue(0);
  }

  @Test
  void aDecoratorThatRefusesATaskDoesNotLeakACount() {
    AtomicInteger inFlight = new AtomicInteger();
    TaskDecorator tracking =
        InstalledTaskDecorator.tracking(
            runnable -> {
              throw new IllegalStateException("cannot decorate");
            },
            inFlight);

    assertThatThrownBy(() -> tracking.decorate(() -> {})).isInstanceOf(IllegalStateException.class);
    assertThat(inFlight).as("a rejected task must not stay outstanding forever").hasValue(0);
  }

  @Test
  void aPoolExecutorIsStillAwaitedThroughItsOwnState() throws Exception {
    java.util.concurrent.ThreadPoolExecutor pool =
        new java.util.concurrent.ThreadPoolExecutor(
            2,
            2,
            0,
            java.util.concurrent.TimeUnit.MILLISECONDS,
            new java.util.concurrent.LinkedBlockingQueue<>());
    beans.addBean(NAME, pool);
    CountDownLatch finished = new CountDownLatch(1);
    pool.execute(
        () -> {
          sleep(120);
          finished.countDown();
        });

    new ExecutorIdleAwaiter(beans, List.of(NAME), tracker).run();

    assertThat(finished.getCount()).isZero();
    pool.shutdownNow();
  }

  private static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(interrupted);
    }
  }
}
