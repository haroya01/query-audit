package io.queryaudit.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

import com.jayway.jsonpath.JsonPath;
import io.queryaudit.core.reporter.HtmlReportAggregator;
import io.queryaudit.junit5.EnableQueryInspector;
import io.queryaudit.junit5.ExpectQueries;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;

/**
 * Issue #344: SQL run by an {@code @Async} method on a {@link SimpleAsyncTaskExecutor} — the
 * executor Spring uses when virtual threads are enabled — has to be awaited, and therefore counted,
 * exactly like work handed to a thread pool.
 *
 * <p>Each test asserts the executed query total, so a build that stops waiting fails rather than
 * quietly reporting a smaller budget.
 */
@ResourceLock(Resources.SYSTEM_PROPERTIES)
class VirtualThreadBackgroundWorkTest {
  private static final String ENABLED = "queryaudit.test.virtualThreads";

  /** Records what the awaited executor actually did, read by the driving test after the run. */
  static final class Evidence {
    static final AtomicInteger decorated = new AtomicInteger();
    static volatile String executorThreadType = "never-ran";

    private Evidence() {}
  }

  @Test
  void asyncSqlOnAVirtualThreadExecutorCountsTowardTheBudget(@TempDir Path directory)
      throws Exception {
    Map<String, Object> report = passingRun(directory, VirtualThreadFixture.class);

    assertThat(report.get("outcome")).isEqualTo("PASS");
    assertThat(JsonPath.<Integer>read(report, "$.reports[0].summary.totalQueries"))
        .as("the async SELECT must be awaited and counted alongside the synchronous one")
        .isEqualTo(2);
    assertThat(Evidence.executorThreadType)
        .as("on a JDK with virtual threads the async work must run on one")
        .isEqualTo(expectedThreadType());
    assertThat(Evidence.decorated.get())
        .as("the application's own TaskDecorator must still be in the chain")
        .isPositive();
  }

  @Test
  void everyAsyncTaskIsAwaitedNotJustTheFirst(@TempDir Path directory) throws Exception {
    Map<String, Object> report = passingRun(directory, SeveralAsyncTasksFixture.class);

    assertThat(report.get("outcome")).isEqualTo("PASS");
    assertThat(JsonPath.<Integer>read(report, "$.reports[0].summary.totalQueries"))
        .as("all three async SELECTs must be counted")
        .isEqualTo(4);
  }

  @Test
  void aTestWithoutBackgroundWorkIsUnaffected(@TempDir Path directory) throws Exception {
    Map<String, Object> report = passingRun(directory, SynchronousOnlyFixture.class);

    assertThat(report.get("outcome")).isEqualTo("PASS");
    assertThat(JsonPath.<Integer>read(report, "$.reports[0].summary.totalQueries")).isEqualTo(1);
  }

  /**
   * Virtual threads are a JDK 21 feature; below that the executor is a plain thread-per-task one.
   */
  private static String expectedThreadType() {
    return Integer.parseInt(System.getProperty("java.specification.version")) >= 21
        ? "VirtualThread"
        : "Thread";
  }

  private static Map<String, Object> passingRun(Path directory, Class<?> fixture) throws Exception {
    SummaryGeneratingListener listener = launch(directory, fixture);
    assertThat(listener.getSummary().getFailures()).isEmpty();
    assertThat(listener.getSummary().getTestsSucceededCount()).isEqualTo(1);
    return JsonPath.parse(Files.readString(directory.resolve("report.json"))).json();
  }

  private static SummaryGeneratingListener launch(Path directory, Class<?> fixture) {
    Map<String, String> saved = new HashMap<>();
    List<String> keys =
        List.of(
            ENABLED,
            "queryAudit.report.outputDir",
            "queryAudit.report.format",
            "queryAudit.autoOpenReport");
    for (String key : keys) saved.put(key, System.getProperty(key));
    Evidence.decorated.set(0);
    Evidence.executorThreadType = "never-ran";
    HtmlReportAggregator.getInstance().reset();
    try {
      System.setProperty(ENABLED, "true");
      System.setProperty("queryAudit.report.outputDir", directory.toString());
      System.setProperty("queryAudit.report.format", "json");
      System.setProperty("queryAudit.autoOpenReport", "false");
      SummaryGeneratingListener listener = new SummaryGeneratingListener();
      LauncherFactory.create()
          .execute(
              LauncherDiscoveryRequestBuilder.request()
                  .selectors(selectClass(fixture))
                  .configurationParameter("junit.jupiter.execution.parallel.enabled", "false")
                  .build(),
              listener);
      return listener;
    } finally {
      saved.forEach(
          (key, value) -> {
            if (value == null) System.clearProperty(key);
            else System.setProperty(key, value);
          });
      HtmlReportAggregator.getInstance().reset();
    }
  }

  /** Exactly what Spring itself looks up for {@code @Async}, so the executor is not ambiguous. */
  static final String EXECUTOR_BEAN = "taskExecutor";

  @SpringBootTest(
      classes = App.class,
      properties = {
        "spring.threads.virtual.enabled=true",
        "query-audit.await-executors=taskExecutor"
      })
  @EnableQueryInspector
  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class VirtualThreadFixture {
    @Autowired JdbcTemplate jdbc;
    @Autowired VirtualReads reads;

    @Test
    @ExpectQueries(select = 2)
    void readsNowAndLater() {
      jdbc.queryForObject("SELECT 1", Integer.class);
      reads.later();
    }
  }

  @SpringBootTest(
      classes = App.class,
      properties = {
        "spring.threads.virtual.enabled=true",
        "query-audit.await-executors=taskExecutor"
      })
  @EnableQueryInspector
  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class SeveralAsyncTasksFixture {
    @Autowired JdbcTemplate jdbc;
    @Autowired VirtualReads reads;

    @Test
    @ExpectQueries(select = 4)
    void startsThreeAsyncReads() {
      jdbc.queryForObject("SELECT 1", Integer.class);
      reads.later();
      reads.alsoLater();
      reads.laterAgain();
    }
  }

  @SpringBootTest(
      classes = App.class,
      properties = {
        "spring.threads.virtual.enabled=true",
        "query-audit.await-executors=taskExecutor"
      })
  @EnableQueryInspector
  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class SynchronousOnlyFixture {
    @Autowired JdbcTemplate jdbc;

    @Test
    @ExpectQueries(select = 1)
    void readsOnlyOnTheTestThread() {
      jdbc.queryForObject("SELECT 1", Integer.class);
    }
  }

  static class VirtualReads {
    @Autowired JdbcTemplate jdbc;

    @Async(EXECUTOR_BEAN)
    public void later() {
      sleep(120);
      Evidence.executorThreadType = Thread.currentThread().getClass().getSimpleName();
      jdbc.queryForObject("SELECT 2", Integer.class);
    }

    @Async(EXECUTOR_BEAN)
    public void alsoLater() {
      sleep(60);
      jdbc.queryForObject("SELECT 3", Integer.class);
    }

    @Async(EXECUTOR_BEAN)
    public void laterAgain() {
      sleep(30);
      jdbc.queryForObject("SELECT 4", Integer.class);
    }
  }

  @SpringBootConfiguration
  @EnableAutoConfiguration
  @EnableAsync
  @Import(VirtualReads.class)
  static class App {
    @Bean(EXECUTOR_BEAN)
    SimpleAsyncTaskExecutor taskExecutor() {
      SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor("qa-virtual-");
      executor.setConcurrencyLimit(16);
      if (Integer.parseInt(System.getProperty("java.specification.version")) >= 21) {
        executor.setVirtualThreads(true);
      }
      // A decorator the application already relies on, e.g. for MDC propagation.
      executor.setTaskDecorator(
          runnable -> {
            Evidence.decorated.incrementAndGet();
            return runnable;
          });
      return executor;
    }
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
