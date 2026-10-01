package io.queryaudit.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

import com.jayway.jsonpath.JsonPath;
import io.queryaudit.core.reporter.HtmlReportAggregator;
import io.queryaudit.junit5.EnableQueryInspector;
import io.queryaudit.junit5.ExpectQueries;
import io.queryaudit.junit5.QueryAudit;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
class SpringBackgroundThreadsTest {
  private static final String ENABLED = "queryaudit.test.springThreads";

  @Test
  void anAsyncMethodIsAwaitedAndCounted(@TempDir Path directory) throws Exception {
    Map<String, Object> report = passingRun(directory, AsyncMethodFixture.class);

    assertThat(report.get("outcome")).isEqualTo("PASS");
    assertThat(JsonPath.<Integer>read(report, "$.reports[0].summary.totalQueries")).isEqualTo(2);
  }

  @Test
  void anAsyncEventListenerIsAwaitedAndCounted(@TempDir Path directory) throws Exception {
    Map<String, Object> report = passingRun(directory, AsyncListenerFixture.class);

    assertThat(report.get("outcome")).isEqualTo("PASS");
    assertThat(JsonPath.<Integer>read(report, "$.reports[0].summary.totalQueries")).isEqualTo(1);
  }

  @Test
  void serverThreadSqlCountsWithoutConfiguration(@TempDir Path directory) throws Exception {
    Map<String, Object> report = passingRun(directory, ServerWithoutPoolsFixture.class);

    assertThat(report.get("outcome")).isEqualTo("PASS");
    assertThat(JsonPath.<Integer>read(report, "$.reports[0].summary.totalQueries")).isEqualTo(1);
  }

  @Test
  void anNPlusOneInsideAServerRequestFailsTheTest(@TempDir Path directory) {
    SummaryGeneratingListener listener = launch(directory, ServerNPlusOneFixture.class);

    assertThat(listener.getSummary().getFailures())
        .singleElement()
        .satisfies(
            failure ->
                assertThat(failure.getException())
                    .hasMessageContaining("N+1 Query detected")
                    .hasMessageContaining(ReadController.class.getName() + ".customers:"));
  }

  @Test
  void serverThreadSqlCountsOnceAPoolIsNamed(@TempDir Path directory) throws Exception {
    Map<String, Object> report = passingRun(directory, ServerWithPoolsFixture.class);

    assertThat(report.get("outcome")).isEqualTo("PASS");
    assertThat(JsonPath.<Integer>read(report, "$.reports[0].summary.totalQueries")).isEqualTo(1);
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

  @SpringBootTest(classes = App.class, properties = "query-audit.await-executors=taskExecutor")
  @EnableQueryInspector
  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class AsyncMethodFixture {
    @Autowired JdbcTemplate jdbc;
    @Autowired Reads reads;

    @Test
    @ExpectQueries(select = 2)
    void readsNowAndLater() {
      jdbc.queryForObject("SELECT 1", Integer.class);
      reads.later();
    }
  }

  @SpringBootTest(classes = App.class, properties = "query-audit.await-executors=taskExecutor")
  @EnableQueryInspector
  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class AsyncListenerFixture {
    @Autowired ApplicationEventPublisher events;

    @Test
    @ExpectQueries(select = 1)
    void publishesAClick() {
      events.publishEvent(new Clicked());
    }
  }

  @SpringBootTest(classes = App.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
  @EnableQueryInspector
  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class ServerWithoutPoolsFixture {
    @LocalServerPort int port;

    @Test
    void callsTheServer() throws Exception {
      assertThat(get(port)).isEqualTo("4");
    }
  }

  @SpringBootTest(
      classes = App.class,
      webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
      properties = "query-audit.await-executors=taskExecutor")
  @EnableQueryInspector
  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class ServerWithPoolsFixture {
    @LocalServerPort int port;

    @Test
    @ExpectQueries(select = 1)
    void callsTheServer() throws Exception {
      assertThat(get(port)).isEqualTo("4");
    }
  }

  @SpringBootTest(classes = App.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
  @QueryAudit
  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class ServerNPlusOneFixture {
    @LocalServerPort int port;

    @Test
    void listsCustomers() throws Exception {
      assertThat(get(port, "/customers")).isEqualTo("15");
    }
  }

  private static String get(int port) throws Exception {
    return get(port, "/read");
  }

  private static String get(int port, String path) throws Exception {
    return HttpClient.newHttpClient()
        .send(
            HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).build(),
            HttpResponse.BodyHandlers.ofString())
        .body();
  }

  record Clicked() {}

  @Component
  static class Reads {
    @Autowired JdbcTemplate jdbc;

    @Async
    public void later() {
      sleep(150);
      jdbc.queryForObject("SELECT 2", Integer.class);
    }

    @Async
    @EventListener
    public void onClick(Clicked click) {
      sleep(150);
      jdbc.queryForObject("SELECT 3", Integer.class);
    }
  }

  @RestController
  static class ReadController {
    @Autowired JdbcTemplate jdbc;

    @GetMapping("/read")
    int read() {
      return jdbc.queryForObject("SELECT 4", Integer.class);
    }

    @GetMapping("/customers")
    int customers() {
      int sum = 0;
      for (int id = 1; id <= 5; id++) {
        sum += jdbc.queryForObject("SELECT CAST(? AS INT)", Integer.class, id);
      }
      return sum;
    }
  }

  @SpringBootConfiguration
  @EnableAutoConfiguration
  @EnableAsync
  @Import({Reads.class, ReadController.class})
  static class App {
    @Bean
    ThreadPoolTaskExecutor taskExecutor() {
      ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
      executor.setCorePoolSize(2);
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
