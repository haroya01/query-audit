package io.queryaudit.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.queryaudit.core.contract.QueryContractScope;
import io.queryaudit.core.interceptor.QueryInterceptor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

class ScopedQueryContractIntegrationTest {
  @TempDir Path contracts;
  private AnnotationConfigApplicationContext context;
  private JdbcTemplate jdbc;
  private QueryContractScope scope;
  private final ExecutorService worker = Executors.newSingleThreadExecutor();

  @BeforeEach
  void startApplication() throws Exception {
    context = new AnnotationConfigApplicationContext(Application.class);
    jdbc = new JdbcTemplate(context.getBean(DataSource.class));
    jdbc.execute("CREATE TABLE links (id BIGINT PRIMARY KEY, code VARCHAR(16))");
    jdbc.execute("CREATE TABLE clicks (link_id BIGINT)");
    Files.write(
        contracts.resolve("http.contracts"),
        List.of(
            "# Reviewed HTTP round trips",
            "@junit | link-create | 1 | 1 | 0 | 0 | 2",
            "@junit | link-click | 1 | 1 | 0 | 0 | 2"));
    scope = QueryContractScope.of(context.getBean(QueryInterceptor.class), contracts);
  }

  @AfterEach
  void stopApplication() {
    worker.shutdownNow();
    context.close();
  }

  @Test
  void verifiesTheQueriesOfOneRequestThroughTheAutoConfiguredInterceptor() throws Exception {
    Integer created = scope.verify("link-create", this::createLink);

    assertThat(created).isEqualTo(1);
  }

  @Test
  void failsAnAddedQueryEvenWhenTheRequestStillSucceeds() {
    assertThatThrownBy(
            () ->
                scope.verify(
                    "link-create",
                    () -> {
                      createLink();
                      return jdbc.queryForObject("SELECT count(*) FROM links", Integer.class);
                    }))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("SELECT: contract 1, executed 2 (+1)")
        .hasMessageContaining("SELECT count(*) FROM links");
  }

  @Test
  void includesBackgroundWorkTheRequestTriggeredOnceTheScopeWaitsForIt() throws Exception {
    Future<?>[] background = new Future<?>[1];

    scope
        .awaitingCompletion(() -> join(background[0]))
        .verify(
            "link-click",
            () -> {
              jdbc.queryForObject("SELECT count(*) FROM links WHERE code = 'abc'", Integer.class);
              background[0] = worker.submit(() -> jdbc.update("INSERT INTO clicks VALUES (1)"));
              return null;
            });
  }

  private Integer createLink() {
    jdbc.queryForObject("SELECT count(*) FROM links WHERE code = 'abc'", Integer.class);
    return jdbc.update("INSERT INTO links VALUES (1, 'abc')");
  }

  private static void join(Future<?> future) {
    try {
      future.get(5, TimeUnit.SECONDS);
    } catch (Exception failure) {
      throw new IllegalStateException(failure);
    }
  }

  @Configuration
  @Import(QueryAuditAutoConfiguration.class)
  static class Application {
    @Bean
    DataSource dataSource() {
      JdbcDataSource dataSource = new JdbcDataSource();
      dataSource.setURL("jdbc:h2:mem:scoped-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
      return dataSource;
    }
  }
}
