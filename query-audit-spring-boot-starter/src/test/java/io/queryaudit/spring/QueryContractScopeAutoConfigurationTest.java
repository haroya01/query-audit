package io.queryaudit.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.contract.QueryContractScope;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

class QueryContractScopeAutoConfigurationTest {
  @TempDir Path contracts;

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(QueryAuditAutoConfiguration.class))
          .withUserConfiguration(Application.class);

  @Test
  void injectsAScopeThatReadsTheConfiguredContractsForMethodAndScopedContracts() throws Exception {
    Files.write(
        contracts.resolve("api.contracts"), List.of("@junit | link-count | 1 | 0 | 0 | 0 | 1"));

    runner
        .withPropertyValues("query-audit.contracts.path=" + contracts)
        .run(
            context -> {
              QueryContractScope scope = context.getBean(QueryContractScope.class);
              JdbcTemplate jdbc = new JdbcTemplate(context.getBean(DataSource.class));

              assertThat(scope.location()).isEqualTo(contracts);
              assertThat(context.getBean(QueryAuditConfig.class).getContractsPath())
                  .isEqualTo(contracts.toString());
              assertThat(
                      scope.verify(
                          "link-count", () -> jdbc.queryForObject("SELECT 1", Integer.class)))
                  .isEqualTo(1);
            });
  }

  @Test
  void defaultsToTheSameFileAsMethodContracts() {
    runner.run(
        context ->
            assertThat(context.getBean(QueryContractScope.class).location())
                .isEqualTo(Path.of(".query-audit-contracts")));
  }

  @Test
  void waitsForTheNamedExecutorsBeforeTheScopeCloses() throws Exception {
    Files.write(
        contracts.resolve("api.contracts"), List.of("@junit | link-click | 1 | 0 | 0 | 0 | 1"));

    runner
        .withPropertyValues(
            "query-audit.contracts.path=" + contracts, "query-audit.await-executors=clickExecutor")
        .run(
            context -> {
              QueryContractScope scope = context.getBean(QueryContractScope.class);
              ThreadPoolTaskExecutor executor =
                  context.getBean("clickExecutor", ThreadPoolTaskExecutor.class);
              JdbcTemplate jdbc = new JdbcTemplate(context.getBean(DataSource.class));

              scope.verify(
                  "link-click",
                  () -> {
                    executor.execute(
                        () -> {
                          sleep(100);
                          jdbc.queryForObject("SELECT 1", Integer.class);
                        });
                    return null;
                  });
            });
  }

  @Test
  void registersTheBackgroundWorkOnlyWhenExecutorsAreNamed() {
    runner.run(context -> assertThat(context).doesNotHaveBean("queryAuditBackgroundWork"));
    runner
        .withPropertyValues("query-audit.await-executors=clickExecutor")
        .run(context -> assertThat(context).hasBean("queryAuditBackgroundWork"));
  }

  @Test
  void namesAnExecutorThatDoesNotExist() {
    runner
        .withPropertyValues(
            "query-audit.contracts.path=" + contracts,
            "query-audit.await-executors=missingExecutor")
        .run(
            context ->
                assertThatThrownBy(
                        () ->
                            context
                                .getBean(QueryContractScope.class)
                                .verify("anything", () -> null))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("missingExecutor"));
  }

  private static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  @Configuration
  static class Application {
    @Bean
    DataSource dataSource() {
      JdbcDataSource dataSource = new JdbcDataSource();
      dataSource.setURL("jdbc:h2:mem:scope-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
      return dataSource;
    }

    @Bean
    ThreadPoolTaskExecutor clickExecutor() {
      ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
      executor.setCorePoolSize(1);
      executor.initialize();
      return executor;
    }
  }
}
