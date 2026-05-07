package io.queryaudit.spring;

import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * Reproduction harness and regression coverage for issue #134 — when multiple Spring application
 * contexts that each carry the QueryAudit autoconfig coexist, the {@code DataSource} of a still-live
 * context must not be closed prematurely.
 *
 * <p>The bug as reported is observed under Spring Boot 4.x; with the Spring Boot 3.4.x baseline
 * this project compiles against, the multi-context scenario already behaves correctly. These tests
 * pin that good behavior so a future Spring Boot upgrade that regresses it is caught immediately,
 * and exercise the {@code query-audit.wrap-data-source.enabled=false} escape hatch that users on
 * the affected Boot 4.x versions can opt into today.
 */
class MultiContextDataSourceLifecycleTest {

  private final List<ConfigurableApplicationContext> contexts = new ArrayList<>();

  @AfterEach
  void closeAllContexts() {
    for (ConfigurableApplicationContext ctx : contexts) {
      try {
        ctx.close();
      } catch (Exception ignored) {
        // best effort cleanup
      }
    }
    contexts.clear();
  }

  private ConfigurableApplicationContext openContextWithHikari() {
    AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
    ctx.register(HikariConfig.class, QueryAuditAutoConfiguration.class);
    ctx.refresh();
    contexts.add(ctx);
    return ctx;
  }

  @Test
  @DisplayName("All four contexts' DataSources stay usable after all are open (regression #134)")
  void allDataSourcesUsableWithFourCoexistingContexts() throws Exception {
    ConfigurableApplicationContext c1 = openContextWithHikari();
    ConfigurableApplicationContext c2 = openContextWithHikari();
    ConfigurableApplicationContext c3 = openContextWithHikari();
    ConfigurableApplicationContext c4 = openContextWithHikari();

    for (ConfigurableApplicationContext ctx : List.of(c1, c2, c3, c4)) {
      DataSource ds = ctx.getBean(DataSource.class);
      try (Connection conn = ds.getConnection()) {
        assertThat(conn.isValid(1)).as("connection from %s should be valid", ctx).isTrue();
      }
    }
  }

  @Test
  @DisplayName("Closing one context does not close other contexts' DataSources (regression #134)")
  void closingOneContextDoesNotAffectOthers() throws Exception {
    ConfigurableApplicationContext c1 = openContextWithHikari();
    ConfigurableApplicationContext c2 = openContextWithHikari();
    ConfigurableApplicationContext c3 = openContextWithHikari();

    DataSource dsB = c2.getBean(DataSource.class);
    DataSource dsC = c3.getBean(DataSource.class);

    c1.close();
    contexts.remove(c1);

    try (Connection conn = dsB.getConnection()) {
      assertThat(conn.isValid(1)).as("dbB connection valid after dbA closed").isTrue();
    }
    try (Connection conn = dsC.getConnection()) {
      assertThat(conn.isValid(1)).as("dbC connection valid after dbA closed").isTrue();
    }
  }

  @Test
  @DisplayName("Escape hatch query-audit.wrap-data-source.enabled=false skips the BPP")
  void escapeHatchSkipsBeanPostProcessor() {
    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(QueryAuditAutoConfiguration.class))
        .withPropertyValues("query-audit.wrap-data-source.enabled=false")
        .run(
            (AssertableApplicationContext context) -> {
              assertThat(context).doesNotHaveBean("queryAuditDataSourcePostProcessor");
              assertThat(context).doesNotHaveBean("queryGuardDataSourcePostProcessor");
              assertThat(context).hasBean("queryAuditConfig");
              assertThat(context).hasBean("queryAuditInterceptor");
            });
  }

  @Configuration
  @Import(QueryAuditAutoConfiguration.class)
  static class HikariConfig {

    @Bean(destroyMethod = "close")
    public DataSource dataSource() {
      HikariDataSource ds = new HikariDataSource();
      ds.setDriverClassName("org.h2.Driver");
      ds.setJdbcUrl("jdbc:h2:mem:" + java.util.UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
      ds.setUsername("sa");
      ds.setPassword("");
      ds.setMaximumPoolSize(2);
      return ds;
    }
  }
}
