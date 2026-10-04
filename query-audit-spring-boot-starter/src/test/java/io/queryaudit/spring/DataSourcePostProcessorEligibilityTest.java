package io.queryaudit.spring;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.interceptor.QueryInterceptor;
import java.sql.Connection;
import java.sql.Statement;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

@ExtendWith(OutputCaptureExtension.class)
class DataSourcePostProcessorEligibilityTest {

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withPropertyValues("spring.datasource.url=jdbc:h2:mem:bpp-eligibility")
          .withConfiguration(
              AutoConfigurations.of(
                  DataSourceAutoConfiguration.class, QueryAuditAutoConfiguration.class));

  @Test
  void wrappingTheDataSourceDoesNotCreateQueryAuditBeansBeforePostProcessors(
      CapturedOutput output) {
    runner.run(
        context -> {
          QueryInterceptor interceptor = context.getBean(QueryInterceptor.class);
          interceptor.start();
          try (Connection connection = context.getBean(DataSource.class).getConnection();
              Statement statement = connection.createStatement()) {
            statement.execute("SELECT 1");
          } finally {
            interceptor.stop();
          }
          assertThat(interceptor.snapshot().queries()).hasSize(1);
        });

    assertThat(output.getAll())
        .doesNotContain("not eligible for getting processed by all BeanPostProcessors");
  }
}
