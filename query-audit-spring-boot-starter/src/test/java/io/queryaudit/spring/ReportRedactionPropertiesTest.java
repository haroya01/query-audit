package io.queryaudit.spring;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.config.ReportRedaction;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

class ReportRedactionPropertiesTest {

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(QueryAuditAutoConfiguration.class));

  @Test
  void defaultsToRedacted() {
    runner.run(
        context ->
            assertThat(context.getBean(QueryAuditConfig.class).getReportRedaction())
                .isEqualTo(ReportRedaction.REDACTED));
  }

  @Test
  void bindsFullDetailOptIn() {
    runner
        .withPropertyValues("query-audit.report.redaction=full")
        .run(
            context ->
                assertThat(context.getBean(QueryAuditConfig.class).getReportRedaction())
                    .isEqualTo(ReportRedaction.FULL));
  }

  @Test
  void rejectsAnUnknownModeAtStartup() {
    runner
        .withPropertyValues("query-audit.report.redaction=typo")
        .run(context -> assertThat(context).hasFailed());
  }
}
