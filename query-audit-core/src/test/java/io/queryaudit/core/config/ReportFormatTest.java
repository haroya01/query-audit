package io.queryaudit.core.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import org.junit.jupiter.api.Test;

class ReportFormatTest {

  @Test
  void parsesSupportedValuesWithoutCaseOrWhitespaceSensitivity() {
    assertThat(ReportFormat.parse("console")).isEqualTo(ReportFormat.CONSOLE);
    assertThat(ReportFormat.parse(" JSON ")).isEqualTo(ReportFormat.JSON);
    assertThat(ReportFormat.parse("Html")).isEqualTo(ReportFormat.HTML);
  }

  @Test
  void rejectsMissingAndUnsupportedValues() {
    assertThatIllegalArgumentException().isThrownBy(() -> ReportFormat.parse(null));
    assertThatIllegalArgumentException().isThrownBy(() -> ReportFormat.parse(" "));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> ReportFormat.parse("xml"))
        .withMessageContaining("console")
        .withMessageContaining("json")
        .withMessageContaining("html");
  }

  @Test
  void configDefaultsToConsoleAndBuilderCopyPreservesSelection() {
    QueryAuditConfig defaults = QueryAuditConfig.defaults();
    QueryAuditConfig json = QueryAuditConfig.builder().reportFormat(ReportFormat.JSON).build();

    assertThat(defaults.getReportFormat()).isEqualTo(ReportFormat.CONSOLE);
    assertThat(QueryAuditConfig.Builder.from(json).build().getReportFormat())
        .isEqualTo(ReportFormat.JSON);
  }
}
