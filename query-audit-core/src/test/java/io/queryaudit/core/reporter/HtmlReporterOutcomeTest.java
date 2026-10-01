package io.queryaudit.core.reporter;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.AuditIncompleteReason;
import io.queryaudit.core.model.AuditRunResult;
import io.queryaudit.core.model.IncompleteReasonCode;
import io.queryaudit.core.model.QueryAuditReport;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HtmlReporterOutcomeTest {
  private static final String CLASS_PAGE = "com.example.OrderServiceTest.html";

  @Test
  void inconclusiveRunWithoutFindingsIsNotShownAsClean(@TempDir Path output) throws Exception {
    AuditRunResult run =
        AuditRunResult.inconclusive(
            List.of(findingFreeReport()),
            new AuditIncompleteReason(
                IncompleteReasonCode.QUERY_LIMIT_REACHED, "<b>loadsOrders</b> reached 2 queries"));

    new HtmlReporter().writeToFile(output, run, List.of(), List.of());

    String index = Files.readString(output.resolve("index.html"));
    String detail = Files.readString(output.resolve(CLASS_PAGE));
    assertThat(index)
        .contains("data-outcome=\"INCONCLUSIVE\"", "Outcome: INCONCLUSIVE")
        .contains("<code>QUERY_LIMIT_REACHED</code> &lt;b&gt;loadsOrders&lt;/b&gt;")
        .contains("class=\"row-unverified\"", "unverified-dot")
        .doesNotContain("class=\"row-pass\"", "status-dot ok-dot", "<b>loadsOrders</b>");
    assertThat(detail)
        .contains("data-outcome=\"INCONCLUSIVE\"", "stat unverified\">no findings")
        .contains("method method-unverified", "No findings for this test.")
        .doesNotContain("stat ok\">", "status-indicator ok-dot", "all clean");
  }

  @Test
  void failedRunWithoutFindingsIsNotShownAsClean(@TempDir Path output) throws Exception {
    new HtmlReporter()
        .writeToFile(
            output, AuditRunResult.fail(List.of(findingFreeReport())), List.of(), List.of());

    String index = Files.readString(output.resolve("index.html"));
    assertThat(index)
        .contains("data-outcome=\"FAIL\"", "class=\"row-unverified\"")
        .doesNotContain("class=\"row-pass\"");
    assertThat(Files.readString(output.resolve(CLASS_PAGE))).contains("data-outcome=\"FAIL\"");
  }

  @Test
  void passingRunKeepsTheCleanStatus(@TempDir Path output) throws Exception {
    new HtmlReporter()
        .writeToFile(
            output, AuditRunResult.pass(List.of(findingFreeReport())), List.of(), List.of());

    String index = Files.readString(output.resolve("index.html"));
    String detail = Files.readString(output.resolve(CLASS_PAGE));
    assertThat(index).contains("data-outcome=\"PASS\"", "class=\"row-pass\"", "status-dot ok-dot");
    assertThat(detail)
        .contains("stat ok\">no findings", "method method-ok")
        .doesNotContain("data-outcome=");
  }

  @Test
  void reportsWithoutARunShowNoOutcome(@TempDir Path output) throws Exception {
    new HtmlReporter().writeToFile(output, List.of(findingFreeReport()));

    String index = Files.readString(output.resolve("index.html"));
    assertThat(index).doesNotContain("data-outcome=").contains("class=\"row-pass\"");
  }

  private static QueryAuditReport findingFreeReport() {
    return new QueryAuditReport(
        "com.example.OrderServiceTest",
        "loadsOrders",
        List.of(),
        List.of(),
        List.of(),
        List.of(),
        0,
        1,
        1_000L);
  }
}
