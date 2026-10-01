package io.queryaudit.core.reporter;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.config.ReportRedaction;
import io.queryaudit.core.extension.FindingKindId;
import io.queryaudit.core.model.Finding;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.Severity;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CustomFindingReporterTest {
  private static final String SECRET = "sensitive-value";
  private static final Finding ERROR =
      new Finding(
          FindingKindId.of("acme:query/budget"),
          Severity.ERROR,
          "SELECT * FROM users WHERE password = '" + SECRET + "'",
          "users",
          "password",
          "<script>alert('" + SECRET + "')</script>",
          "Avoid " + SECRET,
          "com.example.Repository.find:42");
  private static final Finding INFO =
      new Finding(
          FindingKindId.of("acme:query/info"),
          Severity.INFO,
          null,
          null,
          null,
          "informational finding",
          null);
  private static final Finding ACKNOWLEDGED =
      new Finding(
          FindingKindId.of("acme:query/accepted"),
          Severity.WARNING,
          null,
          null,
          null,
          "accepted finding",
          null);

  @Test
  void htmlShowsAllCustomBucketsAndIncludesThemInClassAndMethodStatus(@TempDir Path output)
      throws Exception {
    QueryAuditReport report = report();

    new HtmlReporter().writeToFile(output, List.of(report));

    String index = Files.readString(output.resolve("index.html"));
    String detail = Files.readString(output.resolve("ExampleTest.html"));
    assertThat(index).contains("class=\"row-fail\"", "badge-error\">2</span>");
    assertThat(detail)
        .contains("<details class=\"method method-error\">", "2 issues", "1 errors", "1 info")
        .contains("acme:query/budget", "acme:query/info", "acme:query/accepted", "ACKNOWLEDGED")
        .contains("data-key=\"" + FindingId.of(report.getTestId(), ERROR) + "\"")
        .contains("&lt;script&gt;alert(&#39;" + SECRET + "&#39;)&lt;/script&gt;")
        .doesNotContain("<script>alert(", "No issues detected. All queries look good.");
  }

  @Test
  void consolePreservesRawLocalDetailsAndCountsAllCustomBuckets() {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    new ConsoleReporter(new PrintStream(bytes, true, StandardCharsets.UTF_8), false)
        .report(report());

    String output = bytes.toString(StandardCharsets.UTF_8);
    assertThat(output)
        .contains("--- CONFIRMED (", "--- INFO (", "--- ACKNOWLEDGED (")
        .doesNotContain("CUSTOM CONFIRMED", "CUSTOM INFO", "CUSTOM ACKNOWLEDGED")
        .contains("acme:query/budget", FindingId.of(report().getTestId(), ERROR), SECRET)
        .contains("1 error", "1 info", "1 acknowledged");
  }

  @Test
  void githubPublishesTypedAnnotationsAndCountsWithoutReopeningAcknowledgedFindings(
      @TempDir Path output) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    Path summary = output.resolve("summary.md");
    new GitHubActionsReporter(new PrintStream(bytes, true, StandardCharsets.UTF_8), summary)
        .report(report());

    String annotations = bytes.toString(StandardCharsets.UTF_8);
    assertThat(annotations.lines().filter(line -> line.startsWith("::")).toList()).hasSize(2);
    assertThat(annotations)
        .contains("::error ", "::notice ", "acme:query/budget", "acme:query/info")
        .contains(FindingId.of(report().getTestId(), ERROR))
        .doesNotContain(SECRET, "::warning ", "acme:query/accepted");
    String markdown = Files.readString(summary);
    assertThat(markdown)
        .contains("| ERROR | 1 |", "| WARNING | 0 |", "| INFO | 1 |", "Acknowledged findings: 1")
        .contains("acme:query/budget", "acme:query/accepted")
        .doesNotContain(SECRET);
  }

  @Test
  void githubFullModeRetainsDetailsButEscapesWorkflowCommandNewlines() {
    Finding injected =
        new Finding(
            FindingKindId.of("acme:unsafe-text"),
            Severity.ERROR,
            null,
            null,
            null,
            "first\n::error title=Injected::" + SECRET,
            null);
    QueryAuditReport report = report().withCustomFindings(List.of(injected), List.of(), List.of());
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();

    new GitHubActionsReporter(
            new PrintStream(bytes, true, StandardCharsets.UTF_8), null, ReportRedaction.FULL)
        .report(report);

    String output = bytes.toString(StandardCharsets.UTF_8);
    assertThat(output.lines().toList()).hasSize(1);
    assertThat(output).contains("first%0A::error title=Injected::" + SECRET);
  }

  private static QueryAuditReport report() {
    return new QueryAuditReport(
            "ExampleTest", "findUsers", List.of(), List.of(), List.of(), 0, 0, 0)
        .withCustomFindings(List.of(ERROR), List.of(INFO), List.of(ACKNOWLEDGED));
  }
}
