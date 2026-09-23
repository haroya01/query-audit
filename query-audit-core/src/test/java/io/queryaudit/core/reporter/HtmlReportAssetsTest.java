package io.queryaudit.core.reporter;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HtmlReportAssetsTest {
  @Test
  void extractedAssetsPreserveTheExistingRenderedBytes() throws Exception {
    assertThat(digest(asset("report.css")))
        .isEqualTo("8df1719eb6bb0e65208ffe6f9f860566aee6866f30624f97619d997d1f436a19");
    assertThat(digest(asset("report.js")))
        .isEqualTo("0bee2c2cd37c26140d35590af002ac4ed15733aad0a69c1e93b4d028540c5de1");
  }

  @Test
  void everyPageEmbedsTheAssetsAndNeedsNoCompanionFiles(@TempDir Path output) throws Exception {
    Issue issue =
        new Issue(
            IssueType.SELECT_ALL,
            Severity.INFO,
            "SELECT * FROM users",
            "users",
            null,
            "detail",
            "Select only needed columns");
    QueryAuditReport report =
        new QueryAuditReport(
            "ExampleTest",
            "findUsers",
            List.of(),
            List.of(issue),
            List.of(new QueryRecord("SELECT * FROM users", 1, 0, null)),
            1,
            1,
            1);

    new HtmlReporter().writeToFile(output, List.of(report));

    try (var files = Files.list(output)) {
      assertThat(files.map(path -> path.getFileName().toString()).toList())
          .containsExactlyInAnyOrder("index.html", "ExampleTest.html");
    }
    for (String file : List.of("index.html", "ExampleTest.html")) {
      String html = Files.readString(output.resolve(file));
      assertThat(html)
          .contains("<style>\n" + asset("report.css") + "</style>")
          .contains("<script>\n" + asset("report.js") + "</script>")
          .doesNotContain("<script src=", "rel=\"stylesheet\"");
    }
    String classHtml = Files.readString(output.resolve("ExampleTest.html"));
    assertThat(classHtml)
        .contains("<details class=\"method method-ok\">", "findUsers", "Select only needed columns")
        .doesNotContain("<details class=\"test-card", "<details class=\"method-card");
  }

  private static String asset(String name) throws Exception {
    try (var stream =
        HtmlReportAssetsTest.class.getResourceAsStream(
            "/io/queryaudit/core/reporter/html/" + name)) {
      assertThat(stream).as("packaged HTML asset %s", name).isNotNull();
      return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  private static String digest(String value) throws Exception {
    return HexFormat.of()
        .formatHex(
            MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
  }
}
