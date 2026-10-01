package io.queryaudit.core.reporter;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.dedup.IssueFingerprintDeduplicator;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.Severity;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HtmlTestLinkTest {
  private static final Pattern LINK =
      Pattern.compile("href=\"com\\.example\\.UserTest\\.html#([^\"]+)\"");

  @Test
  void everyAffectedTestLinkTargetsAMethodOnItsClassPage(@TempDir Path output) throws Exception {
    List<QueryAuditReport> reports =
        List.of(report("findUsers()"), report("find users()"), report("[1] find, users"));

    new HtmlReporter()
        .writeToFile(output, reports, List.of(), IssueFingerprintDeduplicator.deduplicate(reports));

    String index = Files.readString(output.resolve("index.html"));
    String classPage = Files.readString(output.resolve("com.example.UserTest.html"));
    Matcher links = LINK.matcher(index);
    int linked = 0;
    while (links.find()) {
      assertThat(classPage).contains("<details id=\"" + links.group(1) + "\"");
      linked++;
    }
    assertThat(linked).isEqualTo(3);
    assertThat(HtmlReporter.testAnchor("findUsers()"))
        .isNotEqualTo(HtmlReporter.testAnchor("find users()"));
  }

  @Test
  void repeatedTestNamesOnOnePageGetDistinctIds(@TempDir Path output) throws Exception {
    new HtmlReporter().writeToFile(output, List.of(report("readsTwice()"), report("readsTwice()")));

    String classPage = Files.readString(output.resolve("com.example.UserTest.html"));
    String anchor = HtmlReporter.testAnchor("readsTwice()");
    assertThat(classPage)
        .contains("<details id=\"" + anchor + "\"", "<details id=\"" + anchor + "-");
  }

  private static QueryAuditReport report(String testName) {
    Issue issue =
        new Issue(
            IssueType.N_PLUS_ONE,
            Severity.ERROR,
            "SELECT * FROM users WHERE id = ?",
            "users",
            null,
            "repeated",
            "batch it");
    return new QueryAuditReport(
        "com.example.UserTest",
        testName,
        List.of(issue),
        List.of(),
        List.of(),
        List.of(),
        1,
        3,
        1L);
  }
}
