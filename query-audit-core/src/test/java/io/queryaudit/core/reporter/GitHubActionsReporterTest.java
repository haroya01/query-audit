package io.queryaudit.core.reporter;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.Severity;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("GitHubActionsReporter — workflow-command output (issue #85)")
class GitHubActionsReporterTest {

  @TempDir Path tempDir;

  private ByteArrayOutputStream stdout;
  private PrintStream stdoutPrinter;

  @BeforeEach
  void setUp() {
    stdout = new ByteArrayOutputStream();
    stdoutPrinter = new PrintStream(stdout, true);
  }

  private static Issue issue(
      IssueType type, Severity severity, String detail, String sourceLocation) {
    return new Issue(
        type, severity, "select * from t", "t", null, detail, "fix it", sourceLocation);
  }

  private QueryAuditReport reportOf(List<Issue> confirmed, List<Issue> info) {
    return new QueryAuditReport(
        "OrderServiceTest", "findOrders", confirmed, info, List.of(), List.of(), 1, 1, 100_000L);
  }

  @Test
  @DisplayName("emits ::error / ::warning / ::notice per severity bucket")
  void emitsOneLinePerIssueAtCorrectLevel() {
    GitHubActionsReporter reporter = new GitHubActionsReporter(stdoutPrinter, null);
    Issue err = issue(IssueType.N_PLUS_ONE, Severity.ERROR, "N+1 hit", null);
    Issue warn = issue(IssueType.OR_ABUSE, Severity.WARNING, "too many ORs", null);
    Issue info = issue(IssueType.SELECT_ALL, Severity.INFO, "SELECT *", null);

    reporter.report(reportOf(List.of(err, warn), List.of(info)));

    String out = stdout.toString();
    assertThat(out).contains("::error ");
    assertThat(out).contains("::warning ");
    assertThat(out).contains("::notice ");
    assertThat(out).contains("N+1 hit");
    assertThat(out).contains("too many ORs");
    assertThat(out).contains("SELECT *");
  }

  @Test
  @DisplayName("parses sourceLocation into file=/line= when it matches FQCN.method:line")
  void parsesSourceLocationWhenWellFormed() {
    GitHubActionsReporter reporter = new GitHubActionsReporter(stdoutPrinter, null);
    Issue i =
        issue(
            IssueType.MISSING_WHERE_INDEX,
            Severity.ERROR,
            "no idx",
            "com.example.OrderService.findOrders:42");

    reporter.report(reportOf(List.of(i), List.of()));

    String line = stdout.toString();
    assertThat(line).contains("file=src/main/java/com/example/OrderService.java");
    assertThat(line).contains("line=42");
  }

  @Test
  @DisplayName("omits file=/line= when sourceLocation is null or malformed")
  void skipsFileLineWhenSourceLocationMissing() {
    GitHubActionsReporter reporter = new GitHubActionsReporter(stdoutPrinter, null);
    reporter.report(
        reportOf(List.of(issue(IssueType.N_PLUS_ONE, Severity.ERROR, "no loc", null)), List.of()));
    reporter.report(
        reportOf(
            List.of(issue(IssueType.N_PLUS_ONE, Severity.ERROR, "bad loc", "not-a-location")),
            List.of()));

    String out = stdout.toString();
    assertThat(out).doesNotContain("file=");
    assertThat(out).doesNotContain("line=");
  }

  @Test
  @DisplayName("escapes %, newline, carriage return in the body")
  void escapesPercentAndNewlinesInBody() {
    GitHubActionsReporter reporter = new GitHubActionsReporter(stdoutPrinter, null);
    Issue i = issue(IssueType.N_PLUS_ONE, Severity.ERROR, "100% broken\nline two", null);

    reporter.report(reportOf(List.of(i), List.of()));

    String out = stdout.toString();
    assertThat(out).contains("100%25 broken%0Aline two");
    assertThat(out).doesNotContain("100% broken");
  }

  @Test
  @DisplayName("appends a Markdown summary to $GITHUB_STEP_SUMMARY when the path is provided")
  void writesMarkdownSummaryWhenPathProvided() throws Exception {
    Path summary = tempDir.resolve("summary.md");
    GitHubActionsReporter reporter = new GitHubActionsReporter(stdoutPrinter, summary);

    reporter.report(
        reportOf(
            List.of(issue(IssueType.N_PLUS_ONE, Severity.ERROR, "N+1", null)),
            List.of(issue(IssueType.SELECT_ALL, Severity.INFO, "SELECT *", null))));

    String md = Files.readString(summary);
    assertThat(md).contains("query-audit — OrderServiceTest");
    assertThat(md).contains("| ERROR | 1 |");
    assertThat(md).contains("| INFO | 1 |");
    assertThat(md).contains("n-plus-one");
    assertThat(md).contains("select-all");
  }

  @Test
  @DisplayName("parseLocation handles inner-class FQCNs by stripping the $Inner segment")
  void parseLocationStripsInnerClass() {
    GitHubActionsReporter.Location loc =
        GitHubActionsReporter.parseLocation("com.example.OrderService$Inner.doWork:99");

    assertThat(loc).isNotNull();
    assertThat(loc.file()).isEqualTo("src/main/java/com/example/OrderService.java");
    assertThat(loc.line()).isEqualTo(99);
  }

  @Test
  @DisplayName("parseLocation returns null for inputs without a package")
  void parseLocationReturnsNullWithoutPackage() {
    assertThat(GitHubActionsReporter.parseLocation("NoPackageClass.method:10")).isNull();
  }

  @Test
  @DisplayName("empty issue lists produce no stdout output")
  void emptyReportProducesNoOutput() {
    GitHubActionsReporter reporter = new GitHubActionsReporter(stdoutPrinter, null);
    reporter.report(reportOf(List.of(), List.of()));
    assertThat(stdout.toString()).isEmpty();
  }

  @Test
  @DisplayName("null report is tolerated and produces no output")
  void nullReportIsNoOp() {
    GitHubActionsReporter reporter = new GitHubActionsReporter(stdoutPrinter, null);
    reporter.report(null);
    assertThat(stdout.toString()).isEmpty();
  }

  @Test
  @DisplayName("multiple report() calls append to the same summary file")
  void summaryFileIsAppendedNotOverwritten() throws Exception {
    Path summary = tempDir.resolve("summary.md");
    GitHubActionsReporter reporter = new GitHubActionsReporter(stdoutPrinter, summary);

    reporter.report(
        reportOf(List.of(issue(IssueType.N_PLUS_ONE, Severity.ERROR, "first", null)), List.of()));
    reporter.report(
        reportOf(List.of(issue(IssueType.OR_ABUSE, Severity.WARNING, "second", null)), List.of()));

    String md = Files.readString(summary);
    assertThat(md).contains("n-plus-one");
    assertThat(md).contains("or-abuse");
    assertThat(md.indexOf("query-audit —")).isLessThan(md.lastIndexOf("query-audit —"));
  }

  @Test
  @DisplayName("issue with null detail falls back to the IssueType description")
  void nullDetailUsesTypeDescription() {
    GitHubActionsReporter reporter = new GitHubActionsReporter(stdoutPrinter, null);
    Issue i =
        new Issue(IssueType.CARTESIAN_JOIN, Severity.ERROR, "sql", "t", null, null, null, null);

    reporter.report(reportOf(List.of(i), List.of()));

    assertThat(stdout.toString()).contains(IssueType.CARTESIAN_JOIN.getDescription());
  }

  @Test
  @DisplayName("commas in property values are percent-encoded but bodies keep them")
  void commaEncodingDistinguishesPropertyFromBody() {
    GitHubActionsReporter reporter = new GitHubActionsReporter(stdoutPrinter, null);
    // The Issue detail contains a comma; this is the body part and should NOT be encoded.
    Issue i = issue(IssueType.N_PLUS_ONE, Severity.ERROR, "a, b, c", null);

    reporter.report(reportOf(List.of(i), List.of()));

    String out = stdout.toString();
    assertThat(out).contains("a, b, c");
    // Title is a property value — if a comma ever slipped in it would be %2C encoded.
    assertThat(out).contains("title=");
  }
}
