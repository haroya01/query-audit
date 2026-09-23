package io.queryaudit.core.reporter;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.baseline.BaselineEntry;
import io.queryaudit.core.config.ReportRedaction;
import io.queryaudit.core.extension.FindingKindId;
import io.queryaudit.core.model.AuditFindings;
import io.queryaudit.core.model.Finding;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.parser.SqlParser;
import io.queryaudit.core.ranking.ImpactScorer;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class UnifiedFindingPresentationTest {
  private static final String SECRET = "private-renderer-value";
  private static final Pattern ID = Pattern.compile("qa-finding-v1:[a-f0-9]{64}");
  private static final Finding BUILTIN_ERROR =
      finding("n-plus-one", Severity.ERROR, "builtin-error");
  private static final Finding BUILTIN_WARNING =
      finding("select-all", Severity.WARNING, "builtin-warning");
  private static final Finding BUILTIN_INFO = finding("full-scan", Severity.INFO, "builtin-info");
  private static final Finding BUILTIN_ACK = finding("or-abuse", Severity.WARNING, "builtin-ack");
  private static final Finding CUSTOM_ERROR =
      finding("shop:budget", Severity.ERROR, "custom-error");
  private static final Finding CUSTOM_WARNING =
      finding("shop:latency", Severity.WARNING, "custom-warning");
  private static final Finding CUSTOM_INFO = finding("shop:info", Severity.INFO, "custom-info");
  private static final Finding CUSTOM_ACK =
      finding("shop:accepted", Severity.WARNING, "custom-ack");

  @TempDir Path output;

  @Test
  void everyBuiltInAndCustomNormalRowHasItsStableIdInEveryRenderer() throws Exception {
    QueryAuditReport report = report(mixedFindings());
    List<String> allIds = ids(report, report.getFindings().all());
    String console = console(report, List.of());
    assertThat(
            console
                .lines()
                .filter(line -> line.startsWith("    ID:     "))
                .map(line -> line.substring("    ID:     ".length()))
                .toList())
        .containsExactlyInAnyOrderElementsOf(allIds);
    assertThat(console)
        .contains("2 errors", "2 warnings", "2 info", "2 acknowledged")
        .doesNotContain("CUSTOM CONFIRMED", "CUSTOM INFO", "CUSTOM ACKNOWLEDGED");

    new HtmlReporter().writeToFile(output, List.of(report));
    String html = Files.readString(output.resolve("MixedTest.html"));
    assertThat(attributeIds(html, "data-finding-id")).containsExactlyInAnyOrderElementsOf(allIds);
    assertThat(html.lines().filter(line -> line.contains("Finding ID: <code>")).count())
        .isEqualTo(8);
    for (String id : allIds) assertThat(html).contains("Finding ID: <code>" + id + "</code>");

    var annotations = new ByteArrayOutputStream();
    Path summary = output.resolve("summary.md");
    new GitHubActionsReporter(printer(annotations), summary).report(report);
    String commands = annotations.toString(StandardCharsets.UTF_8);
    assertThat(commands.lines().count()).isEqualTo(6);
    assertThat(extractIds(commands))
        .containsExactlyInAnyOrderElementsOf(
            ids(
                report,
                List.of(
                    BUILTIN_ERROR,
                    BUILTIN_WARNING,
                    BUILTIN_INFO,
                    CUSTOM_ERROR,
                    CUSTOM_WARNING,
                    CUSTOM_INFO)));
    assertThat(extractIds(Files.readString(summary))).containsExactlyInAnyOrderElementsOf(allIds);
    assertThat(commands)
        .doesNotContain(
            FindingId.of(report.getTestId(), BUILTIN_ACK),
            FindingId.of(report.getTestId(), CUSTOM_ACK));
  }

  @Test
  void compactOccurrencesHaveIdsForBothBuiltInAndCustomKinds() {
    List<Finding> findings =
        List.of(
            finding("n-plus-one", Severity.ERROR, "first"),
            finding("n-plus-one", Severity.ERROR, "second"),
            finding("n-plus-one", Severity.ERROR, "third"),
            finding("shop:budget", Severity.ERROR, "fourth"),
            finding("shop:budget", Severity.ERROR, "fifth"),
            finding("shop:budget", Severity.ERROR, "sixth"));
    QueryAuditReport report = report(new AuditFindings(findings, List.of(), List.of()));

    String console = console(report, List.of());

    assertThat(extractIds(console)).containsExactlyInAnyOrderElementsOf(ids(report, findings));
    assertThat(console).contains("(3 occurrences)", "second", "third", "fifth", "sixth");
  }

  @Test
  void acknowledgedRowsKeepBaselineReasonAndReviewerForEveryKind() throws Exception {
    QueryAuditReport report =
        report(new AuditFindings(List.of(), List.of(), List.of(BUILTIN_ACK, CUSTOM_ACK)));
    List<BaselineEntry> baseline =
        List.of(
            baseline(BUILTIN_ACK, "built-in reviewer", "built-in reason"),
            baseline(CUSTOM_ACK, "custom <reviewer>", "custom <reason>"));

    assertThat(console(report, baseline))
        .contains(
            "Reason: built-in reason",
            "By:     built-in reviewer",
            "Reason: custom <reason>",
            "By:     custom <reviewer>");
    new HtmlReporter(baseline).writeToFile(output, List.of(report));
    String html = Files.readString(output.resolve("MixedTest.html"));
    assertThat(html)
        .contains(
            "built-in reason",
            "built-in reviewer",
            "custom &lt;reason&gt;",
            "custom &lt;reviewer&gt;",
            "ack-reason",
            "Acknowledged By");
    assertThat(attributeIds(html, "data-finding-id"))
        .containsExactlyInAnyOrderElementsOf(ids(report, List.of(BUILTIN_ACK, CUSTOM_ACK)));
    assertThat(html).doesNotContain("class=\"issue-check\"", "custom <reason>");
  }

  @Test
  void htmlKeepsPersistedCheckboxKeysAndHashSeparateFromNewOccurrenceIds() throws Exception {
    QueryAuditReport report = report(mixedFindings());
    // Explicitly retain the pre-unification persistence protocol: enum-backed errors/warnings/info,
    // then custom confirmed/info. Acknowledged rows never participated in this hash.
    List<String> persistedKeys =
        List.of(
            legacyKey(BUILTIN_ERROR),
            legacyKey(BUILTIN_WARNING),
            legacyKey(BUILTIN_INFO),
            FindingId.of(report.getTestId(), CUSTOM_ERROR),
            FindingId.of(report.getTestId(), CUSTOM_WARNING),
            FindingId.of(report.getTestId(), CUSTOM_INFO));
    int expectedHash = 0;
    for (String key : persistedKeys) expectedHash = 31 * expectedHash + key.hashCode();
    String hash = Integer.toHexString(expectedHash);

    new HtmlReporter().writeToFile(output, List.of(report));

    String html = Files.readString(output.resolve("MixedTest.html"));
    assertThat(attributeIds(html, "data-key")).containsExactlyInAnyOrderElementsOf(persistedKeys);
    assertThat(html).contains("<meta name=\"qg-report-hash\" content=\"" + hash + "\">");
    assertThat(Files.readString(output.resolve("index.html")))
        .contains("data-class=\"MixedTest\" data-hash=\"" + hash + "\"");
    assertThat(html)
        .contains(
            "data-key=\"" + legacyKey(BUILTIN_ERROR) + "\"",
            "data-finding-id=\"" + FindingId.of(report.getTestId(), BUILTIN_ERROR) + "\"");
  }

  @Test
  void identicalBuiltInSqlInDifferentTestsRetainsTheOldCheckKeyButGetsDifferentOccurrenceIds()
      throws Exception {
    QueryAuditReport first =
        report(new AuditFindings(List.of(BUILTIN_ERROR), List.of(), List.of()));
    QueryAuditReport second = first.withTestIdentity("another:test", null);

    new HtmlReporter().writeToFile(output, List.of(first, second));

    String html = Files.readString(output.resolve("MixedTest.html"));
    assertThat(attributeIds(html, "data-key"))
        .containsExactly(legacyKey(BUILTIN_ERROR), legacyKey(BUILTIN_ERROR));
    assertThat(attributeIds(html, "data-finding-id"))
        .containsExactly(
            FindingId.of(first.getTestId(), BUILTIN_ERROR),
            FindingId.of(second.getTestId(), BUILTIN_ERROR));
    assertThat(FindingId.of(first.getTestId(), BUILTIN_ERROR))
        .isNotEqualTo(FindingId.of(second.getTestId(), BUILTIN_ERROR));
  }

  @ParameterizedTest
  @EnumSource(ReportRedaction.class)
  void githubUsesOriginalIdsBeforeRedactionInAnnotationsAndSummary(ReportRedaction mode)
      throws Exception {
    QueryAuditReport report = report(mixedFindings());
    var bytes = new ByteArrayOutputStream();
    Path summary = output.resolve(mode + ".md");

    new GitHubActionsReporter(printer(bytes), summary, mode).report(report);

    String commands = bytes.toString(StandardCharsets.UTF_8);
    String markdown = Files.readString(summary);
    assertThat(extractIds(markdown))
        .containsExactlyInAnyOrderElementsOf(ids(report, report.getFindings().all()));
    for (Finding finding : List.of(BUILTIN_ERROR, CUSTOM_ERROR, BUILTIN_INFO, CUSTOM_INFO)) {
      assertThat(commands).contains(FindingId.of(report.getTestId(), finding));
    }
    if (mode == ReportRedaction.REDACTED) {
      assertThat(commands).doesNotContain(SECRET);
      assertThat(markdown).doesNotContain(SECRET);
    } else {
      assertThat(commands).contains(SECRET);
      assertThat(markdown).contains(SECRET);
    }
  }

  @Test
  void unifiedHtmlRowsEscapeUntrustedDetailsForBothBuiltInAndCustomKinds() throws Exception {
    String attack = "<script>alert('unsafe&value')</script>";
    Finding builtin =
        new Finding(
            FindingKindId.builtin(IssueType.SELECT_ALL),
            Severity.WARNING,
            attack,
            attack,
            attack,
            attack,
            attack,
            attack);
    Finding custom =
        new Finding(
            FindingKindId.of("shop:unsafe"),
            Severity.WARNING,
            attack,
            attack,
            attack,
            attack,
            attack,
            attack);
    QueryAuditReport report =
        report(new AuditFindings(List.of(builtin, custom), List.of(), List.of()));

    new HtmlReporter().writeToFile(output, List.of(report));

    String html = Files.readString(output.resolve("MixedTest.html"));
    assertThat(html)
        .doesNotContain(attack)
        .contains("&lt;script&gt;alert(&#39;unsafe&amp;value&#39;)&lt;/script&gt;");
    assertThat(attributeIds(html, "data-finding-id"))
        .containsExactlyInAnyOrderElementsOf(ids(report, List.of(builtin, custom)));
  }

  @Test
  void unifiedGithubCommandsKeepPropertyEscapingAndCannotInjectExtraAnnotations() {
    String attack = "100% broken\r\n::error title=Injected::" + SECRET;
    Finding builtin =
        new Finding(
            FindingKindId.builtin(IssueType.SELECT_ALL),
            Severity.WARNING,
            null,
            null,
            null,
            attack,
            null,
            "com.example.Repository.find:42");
    Finding custom =
        new Finding(
            FindingKindId.of("shop:unsafe"),
            Severity.ERROR,
            null,
            null,
            null,
            attack,
            null,
            "com.example.Repository.find:42");
    QueryAuditReport report =
        new QueryAuditReport("Example,100%Test", "method", List.of(), List.of(), List.of(), 0, 0, 0)
            .withFindings(new AuditFindings(List.of(builtin, custom), List.of(), List.of()));
    var bytes = new ByteArrayOutputStream();

    new GitHubActionsReporter(printer(bytes), null, ReportRedaction.FULL).report(report);

    String commands = bytes.toString(StandardCharsets.UTF_8);
    assertThat(commands.lines().toList())
        .hasSize(2)
        .allSatisfy(
            line ->
                assertThat(line)
                    .contains(
                        "file=src/main/java/com/example/Repository.java,line=42,",
                        "title=Example%2C100%25Test",
                        "100%25 broken%0D%0A::error title=Injected::"));
    assertThat(commands).contains("::error ", "::warning ");
    assertThat(extractIds(commands))
        .containsExactlyInAnyOrderElementsOf(ids(report, List.of(builtin, custom)));
  }

  @ParameterizedTest
  @EnumSource(ReportRedaction.class)
  void githubSummaryKeepsUntrustedTablesInsideSingleLineCodeSpans(ReportRedaction mode)
      throws Exception {
    List<String[]> cases =
        List.of(
            new String[] {"orders", "`orders`"},
            new String[] {
              "x` </details><h1>PASS</h1><details> `y", "``x` </details><h1>PASS</h1><details> `y``"
            },
            new String[] {"`orders`", "`` `orders` ``"},
            new String[] {"```orders```", "```` ```orders``` ````"},
            new String[] {
              "`orders\r\n</details>\n### PASS\n`", "`` `orders </details> ### PASS ` ``"
            });
    for (int index = 0; index < cases.size(); index++) {
      String[] example = cases.get(index);
      QueryAuditReport report = summaryReport(example[0], "inspect table");
      String markdown = githubSummary(report, mode, "tables-" + index);

      assertThat(markdown.lines().filter(line -> line.startsWith("- ")).toList())
          .hasSize(2)
          .allSatisfy(line -> assertThat(line).contains(" on " + example[1] + " — "));
      assertThat(markdown.lines().filter(line -> line.startsWith("### ")).toList())
          .containsExactly("### query-audit — MixedTest");
      assertThat(markdown).doesNotContain("\r", "\n```", "\n### PASS");
      assertThat(extractIds(markdown))
          .containsExactlyInAnyOrderElementsOf(ids(report, report.getFindings().all()));
    }
  }

  @ParameterizedTest
  @EnumSource(ReportRedaction.class)
  void githubSummaryTreatsFreeTextAsTextForBuiltInAndCustomFindings(ReportRedaction mode)
      throws Exception {
    String attack =
        "</details><h1>PASS</h1><details> [click](https://example.invalid)"
            + " ![image](https://example.invalid) **strong** _emphasis_ ~~strike~~ `code`"
            + " | cell | \\escape &lt;script&gt;";
    QueryAuditReport report = summaryReport("orders", attack);
    String markdown = githubSummary(report, mode, "details");

    assertThat(markdown).doesNotContain("</details><h1>", "[click]", "![image]", "~~strike~~");
    if (mode == ReportRedaction.FULL) {
      assertThat(markdown.lines().filter(line -> line.startsWith("- ")).toList())
          .hasSize(2)
          .allSatisfy(
              line ->
                  assertThat(line)
                      .contains(
                          "&lt;/details&gt;&lt;h1&gt;PASS&lt;/h1&gt;&lt;details&gt;",
                          "\\[click\\](https://example.invalid)",
                          "!\\[image\\](https://example.invalid)",
                          "\\*\\*strong\\*\\*",
                          "\\_emphasis\\_",
                          "\\~\\~strike\\~\\~",
                          "\\`code\\`",
                          "\\| cell \\|",
                          "\\\\escape",
                          "&amp;lt;script&amp;gt;"));
    } else {
      assertThat(markdown)
          .contains("Details omitted by report redaction")
          .doesNotContain("example.invalid", "strong", "escape");
    }
    assertThat(extractIds(markdown))
        .containsExactlyInAnyOrderElementsOf(ids(report, report.getFindings().all()));
  }

  @Test
  void githubSummaryCannotCreateBlocksThroughAnyLineBreakInHeadingsOrDetails() throws Exception {
    List<String> separators =
        List.of("\n", "\r", "\r\n", "\u000b", "\f", "\u0085", "\u2028", "\u2029");
    for (int index = 0; index < separators.size(); index++) {
      String separator = separators.get(index);
      String injected =
          separator
              + "```markdown"
              + separator
              + "# forged heading"
              + separator
              + "<h1>forged html</h1>";
      QueryAuditReport report =
          new QueryAuditReport(
                  "MixedTest" + injected, "inspect", List.of(), List.of(), List.of(), 0, 2, 0)
              .withFindings(summaryReport("orders", "ordinary text" + injected).getFindings());
      for (ReportRedaction mode : ReportRedaction.values()) {
        String markdown = githubSummary(report, mode, "lines-" + index);
        assertThat(markdown.lines().filter(line -> line.startsWith("### ")).toList())
            .hasSize(1)
            .allSatisfy(
                line ->
                    assertThat(line)
                        .contains(
                            "MixedTest \\`\\`\\`markdown # forged heading &lt;h1&gt;forged html&lt;/h1&gt;"));
        assertThat(markdown)
            .doesNotContain(
                "\n```", "\n# forged", "<h1>", "\r", "\u000b", "\f", "\u0085", "\u2028", "\u2029");
        assertThat(markdown.lines().filter(line -> line.startsWith("- ")).toList())
            .hasSize(2)
            .allSatisfy(line -> assertThat(line).doesNotContain("forged", "markdown"));
        if (mode == ReportRedaction.FULL)
          assertThat(markdown).contains(" — ordinary text — Finding ID:");
        assertThat(extractIds(markdown))
            .containsExactlyInAnyOrderElementsOf(ids(report, report.getFindings().all()));
      }
    }
  }

  private static QueryAuditReport summaryReport(String table, String detail) {
    Finding builtin =
        new Finding(
            FindingKindId.builtin(IssueType.SELECT_ALL),
            Severity.WARNING,
            "SELECT id FROM orders",
            table,
            null,
            detail,
            null);
    Finding custom =
        new Finding(
            FindingKindId.of("shop:summary"),
            Severity.WARNING,
            "SELECT id FROM orders",
            table,
            null,
            detail,
            null);
    return report(new AuditFindings(List.of(builtin, custom), List.of(), List.of()));
  }

  private String githubSummary(QueryAuditReport report, ReportRedaction mode, String name)
      throws Exception {
    Path summary = output.resolve(name + "-" + mode + ".md");
    new GitHubActionsReporter(printer(new ByteArrayOutputStream()), summary, mode).report(report);
    return Files.readString(summary);
  }

  @Test
  void rankingStillUsesOnlyGenuineBuiltInFindings() throws Exception {
    QueryAuditReport report = report(mixedFindings());
    var expected =
        ImpactScorer.rank(
            List.of(
                BUILTIN_ERROR.toIssue().orElseThrow(), BUILTIN_WARNING.toIssue().orElseThrow()));
    assertThat(LegacyFindingPresentation.rankBuiltIns(report.getFindings().confirmed()))
        .isEqualTo(expected);
    String console = console(report, List.of());
    String ranking =
        console.substring(
            console.indexOf("TOP ISSUES BY IMPACT"), console.indexOf("--- CONFIRMED"));
    assertThat(ranking)
        .contains("#1", "pts", IssueType.N_PLUS_ONE.getDescription())
        .doesNotContain("shop:");
    new HtmlReporter().writeToFile(output, List.of(report), expected);
    assertThat(Files.readString(output.resolve("index.html")))
        .contains("Top Issues by Impact", IssueType.N_PLUS_ONE.getDescription());
  }

  @Test
  void renderersNeedOnlyTheUnifiedFindingViewNotCompatibilityGetters() throws Exception {
    QueryAuditReport report = new NativeOnlyReport(mixedFindings());

    assertThat(extractIds(console(report, List.of()))).hasSize(8);
    new HtmlReporter().writeToFile(output, List.of(report));
    assertThat(attributeIds(Files.readString(output.resolve("MixedTest.html")), "data-finding-id"))
        .hasSize(8);
    var bytes = new ByteArrayOutputStream();
    Path summary = output.resolve("native.md");
    new GitHubActionsReporter(printer(bytes), summary).report(report);
    assertThat(extractIds(bytes.toString(StandardCharsets.UTF_8))).hasSize(6);
    assertThat(extractIds(Files.readString(summary))).hasSize(8);
  }

  private static AuditFindings mixedFindings() {
    return new AuditFindings(
        List.of(BUILTIN_WARNING, CUSTOM_ERROR, BUILTIN_ERROR, CUSTOM_WARNING),
        List.of(BUILTIN_INFO, CUSTOM_INFO),
        List.of(BUILTIN_ACK, CUSTOM_ACK));
  }

  private static QueryAuditReport report(AuditFindings findings) {
    return new QueryAuditReport("MixedTest", "inspect", List.of(), List.of(), List.of(), 0, 8, 0)
        .withFindings(findings);
  }

  private static Finding finding(String kind, Severity severity, String label) {
    return new Finding(
        FindingKindId.of(kind),
        severity,
        "select id from orders where note = '" + SECRET + "'",
        "orders",
        label,
        label + " " + SECRET,
        "review " + label,
        "com.example.Repository.find:42");
  }

  private static BaselineEntry baseline(Finding finding, String by, String reason) {
    return new BaselineEntry(
        finding.kindId().value(), finding.table(), finding.column(), finding.query(), by, reason);
  }

  private static String console(QueryAuditReport report, List<BaselineEntry> baseline) {
    var bytes = new ByteArrayOutputStream();
    new ConsoleReporter(printer(bytes), false, baseline).report(report);
    return bytes.toString(StandardCharsets.UTF_8);
  }

  private static PrintStream printer(ByteArrayOutputStream bytes) {
    return new PrintStream(bytes, true, StandardCharsets.UTF_8);
  }

  private static List<String> ids(QueryAuditReport report, List<Finding> findings) {
    return findings.stream().map(finding -> FindingId.of(report.getTestId(), finding)).toList();
  }

  private static List<String> extractIds(String text) {
    return ID.matcher(text).results().map(match -> match.group()).toList();
  }

  private static List<String> attributeIds(String html, String attribute) {
    return Pattern.compile(attribute + "=\"([^\"]*)\"")
        .matcher(html)
        .results()
        .map(match -> match.group(1))
        .toList();
  }

  private static String legacyKey(Finding finding) {
    return finding.kindId().value()
        + "-"
        + Integer.toHexString(SqlParser.normalize(finding.query()).hashCode());
  }

  private static final class NativeOnlyReport extends QueryAuditReport {
    private final AuditFindings complete;

    NativeOnlyReport(AuditFindings complete) {
      super("MixedTest", "inspect", List.of(), List.of(), List.of(), 0, 8, 0);
      this.complete = complete;
    }

    @Override
    public AuditFindings getFindings() {
      return complete;
    }

    @Override
    public List<Issue> getErrors() {
      throw compatibilityGetter();
    }

    @Override
    public List<Issue> getWarnings() {
      throw compatibilityGetter();
    }

    @Override
    public List<Issue> getConfirmedIssues() {
      throw compatibilityGetter();
    }

    @Override
    public List<Issue> getInfoIssues() {
      throw compatibilityGetter();
    }

    @Override
    public List<Issue> getAcknowledgedIssues() {
      throw compatibilityGetter();
    }

    @Override
    public List<Finding> getCustomConfirmedFindings() {
      throw compatibilityGetter();
    }

    @Override
    public List<Finding> getCustomInfoFindings() {
      throw compatibilityGetter();
    }

    @Override
    public List<Finding> getCustomAcknowledgedFindings() {
      throw compatibilityGetter();
    }

    @Override
    public List<Finding> getConfirmedFindings() {
      throw compatibilityGetter();
    }

    @Override
    public List<Finding> getInfoFindings() {
      throw compatibilityGetter();
    }

    @Override
    public List<Finding> getAcknowledgedFindings() {
      throw compatibilityGetter();
    }

    private AssertionError compatibilityGetter() {
      return new AssertionError("Use getFindings() for presentation, not a compatibility getter");
    }
  }
}
