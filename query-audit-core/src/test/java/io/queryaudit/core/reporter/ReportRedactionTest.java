package io.queryaudit.core.reporter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.config.ReportRedaction;
import io.queryaudit.core.model.AuditIncompleteReason;
import io.queryaudit.core.model.AuditOutcome;
import io.queryaudit.core.model.AuditRunResult;
import io.queryaudit.core.model.IncompleteReasonCode;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ReportRedactionTest {

  @ParameterizedTest
  @ValueSource(
      strings = {
        "'fixture-secret'",
        "'fixture-secret''quoted'",
        "E'fixture-secret\\\\escaped'",
        "N'fixture-secret'",
        "U&'fixture-secret'",
        "_utf8mb4'fixture-secret'",
        "\"fixture-secret\"",
        "$$fixture-secret$$",
        "$body$fixture-secret$body$",
        "q'[fixture-secret'quoted]'",
        "q'!fixture-secret'quoted!'",
        "B'101101'",
        "X'FA1234'",
        "0xFA1234",
        "0x_FA1234",
        "0o_101101",
        "1_884455",
        "1.2_884455",
        "1e1_884455",
        "0b101101",
        "884455",
        "884455.25",
        ".884455",
        "884455e-12",
        "DATE '2044-05-19'",
        "TIMESTAMP '2044-05-19 22:41:38'",
        "INTERVAL '884455 days'",
        "'fixture-secret",
        "$body$fixture-secret",
        "/* fixture-secret */ 884455",
        "/* outer /* fixture-secret */ still-secret */ 884455",
        "-- fixture-secret\n884455",
        "# fixture-secret\n884455",
        "#>fixture-secret",
        "#>> fixture-secret",
        "/* fixture-secret",
        "TRUE",
        "FALSE",
        "NULL"
      })
  void removesLiteralAndCommentPayloads(String expression) {
    String sql = "SELECT " + expression;
    ReportRedactor redactor = new ReportRedactor(ReportRedaction.REDACTED);
    String redacted = redactor.sql(sql);
    assertThat(redacted)
        .doesNotContain(
            "fixture-secret",
            "still-secret",
            "101101",
            "FA1234",
            "884455",
            "2044",
            "22:41:38",
            "TRUE",
            "FALSE",
            "NULL");
    assertThat(redactor.sql(redacted)).isEqualTo(redacted);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT 'path\\', 'fixture-secret'",
        "SELECT E'path\\\\', 'fixture-secret'",
        "SELECT \"path\\\", \"fixture-secret\"",
        "SELECT $태그$fixture-secret$태그$",
        "SELECT $é$fixture-secret$é$",
        "SELECT `unfinished, 'fixture-secret'",
        "SELECT `path\\`, 'fixture-secret'"
      })
  void ambiguousEscapesAndUnicodeDollarTagsCannotExposeLaterValues(String sql) {
    assertThat(new ReportRedactor(ReportRedaction.REDACTED).sql(sql))
        .doesNotContain("fixture-secret");
  }

  @Test
  void retainsStatementStructureAndParameterMarkers() {
    String sql =
        "SELECT account2.id FROM `account2` WHERE id = $1 AND email = ? AND token ="
            + " 'fixture-secret'";
    assertThat(new ReportRedactor(ReportRedaction.REDACTED).sql(sql))
        .isEqualTo("SELECT account2.id FROM `account2` WHERE id = $1 AND email = ? AND token = ?");
  }

  @Test
  void defaultsProtectEveryIssueArrayRawQueriesAndFreeFormDiagnostics() throws Exception {
    QueryAuditReport report = report();
    JsonReporter defaultReporter = new JsonReporter();
    defaultReporter.report(report);
    assertThat(defaultReporter.getJson()).doesNotContain("fixture-secret");
    String redacted = JsonReporter.toRunEnvelopeJson(AuditRunResult.fail(List.of(report)));
    Map<?, ?> envelope = parse(redacted);
    assertThat(redacted)
        .doesNotContain("fixture-secret", "884455", "Users", "private-work", "hibernate");
    assertThat(envelope.get("redaction")).isEqualTo("REDACTED");
    assertThat(envelope.get("outcome")).isEqualTo("FAIL");
    assertThat(redacted)
        .contains(
            "Missing index on WHERE column",
            "add-index",
            "account_id",
            "at com.example.AccountRepository.load(AccountRepository.java:42)");
    assertThat(report.getConfirmedIssues().get(0).detail()).contains("fixture-secret");
    assertThat(report.getAllQueries().get(0).sql()).contains("fixture-secret");
    assertThat(redacted)
        .isEqualTo(JsonReporter.toRunEnvelopeJson(AuditRunResult.fail(List.of(report))));

    var schema =
        JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7)
            .getSchema(getClass().getResourceAsStream("/report-1.4.schema.json"));
    var mapper = new ObjectMapper();
    assertThat(schema.validate(mapper.readTree(redacted))).isEmpty();
    assertThat(schema.validate(mapper.readTree(redacted.replace("REDACTED", "unknown"))))
        .isNotEmpty();
  }

  @Test
  void fullModeIsAnExplicitOptInAndCoreConfigPreservesIt() {
    QueryAuditConfig defaults = QueryAuditConfig.defaults();
    assertThat(defaults.getReportRedaction()).isEqualTo(ReportRedaction.REDACTED);
    QueryAuditConfig fullConfig =
        QueryAuditConfig.builder().reportRedaction(ReportRedaction.FULL).build();
    assertThat(QueryAuditConfig.Builder.from(fullConfig).build().getReportRedaction())
        .isEqualTo(ReportRedaction.FULL);
    JsonReporter reporter = new JsonReporter(fullConfig);
    reporter.report(report());
    assertThat(reporter.getJson()).contains("fixture-secret", "884455", "private-work");
    assertThat(
            JsonReporter.toRunEnvelopeJson(
                AuditRunResult.pass(List.of(report())), ReportRedaction.FULL))
        .contains("\"redaction\": \"FULL\"", "fixture-secret");
    assertThat(ReportRedaction.parse(" full ")).isEqualTo(ReportRedaction.FULL);
    assertThatThrownBy(() -> ReportRedaction.parse("sometimes"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void trimsFrameworkFramesAndAbsolutePathsAndBoundsApplicationFrames() {
    String stack =
        "at org.hibernate.Loader.load(Loader.java:17)\n"
            + "at com.example.Repository.load(/Users/developer/private-work/Repository.java:42)\n"
            + "at com.example.Service.run(C:\\private-work\\Service.java:7)\n"
            + "at com.example.Call.a(Call.java:1)\nat com.example.Call.b(Call.java:2)\n"
            + "at com.example.Call.c(Call.java:3)\nat com.example.Call.d(Call.java:4)\n"
            + "unrecognized fixture-secret /private-work/path";
    String redacted = new ReportRedactor(ReportRedaction.REDACTED).stackTrace(stack);
    assertThat(redacted.lines()).hasSize(5);
    assertThat(redacted)
        .contains("Repository.java:42", "Service.java:7")
        .doesNotContain("hibernate", "Users", "private-work", "fixture-secret", "Call.d");
  }

  @Test
  void mismatchCannotLookLikeResolvedFindings() {
    String full =
        JsonReporter.toRunEnvelopeJson(
            AuditRunResult.pass(List.of(report())), ReportRedaction.FULL);
    String redacted = JsonReporter.toRunEnvelopeJson(AuditRunResult.pass(List.of(report())));
    var verdict = ReportComparator.compare(full, redacted);
    assertThat(verdict.outcome()).isEqualTo(AuditOutcome.INCONCLUSIVE);
    assertThat(verdict.incompleteReasons())
        .extracting(AuditIncompleteReason::code)
        .containsExactly(IncompleteReasonCode.REPORT_REDACTION_MISMATCH);
    assertThat(verdict.resolved()).isEmpty();
    assertThat(ReportComparator.exitCode(verdict)).isEqualTo(2);
    var sameMode = ReportComparator.compare(redacted, redacted);
    assertThat(sameMode.outcome()).isEqualTo(AuditOutcome.PASS);
    assertThat(sameMode.persisting()).hasSize(1);
    assertThat(
            ReportComparator.compare(redacted, redacted.replace("REDACTED", "future-mode"))
                .outcome())
        .isEqualTo(AuditOutcome.INCONCLUSIVE);
    assertThatThrownBy(
            () ->
                ReportComparator.compare(
                    redacted, redacted.replace("  \"redaction\": \"REDACTED\",\n", "")))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void verdictAndLegacyWritersDoNotReExposeFullInputDetails() {
    String full =
        JsonReporter.toRunEnvelopeJson(
            AuditRunResult.pass(List.of(report())), ReportRedaction.FULL);
    var verdict = ReportComparator.compare(full, full);
    assertThat(ReportComparator.toJson(verdict)).doesNotContain("fixture-secret", "private-work");
    assertThat(ReportComparator.toJson(verdict, ReportRedaction.FULL)).contains("fixture-secret");
    assertThat(JsonReporter.toEnvelopeJson(List.of(report()))).doesNotContain("fixture-secret");
    String incomplete =
        JsonReporter.toRunEnvelopeJson(
            AuditRunResult.inconclusive(
                List.of(),
                new AuditIncompleteReason(
                    IncompleteReasonCode.AUDIT_ANALYSIS_FAILED,
                    "fixture-secret at /private-work/path")));
    assertThat(incomplete)
        .doesNotContain("fixture-secret", "private-work")
        .contains("AUDIT_ANALYSIS_FAILED");
  }

  @Test
  void syntheticFindByIdEvidenceNeverContainsTheEntityId() {
    Issue issue =
        new Issue(
            IssueType.FIND_BY_ID_FOR_ASSOCIATION,
            Severity.INFO,
            "findById: com.example.Account#>fixture-secret",
            "Account",
            null,
            "Loaded fixture-secret",
            "getReferenceById(fixture-secret)");
    QueryAuditReport report =
        new QueryAuditReport(
            "AccountTest", "loadsAccount", List.of(), List.of(issue), List.of(), 0, 0, 0);
    String json = JsonReporter.toRunEnvelopeJson(AuditRunResult.pass(List.of(report)));
    assertThat(json).contains("findById(?)", "batch-fetch").doesNotContain("fixture-secret");
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    new GitHubActionsReporter(new PrintStream(bytes), null).report(report);
    assertThat(bytes.toString()).doesNotContain("fixture-secret");
  }

  @Test
  void githubAnnotationsAndSummaryUseTheSameSafeDefault(@TempDir Path temp) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    Path summary = temp.resolve("summary.md");
    new GitHubActionsReporter(new PrintStream(bytes), summary).report(report());
    assertThat(bytes.toString()).doesNotContain("fixture-secret", "884455", "private-work");
    assertThat(Files.readString(summary))
        .doesNotContain("fixture-secret", "884455", "private-work");
    assertThat(bytes.toString()).contains("missing-where-index", "account_id");
  }

  private static Map<?, ?> parse(String json) {
    return (Map<?, ?>) MiniJsonParser.parse(json);
  }

  private static QueryAuditReport report() {
    String sql =
        "SELECT * FROM accounts WHERE account_id = 884455 AND token = 'fixture-secret' /*"
            + " fixture-secret */";
    String stack =
        "at org.hibernate.Loader.load(Loader.java:17)\n"
            + "at com.example.AccountRepository.load(/Users/developer/private-work/AccountRepository.java:42)";
    Issue issue =
        new Issue(
            IssueType.MISSING_WHERE_INDEX,
            Severity.ERROR,
            sql,
            "accounts",
            "account_id",
            "Unquoted fixture-secret from " + sql,
            "Apply " + sql,
            stack);
    QueryRecord query = new QueryRecord(sql, sql, 120, 0, stack, 1);
    return new QueryAuditReport(
        "AccountTest",
        "loadsAccount",
        List.of(issue),
        List.of(issue),
        List.of(issue),
        List.of(query),
        1,
        1,
        120);
  }
}
