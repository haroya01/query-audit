package io.queryaudit.core.reporter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
import io.queryaudit.core.model.TestSelector;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Round-trip tests for the delta verdict (issue #167): envelopes are produced by the real {@link
 * JsonReporter} writer and read back through the real parser, so writer and reader can never
 * silently drift apart.
 */
@DisplayName("ReportComparator (issue #167)")
class ReportComparatorTest {

  private static Issue nPlusOne(String pattern, String location) {
    return new Issue(
        IssueType.N_PLUS_ONE,
        Severity.ERROR,
        pattern,
        "order_items",
        null,
        "Query repeated 5 times",
        "Batch it",
        location);
  }

  private static String envelope(QueryAuditReport... reports) {
    return JsonReporter.toRunEnvelopeJson(
        AuditRunResult.pass(List.of(reports)), ReportRedaction.FULL);
  }

  private static String envelopeWithRawReport(String report) {
    return "{\"schemaVersion\":\"1.0.0\",\"reports\":[" + report + "]}";
  }

  private static void assertInvalidReport(String report, String expectedMessage) {
    assertThatThrownBy(() -> ReportComparator.compare(envelope(), envelopeWithRawReport(report)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(expectedMessage);
  }

  private static QueryAuditReport report(String testName, List<Issue> confirmed, int queries) {
    return new QueryAuditReport(
        "OrderServiceTest",
        testName,
        confirmed,
        List.of(),
        List.of(),
        List.of(new QueryRecord("SELECT 1", 2_000_000L, 0L, "at T.m:1")),
        1,
        queries,
        2_000_000L);
  }

  private static QueryAuditReport reportWithIdentity(
      String testClass, String testName, String testId, List<Issue> confirmed, int queries) {
    return new QueryAuditReport(
            testClass,
            testName,
            confirmed,
            List.of(),
            List.of(),
            List.of(new QueryRecord("SELECT 1", 2_000_000L, 0L, "at T.m:1")),
            1,
            queries,
            2_000_000L)
        .withTestIdentity(testId, new TestSelector("junit-unique-id", testId));
  }

  private static String legacyEnvelope(QueryAuditReport report) {
    return "{\"schemaVersion\":\"1.0.0\",\"reports\":[" + reportWithoutIdentity(report) + "]}";
  }

  private static String reportWithoutIdentity(QueryAuditReport report) {
    String json = JsonReporter.toJson(report, ReportRedaction.FULL);
    String withoutTestId = "{\n" + json.substring(json.indexOf("  \"testClass\""));
    return withoutTestId.replace("  \"testSelector\": null,\n", "");
  }

  @Nested
  @DisplayName("MiniJsonParser")
  class Parser {

    @Test
    @DisplayName("parses objects, arrays, escapes, numbers, booleans, null")
    void parsesFullGrammar() {
      Object parsed =
          MiniJsonParser.parse(
              "{\"s\": \"a\\\"b\\\\c\\nd\\u0041\", \"n\": -3, \"d\": 1.5e2,"
                  + " \"b\": true, \"x\": null, \"arr\": [1, {\"k\": []}, false]}");
      Map<?, ?> map = (Map<?, ?>) parsed;
      assertThat(map.get("s")).isEqualTo("a\"b\\c\ndA");
      assertThat(map.get("n")).isEqualTo(-3L);
      assertThat(map.get("d")).isEqualTo(150.0);
      assertThat(map.get("b")).isEqualTo(Boolean.TRUE);
      assertThat(map.containsKey("x")).isTrue();
      assertThat(map.get("x")).isNull();
      assertThat((List<?>) map.get("arr")).hasSize(3);
    }

    @Test
    @DisplayName("rejects trailing content and truncated input")
    void rejectsMalformed() {
      assertThatThrownBy(() -> MiniJsonParser.parse("{} extra"))
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> MiniJsonParser.parse("{\"a\": "))
          .isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Nested
  @DisplayName("compare()")
  class Compare {

    @Test
    @DisplayName("classifies resolved, new, and persisting findings by stable key")
    void classifiesFindings() {
      Issue stays = nPlusOne("select * from order_items where order_id = ?", "S.load:10");
      Issue fixed = nPlusOne("select * from payments where order_id = ?", "S.pay:20");
      Issue introduced = nPlusOne("select * from refunds where order_id = ?", "S.refund:30");

      String before = envelope(report("findOrders", List.of(stays, fixed), 11));
      String after = envelope(report("findOrders", List.of(stays, introduced), 7));

      ReportComparator.Verdict verdict = ReportComparator.compare(before, after);

      assertThat(verdict.resolved()).hasSize(1);
      assertThat(verdict.resolved().get(0).detail()).isEqualTo("Query repeated 5 times");
      assertThat(verdict.newFindings()).hasSize(1);
      assertThat(verdict.persisting()).hasSize(1);
      assertThat(verdict.queriesBefore()).isEqualTo(11);
      assertThat(verdict.queriesAfter()).isEqualTo(7);
      assertThat(verdict.executionTimeMsBefore()).isEqualTo(2);
      assertThat(verdict.complete()).isTrue();
      assertThat(ReportComparator.exitCode(verdict)).isEqualTo(1);
    }

    @Test
    @DisplayName("stable test identity survives a display-name change")
    void displayNameChangeDoesNotSplitTheFinding() {
      String testId = "[engine:junit-jupiter]/[class:com.example.OrderTest]/[method:findOrders()]";
      Issue finding = nPlusOne("select * from order_items where order_id = ?", "S.load:10");

      ReportComparator.Verdict verdict =
          ReportComparator.compare(
              envelope(
                  reportWithIdentity(
                      "OrderTest", "loads recent orders", testId, List.of(finding), 5)),
              envelope(
                  reportWithIdentity(
                      "OrderTest", "recent order query", testId, List.of(finding), 5)));

      assertThat(verdict.persisting()).hasSize(1);
      assertThat(verdict.newFindings()).isEmpty();
      assertThat(verdict.resolved()).isEmpty();
      assertThat(verdict.missingTests()).isEmpty();
    }

    @Test
    @DisplayName("duplicate display names remain different tests")
    void stableIdsDistinguishDuplicateDisplayNames() {
      Issue finding = nPlusOne("select * from order_items where order_id = ?", "S.load:10");
      QueryAuditReport alpha =
          reportWithIdentity(
              "OrderTest",
              "duplicate",
              "[engine:junit-jupiter]/[class:com.alpha.OrderTest]/[method:load()]",
              List.of(finding),
              5);
      QueryAuditReport beta =
          reportWithIdentity(
              "OrderTest",
              "duplicate",
              "[engine:junit-jupiter]/[class:com.beta.OrderTest]/[method:load()]",
              List.of(finding),
              5);

      ReportComparator.Verdict verdict = ReportComparator.compare(envelope(alpha), envelope(beta));

      assertThat(verdict.newFindings()).hasSize(1);
      assertThat(verdict.resolved()).isEmpty();
      assertThat(verdict.persisting()).isEmpty();
      assertThat(verdict.missingTests())
          .containsExactly(
              new ReportComparator.TestRef(
                  "[engine:junit-jupiter]/[class:com.alpha.OrderTest]/[method:load()]",
                  "OrderTest",
                  "duplicate"));
    }

    @Test
    @DisplayName("a 0.5 report matches one new report through its exact legacy identity")
    void legacyReportHasAnExactCompatibilityPath() {
      Issue finding = nPlusOne("select * from order_items where order_id = ?", "S.load:10");
      QueryAuditReport oldReport = report("findOrders", List.of(finding), 5);
      QueryAuditReport newReport =
          reportWithIdentity(
              "OrderServiceTest",
              "findOrders",
              "[engine:junit-jupiter]/[class:example.OrderServiceTest]/[method:findOrders()]",
              List.of(finding),
              5);

      ReportComparator.Verdict verdict =
          ReportComparator.compare(legacyEnvelope(oldReport), envelope(newReport));

      assertThat(verdict.persisting()).hasSize(1);
      assertThat(verdict.newFindings()).isEmpty();
      assertThat(verdict.resolved()).isEmpty();
      assertThat(verdict.outcome()).isEqualTo(AuditOutcome.INCONCLUSIVE);
      assertThat(verdict.incompleteReasons())
          .extracting(reason -> reason.code())
          .containsExactly(IncompleteReasonCode.UNSUPPORTED_SCHEMA);
    }

    @Test
    @DisplayName("a schema 1.1 report uses the legacy identity fallback")
    void outcomeEnvelopeWithoutStableIdentityHasACompatibilityPath() {
      Issue finding = nPlusOne("select * from order_items where order_id = ?", "S.load:10");
      QueryAuditReport oldReport = report("findOrders", List.of(finding), 5);
      QueryAuditReport newReport =
          reportWithIdentity(
              "OrderServiceTest",
              "findOrders",
              "[engine:junit-jupiter]/[class:example.OrderServiceTest]/[method:findOrders()]",
              List.of(finding),
              5);
      String schema11 =
          "{\"schemaVersion\":\"1.1.0\",\"outcome\":\"PASS\","
              + "\"incompleteReasons\":[],\"reports\":["
              + reportWithoutIdentity(oldReport)
              + "]}";

      ReportComparator.Verdict verdict = ReportComparator.compare(schema11, envelope(newReport));

      assertThat(verdict.persisting()).hasSize(1);
      assertThat(verdict.newFindings()).isEmpty();
      assertThat(verdict.resolved()).isEmpty();
      assertThat(verdict.outcome()).isEqualTo(AuditOutcome.PASS);
    }

    @Test
    @DisplayName("an ambiguous legacy identity fails with a migration diagnostic")
    void ambiguousLegacyIdentityIsRejected() {
      QueryAuditReport oldReport = report("duplicate", List.of(), 1);
      QueryAuditReport first =
          reportWithIdentity("OrderServiceTest", "duplicate", "junit:first", List.of(), 1);
      QueryAuditReport second =
          reportWithIdentity("OrderServiceTest", "duplicate", "junit:second", List.of(), 1);

      assertThatThrownBy(
              () -> ReportComparator.compare(legacyEnvelope(oldReport), envelope(first, second)))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("legacy test identity is ambiguous")
          .hasMessageContaining("regenerate the 0.5 report");
    }

    @Test
    @DisplayName("duplicate stable IDs are rejected")
    void duplicateStableIdsAreRejected() {
      QueryAuditReport first =
          reportWithIdentity("OrderServiceTest", "first", "junit:same", List.of(), 1);
      QueryAuditReport second =
          reportWithIdentity("OrderServiceTest", "second", "junit:same", List.of(), 1);

      assertThatThrownBy(() -> ReportComparator.compare(envelope(first, second), envelope()))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("duplicate testId junit:same");
    }

    @Test
    @DisplayName("keeps a finding when only its source line changes")
    void ignoresSourceLineChanges() {
      Issue beforeFinding =
          nPlusOne(
              "select * from order_items where order_id = ?",
              "com.example.OrderService.load:42\ncom.example.OrderController.list:18");
      Issue afterFinding =
          nPlusOne(
              "select * from order_items where order_id = ?",
              "com.example.OrderService.load:43\ncom.example.OrderController.list:21");

      ReportComparator.Verdict verdict =
          ReportComparator.compare(
              envelope(report("findOrders", List.of(beforeFinding), 5)),
              envelope(report("findOrders", List.of(afterFinding), 5)));

      assertThat(verdict.persisting()).hasSize(1);
      assertThat(verdict.resolved()).isEmpty();
      assertThat(verdict.newFindings()).isEmpty();
    }

    @Test
    @DisplayName("keeps different source methods as different findings")
    void distinguishesDifferentSourceMethods() {
      Issue beforeFinding =
          nPlusOne(
              "select * from order_items where order_id = ?", "com.example.OrderService.load:42");
      Issue afterFinding =
          nPlusOne(
              "select * from order_items where order_id = ?",
              "com.example.OrderService.refresh:42");

      ReportComparator.Verdict verdict =
          ReportComparator.compare(
              envelope(report("findOrders", List.of(beforeFinding), 5)),
              envelope(report("findOrders", List.of(afterFinding), 5)));

      assertThat(verdict.persisting()).isEmpty();
      assertThat(verdict.resolved()).hasSize(1);
      assertThat(verdict.newFindings()).hasSize(1);
    }

    @Test
    @DisplayName("a clean fix: resolved findings, nothing new — the loop's success signal")
    void cleanFix() {
      Issue finding = nPlusOne("select * from order_items where order_id = ?", "S.load:10");
      String before = envelope(report("findOrders", List.of(finding), 11));
      String after = envelope(report("findOrders", List.of(), 7));

      ReportComparator.Verdict verdict = ReportComparator.compare(before, after);

      assertThat(verdict.newFindings()).isEmpty();
      assertThat(verdict.resolved()).hasSize(1);
      assertThat(verdict.persisting()).isEmpty();
      assertThat(verdict.missingTests()).isEmpty();
      assertThat(verdict.complete()).isTrue();
      assertThat(ReportComparator.exitCode(verdict)).isZero();
    }

    @Test
    @DisplayName("does not resolve findings when their audited test is missing")
    void missingFindingProducerIsIncomplete() {
      Issue finding = nPlusOne("select * from order_items where order_id = ?", "S.load:10");
      QueryAuditReport baseline = report("findOrders", List.of(finding), 11);

      ReportComparator.Verdict verdict = ReportComparator.compare(envelope(baseline), envelope());

      assertThat(verdict.resolved()).isEmpty();
      assertThat(verdict.newFindings()).isEmpty();
      assertThat(verdict.persisting()).isEmpty();
      assertThat(verdict.missingTests())
          .containsExactly(
              new ReportComparator.TestRef(baseline.getTestId(), "OrderServiceTest", "findOrders"));
      assertThat(verdict.incompleteReasons())
          .extracting(reason -> reason.code())
          .containsExactly(IncompleteReasonCode.EXPECTED_TEST_MISSING);
      assertThat(verdict.complete()).isFalse();
      assertThat(ReportComparator.exitCode(verdict)).isEqualTo(2);
    }

    @Test
    @DisplayName("is incomplete when a clean audited test is missing")
    void missingCleanTestIsIncomplete() {
      QueryAuditReport baseline = report("findOrders", List.of(), 3);
      ReportComparator.Verdict verdict = ReportComparator.compare(envelope(baseline), envelope());

      assertThat(verdict.resolved()).isEmpty();
      assertThat(verdict.missingTests())
          .containsExactly(
              new ReportComparator.TestRef(baseline.getTestId(), "OrderServiceTest", "findOrders"));
      assertThat(verdict.complete()).isFalse();
      assertThat(ReportComparator.exitCode(verdict)).isEqualTo(2);
    }

    @Test
    @DisplayName("keeps partial finding deltas while a missing test makes the verdict incomplete")
    void mixedCompleteAndMissingTests() {
      Issue unverified = nPlusOne("select * from order_items where order_id = ?", "S.load:10");
      Issue fixed = nPlusOne("select * from payments where order_id = ?", "S.pay:20");
      Issue introduced = nPlusOne("select * from refunds where order_id = ?", "S.refund:30");
      QueryAuditReport missing = report("missingOrders", List.of(unverified), 11);

      String before = envelope(missing, report("findPayments", List.of(fixed), 5));
      String after =
          envelope(
              report("findPayments", List.of(), 3), report("findRefunds", List.of(introduced), 7));

      ReportComparator.Verdict verdict = ReportComparator.compare(before, after);

      assertThat(verdict.resolved())
          .extracting(ReportComparator.Finding::testName)
          .containsExactly("findPayments");
      assertThat(verdict.newFindings())
          .extracting(ReportComparator.Finding::testName)
          .containsExactly("findRefunds");
      assertThat(verdict.missingTests())
          .containsExactly(
              new ReportComparator.TestRef(
                  missing.getTestId(), "OrderServiceTest", "missingOrders"));
      assertThat(verdict.complete()).isFalse();
      assertThat(ReportComparator.exitCode(verdict)).isEqualTo(2);
    }

    @Test
    @DisplayName("rejects a pre-envelope (bare array) report with a versioning hint")
    void rejectsBareArray() {
      assertThatThrownBy(() -> ReportComparator.compare("[]", "[]"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("envelope");
    }

    @Test
    @DisplayName("accepts compatible 1.x schema versions")
    void acceptsCompatibleSchemaVersion() {
      String compatibleEnvelope =
          "{\"schemaVersion\":\"1.42.7\",\"redaction\":\"FULL\",\"outcome\":\"PASS\","
              + "\"incompleteReasons\":[],\"reports\":[]}";

      ReportComparator.Verdict verdict =
          ReportComparator.compare(compatibleEnvelope, compatibleEnvelope);

      assertThat(verdict.complete()).isTrue();
    }

    @Test
    @DisplayName("future outcome values produce a structured inconclusive verdict")
    void futureOutcomeIsInconclusive() {
      Issue introduced = nPlusOne("select * from refunds where order_id = ?", "S.refund:30");
      String futureEnvelope =
          "{\"schemaVersion\":\"1.42.7\",\"redaction\":\"FULL\",\"outcome\":\"SKIPPED\","
              + "\"incompleteReasons\":[],\"reports\":["
              + JsonReporter.toJson(
                  report("findOrders", List.of(introduced), 9), ReportRedaction.FULL)
              + "]}";

      ReportComparator.Verdict verdict =
          ReportComparator.compare(envelope(report("findOrders", List.of(), 5)), futureEnvelope);

      assertThat(verdict.outcome()).isEqualTo(AuditOutcome.INCONCLUSIVE);
      assertThat(verdict.newFindings()).hasSize(1);
      assertThat(verdict.queriesBefore()).isEqualTo(5);
      assertThat(verdict.queriesAfter()).isEqualTo(9);
      assertThat(verdict.incompleteReasons())
          .singleElement()
          .satisfies(
              reason -> {
                assertThat(reason.code()).isEqualTo(IncompleteReasonCode.UNSUPPORTED_SCHEMA);
                assertThat(reason.detail()).contains("unknown outcome 'SKIPPED'");
              });
    }

    @Test
    @DisplayName("future incomplete reason values produce a structured inconclusive verdict")
    void futureIncompleteReasonIsInconclusive() {
      Issue resolved = nPlusOne("select * from payments where order_id = ?", "S.pay:20");
      String futureEnvelope =
          "{\"schemaVersion\":\"1.42.7\",\"redaction\":\"FULL\",\"outcome\":\"INCONCLUSIVE\","
              + "\"incompleteReasons\":[{\"code\":\"FUTURE_REASON\",\"detail\":null}],"
              + "\"reports\":["
              + JsonReporter.toJson(
                  report("findOrders", List.of(resolved), 8), ReportRedaction.FULL)
              + "]}";

      ReportComparator.Verdict verdict =
          ReportComparator.compare(futureEnvelope, envelope(report("findOrders", List.of(), 3)));

      assertThat(verdict.outcome()).isEqualTo(AuditOutcome.INCONCLUSIVE);
      assertThat(verdict.resolved()).hasSize(1);
      assertThat(verdict.queriesBefore()).isEqualTo(8);
      assertThat(verdict.queriesAfter()).isEqualTo(3);
      assertThat(verdict.incompleteReasons())
          .singleElement()
          .satisfies(
              reason -> {
                assertThat(reason.code()).isEqualTo(IncompleteReasonCode.UNSUPPORTED_SCHEMA);
                assertThat(reason.detail()).contains("unknown incomplete reason 'FUTURE_REASON'");
              });
    }

    @Test
    @DisplayName("a legacy 1.0 input is inconclusive instead of being inferred as PASS")
    void legacyEnvelopeWithoutOutcomeIsInconclusive() {
      Issue finding = nPlusOne("select * from order_items where order_id = ?", "S.load:10");
      QueryAuditReport baselineReport = report("findOrders", List.of(finding), 11);
      String legacyEnvelope =
          "{\"schemaVersion\":\"1.0.0\",\"reports\":["
              + JsonReporter.toJson(baselineReport, ReportRedaction.FULL)
              + "]}";

      ReportComparator.Verdict verdict =
          ReportComparator.compare(legacyEnvelope, envelope(report("findOrders", List.of(), 7)));

      assertThat(verdict.resolved()).hasSize(1);
      assertThat(verdict.outcome()).isEqualTo(AuditOutcome.INCONCLUSIVE);
      assertThat(verdict.incompleteReasons())
          .extracting(reason -> reason.code())
          .containsExactly(IncompleteReasonCode.UNSUPPORTED_SCHEMA);
      assertThat(ReportComparator.exitCode(verdict)).isEqualTo(2);
    }

    @Test
    @DisplayName("an inconclusive candidate retains partial finding deltas")
    void inconclusiveCandidateRetainsPartialDeltas() {
      Issue fixed = nPlusOne("select * from payments where order_id = ?", "S.pay:20");
      Issue introduced = nPlusOne("select * from refunds where order_id = ?", "S.refund:30");
      String before = envelope(report("findPayments", List.of(fixed), 5));
      String after =
          JsonReporter.toRunEnvelopeJson(
              AuditRunResult.inconclusive(
                  List.of(
                      report("findPayments", List.of(), 3),
                      report("findRefunds", List.of(introduced), 7)),
                  new AuditIncompleteReason(
                      IncompleteReasonCode.QUERY_LIMIT_REACHED, "findRefunds retained 7 queries")),
              ReportRedaction.FULL);

      ReportComparator.Verdict verdict = ReportComparator.compare(before, after);

      assertThat(verdict.resolved()).hasSize(1);
      assertThat(verdict.newFindings()).hasSize(1);
      assertThat(verdict.outcome()).isEqualTo(AuditOutcome.INCONCLUSIVE);
      assertThat(verdict.incompleteReasons())
          .extracting(reason -> reason.code())
          .containsExactly(IncompleteReasonCode.QUERY_LIMIT_REACHED);
      assertThat(ReportComparator.exitCode(verdict)).isEqualTo(2);
    }

    @Test
    @DisplayName("a completed candidate policy failure remains FAIL without new findings")
    void candidateFailureIsNotHiddenByAnEmptyDelta() {
      QueryAuditReport unchanged = report("findOrders", List.of(), 3);

      ReportComparator.Verdict verdict =
          ReportComparator.compare(
              envelope(unchanged),
              JsonReporter.toRunEnvelopeJson(
                  AuditRunResult.fail(List.of(unchanged)), ReportRedaction.FULL));

      assertThat(verdict.newFindings()).isEmpty();
      assertThat(verdict.outcome()).isEqualTo(AuditOutcome.FAIL);
      assertThat(verdict.complete()).isTrue();
      assertThat(ReportComparator.exitCode(verdict)).isEqualTo(1);
    }

    @Test
    @DisplayName("rejects an envelope without a schema version")
    void rejectsMissingSchemaVersion() {
      assertThatThrownBy(() -> ReportComparator.compare("{\"reports\":[]}", envelope()))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("schemaVersion")
          .hasMessageContaining("required");
    }

    @Test
    @DisplayName("schema 1.1 requires explicit outcome fields")
    void rejectsMissingOutcomeFields() {
      assertThatThrownBy(
              () ->
                  ReportComparator.compare(
                      "{\"schemaVersion\":\"1.1.0\",\"reports\":[]}", envelope()))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("envelope.outcome is required");

      assertThatThrownBy(
              () ->
                  ReportComparator.compare(
                      "{\"schemaVersion\":\"1.1.0\",\"outcome\":\"PASS\"," + "\"reports\":[]}",
                      envelope()))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("envelope.incompleteReasons is required");
    }

    @Test
    @DisplayName("schema 1.2 requires stable identity fields")
    void currentSchemaRequiresStableIdentity() {
      QueryAuditReport report = report("findOrders", List.of(), 1);
      String withoutIdentity =
          "{\"schemaVersion\":\"1.2.0\",\"outcome\":\"PASS\","
              + "\"incompleteReasons\":[],\"reports\":["
              + reportWithoutIdentity(report)
              + "]}";

      assertThatThrownBy(() -> ReportComparator.compare(envelope(), withoutIdentity))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("reports[0].testId is required");

      String withoutSelector =
          JsonReporter.toRunEnvelopeJson(AuditRunResult.pass(List.of(report)), ReportRedaction.FULL)
              .replace("      \"testSelector\": null,\n", "");
      assertThatThrownBy(() -> ReportComparator.compare(envelope(), withoutSelector))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("reports[0].testSelector is required");
    }

    @Test
    @DisplayName("outcome and incomplete reasons must form a valid run result")
    void validatesOutcomeAndIncompleteReasons() {
      assertThatThrownBy(
              () ->
                  ReportComparator.compare(
                      "{\"schemaVersion\":\"1.1.0\",\"outcome\":\"PASS\","
                          + "\"incompleteReasons\":[{\"code\":\"QUERY_LIMIT_REACHED\","
                          + "\"detail\":null}],\"reports\":[]}",
                      envelope()))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("inconsistent")
          .hasMessageContaining("PASS must not carry");

      assertThatThrownBy(
              () ->
                  ReportComparator.compare(
                      "{\"schemaVersion\":\"1.1.0\",\"outcome\":\"INCONCLUSIVE\","
                          + "\"incompleteReasons\":[],\"reports\":[]}",
                      envelope()))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("requires at least one");
    }

    @Test
    @DisplayName("an unsupported schema major produces a structured inconclusive verdict")
    void unsupportedSchemaVersionIsInconclusive() {
      ReportComparator.Verdict verdict =
          ReportComparator.compare("{\"schemaVersion\":\"999.0.0\",\"reports\":[]}", envelope());

      assertThat(verdict.outcome()).isEqualTo(AuditOutcome.INCONCLUSIVE);
      assertThat(verdict.incompleteReasons())
          .singleElement()
          .satisfies(
              reason -> {
                assertThat(reason.code()).isEqualTo(IncompleteReasonCode.UNSUPPORTED_SCHEMA);
                assertThat(reason.detail()).contains("999.0.0").contains("1.x");
              });
      assertThat(ReportComparator.exitCode(verdict)).isEqualTo(2);
    }

    @Test
    @DisplayName("rejects a malformed schema version")
    void rejectsMalformedSchemaVersion() {
      assertThatThrownBy(
              () ->
                  ReportComparator.compare(
                      "{\"schemaVersion\":\"1.0\",\"reports\":[]}", envelope()))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("invalid schemaVersion")
          .hasMessageContaining("major.minor.patch");
    }

    @Test
    @DisplayName("does not treat a missing confirmedIssues array as a resolved finding")
    void rejectsMissingConfirmedIssues() {
      Issue finding = nPlusOne("select * from order_items where order_id = ?", "S.load:10");
      String baseline = envelope(report("findOrders", List.of(finding), 11));
      String candidate =
          envelopeWithRawReport(
              """
              {
                "testClass": "OrderServiceTest",
                "testName": "findOrders",
                "summary": {"totalQueries": 7, "executionTimeMs": 2}
              }
              """);

      assertThatThrownBy(() -> ReportComparator.compare(baseline, candidate))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("reports[0].confirmedIssues is required");
    }

    @Test
    @DisplayName("rejects non-object report entries")
    void rejectsNonObjectReportEntries() {
      assertInvalidReport("42", "reports[0] must be an object");
    }

    @Test
    @DisplayName("rejects non-object confirmed finding entries")
    void rejectsNonObjectFindingEntries() {
      assertInvalidReport(
          """
          {
            "testClass": "OrderServiceTest",
            "testName": "findOrders",
            "summary": {"totalQueries": 7, "executionTimeMs": 2},
            "confirmedIssues": [false]
          }
          """,
          "reports[0].confirmedIssues[0] must be an object");
    }

    @Test
    @DisplayName("requires correctly typed test identity fields")
    void validatesTestIdentity() {
      assertInvalidReport(
          """
          {
            "testName": "findOrders",
            "summary": {"totalQueries": 7, "executionTimeMs": 2},
            "confirmedIssues": []
          }
          """,
          "reports[0].testClass is required");
      assertInvalidReport(
          """
          {
            "testClass": "OrderServiceTest",
            "testName": false,
            "summary": {"totalQueries": 7, "executionTimeMs": 2},
            "confirmedIssues": []
          }
          """,
          "reports[0].testName must be a string");
    }

    @Test
    @DisplayName("requires integer summary values used by the verdict")
    void validatesSummaryValues() {
      assertInvalidReport(
          """
          {
            "testClass": "OrderServiceTest",
            "testName": "findOrders",
            "summary": {"totalQueries": "7", "executionTimeMs": 2},
            "confirmedIssues": []
          }
          """,
          "reports[0].summary.totalQueries must be an integer");
      assertInvalidReport(
          """
          {
            "testClass": "OrderServiceTest",
            "testName": "findOrders",
            "summary": {"totalQueries": 7},
            "confirmedIssues": []
          }
          """,
          "reports[0].summary.executionTimeMs is required");
    }

    @Test
    @DisplayName("requires correctly typed confirmed finding fields used by the verdict")
    void validatesFindingFields() {
      assertInvalidReport(
          """
          {
            "testClass": "OrderServiceTest",
            "testName": "findOrders",
            "summary": {"totalQueries": 7, "executionTimeMs": 2},
            "confirmedIssues": [{
              "query": "select * from order_items where order_id = ?",
              "sourceLocation": null,
              "table": "order_items",
              "detail": "Query repeated 5 times"
            }]
          }
          """,
          "reports[0].confirmedIssues[0].type is required");
      assertInvalidReport(
          """
          {
            "testClass": "OrderServiceTest",
            "testName": "findOrders",
            "summary": {"totalQueries": 7, "executionTimeMs": 2},
            "confirmedIssues": [{
              "type": "n-plus-one",
              "query": 7,
              "sourceLocation": null,
              "table": "order_items",
              "detail": "Query repeated 5 times"
            }]
          }
          """,
          "reports[0].confirmedIssues[0].query must be a string or null");
    }
  }

  @Nested
  @DisplayName("command-line exit contract")
  class CommandLine {

    @Test
    @DisplayName("a verdict write failure exits as inconclusive")
    void verdictWriteFailureUsesExitCodeTwo(@TempDir Path tempDir) throws IOException {
      Path before = tempDir.resolve("before.json");
      Path after = tempDir.resolve("after.json");
      Files.writeString(before, envelope());
      Files.writeString(after, envelope());
      Path blockedVerdict = Files.createDirectory(tempDir.resolve("verdict.json"));

      int exitCode =
          ReportComparator.run(
              new String[] {before.toString(), after.toString(), blockedVerdict.toString()});

      assertThat(exitCode).isEqualTo(2);
    }
  }

  @Nested
  @DisplayName("verdict rendering")
  class Rendering {

    @Test
    @DisplayName("verdict.json parses back and carries the classification")
    void verdictJsonRoundTrips() {
      Issue introduced = nPlusOne("select * from refunds where order_id = ?", "S.refund:30");
      String before = envelope(report("findOrders", List.of(), 5));
      QueryAuditReport candidate = report("findOrders", List.of(introduced), 9);
      String after = envelope(candidate);

      ReportComparator.Verdict verdict = ReportComparator.compare(before, after);
      String json = ReportComparator.toJson(verdict);

      Map<?, ?> parsed = (Map<?, ?>) MiniJsonParser.parse(json);
      assertThat(parsed.get("outcome")).isEqualTo("FAIL");
      assertThat((List<?>) parsed.get("incompleteReasons")).isEmpty();
      assertThat((List<?>) parsed.get("newFindings")).hasSize(1);
      assertThat((List<?>) parsed.get("newFindings"))
          .singleElement()
          .satisfies(
              finding ->
                  assertThat(((Map<?, ?>) finding).get("testId")).isEqualTo(candidate.getTestId()));
      assertThat((List<?>) parsed.get("resolved")).isEmpty();
      assertThat(parsed.get("complete")).isEqualTo(Boolean.TRUE);
      assertThat((List<?>) parsed.get("missingTests")).isEmpty();
      Map<?, ?> delta = (Map<?, ?>) parsed.get("queryCountDelta");
      assertThat(delta.get("before")).isEqualTo(5L);
      assertThat(delta.get("after")).isEqualTo(9L);
    }

    @Test
    @DisplayName("verdict.json and summary identify an incomplete comparison")
    void rendersMissingTests() {
      Issue finding = nPlusOne("select * from refunds where order_id = ?", "S.refund:30");
      QueryAuditReport baseline = report("findOrders", List.of(finding), 5);
      ReportComparator.Verdict verdict = ReportComparator.compare(envelope(baseline), envelope());

      Map<?, ?> parsed = (Map<?, ?>) MiniJsonParser.parse(ReportComparator.toJson(verdict));
      assertThat(parsed.get("outcome")).isEqualTo("INCONCLUSIVE");
      assertThat(parsed.get("complete")).isEqualTo(Boolean.FALSE);
      assertThat((List<?>) parsed.get("resolved")).isEmpty();
      assertThat((List<?>) parsed.get("incompleteReasons"))
          .singleElement()
          .satisfies(
              entry -> {
                Map<?, ?> reason = (Map<?, ?>) entry;
                assertThat(reason.get("code")).isEqualTo("EXPECTED_TEST_MISSING");
                assertThat(reason.get("detail")).isNull();
              });
      assertThat((List<?>) parsed.get("missingTests"))
          .singleElement()
          .satisfies(
              entry -> {
                Map<?, ?> missing = (Map<?, ?>) entry;
                assertThat(missing.get("testId")).isEqualTo(baseline.getTestId());
                assertThat(missing.get("testClass")).isEqualTo("OrderServiceTest");
                assertThat(missing.get("testName")).isEqualTo("findOrders");
              });

      String summary = ReportComparator.toSummary(verdict);
      assertThat(summary).contains("INCOMPLETE: 1 baseline test missing");
      assertThat(summary).contains("MISSING  OrderServiceTest.findOrders");
      assertThat(summary).doesNotContain("RESOLVED");
    }

    @Test
    @DisplayName("console summary names new and resolved findings")
    void summaryNamesFindings() {
      Issue finding = nPlusOne("select * from refunds where order_id = ?", "S.refund:30");
      ReportComparator.Verdict verdict =
          ReportComparator.compare(
              envelope(report("findOrders", List.of(finding), 5)),
              envelope(report("findOrders", List.of(), 5)));

      String summary = ReportComparator.toSummary(verdict);
      assertThat(summary).contains("0 new, 1 resolved, 0 persisting");
      assertThat(summary).contains("RESOLVED n-plus-one (table: order_items)");
    }
  }
}
