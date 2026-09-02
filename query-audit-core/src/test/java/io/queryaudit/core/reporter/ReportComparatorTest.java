package io.queryaudit.core.reporter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

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
    return JsonReporter.toEnvelopeJson(List.of(reports));
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

      ReportComparator.Verdict verdict =
          ReportComparator.compare(
              envelope(report("findOrders", List.of(finding), 11)), envelope());

      assertThat(verdict.resolved()).isEmpty();
      assertThat(verdict.newFindings()).isEmpty();
      assertThat(verdict.persisting()).isEmpty();
      assertThat(verdict.missingTests())
          .containsExactly(new ReportComparator.TestRef("OrderServiceTest", "findOrders"));
      assertThat(verdict.complete()).isFalse();
      assertThat(ReportComparator.exitCode(verdict)).isEqualTo(2);
    }

    @Test
    @DisplayName("is incomplete when a clean audited test is missing")
    void missingCleanTestIsIncomplete() {
      ReportComparator.Verdict verdict =
          ReportComparator.compare(envelope(report("findOrders", List.of(), 3)), envelope());

      assertThat(verdict.resolved()).isEmpty();
      assertThat(verdict.missingTests())
          .containsExactly(new ReportComparator.TestRef("OrderServiceTest", "findOrders"));
      assertThat(verdict.complete()).isFalse();
      assertThat(ReportComparator.exitCode(verdict)).isEqualTo(2);
    }

    @Test
    @DisplayName("keeps partial finding deltas while a missing test makes the verdict incomplete")
    void mixedCompleteAndMissingTests() {
      Issue unverified = nPlusOne("select * from order_items where order_id = ?", "S.load:10");
      Issue fixed = nPlusOne("select * from payments where order_id = ?", "S.pay:20");
      Issue introduced = nPlusOne("select * from refunds where order_id = ?", "S.refund:30");

      String before =
          envelope(
              report("missingOrders", List.of(unverified), 11),
              report("findPayments", List.of(fixed), 5));
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
          .containsExactly(new ReportComparator.TestRef("OrderServiceTest", "missingOrders"));
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
      String compatibleEnvelope = "{\"schemaVersion\":\"1.42.7\",\"reports\":[]}";

      ReportComparator.Verdict verdict =
          ReportComparator.compare(compatibleEnvelope, compatibleEnvelope);

      assertThat(verdict.complete()).isTrue();
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
    @DisplayName("rejects an unsupported schema major version")
    void rejectsUnsupportedSchemaVersion() {
      assertThatThrownBy(
              () ->
                  ReportComparator.compare(
                      "{\"schemaVersion\":\"999.0.0\",\"reports\":[]}", envelope()))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("unsupported schemaVersion")
          .hasMessageContaining("1.x");
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
  @DisplayName("verdict rendering")
  class Rendering {

    @Test
    @DisplayName("verdict.json parses back and carries the classification")
    void verdictJsonRoundTrips() {
      Issue introduced = nPlusOne("select * from refunds where order_id = ?", "S.refund:30");
      String before = envelope(report("findOrders", List.of(), 5));
      String after = envelope(report("findOrders", List.of(introduced), 9));

      ReportComparator.Verdict verdict = ReportComparator.compare(before, after);
      String json = ReportComparator.toJson(verdict);

      Map<?, ?> parsed = (Map<?, ?>) MiniJsonParser.parse(json);
      assertThat((List<?>) parsed.get("newFindings")).hasSize(1);
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
      ReportComparator.Verdict verdict =
          ReportComparator.compare(envelope(report("findOrders", List.of(finding), 5)), envelope());

      Map<?, ?> parsed = (Map<?, ?>) MiniJsonParser.parse(ReportComparator.toJson(verdict));
      assertThat(parsed.get("complete")).isEqualTo(Boolean.FALSE);
      assertThat((List<?>) parsed.get("resolved")).isEmpty();
      assertThat((List<?>) parsed.get("missingTests"))
          .singleElement()
          .satisfies(
              entry -> {
                Map<?, ?> missing = (Map<?, ?>) entry;
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
