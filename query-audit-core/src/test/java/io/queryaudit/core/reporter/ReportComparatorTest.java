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
    }

    @Test
    @DisplayName("rejects a pre-envelope (bare array) report with a versioning hint")
    void rejectsBareArray() {
      assertThatThrownBy(() -> ReportComparator.compare("[]", "[]"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("envelope");
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
      Map<?, ?> delta = (Map<?, ?>) parsed.get("queryCountDelta");
      assertThat(delta.get("before")).isEqualTo(5L);
      assertThat(delta.get("after")).isEqualTo(9L);
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
