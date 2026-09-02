package io.queryaudit.core.reporter;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.AuditIncompleteReason;
import io.queryaudit.core.model.AuditOutcome;
import io.queryaudit.core.model.AuditRunResult;
import io.queryaudit.core.model.IncompleteReasonCode;
import io.queryaudit.core.model.IndexInfo;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JsonReporterTest {

  // ------------------------------------------------------------------
  // Helper
  // ------------------------------------------------------------------

  private String generateJson(QueryAuditReport report) {
    JsonReporter reporter = new JsonReporter();
    reporter.report(report);
    String json = reporter.getJson();
    assertThat(json).isNotNull();
    return json;
  }

  // ------------------------------------------------------------------
  // Tests
  // ------------------------------------------------------------------

  @Test
  void reportContainsAllTopLevelFields() {
    Issue error =
        new Issue(
            IssueType.N_PLUS_ONE,
            Severity.ERROR,
            "select * from order_items where order_id = ?",
            "order_items",
            null,
            "Repeated query pattern detected 5 times",
            "Use JOIN FETCH or @EntityGraph");

    Issue info =
        new Issue(
            IssueType.SELECT_ALL,
            Severity.INFO,
            "select * from users",
            "users",
            null,
            "SELECT * usage",
            "Specify only needed columns");

    QueryRecord query =
        new QueryRecord("SELECT * FROM users WHERE id = 1", 1_234_567L, 0L, "at com.example.Test");

    QueryAuditReport report =
        new QueryAuditReport(
            "OrderServiceTest",
            "findOrders",
            List.of(error),
            List.of(info),
            List.of(),
            List.of(query),
            5,
            12,
            45_000_000L);

    String json = generateJson(report);

    // Top-level fields
    assertThat(json).contains("\"testClass\": \"OrderServiceTest\"");
    assertThat(json).contains("\"testName\": \"findOrders\"");

    // Summary
    assertThat(json).contains("\"summary\":");
    assertThat(json).contains("\"confirmedIssues\": 1");
    assertThat(json).contains("\"infoIssues\": 1");
    assertThat(json).contains("\"acknowledgedIssues\": 0");
    assertThat(json).contains("\"uniquePatterns\": 5");
    assertThat(json).contains("\"totalQueries\": 12");
    assertThat(json).contains("\"executionTimeMs\": 45");

    // Arrays present
    assertThat(json).contains("\"confirmedIssues\": [");
    assertThat(json).contains("\"infoIssues\": [");
    assertThat(json).contains("\"acknowledgedIssues\": []");
    assertThat(json).contains("\"queries\": [");
  }

  @Test
  void issueFieldsArePresent() {
    Issue error =
        new Issue(
            IssueType.N_PLUS_ONE,
            Severity.ERROR,
            "select * from order_items where order_id = ?",
            "order_items",
            "order_id",
            "Repeated query pattern",
            "Use JOIN FETCH");

    QueryAuditReport report =
        new QueryAuditReport(
            "TestClass", "testMethod", List.of(error), List.of(), List.of(), List.of(), 1, 1, 0L);

    String json = generateJson(report);

    assertThat(json).contains("\"type\": \"n-plus-one\"");
    assertThat(json).contains("\"severity\": \"ERROR\"");
    assertThat(json).contains("\"query\": \"select * from order_items where order_id = ?\"");
    assertThat(json).contains("\"table\": \"order_items\"");
    assertThat(json).contains("\"column\": \"order_id\"");
    assertThat(json).contains("\"detail\": \"Repeated query pattern\"");
    assertThat(json).contains("\"suggestion\": \"Use JOIN FETCH\"");
  }

  @Test
  void nullColumnIsJsonNull() {
    Issue issue =
        new Issue(
            IssueType.SELECT_ALL,
            Severity.INFO,
            "select * from users",
            "users",
            null,
            "SELECT * usage",
            "Specify columns");

    QueryAuditReport report =
        new QueryAuditReport("TC", "tm", List.of(), List.of(issue), List.of(), List.of(), 1, 1, 0L);

    String json = generateJson(report);

    assertThat(json).contains("\"column\": null");
  }

  @Test
  void queryRecordFieldsArePresent() {
    QueryRecord query =
        new QueryRecord(
            "SELECT id FROM users", 9_876_543L, 0L, "at com.example.Foo.bar(Foo.java:42)");

    QueryAuditReport report =
        new QueryAuditReport("TC", "tm", List.of(), List.of(), List.of(), List.of(query), 1, 1, 0L);

    String json = generateJson(report);

    assertThat(json).contains("\"sql\": \"SELECT id FROM users\"");
    assertThat(json).contains("\"normalizedSql\":");
    assertThat(json).contains("\"executionTimeNanos\": 9876543");
    assertThat(json).contains("\"stackTrace\": \"at com.example.Foo.bar(Foo.java:42)\"");
  }

  @Test
  void emptyReportProducesValidJson() {
    QueryAuditReport report =
        new QueryAuditReport("TC", "tm", List.of(), List.of(), List.of(), List.of(), 0, 0, 0L);

    String json = generateJson(report);

    // Must start and end with braces
    assertThat(json.trim()).startsWith("{");
    assertThat(json.trim()).endsWith("}");

    // Empty arrays
    assertThat(json).contains("\"confirmedIssues\": []");
    assertThat(json).contains("\"infoIssues\": []");
    assertThat(json).contains("\"acknowledgedIssues\": []");
    assertThat(json).contains("\"queries\": []");

    // Summary zeroes
    assertThat(json).contains("\"confirmedIssues\": 0");
    assertThat(json).contains("\"infoIssues\": 0");
    assertThat(json).contains("\"totalQueries\": 0");
  }

  @Test
  void specialCharactersInSqlAreEscaped() {
    String sqlWithQuotes = "SELECT * FROM users WHERE name = \"O'Brien\"";
    String detailWithBackslash = "Path: C:\\temp\\data";
    String suggestionWithNewline = "Use:\nJOIN FETCH";

    Issue issue =
        new Issue(
            IssueType.SELECT_ALL,
            Severity.INFO,
            sqlWithQuotes,
            "users",
            null,
            detailWithBackslash,
            suggestionWithNewline);

    QueryAuditReport report =
        new QueryAuditReport("TC", "tm", List.of(), List.of(issue), List.of(), List.of(), 1, 1, 0L);

    String json = generateJson(report);

    // Double quotes in value must be escaped
    assertThat(json).contains("\\\"O'Brien\\\"");
    // Backslashes must be escaped
    assertThat(json).contains("C:\\\\temp\\\\data");
    // Newlines must be escaped
    assertThat(json).contains("Use:\\nJOIN FETCH");

    // Verify no unescaped control characters
    // (after removing the known escaped sequences, no raw control chars should remain in values)
    assertThat(json).doesNotContain("\n\"");
  }

  @Test
  void escapeJsonHandlesAllControlCharacters() {
    assertThat(JsonReporter.escapeJson("tab\there")).isEqualTo("tab\\there");
    assertThat(JsonReporter.escapeJson("line\nbreak")).isEqualTo("line\\nbreak");
    assertThat(JsonReporter.escapeJson("cr\rreturn")).isEqualTo("cr\\rreturn");
    assertThat(JsonReporter.escapeJson("back\\slash")).isEqualTo("back\\\\slash");
    assertThat(JsonReporter.escapeJson("quote\"mark")).isEqualTo("quote\\\"mark");
    assertThat(JsonReporter.escapeJson("slash/path")).isEqualTo("slash\\/path");
    assertThat(JsonReporter.escapeJson(null)).isNull();
  }

  @Test
  void nullTestClassIsJsonNull() {
    QueryAuditReport report =
        new QueryAuditReport("testName", List.of(), List.of(), List.of(), 0, 0, 0L);

    String json = generateJson(report);

    assertThat(json).contains("\"testClass\": null");
  }

  @Test
  void multipleIssuesAndQueriesProduceValidJson() {
    Issue issue1 = new Issue(IssueType.N_PLUS_ONE, Severity.ERROR, "sql1", "t1", null, "d1", "s1");
    Issue issue2 = new Issue(IssueType.OR_ABUSE, Severity.WARNING, "sql2", "t2", "c2", "d2", "s2");

    QueryRecord q1 = new QueryRecord("SELECT 1", 100L, 0L, "stack1");
    QueryRecord q2 = new QueryRecord("SELECT 2", 200L, 0L, "stack2");

    QueryAuditReport report =
        new QueryAuditReport(
            "TC", "tm", List.of(issue1, issue2), List.of(), List.of(), List.of(q1, q2), 2, 2, 300L);

    String json = generateJson(report);

    // Count opening braces for issues
    long issueObjectCount = json.chars().filter(c -> c == '{').count();
    // 1 root + 1 summary + 2 issues + 1 remediation (n-plus-one only) + 2 queries = 7
    assertThat(issueObjectCount).isEqualTo(7);

    assertThat(json).contains("\"type\": \"n-plus-one\"");
    assertThat(json).contains("\"type\": \"or-abuse\"");
  }

  @Test
  void toJsonStaticMethodWorks() {
    QueryAuditReport report =
        new QueryAuditReport("TC", "tm", List.of(), List.of(), List.of(), List.of(), 0, 0, 0L);

    String json = JsonReporter.toJson(report);

    assertThat(json).startsWith("{");
    assertThat(json).endsWith("}");
    assertThat(json).contains("\"testClass\": \"TC\"");
  }

  // ------------------------------------------------------------------
  // Schema contract (issue #165)
  // ------------------------------------------------------------------

  @Test
  void sourceLocationIsSerialized() {
    Issue withLocation =
        new Issue(
            IssueType.N_PLUS_ONE,
            Severity.ERROR,
            "sql",
            "orders",
            null,
            "d",
            "s",
            "com.example.OrderService.load:42");
    QueryAuditReport report =
        new QueryAuditReport(
            "TC", "tm", List.of(withLocation), List.of(), List.of(), List.of(), 1, 1, 0L);

    assertThat(generateJson(report))
        .contains("\"sourceLocation\": \"com.example.OrderService.load:42\"");
  }

  @Test
  void remediationEmittedForMappedTypesOnly() {
    Issue missingIndex =
        new Issue(
            IssueType.MISSING_WHERE_INDEX, Severity.ERROR, "sql", "orders", "user_id", "d", "s");
    Issue unmapped =
        new Issue(IssueType.WHERE_FUNCTION, Severity.ERROR, "sql", "orders", "email", "d", "s");
    Issue batchUpdate =
        new Issue(
            IssueType.REPEATED_SINGLE_UPDATE, Severity.WARNING, "sql", "orders", null, "d", "s");
    QueryAuditReport report =
        new QueryAuditReport(
            "TC",
            "tm",
            List.of(missingIndex, unmapped, batchUpdate),
            List.of(),
            List.of(),
            List.of(),
            3,
            3,
            0L);

    String json = generateJson(report);

    assertThat(json)
        .contains(
            "\"remediation\": {\"kind\": \"add-index\", \"table\": \"orders\","
                + " \"columns\": [\"user_id\"]}");
    assertThat(json)
        .contains("\"remediation\": {\"kind\": \"batch-update\", \"table\": \"orders\"}");
    assertThat(json.split("\"remediation\"", -1)).hasSize(3); // exactly two remediations
  }

  @Test
  void indexMetadataNullWhenNotCollected() {
    QueryAuditReport report =
        new QueryAuditReport("TC", "tm", List.of(), List.of(), List.of(), List.of(), 0, 0, 0L);

    assertThat(generateJson(report)).contains("\"indexMetadata\": null");
  }

  @Test
  void indexMetadataTrimmedToFindingTablesAndGroupedByIndex() {
    Issue onOrders =
        new Issue(IssueType.MISSING_WHERE_INDEX, Severity.ERROR, "sql", "orders", "a", "d", "s");
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "orders",
                List.of(
                    new IndexInfo("orders", "idx_ab", "a", 1, true, 10L),
                    new IndexInfo("orders", "idx_ab", "b", 2, true, 500L)),
                "users",
                List.of(new IndexInfo("users", "pk", "id", 1, false, 99L))));
    QueryAuditReport report =
        new QueryAuditReport(
                "TC", "tm", List.of(onOrders), List.of(), List.of(), List.of(), 1, 1, 0L)
            .withIndexMetadata(metadata);

    String json = generateJson(report);

    assertThat(json)
        .contains(
            "\"orders\": [{\"name\": \"idx_ab\", \"unique\": false,"
                + " \"columns\": [\"a\", \"b\"], \"cardinality\": 500}]");
    assertThat(json).doesNotContain("\"users\""); // no finding references it
  }

  @Test
  void envelopeCarriesSchemaVersionOutcomeAndReports() {
    QueryAuditReport r1 =
        new QueryAuditReport("TC", "first", List.of(), List.of(), List.of(), List.of(), 0, 0, 0L);
    QueryAuditReport r2 =
        new QueryAuditReport("TC", "second", List.of(), List.of(), List.of(), List.of(), 0, 0, 0L);

    String json = JsonReporter.toRunEnvelopeJson(AuditRunResult.pass(List.of(r1, r2)));

    assertThat(json).startsWith("{");
    assertThat(json).endsWith("}");
    assertThat(json).contains("\"schemaVersion\": \"" + JsonReporter.SCHEMA_VERSION + "\"");
    assertThat(json).contains("\"outcome\": \"PASS\"");
    assertThat(json).contains("\"incompleteReasons\": []");
    assertThat(json).contains("\"reports\": [");
    assertThat(json).contains("\"testName\": \"first\"");
    assertThat(json).contains("\"testName\": \"second\"");
  }

  @Test
  @SuppressWarnings("deprecation")
  void listOnlyEnvelopeRetainsTheLegacyShape() {
    String json = JsonReporter.toEnvelopeJson(List.of());

    assertThat(json).contains("\"schemaVersion\": \"1.0.0\"");
    assertThat(json).doesNotContain("\"outcome\"").doesNotContain("\"incompleteReasons\"");
  }

  @Test
  void envelopeSerializesEveryIncompleteReason() {
    AuditRunResult result =
        new AuditRunResult(
            List.of(),
            AuditOutcome.INCONCLUSIVE,
            Arrays.stream(IncompleteReasonCode.values()).map(AuditIncompleteReason::of).toList());

    String json = JsonReporter.toRunEnvelopeJson(result);

    assertThat(json).contains("\"outcome\": \"INCONCLUSIVE\"");
    for (IncompleteReasonCode code : IncompleteReasonCode.values()) {
      assertThat(json).contains("\"code\": \"" + code + "\"");
    }
  }

  @Test
  void withIndexMetadataCopiesFieldsAndKeepsNullAsSameInstance() {
    QueryAuditReport report =
        new QueryAuditReport("TC", "tm", List.of(), List.of(), List.of(), List.of(), 3, 7, 42L);

    assertThat(report.withIndexMetadata(null)).isSameAs(report);

    IndexMetadata metadata = new IndexMetadata(Map.of());
    QueryAuditReport copy = report.withIndexMetadata(metadata);
    assertThat(copy.getIndexMetadata()).isSameAs(metadata);
    assertThat(copy.getTestClass()).isEqualTo("TC");
    assertThat(copy.getTestName()).isEqualTo("tm");
    assertThat(copy.getUniquePatternCount()).isEqualTo(3);
    assertThat(copy.getTotalQueryCount()).isEqualTo(7);
    assertThat(copy.getTotalExecutionTimeNanos()).isEqualTo(42L);
  }
}
