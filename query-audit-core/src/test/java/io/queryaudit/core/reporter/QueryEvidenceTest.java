package io.queryaudit.core.reporter;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import io.queryaudit.core.model.AuditOutcome;
import io.queryaudit.core.model.AuditRunResult;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryEvidenceStatus;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.model.TestSelector;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class QueryEvidenceTest {

  private final HtmlReportAggregator aggregator = HtmlReportAggregator.getInstance();

  @AfterEach
  void resetAggregator() {
    aggregator.reset();
    aggregator.setMaxInMemoryReports(HtmlReportAggregator.DEFAULT_MAX_IN_MEMORY_REPORTS);
  }

  @Test
  void preservesTheFirstTwoHundredReportsAndMarksTheNextOne() throws Exception {
    aggregator.reset();
    aggregator.setMaxInMemoryReports(200);
    for (int i = 0; i < 201; i++) {
      aggregator.addReport(report("test" + i));
    }

    QueryAuditReport lastRetained = aggregator.getReports().get(199);
    QueryAuditReport firstCompacted = aggregator.getReports().get(200);
    assertThat(lastRetained.getQueryEvidenceStatus()).isEqualTo(QueryEvidenceStatus.COMPLETE);
    assertThat(lastRetained.getRetainedQueryCount()).isEqualTo(2);
    assertThat(lastRetained.getOmittedQueryCount()).isZero();
    assertThat(firstCompacted.getQueryEvidenceStatus()).isEqualTo(QueryEvidenceStatus.OMITTED);
    assertThat(firstCompacted.getAllQueries()).isEmpty();
    assertThat(firstCompacted.getRetainedQueryCount()).isZero();
    assertThat(firstCompacted.getOmittedQueryCount()).isEqualTo(2);
    assertThat(firstCompacted.getTotalQueryCount()).isEqualTo(2);
    assertThat(firstCompacted.getConfirmedIssues()).hasSize(1);
    assertThat(firstCompacted.getTotalExecutionTimeNanos()).isEqualTo(300);
    assertThat(firstCompacted.getTestId()).isEqualTo("test200");
    assertThat(firstCompacted.getTestSelector()).isEqualTo(new TestSelector("test", "test200"));

    Map<?, ?> envelope =
        parse(JsonReporter.toRunEnvelopeJson(AuditRunResult.pass(aggregator.getReports())));
    List<?> reports = (List<?>) envelope.get("reports");
    assertThat(((Map<?, ?>) reports.get(199)).get("queryEvidence"))
        .isEqualTo(Map.of("status", "COMPLETE", "retainedQueries", 2L, "omittedQueries", 0L));
    assertThat(((Map<?, ?>) reports.get(200)).get("queryEvidence"))
        .isEqualTo(Map.of("status", "OMITTED", "retainedQueries", 0L, "omittedQueries", 2L));
  }

  @Test
  void distinguishesPartialEvidenceFromAZeroQueryTest() {
    QueryAuditReport partial =
        new QueryAuditReport(
            "Test",
            "partial",
            List.of(),
            List.of(),
            List.of(new QueryRecord("SELECT 1", 1, 0, null)),
            1,
            3,
            1);
    QueryAuditReport empty =
        new QueryAuditReport("Test", "empty", List.of(), List.of(), List.of(), 0, 0, 0);

    assertThat(partial.getQueryEvidenceStatus()).isEqualTo(QueryEvidenceStatus.PARTIAL);
    assertThat(partial.getRetainedQueryCount()).isEqualTo(1);
    assertThat(partial.getOmittedQueryCount()).isEqualTo(2);
    assertThat(empty.withoutQueryEvidence().getQueryEvidenceStatus())
        .isEqualTo(QueryEvidenceStatus.COMPLETE);
    assertThat(parse(JsonReporter.toJson(empty)).get("queryEvidence"))
        .isEqualTo(Map.of("status", "COMPLETE", "retainedQueries", 0L, "omittedQueries", 0L));
  }

  @Test
  void compactionDoesNotChangeTheVerdictOrFindingComparison() {
    QueryAuditReport full = report("orders");
    QueryAuditReport compact = full.withoutQueryEvidence();
    String before = ComparisonInputFixtures.json(AuditRunResult.pass(List.of(full)));
    String after = ComparisonInputFixtures.json(AuditRunResult.pass(List.of(compact)));

    assertThat(parse(after).get("outcome")).isEqualTo("PASS");
    ReportComparator.Verdict verdict = ReportComparator.compare(before, after);
    assertThat(verdict.outcome()).isEqualTo(AuditOutcome.PASS);
    assertThat(verdict.persisting()).hasSize(1);
    assertThat(verdict.resolved()).isEmpty();
    assertThat(verdict.queriesAfter()).isEqualTo(2);
    assertThat(
            parse(JsonReporter.toRunEnvelopeJson(AuditRunResult.fail(List.of(compact))))
                .get("outcome"))
        .isEqualTo("FAIL");
  }

  @Test
  void htmlExplainsMissingQueryRows(@TempDir Path output) throws Exception {
    new HtmlReporter().writeToFile(output, List.of(report("orders").withoutQueryEvidence()));
    try (var files = Files.walk(output)) {
      String pages =
          files
              .filter(p -> p.toString().endsWith(".html"))
              .map(
                  p -> {
                    try {
                      return Files.readString(p);
                    } catch (Exception e) {
                      throw new IllegalStateException(e);
                    }
                  })
              .reduce("", String::concat);
      assertThat(pages).contains("0 retained, 2 omitted from this report");
    }
  }

  @Test
  void currentSchemaAcceptsRetainedAndCompactedEvidenceAndRejectsContradictions() throws Exception {
    var mapper = new ObjectMapper();
    var schema =
        JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7)
            .getSchema(getClass().getResourceAsStream("/report-1.6.schema.json"));
    String full = JsonReporter.toRunEnvelopeJson(AuditRunResult.pass(List.of(report("full"))));
    String compact =
        JsonReporter.toRunEnvelopeJson(
            AuditRunResult.pass(List.of(report("compact").withoutQueryEvidence())));
    assertThat(schema.validate(mapper.readTree(full))).isEmpty();
    assertThat(schema.validate(mapper.readTree(compact))).isEmpty();
    assertThat(schema.validate(mapper.readTree(compact.replace("OMITTED", "COMPLETE"))))
        .isNotEmpty();
    assertThat(
            schema.validate(
                mapper.readTree(
                    compact.replace("\"omittedQueries\": 2", "\"omittedQueries\": -1"))))
        .isNotEmpty();
  }

  private static Map<?, ?> parse(String json) {
    return (Map<?, ?>) MiniJsonParser.parse(json);
  }

  private static QueryAuditReport report(String name) {
    Issue issue =
        new Issue(
            IssueType.SELECT_ALL,
            Severity.WARNING,
            "SELECT * FROM orders",
            "orders",
            null,
            "Explicit columns are easier to maintain",
            "List the required columns");
    return new QueryAuditReport(
            "OrderTest",
            name,
            List.of(issue),
            List.of(),
            List.of(),
            List.of(
                new QueryRecord("SELECT * FROM orders", 100, 0, null),
                new QueryRecord("SELECT 1", 200, 0, null)),
            2,
            2,
            300)
        .withTestIdentity(name, new TestSelector("test", name));
  }
}
