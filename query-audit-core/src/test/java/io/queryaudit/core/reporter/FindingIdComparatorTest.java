package io.queryaudit.core.reporter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.queryaudit.core.config.ReportRedaction;
import io.queryaudit.core.model.AuditOutcome;
import io.queryaudit.core.model.AuditRunResult;
import io.queryaudit.core.model.IncompleteReasonCode;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.model.TestSelector;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FindingIdComparatorTest {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final List<String> CATEGORIES =
      List.of("confirmedIssues", "infoIssues", "acknowledgedIssues");

  @Test
  void nativeIdsRoundTripToResolvedNewAndPersistingVerdictFindings() throws Exception {
    Issue persisting = issue("orders", "customer_id", "OrderService.load:10");
    Issue resolved = issue("payments", "order_id", "OrderService.pay:20");
    Issue introduced = issue("refunds", "order_id", "OrderService.refund:30");
    ObjectNode before = envelope(report("load", persisting, resolved));
    ObjectNode after = envelope(report("load", persisting, introduced));

    var verdict = compare(before, after);

    assertThat(verdict.findingIdentity())
        .isEqualTo(new ReportComparator.FindingIdentity("RECORDED", "1.7.0", "1.7.0"));
    assertThat(verdict.resolved())
        .singleElement()
        .satisfies(finding -> assertThat(finding.findingId()).isEqualTo(id(before, 0, 1)));
    assertThat(verdict.persisting())
        .singleElement()
        .satisfies(
            finding -> {
              assertThat(finding.findingId()).isEqualTo(id(after, 0, 0));
              assertThat(finding.column()).isEqualTo("customer_id");
            });
    assertThat(verdict.newFindings())
        .singleElement()
        .satisfies(finding -> assertThat(finding.findingId()).isEqualTo(id(after, 0, 1)));
    JsonNode serialized = JSON.readTree(ReportComparator.toJson(verdict));
    assertThat(serialized.path("resolved").get(0).path("findingId").asText())
        .isEqualTo(id(before, 0, 1));
    assertThat(serialized.path("persisting").get(0).path("column").asText())
        .isEqualTo("customer_id");
    assertThat(serialized.path("findingIdentity").path("mode").asText()).isEqualTo("RECORDED");
    assertThat(serialized.path("findingIdentity").path("baselineSchemaVersion").asText())
        .isEqualTo("1.7.0");
  }

  @Test
  void recordedIdsAreNotReconstructedFromRedactedDisplayFields() throws Exception {
    ObjectNode before =
        envelope(
            ReportRedaction.REDACTED,
            report("load", issue("orders", "customer_id", "OrderService.load:10")));
    ObjectNode after = before.deepCopy();
    ObjectNode finding = finding(after, "confirmedIssues", 0);
    finding.put("query", "select ?");
    finding.putNull("sourceLocation");
    finding.put("table", "display_only");
    finding.put("column", "display_only");
    finding.put("detail", "Reformatted diagnostic");

    var verdict = compare(before, after);

    assertThat(verdict.persisting()).hasSize(1);
    assertThat(verdict.persisting().get(0).findingId()).isEqualTo(id(before, 0, 0));
    assertThat(verdict.newFindings()).isEmpty();
    assertThat(verdict.resolved()).isEmpty();
  }

  @Test
  void redactedColumnsCannotCollapseTwoDifferentNativeFindings() throws Exception {
    Issue owner = issue("orders", "\"owner_id\"", "OrderService.load:10");
    Issue tenant = issue("orders", "\"tenant_id\"", "OrderService.load:10");
    ObjectNode before = envelope(ReportRedaction.REDACTED, report("load", owner, tenant));
    ObjectNode after = envelope(ReportRedaction.REDACTED, report("load", tenant));

    assertThat(finding(before, "confirmedIssues", 0).path("column").asText()).isEqualTo("?");
    assertThat(finding(before, "confirmedIssues", 1).path("column").asText()).isEqualTo("?");
    assertThat(id(before, 0, 0)).isNotEqualTo(id(before, 0, 1));

    var verdict = compare(before, after);

    assertThat(verdict.findingIdentity().mode()).isEqualTo("RECORDED");
    assertThat(verdict.resolved())
        .singleElement()
        .satisfies(finding -> assertThat(finding.findingId()).isEqualTo(id(before, 0, 0)));
    assertThat(verdict.persisting())
        .singleElement()
        .satisfies(finding -> assertThat(finding.findingId()).isEqualTo(id(before, 0, 1)));
    assertThat(verdict.newFindings()).isEmpty();
  }

  @Test
  void aDifferentColumnOrSourceMethodIsANewNativeFinding() throws Exception {
    ObjectNode before =
        envelope(report("load", issue("orders", "customer_id", "OrderService.load:10")));
    for (Issue changed :
        List.of(
            issue("orders", "account_id", "OrderService.load:10"),
            issue("orders", "customer_id", "OrderService.refresh:10"))) {
      ObjectNode after = envelope(report("load", changed));
      var verdict = compare(before, after);

      assertThat(id(after, 0, 0)).isNotEqualTo(id(before, 0, 0));
      assertThat(verdict.persisting()).isEmpty();
      assertThat(verdict.newFindings()).hasSize(1);
      assertThat(verdict.resolved()).hasSize(1);
    }
  }

  @Test
  void sourceLineChangesPreserveTheRecordedFinding() throws Exception {
    ObjectNode before = envelope(report("load", issue("orders", "id", "OrderService.load:10")));
    ObjectNode after = envelope(report("load", issue("orders", "id", "OrderService.load:90")));

    var verdict = compare(before, after);

    assertThat(id(after, 0, 0)).isEqualTo(id(before, 0, 0));
    assertThat(verdict.persisting()).hasSize(1);
    assertThat(verdict.resolved()).isEmpty();
    assertThat(verdict.newFindings()).isEmpty();
  }

  @Test
  void identicalRecordedIdsCannotMoveAResolutionAcrossTests() throws Exception {
    Issue issue = issue("orders", "id", "OrderService.load:10");
    ObjectNode before = envelope(report("alpha", issue), report("beta"));
    ObjectNode after = envelope(report("alpha"), report("beta", issue));
    ((ObjectNode) after.path("reports").get(1).path("confirmedIssues").get(0))
        .put("findingId", id(before, 0, 0));

    var verdict = compare(before, after);

    assertThat(verdict.persisting()).isEmpty();
    assertThat(verdict.resolved())
        .extracting(ReportComparator.Finding::testName)
        .containsExactly("alpha");
    assertThat(verdict.newFindings())
        .extracting(ReportComparator.Finding::testName)
        .containsExactly("beta");
    assertThat(verdict.outcome()).isEqualTo(AuditOutcome.FAIL);
  }

  @Test
  void theSameIdInDifferentTestsIsNotADuplicateWithinATest() throws Exception {
    Issue issue = issue("orders", "id", "OrderService.load:10");
    ObjectNode document = envelope(report("alpha", issue), report("beta", issue));
    ((ObjectNode) document.path("reports").get(1).path("confirmedIssues").get(0))
        .put("findingId", id(document, 0, 0));

    assertThat(compare(document, document).persisting()).hasSize(2);
  }

  @ParameterizedTest
  @ValueSource(strings = {"confirmedIssues", "infoIssues", "acknowledgedIssues"})
  void rejectsDuplicateIdsWithinOrAcrossCategories(String category) throws Exception {
    ObjectNode document = envelope(report("load", issue("orders", "id", "OrderService.load:10")));
    ObjectNode duplicate = finding(document, "confirmedIssues", 0).deepCopy();
    ((ArrayNode) document.path("reports").get(0).path(category)).add(duplicate);

    assertThatThrownBy(() -> compare(document, document))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("findingId duplicates");
  }

  @ParameterizedTest
  @ValueSource(strings = {"confirmedIssues", "infoIssues", "acknowledgedIssues"})
  void modernFindingsRequireAnIdAndNullableColumnInEveryCategory(String category) throws Exception {
    ObjectNode document = envelope(report("load", issue("orders", "id", "OrderService.load:10")));
    ObjectNode issue = finding(document, "confirmedIssues", 0).deepCopy();
    ((ArrayNode) document.path("reports").get(0).path("confirmedIssues")).removeAll();
    ((ArrayNode) document.path("reports").get(0).path(category)).add(issue);
    for (String field : List.of("findingId", "column")) {
      ObjectNode broken = document.deepCopy();
      finding(broken, category, 0).remove(field);
      assertThatThrownBy(() -> compare(broken, broken))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining(field + " is required");
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "",
        "sha256:abc",
        "qa-finding-v1:abc",
        "qa-finding-v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
      })
  void malformedIdsDoNotBecomeAComparison(String invalidId) throws Exception {
    ObjectNode document = envelope(report("load", issue("orders", "id", "OrderService.load:10")));
    finding(document, "confirmedIssues", 0).put("findingId", invalidId);

    assertThatThrownBy(() -> compare(document, document))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("findingId must use");
  }

  @Test
  void nullOrNonStringIdsAreInvalid() throws Exception {
    ObjectNode document = envelope(report("load", issue("orders", "id", "OrderService.load:10")));
    finding(document, "confirmedIssues", 0).putNull("findingId");
    assertThatThrownBy(() -> compare(document, document))
        .hasMessageContaining("findingId must be a string");
    finding(document, "confirmedIssues", 0).put("findingId", 7);
    assertThatThrownBy(() -> compare(document, document))
        .hasMessageContaining("findingId must be a string");
  }

  @Test
  void anUnknownAlgorithmReturnsAnExplicitUnavailableVerdict() throws Exception {
    ObjectNode document = envelope(report("load", issue("orders", "id", "OrderService.load:10")));
    finding(document, "confirmedIssues", 0).put("findingId", "qa-finding-v2:" + "a".repeat(64));

    var verdict = compare(document, document);

    assertThat(verdict.outcome()).isEqualTo(AuditOutcome.INCONCLUSIVE);
    assertThat(verdict.incompleteReasons())
        .extracting(reason -> reason.code())
        .containsExactly(IncompleteReasonCode.UNSUPPORTED_SCHEMA);
    assertThat(verdict.findingIdentity())
        .isEqualTo(new ReportComparator.FindingIdentity("UNAVAILABLE", null, null));
    assertThat(verdict.resolved()).isEmpty();
  }

  @Test
  void compatibleFutureMinorSchemasStillUseRecordedIds() throws Exception {
    ObjectNode document = envelope(report("load", issue("orders", "id", "OrderService.load:10")));
    document.put("schemaVersion", "1.42.7");

    var verdict = compare(document, document);

    assertThat(verdict.complete()).isTrue();
    assertThat(verdict.findingIdentity())
        .isEqualTo(new ReportComparator.FindingIdentity("RECORDED", "1.42.7", "1.42.7"));
    assertThat(verdict.persisting()).hasSize(1);
  }

  @Test
  void mixedSchemasUseLegacyKeysOnBothSidesAndRetainOnlyRecordedIds() throws Exception {
    ObjectNode modern = envelope(report("load", issue("orders", "id", "OrderService.load:10")));
    ObjectNode legacy = legacy(modern);
    finding(modern, "confirmedIssues", 0).put("sourceLocation", "OrderService.load:99");

    var forward = compare(legacy, modern);
    var backward = compare(modern, legacy);

    assertThat(forward.findingIdentity())
        .isEqualTo(new ReportComparator.FindingIdentity("LEGACY", "1.6.0", "1.7.0"));
    assertThat(forward.persisting())
        .singleElement()
        .satisfies(finding -> assertThat(finding.findingId()).isEqualTo(id(modern, 0, 0)));
    assertThat(backward.findingIdentity())
        .isEqualTo(new ReportComparator.FindingIdentity("LEGACY", "1.7.0", "1.6.0"));
    assertThat(backward.persisting())
        .singleElement()
        .satisfies(finding -> assertThat(finding.findingId()).isNull());
    assertThat(forward.newFindings()).isEmpty();
    assertThat(backward.newFindings()).isEmpty();
    JsonNode serialized = JSON.readTree(ReportComparator.toJson(backward));
    assertThat(serialized.path("findingIdentity").path("mode").asText()).isEqualTo("LEGACY");
    assertThat(serialized.path("persisting").get(0).path("findingId").isNull()).isTrue();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void rejectsAmbiguousLegacyMappingInEitherDirection(boolean modernFirst, @TempDir Path directory)
      throws Exception {
    Issue owner = issue("orders", "\"owner_id\"", "OrderService.load:10");
    Issue tenant = issue("orders", "\"tenant_id\"", "OrderService.load:10");
    ObjectNode modern = envelope(ReportRedaction.REDACTED, report("load", owner, tenant));
    ObjectNode legacy = legacy(envelope(ReportRedaction.REDACTED, report("load", owner)));
    ObjectNode before = modernFirst ? modern : legacy;
    ObjectNode after = modernFirst ? legacy : modern;

    assertThat(id(modern, 0, 0)).isNotEqualTo(id(modern, 0, 1));
    assertThat(finding(modern, "confirmedIssues", 0).path("column").asText()).isEqualTo("?");
    assertThat(finding(modern, "confirmedIssues", 1).path("column").asText()).isEqualTo("?");
    assertThatThrownBy(() -> compare(before, after))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "legacy finding identity is ambiguous; regenerate both reports with schema 1.7 or later");

    Path baseline = directory.resolve("baseline.json");
    Path candidate = directory.resolve("candidate.json");
    Path verdict = directory.resolve("verdict.json");
    Files.writeString(baseline, before.toString());
    Files.writeString(candidate, after.toString());
    assertThat(
            ReportComparator.run(
                new String[] {baseline.toString(), candidate.toString(), verdict.toString()}))
        .isEqualTo(2);
    assertThat(verdict).doesNotExist();
  }

  @Test
  void legacyDuplicateObservationsRemainSupported() throws Exception {
    ObjectNode modern = envelope(report("load", issue("orders", "id", "OrderService.load:10")));
    ObjectNode legacy = legacy(modern);
    ((ArrayNode) legacy.path("reports").get(0).path("confirmedIssues"))
        .add(finding(legacy, "confirmedIssues", 0).deepCopy());

    assertThat(compare(legacy, modern).persisting()).hasSize(1);
    assertThat(compare(modern, legacy).persisting()).hasSize(2);
    assertThat(compare(legacy, legacy).persisting()).hasSize(2);
  }

  @Test
  void legacyKeysDistinguishTablesColumnsAndNullValues() throws Exception {
    ObjectNode before =
        legacy(envelope(report("load", issue("orders", null, "OrderService.load:10"))));
    for (String field : List.of("table", "column")) {
      ObjectNode after = before.deepCopy();
      finding(after, "confirmedIssues", 0).put(field, "null");
      var verdict = compare(before, after);
      assertThat(verdict.findingIdentity().mode()).isEqualTo("LEGACY");
      assertThat(verdict.persisting()).isEmpty();
      assertThat(verdict.resolved()).hasSize(1);
      assertThat(verdict.newFindings()).hasSize(1);
    }
  }

  @Test
  void legacyColumnIsOptionalButMustBeStringOrNullWhenPresent() throws Exception {
    ObjectNode document =
        legacy(envelope(report("load", issue("orders", null, "OrderService.load:10"))));
    finding(document, "confirmedIssues", 0).remove("column");
    assertThat(compare(document, document).persisting()).hasSize(1);
    finding(document, "confirmedIssues", 0).put("column", 42);
    assertThatThrownBy(() -> compare(document, document))
        .hasMessageContaining("column must be a string or null");
  }

  @Test
  void aLegacyEnvelopeCannotPresentAnUnvalidatedFieldAsANativeId() throws Exception {
    ObjectNode document =
        legacy(envelope(report("load", issue("orders", null, "OrderService.load:10"))));
    finding(document, "confirmedIssues", 0).put("findingId", "not-a-recorded-id");

    var verdict = compare(document, document);

    assertThat(verdict.findingIdentity().mode()).isEqualTo("LEGACY");
    assertThat(verdict.persisting().get(0).findingId()).isNull();
    assertThat(ReportComparator.toJson(verdict)).doesNotContain("not-a-recorded-id");
  }

  @Test
  void oldPublicConstructorsExplicitlyLackFindingIdentityMetadata() throws Exception {
    var finding =
        new ReportComparator.Finding(
            "test-id", "OrderTest", "load", "N_PLUS_ONE", "orders", "detail", "key");
    var older =
        new ReportComparator.Finding("OrderTest", "load", "N_PLUS_ONE", "orders", "detail", "key");
    var verdict =
        new ReportComparator.Verdict(
            List.of(),
            List.of(),
            List.of(finding),
            1,
            1,
            0,
            0,
            List.of(),
            AuditOutcome.PASS,
            List.of(),
            List.of(),
            List.of());

    assertThat(finding.key()).isEqualTo("key");
    assertThat(finding.findingId()).isNull();
    assertThat(finding.column()).isNull();
    assertThat(older.testId()).isNull();
    assertThat(verdict.findingIdentity().mode()).isEqualTo("UNAVAILABLE");
    JsonNode serialized = JSON.readTree(ReportComparator.toJson(verdict));
    assertThat(serialized.path("findingIdentity").path("baselineSchemaVersion").isNull()).isTrue();
    assertThat(serialized.path("persisting").get(0).path("findingId").isNull()).isTrue();
  }

  private static Issue issue(String table, String column, String source) {
    return new Issue(
        IssueType.N_PLUS_ONE,
        Severity.ERROR,
        "select * from orders where customer_id = ?",
        table,
        column,
        "Repeated query",
        "Batch it",
        source);
  }

  private static QueryAuditReport report(String name, Issue... issues) {
    return new QueryAuditReport(
            "OrderTest", name, List.of(issues), List.of(), List.of(), List.of(), 1, 3, 0)
        .withTestIdentity("test:" + name, new TestSelector("junit-unique-id", "test:" + name));
  }

  private static ObjectNode envelope(QueryAuditReport... reports) throws Exception {
    return envelope(ReportRedaction.FULL, reports);
  }

  private static ObjectNode envelope(ReportRedaction redaction, QueryAuditReport... reports)
      throws Exception {
    return (ObjectNode)
        JSON.readTree(
            ComparisonInputFixtures.json(AuditRunResult.pass(List.of(reports)), redaction));
  }

  private static ObjectNode legacy(ObjectNode document) {
    ObjectNode legacy = document.deepCopy();
    legacy.put("schemaVersion", "1.6.0");
    for (JsonNode report : legacy.path("reports")) {
      for (String category : CATEGORIES) {
        for (JsonNode finding : report.path(category)) {
          ((ObjectNode) finding).remove("findingId");
        }
      }
    }
    return legacy;
  }

  private static ObjectNode finding(ObjectNode document, String category, int index) {
    return (ObjectNode) document.path("reports").get(0).path(category).get(index);
  }

  private static String id(ObjectNode document, int report, int finding) {
    return document
        .path("reports")
        .get(report)
        .path("confirmedIssues")
        .get(finding)
        .path("findingId")
        .asText();
  }

  private static ReportComparator.Verdict compare(ObjectNode before, ObjectNode after) {
    return ReportComparator.compare(before.toString(), after.toString());
  }
}
