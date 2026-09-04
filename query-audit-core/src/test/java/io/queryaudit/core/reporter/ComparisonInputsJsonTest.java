package io.queryaudit.core.reporter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import io.queryaudit.core.model.AuditOutcome;
import io.queryaudit.core.model.AuditRunResult;
import io.queryaudit.core.model.IncompleteReasonCode;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.Severity;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ComparisonInputsJsonTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void inputsRoundTripByTestIdentityAndAreWrittenInStableOrder() {
    QueryAuditReport first = report("first", true);
    QueryAuditReport second = report("second", false);
    AuditRunResult run =
        AuditRunResult.pass(List.of(first, second))
            .withComparisonInputs(
                Map.of(
                    second.getTestId(), ComparisonInputFixtures.defaults(),
                    first.getTestId(), ComparisonInputFixtures.defaults()));
    String json = JsonReporter.toRunEnvelopeJson(run);
    Map<?, ?> envelope = (Map<?, ?>) MiniJsonParser.parse(json);

    assertThat(ComparisonInputsJson.read(envelope, true)).isEqualTo(run.comparisonInputs());
    assertThat(json).isEqualTo(JsonReporter.toRunEnvelopeJson(run));
    assertThat(((Map<?, ?>) envelope.get("comparisonInputs")).keySet().stream().toList())
        .isEqualTo(run.comparisonInputs().keySet().stream().sorted().toList());
  }

  @Test
  void unchangedKnownInputsAllowAResolvedFinding() {
    var verdict = ReportComparator.compare(knownReport(true), knownReport(false));

    assertThat(verdict.outcome()).isEqualTo(AuditOutcome.PASS);
    assertThat(verdict.resolved()).hasSize(1);
    assertThat(verdict.inputDifferences()).isEmpty();
  }

  @Test
  void weakerProfileOrChangedParserCannotResolveTheFinding() throws Exception {
    assertIncompatible("profile", "minimal");
    assertIncompatible("parser.name", "builtin");
    assertIncompatible("parser.version", "6.0");
    assertIncompatible("queryAuditVersion", "0.6.0");
    assertIncompatible("databaseDialect", "postgresql");
  }

  @Test
  void changedThresholdsAndPolicyFingerprintsCannotResolveTheFinding() throws Exception {
    for (String field :
        List.of(
            "ruleSettings", "thresholds", "suppressions", "queryContracts", "findingBaseline")) {
      assertIncompatible("fingerprints." + field, "0".repeat(64));
    }
  }

  @Test
  void missingMetadataCapabilityCannotResolveTheFinding() throws Exception {
    ObjectNode before = parse(knownReport(true));
    ObjectNode metadata = (ObjectNode) inputs(before).path("capabilities").path("indexMetadata");
    metadata.put("state", "AVAILABLE");
    metadata.put("source", "jdbc-provider");

    var verdict = ReportComparator.compare(before.toString(), knownReport(false));

    assertThat(verdict.outcome()).isEqualTo(AuditOutcome.INCONCLUSIVE);
    assertThat(verdict.resolved()).isEmpty();
    assertThat(verdict.inputDifferences())
        .anyMatch(d -> d.field().equals("capabilities.indexMetadata.state"));
  }

  @Test
  void equalUnverifiableCustomInputsDoNotBecomeTrustworthy() throws Exception {
    ObjectNode before = parse(knownReport(true));
    ObjectNode after = parse(knownReport(false));
    inputs(before).put("detectorInputsComplete", false);
    inputs(after).put("detectorInputsComplete", false);
    assertUnavailable(ReportComparator.compare(before.toString(), after.toString()));

    before = parse(knownReport(true));
    after = parse(knownReport(false));
    for (ObjectNode envelope : List.of(before, after)) {
      ObjectNode explain = (ObjectNode) inputs(envelope).path("capabilities").path("explain");
      explain.put("state", "AVAILABLE");
      explain.put("source", "custom-explain");
      explain.put("inputsComplete", false);
    }
    assertUnavailable(ReportComparator.compare(before.toString(), after.toString()));
  }

  @Test
  void absentMetadataOnBothSidesOrOneTestIsExplicitlyUnverified() {
    String before =
        JsonReporter.toRunEnvelopeJson(AuditRunResult.pass(List.of(report("orders", true))));
    String after =
        JsonReporter.toRunEnvelopeJson(AuditRunResult.pass(List.of(report("orders", false))));
    assertUnavailable(ReportComparator.compare(before, after));
    assertUnavailable(ReportComparator.compare(knownReport(true), after));
  }

  @Test
  void newTestsMustIdentifyTheirOwnInputsBeforeTheComparisonCanPass() throws Exception {
    QueryAuditReport added = report("new-test", false);
    ObjectNode candidate =
        parse(
            ComparisonInputFixtures.json(
                AuditRunResult.pass(List.of(report("orders", true), added))));
    var known = ReportComparator.compare(knownReport(true), candidate.toString());
    assertThat(known.outcome()).isEqualTo(AuditOutcome.PASS);
    assertThat(known.unexpectedTests()).hasSize(1);

    ObjectNode addedInputs = (ObjectNode) candidate.path("comparisonInputs").get(added.getTestId());
    addedInputs.put("detectorInputsComplete", false);
    assertUnavailable(ReportComparator.compare(knownReport(true), candidate.toString()));
    addedInputs.put("detectorInputsComplete", true);
    ObjectNode explain = (ObjectNode) addedInputs.path("capabilities").path("explain");
    explain.put("state", "AVAILABLE");
    explain.put("source", "custom-explain");
    explain.put("inputsComplete", false);
    assertUnavailable(ReportComparator.compare(knownReport(true), candidate.toString()));

    explain.put("state", "FAILED");
    var failed = ReportComparator.compare(knownReport(true), candidate.toString());
    assertUnavailable(failed);
    assertThat(failed.incompleteReasons())
        .anyMatch(reason -> reason.code() == IncompleteReasonCode.CAPABILITY_EXECUTION_FAILED);
  }

  @Test
  void legacyReportsCannotClaimVerifiedInputs() throws Exception {
    ObjectNode before = parse(knownReport(true));
    ObjectNode after = parse(knownReport(false));
    for (ObjectNode envelope : List.of(before, after)) {
      envelope.put("schemaVersion", "1.5.0");
    }
    assertUnavailable(ReportComparator.compare(before.toString(), after.toString()));
    before.remove("comparisonInputs");
    after.remove("comparisonInputs");
    assertUnavailable(ReportComparator.compare(before.toString(), after.toString()));
  }

  @Test
  void unknownOrMalformedMetadataIsRejectedWithoutEchoingItsContents() throws Exception {
    ObjectNode candidate = parse(knownReport(false));
    ((ObjectNode) inputs(candidate).path("parser")).put("version", "unknown");
    assertThatThrownBy(() -> ReportComparator.compare(candidate.toString(), candidate.toString()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("unverifiable metadata");

    inputs(candidate).put("privateConfig", "/private/tmp/fixture-secret.sql");
    assertThatThrownBy(() -> ReportComparator.compare(candidate.toString(), candidate.toString()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageNotContaining("fixture-secret");
  }

  @Test
  void differenceDetailsNeverCopyRawPolicyTextOrPaths() throws Exception {
    ObjectNode candidate = parse(knownReport(false));
    inputs(candidate).put("databaseDialect", "/private/tmp/fixture-secret.sql");
    ObjectNode parser = (ObjectNode) inputs(candidate).path("parser");
    parser.put("name", "SELECT 'fixture-secret' FROM credentials");
    var verdict = ReportComparator.compare(knownReport(true), candidate.toString());
    String json = ReportComparator.toJson(verdict);

    assertThat(json).contains("inputDifferences", "databaseDialect", "parser.name", "sha256:");
    assertThat(json).doesNotContain("fixture-secret", "/private/tmp", "SELECT", "credentials");

    Map<String, io.queryaudit.core.provenance.ComparisonInputs> unsafeInputs =
        ComparisonInputsJson.read((Map<?, ?>) MiniJsonParser.parse(candidate.toString()), true);
    String safeReport =
        JsonReporter.toRunEnvelopeJson(
            AuditRunResult.pass(List.of(report("orders", false)))
                .withComparisonInputs(unsafeInputs));
    assertThat(safeReport).contains("sha256:");
    assertThat(safeReport).doesNotContain("fixture-secret", "/private/tmp", "credentials");
  }

  @Test
  void unsafeIdentityHashingDoesNotCollapseMalformedUnicode() {
    assertThat(ComparisonInputsJson.fingerprint("/private/\uD800"))
        .isNotEqualTo(ComparisonInputsJson.fingerprint("/private/\uD801"))
        .isNotEqualTo(ComparisonInputsJson.fingerprint("/private/?"));
  }

  @Test
  void currentSchemaAcceptsKnownOrUnavailableInputsAndRejectsIncompleteEntries() throws Exception {
    var schema =
        JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7)
            .getSchema(getClass().getResourceAsStream("/report-1.6.schema.json"));
    ObjectNode verified = parse(knownReport(true));
    assertThat(schema.validate(verified)).isEmpty();
    ObjectNode unverified = verified.deepCopy();
    unverified.putObject("comparisonInputs");
    assertThat(schema.validate(unverified)).isEmpty();
    unverified.remove("comparisonInputs");
    assertThat(schema.validate(unverified)).isNotEmpty();

    ObjectNode missingFingerprint = verified.deepCopy();
    ((ObjectNode) inputs(missingFingerprint).path("fingerprints")).remove("thresholds");
    assertThat(schema.validate(missingFingerprint)).isNotEmpty();
    ObjectNode invalidCapability = verified.deepCopy();
    ((ObjectNode) inputs(invalidCapability).path("capabilities").path("explain"))
        .put("state", "MAYBE");
    assertThat(schema.validate(invalidCapability)).isNotEmpty();
  }

  private static void assertIncompatible(String field, String value) throws Exception {
    ObjectNode candidate = parse(knownReport(false));
    String[] path = field.split("\\.");
    ObjectNode owner = inputs(candidate);
    for (int index = 0; index < path.length - 1; index++) {
      owner = (ObjectNode) owner.get(path[index]);
    }
    owner.put(path[path.length - 1], value);

    var verdict = ReportComparator.compare(knownReport(true), candidate.toString());

    assertThat(verdict.outcome()).as(field).isEqualTo(AuditOutcome.INCONCLUSIVE);
    assertThat(verdict.resolved()).as(field).isEmpty();
    assertThat(verdict.incompleteReasons())
        .anyMatch(r -> r.code() == IncompleteReasonCode.INCOMPATIBLE_AUDIT_INPUTS);
    assertThat(verdict.inputDifferences()).anyMatch(d -> d.field().equals(field));
  }

  private static void assertUnavailable(ReportComparator.Verdict verdict) {
    assertThat(verdict.outcome()).isEqualTo(AuditOutcome.INCONCLUSIVE);
    assertThat(verdict.resolved()).isEmpty();
    assertThat(verdict.incompleteReasons())
        .anyMatch(r -> r.code() == IncompleteReasonCode.COMPARISON_INPUTS_UNAVAILABLE);
    assertThat(verdict.inputDifferences()).isNotEmpty();
  }

  private static String knownReport(boolean finding) {
    return ComparisonInputFixtures.json(AuditRunResult.pass(List.of(report("orders", finding))));
  }

  private static ObjectNode parse(String json) throws Exception {
    return (ObjectNode) MAPPER.readTree(json);
  }

  private static ObjectNode inputs(ObjectNode envelope) {
    return (ObjectNode) envelope.path("comparisonInputs").elements().next();
  }

  private static QueryAuditReport report(String name, boolean finding) {
    List<Issue> issues =
        finding
            ? List.of(
                new Issue(
                    IssueType.SELECT_ALL,
                    Severity.WARNING,
                    "SELECT * FROM orders",
                    "orders",
                    null,
                    "detail",
                    "suggestion"))
            : List.of();
    return new QueryAuditReport("example.OrderTest", name, issues, List.of(), List.of(), 1, 1, 0L);
  }
}
