package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.detector.QueryAuditAnalyzer;
import io.queryaudit.core.extension.FindingKindId;
import io.queryaudit.core.model.Finding;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.provenance.AuditCapability;
import io.queryaudit.core.provenance.ComparisonInputs;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
class FindingFailurePolicyTest {
  private static final Finding CUSTOM = finding("shop:budget", Severity.ERROR);
  private static final Finding OTHER = finding("shop:other", Severity.WARNING);
  private static final Issue BUILTIN =
      new Issue(
          IssueType.SELECT_ALL,
          Severity.WARNING,
          "SELECT * FROM users",
          null,
          null,
          "detail",
          null);

  static Stream<Arguments> selections() {
    return Stream.of(
        Arguments.of("all", List.of("select-all", "shop:budget", "shop:other")),
        Arguments.of("custom", List.of("shop:budget")),
        Arguments.of("legacy", List.of("select-all")),
        Arguments.of("builtinString", List.of("select-all")),
        Arguments.of("mixed", List.of("select-all", "shop:budget")),
        Arguments.of("other", List.of("shop:other")));
  }

  @ParameterizedTest
  @MethodSource("selections")
  void enumAndKindSelectionsFormAUnionWithoutChangingReportEvidence(
      String method, List<String> selected) throws Exception {
    QueryAuditReport report = report();
    assertThat(AuditAssertions.failableFindings(report, annotation(method)))
        .extracting(finding -> finding.kindId().value())
        .containsExactlyElementsOf(selected);
    assertThat(report.getFindings().confirmed())
        .containsExactly(Finding.fromIssue(BUILTIN), CUSTOM, OTHER);
    assertThat(report.getFindings().informational()).hasSize(1);
    assertThat(report.getFindings().acknowledged()).hasSize(1);
  }

  @Test
  void defaultSelectionStillIncludesCustomKindsWithoutAnAnnotation() {
    assertThat(AuditAssertions.failableFindings(report(), null))
        .containsExactly(Finding.fromIssue(BUILTIN), CUSTOM, OTHER);
  }

  @Test
  void selectedKindsDoNotMakeInformationalOrAcknowledgedEvidenceFailable() throws Exception {
    QueryAuditReport report =
        new QueryAuditReport("test", "query", List.of(), List.of(), List.of(), List.of(), 0, 0, 0)
            .withCustomFindings(
                List.of(), List.of(CUSTOM.withSeverity(Severity.INFO)), List.of(CUSTOM));
    assertThat(AuditAssertions.failableFindings(report, annotation("custom"))).isEmpty();
  }

  @Test
  void duplicateSelectionsAreIdempotentAndPreserveTheOldFingerprintEncoding() throws Exception {
    Map<String, Integer> legacy = new TreeMap<>();
    FindingFailurePolicy.from(annotation("legacy")).recordInto(legacy);
    assertThat(legacy).containsExactlyEntriesOf(Map.of("failOn.select-all", 1));
    Map<String, Integer> defaults = new TreeMap<>();
    FindingFailurePolicy.from(annotation("all")).recordInto(defaults);
    assertThat(defaults).containsExactlyEntriesOf(Map.of("failOnAll", 1));
    assertThat(FindingFailurePolicy.from(annotation("mixed")).kinds())
        .containsExactly("select-all", "shop:budget");
    assertThatThrownBy(() -> FindingFailurePolicy.from(annotation("mixed")).kinds().clear())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"", " ", "private SQL payload", "shop:", "Shop:budget", "nonexistent-builtin"})
  void invalidKindsAreSafeConfigurationErrorsEvenWithAnEmptyReport(String invalid) {
    QueryAudit annotation = mock(QueryAudit.class);
    when(annotation.failOn()).thenReturn(new IssueType[0]);
    when(annotation.failOnKinds()).thenReturn(new String[] {invalid});
    assertThatThrownBy(
            () ->
                AuditAssertions.failableFindings(
                    new QueryAuditReport("empty", List.of(), List.of(), List.of(), 0, 0, 0),
                    annotation))
        .isInstanceOf(ExtensionConfigurationException.class)
        .hasMessage(
            "QueryAudit failOnKinds requires built-in codes or lowercase namespaced finding IDs")
        .hasNoCause();
  }

  @Test
  void malformedPolicyFailsDuringConfigurationEvenWhenDetectionFailureIsDisabled()
      throws Exception {
    AuditSettingsResolver resolver =
        new AuditSettingsResolver(context -> QueryAuditConfig.defaults());
    assertThatThrownBy(() -> resolver.buildConfig(context(Fixtures.class, "invalid"), null))
        .isInstanceOf(ExtensionConfigurationException.class)
        .hasMessageNotContaining("private SQL payload");
  }

  @Test
  void aMethodAnnotationReplacesClassKindSelectionAndUnannotatedMethodsInheritIt()
      throws Exception {
    AuditSettingsResolver resolver = new AuditSettingsResolver(context -> null);
    FindingFailurePolicy inherited =
        FindingFailurePolicy.from(
            resolver.findAnnotation(context(InheritedFixture.class, "inherited")));
    FindingFailurePolicy method =
        FindingFailurePolicy.from(
            resolver.findAnnotation(context(InheritedFixture.class, "replaced")));
    assertThat(inherited.kinds()).containsExactly("shop:budget");
    assertThat(method.kinds()).containsExactly("shop:other");
  }

  @Test
  void theRealComparisonRecorderFingerprintsCustomKindSelectionAndNotItsSpellingChannel()
      throws Exception {
    ComparisonInputs custom = recordedInputs("custom");
    ComparisonInputs other = recordedInputs("other");
    ComparisonInputs legacy = recordedInputs("legacy");
    ComparisonInputs stringBuiltin = recordedInputs("builtinString");
    assertThat(custom.fingerprints().queryContracts())
        .isNotEqualTo(other.fingerprints().queryContracts());
    assertThat(legacy.fingerprints().queryContracts())
        .isEqualTo(stringBuiltin.fingerprints().queryContracts());
    assertThat(custom.fingerprints().thresholds()).isEqualTo(other.fingerprints().thresholds());
  }

  private static ComparisonInputs recordedInputs(String method) throws Exception {
    AuditScope scope = mock(AuditScope.class);
    ExtensionContext context = context(Fixtures.class, method);
    QueryAuditExtension.AuditRunState state = new QueryAuditExtension.AuditRunState();
    AuditCapability absent = AuditCapability.absent();
    when(scope.context()).thenReturn(context);
    when(scope.runState()).thenReturn(state);
    when(scope.inputContext())
        .thenReturn(new AuditInputContext("h2", absent, absent, absent, null));
    when(scope.explainCapability()).thenReturn(absent);
    when(scope.contracts()).thenReturn(Map.of());
    when(scope.countBaseline()).thenReturn(Map.of());
    AuditSettingsResolver resolver =
        new AuditSettingsResolver(ignored -> QueryAuditConfig.defaults());
    new ComparisonInputRecorder(resolver)
        .record(
            scope,
            new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of()),
            "test-id",
            "Fixture",
            "query");
    var run = state.result(List.of());
    assertThat(run.incompleteReasons()).isEmpty();
    assertThat(run.comparisonInputs()).containsKey("test-id");
    return run.comparisonInputs().get("test-id");
  }

  private static ExtensionContext context(Class<?> type, String name) throws Exception {
    ExtensionContext context = mock(ExtensionContext.class);
    Method method = type.getDeclaredMethod(name);
    doReturn(type).when(context).getRequiredTestClass();
    when(context.getTestMethod()).thenReturn(Optional.of(method));
    when(context.getRequiredTestMethod()).thenReturn(method);
    return context;
  }

  private static QueryAudit annotation(String method) throws Exception {
    return Fixtures.class.getDeclaredMethod(method).getAnnotation(QueryAudit.class);
  }

  private static Finding finding(String kind, Severity severity) {
    return new Finding(FindingKindId.of(kind), severity, "SELECT 1", null, null, "detail", null);
  }

  private static QueryAuditReport report() {
    return new QueryAuditReport(
            "test", "query", List.of(BUILTIN), List.of(), List.of(), List.of(), 0, 0, 0)
        .withCustomFindings(
            List.of(CUSTOM, OTHER), List.of(CUSTOM.withSeverity(Severity.INFO)), List.of(CUSTOM));
  }

  static class Fixtures {
    @QueryAudit
    void all() {}

    @QueryAudit(failOnKinds = "shop:budget")
    void custom() {}

    @QueryAudit(failOn = IssueType.SELECT_ALL)
    void legacy() {}

    @QueryAudit(failOnKinds = "select-all")
    void builtinString() {}

    @QueryAudit(
        failOn = {IssueType.SELECT_ALL, IssueType.SELECT_ALL},
        failOnKinds = {"shop:budget", "select-all", "shop:budget"})
    void mixed() {}

    @QueryAudit(failOnKinds = "shop:other")
    void other() {}

    @QueryAudit(failOnKinds = "private SQL payload", failOnDetection = BooleanOverride.FALSE)
    void invalid() {}
  }

  @QueryAudit(failOnKinds = "shop:budget")
  static class InheritedFixture {
    void inherited() {}

    @QueryAudit(failOnKinds = "shop:other")
    void replaced() {}
  }
}
