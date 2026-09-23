package io.queryaudit.core.extension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.detector.DetectionRule;
import io.queryaudit.core.detector.QueryAuditAnalyzer;
import io.queryaudit.core.model.Finding;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class RegisteredRuleDiagnosticsTest {
  private static final FindingKindId KIND = FindingKindId.of("shop:budget");
  private static final RuleId RULE_ID = RuleId.of("shop:budget-rule");
  private static final String SECRET = "SELECT secret FROM private_credentials";
  private static final List<QueryRecord> QUERIES = List.of(new QueryRecord("SELECT 1", 1, 1, null));
  @TempDir Path directory;

  @Test
  void openExecutionFailureRetainsBothIdentitiesWithoutMessageOrCause() {
    AuditRule rule =
        rule(
            () -> descriptor("1"),
            context -> {
              throw new IllegalStateException(SECRET);
            });
    AuditRuleException failure =
        failedAnalysis(AuditExtensions.builder().auditRule("audit-rule:budgetBean", rule).build());
    assertSafe(failure, "audit-rule:budgetBean", AuditRuleException.Reason.EXECUTION_FAILED);
    assertThat(failure.ruleId()).isEqualTo(RULE_ID);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void descriptorFailuresRetainRegistrationButNeverAnInventedRuleIdentity(boolean returnsNull) {
    AuditRule rule =
        rule(
            () -> {
              if (returnsNull) return null;
              throw new IllegalArgumentException(SECRET);
            },
            context -> List.of());
    AuditRuleException failure =
        failedConstruction(AuditExtensions.builder().auditRule("shop:declaration", rule).build());
    assertSafe(failure, "shop:declaration", AuditRuleException.Reason.DECLARATION_FAILED);
    assertThat(failure.ruleId()).isNull();
  }

  @Test
  void reservedDeclarationsAreDistinguishedFromThrowingDeclarations() {
    AuditRule rule =
        rule(
            () ->
                new RuleDescriptor(
                    RULE_ID, "1", Set.of(FindingKindId.builtin(IssueType.SELECT_ALL)), Map.of()),
            context -> List.of());
    assertSafe(
        failedConstruction(AuditExtensions.builder().auditRule("shop:invalid", rule).build()),
        "shop:invalid",
        AuditRuleException.Reason.INVALID_DECLARATION);
  }

  static Stream<List<Finding>> invalidFindings() {
    return Stream.of(
        null, Arrays.asList((Finding) null), List.of(finding(FindingKindId.of("shop:undeclared"))));
  }

  @ParameterizedTest
  @MethodSource("invalidFindings")
  void invalidOpenResultsAreNotMisreportedAsSuccessfulEmptyAnalysis(List<Finding> result) {
    AuditRule rule = rule(() -> descriptor("1"), context -> result);
    assertSafe(
        failedAnalysis(AuditExtensions.builder().auditRule("shop:invalid-result", rule).build()),
        "shop:invalid-result",
        AuditRuleException.Reason.INVALID_RESULT);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void descriptorMutationIsDetectedBeforeAndAfterEvaluation(boolean duringEvaluation) {
    AtomicReference<RuleDescriptor> declared = new AtomicReference<>(descriptor("1"));
    AtomicInteger calls = new AtomicInteger();
    AuditRule rule =
        rule(
            declared::get,
            context -> {
              calls.incrementAndGet();
              declared.set(descriptor("2"));
              return List.of();
            });
    QueryAuditAnalyzer analyzer =
        analyzer(AuditExtensions.builder().auditRule("shop:mutable", rule).build());
    if (!duringEvaluation) declared.set(descriptor("2"));
    AuditRuleException failure = (AuditRuleException) catchThrowable(() -> analyze(analyzer));
    assertSafe(failure, "shop:mutable", AuditRuleException.Reason.DESCRIPTOR_CHANGED);
    assertThat(calls).hasValue(duringEvaluation ? 1 : 0);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void invalidRegistrationTextIsReplacedWithAStableOrdinal(boolean legacy) {
    AuditExtensions.Builder builder = AuditExtensions.builder();
    if (legacy) {
      builder.rule("safe:first", (queries, metadata) -> List.of());
      builder.rule(
          SECRET,
          (queries, metadata) -> {
            throw new IllegalStateException(SECRET);
          });
    } else {
      builder.auditRule(
          "safe:first",
          rule(
              () ->
                  new RuleDescriptor(
                      RuleId.of("shop:first"),
                      "1",
                      Set.of(FindingKindId.of("shop:first")),
                      Map.of()),
              context -> List.of()));
      builder.auditRule(
          SECRET,
          rule(
              () -> descriptor("1"),
              context -> {
                throw new IllegalStateException(SECRET);
              }));
    }
    assertSafe(
        failedAnalysis(builder.build()),
        "unidentified-rule:2",
        AuditRuleException.Reason.EXECUTION_FAILED);
  }

  @Test
  void repeatedLegacyInstancesKeepTheFailingRegistrationNotTheFirstInstanceMatch() {
    AtomicInteger calls = new AtomicInteger();
    DetectionRule shared =
        (queries, metadata) -> {
          if (calls.incrementAndGet() == 2) throw new IllegalStateException(SECRET);
          return List.of();
        };
    AuditExtensions extensions =
        AuditExtensions.builder().rule("shop:first", shared).rule("shop:second", shared).build();
    QueryAuditAnalyzer analyzer = analyzer(extensions);
    assertThat(analyzer.getRules())
        .filteredOn(rule -> rule == shared)
        .containsExactly(shared, shared);
    assertSafe(
        (AuditRuleException) catchThrowable(() -> analyze(analyzer)),
        "shop:second",
        AuditRuleException.Reason.EXECUTION_FAILED);
    assertThat(calls).hasValue(2);
  }

  @Test
  void legacyDeclarationFailureHasItsRegistrationAndNoRawPayload() {
    DetectionRule rule =
        new DetectionRule() {
          public String getRuleCode() {
            throw new IllegalStateException(SECRET);
          }

          public List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata metadata) {
            return List.of();
          }
        };
    assertSafe(
        failedConstruction(AuditExtensions.builder().rule("shop:legacy", rule).build()),
        "shop:legacy",
        AuditRuleException.Reason.DECLARATION_FAILED);
  }

  static Stream<List<Issue>> invalidIssues() {
    return Stream.of(
        null,
        Arrays.asList((Issue) null),
        List.of(new Issue(null, Severity.ERROR, SECRET, null, null, null, null)),
        List.of(new Issue(IssueType.SLOW_QUERY, null, SECRET, null, null, null, null)));
  }

  @ParameterizedTest
  @MethodSource("invalidIssues")
  void legacyInvalidResultsAreAttributedBeforeDownstreamReportCodeRuns(List<Issue> result) {
    assertSafe(
        failedAnalysis(
            AuditExtensions.builder().rule("shop:legacy", (queries, metadata) -> result).build()),
        "shop:legacy",
        AuditRuleException.Reason.INVALID_RESULT);
  }

  @Test
  void disabledRegisteredRulesDoNotRunButDeclarationsStillDefineSelection() {
    AtomicInteger calls = new AtomicInteger();
    DetectionRule legacy =
        new DetectionRule() {
          public String getRuleCode() {
            return "slow-query";
          }

          public List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata metadata) {
            calls.incrementAndGet();
            throw new IllegalStateException(SECRET);
          }
        };
    AuditRule open =
        rule(
            () -> descriptor("1"),
            context -> {
              calls.incrementAndGet();
              throw new IllegalStateException(SECRET);
            });
    QueryAuditAnalyzer analyzer =
        QueryAuditAnalyzer.withExtensions(
            QueryAuditConfig.builder().disabledRules(Set.of("slow-query", KIND.value())).build(),
            directory.resolve("absent"),
            AuditExtensions.builder()
                .rule("shop:legacy", legacy)
                .auditRule("shop:open", open)
                .build());
    analyze(analyzer);
    assertThat(calls).hasValue(0);
    assertThat(analyzer.getAuditRules()).isEmpty();
    assertThat(analyzer.getRules()).doesNotContain(legacy);
  }

  @Test
  void oldListConstructorsAndTheAuthorTestKitKeepTheOriginalExceptionContract() {
    IllegalStateException original = new IllegalStateException(SECRET);
    DetectionRule legacy =
        (queries, metadata) -> {
          throw original;
        };
    QueryAuditAnalyzer oldAnalyzer =
        new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of(), List.of(legacy));
    assertThatThrownBy(() -> analyze(oldAnalyzer)).isSameAs(original);
    AuditRule open =
        rule(
            () -> descriptor("1"),
            context -> {
              throw original;
            });
    assertThatThrownBy(() -> AuditRuleTestKit.verify(open, new RuleContext(QUERIES, null)))
        .isSameAs(original);
    assertThatThrownBy(
            () ->
                AuditRuleTestKit.verify(
                    rule(() -> null, context -> List.of()), new RuleContext(QUERIES, null)))
        .isExactlyInstanceOf(NullPointerException.class);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void virtualMachineFailuresPropagateUnchanged(boolean legacy) {
    OutOfMemoryError fatal = new OutOfMemoryError("synthetic fatal control");
    AuditExtensions.Builder builder = AuditExtensions.builder();
    if (legacy)
      builder.rule(
          "shop:fatal",
          (queries, metadata) -> {
            throw fatal;
          });
    else
      builder.auditRule(
          "shop:fatal",
          rule(
              () -> descriptor("1"),
              context -> {
                throw fatal;
              }));
    QueryAuditAnalyzer analyzer = analyzer(builder.build());
    assertThatThrownBy(() -> analyze(analyzer)).isSameAs(fatal);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void linkageFailuresUseSafeDiagnosticsInsteadOfLeakingImplementationText(boolean legacy) {
    AuditExtensions.Builder builder = AuditExtensions.builder();
    if (legacy)
      builder.rule(
          "shop:linkage",
          (queries, metadata) -> {
            throw new NoClassDefFoundError(SECRET);
          });
    else
      builder.auditRule(
          "shop:linkage",
          rule(
              () -> descriptor("1"),
              context -> {
                throw new NoClassDefFoundError(SECRET);
              }));
    assertSafe(
        failedAnalysis(builder.build()),
        "shop:linkage",
        AuditRuleException.Reason.EXECUTION_FAILED);
  }

  private QueryAuditAnalyzer analyzer(AuditExtensions extensions) {
    return QueryAuditAnalyzer.withExtensions(
        QueryAuditConfig.defaults(), directory.resolve("absent"), extensions);
  }

  private AuditRuleException failedConstruction(AuditExtensions extensions) {
    Throwable failure = catchThrowable(() -> analyzer(extensions));
    assertThat(failure).isInstanceOf(AuditRuleException.class);
    return (AuditRuleException) failure;
  }

  private AuditRuleException failedAnalysis(AuditExtensions extensions) {
    QueryAuditAnalyzer analyzer = analyzer(extensions);
    Throwable failure = catchThrowable(() -> analyze(analyzer));
    assertThat(failure).isInstanceOf(AuditRuleException.class);
    return (AuditRuleException) failure;
  }

  private static void analyze(QueryAuditAnalyzer analyzer) {
    analyzer.analyze("test", QUERIES, null);
  }

  private static void assertSafe(
      AuditRuleException failure, String id, AuditRuleException.Reason reason) {
    assertThat(failure.registrationId()).isEqualTo(id);
    assertThat(failure.reason()).isEqualTo(reason);
    assertThat(failure)
        .hasMessageContaining(id)
        .hasMessageContaining(reason.name())
        .hasMessageNotContaining(SECRET)
        .hasNoCause();
    assertThat(failure.getSuppressed()).isEmpty();
  }

  private static RuleDescriptor descriptor(String version) {
    return new RuleDescriptor(RULE_ID, version, Set.of(KIND), Map.of());
  }

  private static Finding finding(FindingKindId kind) {
    return new Finding(kind, Severity.ERROR, SECRET, null, null, SECRET, SECRET);
  }

  private static AuditRule rule(
      Supplier<RuleDescriptor> descriptor, Function<RuleContext, List<Finding>> evaluation) {
    return new AuditRule() {
      public RuleDescriptor descriptor() {
        return descriptor.get();
      }

      public List<Finding> evaluate(RuleContext context) {
        return evaluation.apply(context);
      }
    };
  }
}
