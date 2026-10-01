package io.queryaudit.core.extension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.queryaudit.core.model.Finding;
import io.queryaudit.core.model.IndexInfo;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class OpenFindingContractTest {
  static final FindingKindId KIND = FindingKindId.of("acme:query-budget");
  static final String SQL = "SELECT id FROM users WHERE id = 1 LIMIT 1";
  static final QueryRecord QUERY = new QueryRecord(SQL, 100, 1, "example.Repository:10");
  static final RuleContext CONTEXT = new RuleContext(List.of(QUERY), new IndexMetadata(Map.of()));

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(
      strings = {
        " ",
        "budget",
        "Acme:budget",
        "acme:",
        ":budget",
        "acme: budget",
        "acme:budget|other"
      })
  void customIdentifiersMustBeNamespacedAndUnambiguous(String value) {
    assertThatThrownBy(() -> RuleId.of(value)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> FindingKindId.of(value)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void identifiersRetainExactValueWithoutSilentTrimming() {
    assertThat(RuleId.of("com.acme:query-budget/v2").value()).isEqualTo("com.acme:query-budget/v2");
    assertThat(RuleId.of("acme:budget").toString()).isEqualTo("acme:budget");
    assertThatThrownBy(() -> RuleId.of("acme:" + "a".repeat(201)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @ParameterizedTest
  @EnumSource(IssueType.class)
  void everyLegacyIssueHasAnExactLosslessOpenView(IssueType type) {
    Issue original =
        new Issue(type, Severity.WARNING, SQL, "users", "id", "detail", "suggestion", "source");
    Finding view = Finding.fromIssue(original);
    assertThat(view.kindId().value()).isEqualTo(type.getCode());
    assertThat(view.kindId().isBuiltin()).isTrue();
    assertThat(view.toIssue()).contains(original);
  }

  @Test
  void customFindingNeverInventsAnEnumTypeAndSeverityCopiesKeepEvidence() {
    Finding original = finding(KIND);
    assertThat(original.toIssue()).isEmpty();
    assertThat(original.withSeverity(Severity.WARNING)).isSameAs(original);
    Finding error = original.withSeverity(Severity.ERROR);
    assertThat(error)
        .isEqualTo(
            new Finding(
                KIND, Severity.ERROR, SQL, "users", "id", "detail", "suggestion", "source"));
    assertThatThrownBy(() -> original.withSeverity(null)).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> finding(null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void descriptorsSnapshotKindsAndSettingsAndRejectIncompleteDeclarations() {
    Set<FindingKindId> kinds = new LinkedHashSet<>(Set.of(KIND));
    Map<String, String> settings = new HashMap<>(Map.of("limit", "3"));
    RuleDescriptor descriptor = new RuleDescriptor(RuleId.of("acme:budget"), "1", kinds, settings);
    kinds.clear();
    settings.put("limit", "9");
    assertThat(descriptor.findingKinds()).containsExactly(KIND);
    assertThat(descriptor.effectiveSettings()).containsEntry("limit", "3");
    assertThatThrownBy(() -> descriptor.findingKinds().clear())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> descriptor.effectiveSettings().clear())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> descriptor("acme:budget", Set.of()))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () -> new RuleDescriptor(RuleId.of("acme:budget"), " ", Set.of(KIND), Map.of()))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () -> new RuleDescriptor(RuleId.of("acme:budget"), "1", Set.of(KIND), Map.of("", "x")))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void ruleEvidenceIsAnOwnedSnapshotAndCannotMutateTheNextRuleInput() {
    List<QueryRecord> queries = new ArrayList<>(List.of(QUERY));
    List<IndexInfo> indexes = new ArrayList<>();
    Map<String, List<IndexInfo>> tables = new HashMap<>(Map.of("users", indexes));
    IndexMetadata metadata = new IndexMetadata(tables);
    RuleContext context = new RuleContext(queries, metadata);
    queries.clear();
    tables.clear();
    assertThat(context.queries()).containsExactly(QUERY);
    assertThat(context.indexMetadata()).isNotSameAs(metadata);
    assertThat(context.indexMetadata().hasTable("users")).isTrue();
    assertThatThrownBy(() -> context.queries().clear())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> context.indexMetadata().getIndexesForTable("users").clear())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThat(new RuleContext(List.of(), null).indexMetadata().isEmpty()).isTrue();
  }

  @Test
  void testkitRejectsNullDescriptorNullResultsNullElementsAndUndeclaredKinds() {
    assertThatThrownBy(() -> AuditRuleTestKit.verify(rule(null, ignored -> List.of()), CONTEXT))
        .isInstanceOf(NullPointerException.class);
    RuleDescriptor descriptor = descriptor("acme:budget", Set.of(KIND));
    assertThatThrownBy(() -> AuditRuleTestKit.verify(rule(descriptor, ignored -> null), CONTEXT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("null findings");
    assertThatThrownBy(
            () ->
                AuditRuleTestKit.verify(
                    rule(descriptor, ignored -> Arrays.asList((Finding) null)), CONTEXT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("null finding");
    assertThatThrownBy(
            () ->
                AuditRuleTestKit.verify(
                    rule(descriptor, ignored -> List.of(finding(FindingKindId.of("acme:other")))),
                    CONTEXT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("undeclared finding kind");
  }

  @Test
  void onlyTheFinalLegacyAdapterCanUseBuiltinFindingKinds() {
    RuleDescriptor builtin =
        descriptor(
            "query-audit:legacy-select", Set.of(FindingKindId.builtin(IssueType.SELECT_ALL)));
    assertThatThrownBy(() -> AuditRuleTestKit.verify(rule(builtin, ignored -> List.of()), CONTEXT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("reserved");
    Issue issue =
        new Issue(IssueType.SELECT_ALL, Severity.INFO, SQL, "users", null, "detail", null);
    LegacyDetectionRuleAdapter adapter =
        new LegacyDetectionRuleAdapter(builtin, (queries, indexes) -> List.of(issue));
    assertThat(AuditRuleTestKit.verify(adapter, CONTEXT)).containsExactly(Finding.fromIssue(issue));
    assertThatThrownBy(
            () ->
                new LegacyDetectionRuleAdapter(
                    descriptor("acme:legacy", Set.of(KIND)), (queries, indexes) -> List.of()))
        .isInstanceOf(IllegalArgumentException.class);
    for (String reserved : List.of("core:kind", "query-audit:kind")) {
      assertThatThrownBy(
              () ->
                  AuditRuleTestKit.verify(
                      rule(
                          descriptor("acme:rule", Set.of(FindingKindId.of(reserved))),
                          ignored -> List.of()),
                      CONTEXT))
          .isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Test
  void testkitChecksDescriptorStabilityAndOptionalDeterminism() {
    AtomicInteger versions = new AtomicInteger();
    AuditRule changing =
        new AuditRule() {
          public RuleDescriptor descriptor() {
            return new RuleDescriptor(
                RuleId.of("acme:budget"),
                String.valueOf(versions.incrementAndGet()),
                Set.of(KIND),
                Map.of());
          }

          public List<Finding> evaluate(RuleContext context) {
            return List.of();
          }
        };
    assertThatThrownBy(() -> AuditRuleTestKit.verify(changing, CONTEXT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("descriptor changed");
    AtomicInteger calls = new AtomicInteger();
    AuditRule random =
        rule(
            descriptor("acme:budget", Set.of(KIND)),
            ignored -> calls.incrementAndGet() == 1 ? List.of() : List.of(finding(KIND)));
    assertThatThrownBy(() -> AuditRuleTestKit.verifyDeterministic(random, CONTEXT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("not deterministic");
    List<Finding> returned = new ArrayList<>(List.of(finding(KIND)));
    List<Finding> verified =
        AuditRuleTestKit.verifyDeterministic(
            rule(descriptor("acme:budget", Set.of(KIND)), ignored -> returned), CONTEXT);
    returned.clear();
    assertThat(verified).hasSize(1);
    assertThatThrownBy(verified::clear).isInstanceOf(UnsupportedOperationException.class);
  }

  static Finding finding(FindingKindId kind) {
    return new Finding(
        kind, Severity.WARNING, SQL, "users", "id", "detail", "suggestion", "source");
  }

  static RuleDescriptor descriptor(String id, Set<FindingKindId> kinds) {
    return new RuleDescriptor(RuleId.of(id), "1", kinds, Map.of());
  }

  static AuditRule rule(RuleDescriptor descriptor, Function<RuleContext, List<Finding>> evaluate) {
    return new AuditRule() {
      public RuleDescriptor descriptor() {
        return descriptor;
      }

      public List<Finding> evaluate(RuleContext context) {
        return evaluate.apply(context);
      }
    };
  }
}
