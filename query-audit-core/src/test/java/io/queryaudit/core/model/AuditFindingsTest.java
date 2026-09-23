package io.queryaudit.core.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.queryaudit.core.extension.FindingKindId;
import io.queryaudit.core.reporter.JsonReporter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AuditFindingsTest {
  private static final Issue BUILTIN =
      new Issue(
          IssueType.N_PLUS_ONE,
          Severity.WARNING,
          "select * from users",
          "users",
          "id",
          "Repeated access",
          "Batch the query");
  private static final Finding CUSTOM =
      new Finding(
          FindingKindId.of("shop:budget"),
          Severity.ERROR,
          "select * from orders",
          "orders",
          null,
          "Budget exceeded",
          "Fetch less");
  private static final Finding INFO =
      new Finding(
          FindingKindId.of("shop:advice"),
          Severity.INFO,
          CUSTOM.query(),
          "orders",
          null,
          "Budget advice",
          "Review the query");

  @Test
  void snapshotsInputsAndExposesEveryKindThroughOneView() {
    List<Finding> confirmed = new ArrayList<>(List.of(CUSTOM, Finding.fromIssue(BUILTIN)));
    AuditFindings findings = new AuditFindings(confirmed, List.of(INFO), List.of(CUSTOM));
    confirmed.clear();

    assertThat(findings.confirmed()).containsExactly(CUSTOM, Finding.fromIssue(BUILTIN));
    assertThat(findings.errors()).containsExactly(CUSTOM);
    assertThat(findings.warnings()).containsExactly(Finding.fromIssue(BUILTIN));
    assertThat(findings.all()).containsExactly(CUSTOM, Finding.fromIssue(BUILTIN), INFO, CUSTOM);
    assertThat(findings.hasConfirmed()).isTrue();
    assertThat(findings.isEmpty()).isFalse();
    assertThat(AuditFindings.empty().isEmpty()).isTrue();
    assertThat(AuditFindings.empty().hasConfirmed()).isFalse();
    assertThatThrownBy(() -> findings.confirmed().clear())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> findings.all().clear())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> new AuditFindings(null, List.of(), List.of()))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new AuditFindings(Arrays.asList((Finding) null), List.of(), List.of()))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void newViewIncludesLegacyAndCustomCategoriesWithoutChangingLegacySemantics() {
    QueryAuditReport report =
        empty()
            .withFindings(
                new AuditFindings(
                    List.of(CUSTOM, Finding.fromIssue(BUILTIN)), List.of(INFO), List.of(CUSTOM)));

    assertThat(report.getFindings().confirmed())
        .containsExactly(CUSTOM, Finding.fromIssue(BUILTIN));
    assertThat(report.getConfirmedFindings()).isEqualTo(report.getFindings().confirmed());
    assertThat(report.getConfirmedIssues()).containsExactly(BUILTIN);
    assertThat(report.getCustomConfirmedFindings()).containsExactly(CUSTOM);
    assertThat(report.getErrors()).isEmpty();
    assertThat(report.getFindings().errors()).containsExactly(CUSTOM);
    assertThat(report.getAcknowledgedCount()).isZero();
    assertThat(report.getFindings().acknowledged()).hasSize(1);
  }

  @Test
  void replacingInformationalFindingsPreservesIdentityEvidenceMetadataAndOtherBuckets() {
    QueryRecord query =
        new QueryRecord(
            "select * from users", "select * from users", 9, 1, null, 0, LifecyclePhase.TEST);
    Issue acknowledged =
        new Issue(
            IssueType.SELECT_ALL,
            Severity.WARNING,
            query.sql(),
            "users",
            null,
            "All columns selected",
            "Name the columns");
    QueryAuditReport report =
        new QueryAuditReport(
                "Example",
                "test",
                List.of(BUILTIN),
                List.of(),
                List.of(acknowledged),
                List.of(query),
                1,
                1,
                9)
            .withCustomFindings(List.of(CUSTOM), List.of(INFO), List.of())
            .withTestIdentity(
                "test-id", new TestSelector("junit-platform-unique-id", "[test:test]"))
            .withIndexMetadata(new IndexMetadata(Map.of()));
    QueryAuditReport hidden = report.withoutInformationalFindings();
    QueryAuditReport restored =
        hidden.withInformationalFindings(report.getFindings().informational());

    assertThat(hidden.getFindings().informational()).isEmpty();
    assertThat(report.getFindings().informational()).containsExactly(INFO);
    for (QueryAuditReport copy : List.of(hidden, restored)) {
      assertThat(copy.getAllQueries()).isSameAs(report.getAllQueries());
      assertThat(copy.getConfirmedIssues()).isSameAs(report.getConfirmedIssues());
      assertThat(copy.getAcknowledgedIssues()).isSameAs(report.getAcknowledgedIssues());
      assertThat(copy.getFindings().confirmed()).isEqualTo(report.getFindings().confirmed());
      assertThat(copy.getIndexMetadata()).isSameAs(report.getIndexMetadata());
      assertThat(copy.getTestId()).isEqualTo(report.getTestId());
      assertThat(copy.getTestSelector()).isEqualTo(report.getTestSelector());
      assertThat(copy.getTotalQueryCount()).isEqualTo(1);
      assertThat(copy.getTotalExecutionTimeNanos()).isEqualTo(9);
    }
    assertThat(JsonReporter.toJson(restored)).isEqualTo(JsonReporter.toJson(report));
    assertThat(restored.withoutQueryEvidence().getFindings()).isEqualTo(restored.getFindings());
  }

  @Test
  void legacyAliasesDoNotValidateUnrelatedNullableCategories() {
    QueryAuditReport report =
        new QueryAuditReport(
            "test", List.of(BUILTIN), Arrays.asList((Issue) null), List.of(), 0, 0, 0);
    assertThat(report.getConfirmedFindings()).containsExactly(Finding.fromIssue(BUILTIN));
    assertThat(report.withoutInformationalFindings().getFindings().confirmed())
        .containsExactly(Finding.fromIssue(BUILTIN));
    assertThatThrownBy(report::getFindings).isInstanceOf(NullPointerException.class);
    assertThat(new QueryAuditReport("test", null, null, null, 0, 0, 0).getFindings())
        .isEqualTo(AuditFindings.empty());
  }

  private QueryAuditReport empty() {
    return new QueryAuditReport("test", List.of(), List.of(), List.of(), 0, 0, 0);
  }
}
