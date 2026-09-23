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

class QueryAuditReportSnapshotTest {

  private static final Issue ISSUE =
      new Issue(
          IssueType.N_PLUS_ONE,
          Severity.WARNING,
          "select * from users where id = 1",
          "users",
          "id",
          "Repeated query",
          "Fetch together");
  private static final Issue INFO_ISSUE =
      new Issue(
          IssueType.SELECT_ALL,
          Severity.INFO,
          ISSUE.query(),
          "users",
          "id",
          "Select all",
          "Name columns");
  private static final Issue ACK_ISSUE =
      new Issue(
          IssueType.MISSING_WHERE_INDEX,
          Severity.ERROR,
          ISSUE.query(),
          "users",
          "id",
          "Missing index",
          "Add index");
  private static final QueryRecord QUERY =
      new QueryRecord(
          "select * from users where id = 1",
          "select * from users where id = ?",
          10,
          1,
          null,
          0,
          LifecyclePhase.TEST);

  @Test
  void snapshotsEveryInputListSoLaterCallerChangesCannotChangeTheResultOrJson() {
    List<Issue> confirmed = new ArrayList<>(List.of(ISSUE));
    List<Issue> info = new ArrayList<>(List.of(INFO_ISSUE));
    List<Issue> acknowledged = new ArrayList<>(List.of(ACK_ISSUE));
    List<QueryRecord> queries = new ArrayList<>(List.of(QUERY));
    QueryAuditReport report =
        new QueryAuditReport(
            "ExampleTest", "findUsers", confirmed, info, acknowledged, queries, 1, 1, 10);
    String json = JsonReporter.toJson(report);

    confirmed.clear();
    info.clear();
    acknowledged.clear();
    queries.clear();

    assertThat(report.getConfirmedIssues()).containsExactly(ISSUE);
    assertThat(report.getInfoIssues()).containsExactly(INFO_ISSUE);
    assertThat(report.getAcknowledgedIssues()).containsExactly(ACK_ISSUE);
    assertThat(report.getAllQueries()).containsExactly(QUERY);
    assertThat(report.getAcknowledgedCount()).isEqualTo(1);
    assertThat(report.getQueryEvidenceStatus()).isEqualTo(QueryEvidenceStatus.COMPLETE);
    assertThat(JsonReporter.toJson(report)).isEqualTo(json);
  }

  @Test
  void collectionGettersCannotMutateTheReport() {
    QueryAuditReport report =
        new QueryAuditReport(
            "ExampleTest",
            "findUsers",
            List.of(ISSUE),
            List.of(ISSUE),
            List.of(ISSUE),
            List.of(QUERY),
            1,
            1,
            10);

    assertThatThrownBy(() -> report.getConfirmedIssues().clear())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> report.getInfoIssues().add(ISSUE))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> report.getAcknowledgedIssues().set(0, ISSUE))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> report.getAllQueries().remove(0))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void copyOperationsPreserveUnchangedStateWithoutRecopyingQueryEvidence() {
    QueryAuditReport original =
        new QueryAuditReport(
            "ExampleTest",
            "findUsers",
            List.of(ISSUE),
            List.of(INFO_ISSUE),
            List.of(ACK_ISSUE),
            List.of(QUERY),
            1,
            1,
            10);
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("users", List.of(new IndexInfo("users", "PRIMARY", "id", 1, false, 100))));
    TestSelector selector = new TestSelector("junit-platform-unique-id", "[test:findUsers]");
    QueryAuditReport identified =
        original.withIndexMetadata(metadata).withTestIdentity("test-id", selector);
    QueryAuditReport compact = identified.withoutQueryEvidence();
    QueryAuditReport alternateOrder =
        original
            .withoutQueryEvidence()
            .withTestIdentity("test-id", selector)
            .withIndexMetadata(metadata);

    assertThat(original.getIndexMetadata()).isNull();
    assertThat(original.getTestSelector()).isNull();
    assertThat(original.withIndexMetadata(null)).isSameAs(original);
    assertThat(identified.getAllQueries()).isSameAs(original.getAllQueries());
    assertThat(compact.getConfirmedIssues()).isSameAs(original.getConfirmedIssues());
    assertThat(compact.getInfoIssues()).containsExactly(INFO_ISSUE);
    assertThat(compact.getAcknowledgedIssues()).containsExactly(ACK_ISSUE);
    assertThat(compact.getIndexMetadata()).isSameAs(metadata);
    assertThat(compact.getTestId()).isEqualTo("test-id");
    assertThat(compact.getTestSelector()).isEqualTo(selector);
    assertThat(compact.getTestClass()).isEqualTo("ExampleTest");
    assertThat(compact.getTestName()).isEqualTo("findUsers");
    assertThat(compact.getUniquePatternCount()).isEqualTo(1);
    assertThat(compact.getTotalQueryCount()).isEqualTo(1);
    assertThat(compact.getTotalExecutionTimeNanos()).isEqualTo(10);
    assertThat(compact.getAllQueries()).isEmpty();
    assertThat(compact.getQueryEvidenceStatus()).isEqualTo(QueryEvidenceStatus.OMITTED);
    assertThat(JsonReporter.toJson(alternateOrder)).isEqualTo(JsonReporter.toJson(compact));
    assertThatThrownBy(() -> original.withTestIdentity(" ", selector))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void legacyConstructorsAlsoSnapshotTheirInputs() {
    List<Issue> issues = new ArrayList<>(List.of(ISSUE));
    List<QueryRecord> queries = new ArrayList<>(List.of(QUERY));
    QueryAuditReport eightArguments =
        new QueryAuditReport("ExampleTest", "findUsers", issues, issues, queries, 1, 1, 10);
    QueryAuditReport sevenArguments =
        new QueryAuditReport("findUsers", issues, issues, queries, 1, 1, 10);

    issues.clear();
    queries.clear();

    for (QueryAuditReport report : List.of(eightArguments, sevenArguments)) {
      assertThat(report.getConfirmedIssues()).containsExactly(ISSUE);
      assertThat(report.getInfoIssues()).containsExactly(ISSUE);
      assertThat(report.getAcknowledgedIssues()).isEmpty();
      assertThat(report.getAllQueries()).containsExactly(QUERY);
    }
    assertThat(sevenArguments.getTestClass()).isNull();
  }

  @Test
  void customBucketsAreSnapshottedAndPreservedByEveryCopyWithoutChangingLegacyCounts() {
    Finding custom =
        new Finding(
            FindingKindId.of("acme:custom"),
            Severity.ERROR,
            QUERY.sql(),
            "users",
            "id",
            "detail",
            "suggestion");
    Finding informational =
        new Finding(
            FindingKindId.of("acme:info"),
            Severity.INFO,
            QUERY.sql(),
            "users",
            "id",
            "detail",
            "suggestion");
    Finding acknowledged =
        new Finding(
            FindingKindId.of("acme:ack"),
            Severity.WARNING,
            QUERY.sql(),
            "users",
            "id",
            "detail",
            "suggestion");
    List<Finding> confirmedInput = new ArrayList<>(List.of(custom));
    List<Finding> infoInput = new ArrayList<>(List.of(informational));
    List<Finding> ackInput = new ArrayList<>(List.of(acknowledged));
    QueryAuditReport empty =
        new QueryAuditReport("custom", List.of(), List.of(), List.of(QUERY), 1, 1, 10);
    QueryAuditReport report = empty.withCustomFindings(confirmedInput, infoInput, ackInput);
    confirmedInput.clear();
    infoInput.clear();
    ackInput.clear();
    QueryAuditReport copied =
        report
            .withTestIdentity("custom-id", null)
            .withIndexMetadata(new IndexMetadata(Map.of()))
            .withoutQueryEvidence();
    assertThat(empty.hasConfirmedIssues()).isFalse();
    for (QueryAuditReport current : List.of(report, copied)) {
      assertThat(current.getCustomConfirmedFindings()).containsExactly(custom);
      assertThat(current.getCustomInfoFindings()).containsExactly(informational);
      assertThat(current.getCustomAcknowledgedFindings()).containsExactly(acknowledged);
      assertThat(current.getConfirmedFindings()).containsExactly(custom);
      assertThat(current.getInfoFindings()).containsExactly(informational);
      assertThat(current.getAcknowledgedFindings()).containsExactly(acknowledged);
      assertThat(current.hasConfirmedIssues()).isTrue();
      assertThat(current.getAcknowledgedCount()).isZero();
      assertThatThrownBy(() -> current.getCustomConfirmedFindings().clear())
          .isInstanceOf(UnsupportedOperationException.class);
      assertThatThrownBy(() -> current.getInfoFindings().clear())
          .isInstanceOf(UnsupportedOperationException.class);
      assertThatThrownBy(() -> current.getAcknowledgedFindings().clear())
          .isInstanceOf(UnsupportedOperationException.class);
    }
    assertThat(copied.withCustomFindings(List.of(), List.of(), List.of()).hasConfirmedIssues())
        .isFalse();
  }

  @Test
  void preservesNullableListsAndTheirExistingGetterConventions() {
    QueryAuditReport report =
        new QueryAuditReport("ExampleTest", "findUsers", null, null, null, null, 0, 0, 0);
    QueryAuditReport identified = report.withTestIdentity("test-id", null);

    for (QueryAuditReport candidate : List.of(report, identified)) {
      assertThat(candidate.getConfirmedIssues()).isNull();
      assertThat(candidate.getInfoIssues()).isNull();
      assertThat(candidate.getAllQueries()).isNull();
      assertThat(candidate.getAcknowledgedIssues()).isEmpty();
      assertThat(candidate.getAcknowledgedCount()).isZero();
      assertThat(candidate.hasConfirmedIssues()).isFalse();
      assertThat(candidate.getErrors()).isEmpty();
      assertThat(candidate.getWarnings()).isEmpty();
      assertThat(candidate.getRetainedQueryCount()).isZero();
    }
    assertThat(report.withoutQueryEvidence().getAllQueries()).isEmpty();
  }

  @Test
  void doesNotIntroduceValidationForLegacyNullableListElements() {
    List<Issue> issues = new ArrayList<>(Arrays.asList((Issue) null));
    List<QueryRecord> queries = new ArrayList<>(Arrays.asList((QueryRecord) null));
    QueryAuditReport report =
        new QueryAuditReport("ExampleTest", "findUsers", issues, issues, issues, queries, 1, 1, 0);

    issues.clear();
    queries.clear();

    assertThat(report.getConfirmedIssues()).containsExactly((Issue) null);
    assertThat(report.getInfoIssues()).containsExactly((Issue) null);
    assertThat(report.getAcknowledgedIssues()).containsExactly((Issue) null);
    assertThat(report.getAllQueries()).containsExactly((QueryRecord) null);
  }
}
