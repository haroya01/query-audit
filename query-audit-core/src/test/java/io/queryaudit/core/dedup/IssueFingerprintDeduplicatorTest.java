package io.queryaudit.core.dedup;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.Severity;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class IssueFingerprintDeduplicatorTest {

  // -----------------------------------------------------------------------
  // fingerprint()
  // -----------------------------------------------------------------------

  @Test
  void fingerprintIncludesTypeTableColumnAndNormalizedQuery() {
    Issue issue =
        new Issue(
            IssueType.MISSING_JOIN_INDEX,
            Severity.ERROR,
            "SELECT * FROM rooms WHERE owner_id = 1",
            "rooms",
            "owner_id",
            "Missing index",
            "Add index on rooms.owner_id");
    String fp = IssueFingerprintDeduplicator.fingerprint(issue);
    assertThat(fp).startsWith("MISSING_JOIN_INDEX|rooms|owner_id|");
    assertThat(fp).contains("select * from rooms where owner_id = ?");
  }

  @Test
  void fingerprintHandlesNullFields() {
    Issue issue =
        new Issue(IssueType.SELECT_ALL, Severity.WARNING, null, null, null, "detail", "suggestion");
    String fp = IssueFingerprintDeduplicator.fingerprint(issue);
    assertThat(fp).isEqualTo("SELECT_ALL|||");
  }

  // -----------------------------------------------------------------------
  // normalizeQuery()
  // -----------------------------------------------------------------------

  @Test
  void normalizeQueryLowercasesAndStripsLiterals() {
    String result =
        IssueFingerprintDeduplicator.normalizeQuery(
            "SELECT * FROM users WHERE name = 'John' AND age = 25");
    assertThat(result).isEqualTo("select * from users where name = ? and age = ?");
  }

  @Test
  void normalizeQueryCollapsesWhitespace() {
    String result =
        IssueFingerprintDeduplicator.normalizeQuery("SELECT   *   FROM   users   WHERE   id  =  1");
    assertThat(result).isEqualTo("select * from users where id = ?");
  }

  @Test
  void normalizeQueryTruncatesTo200Chars() {
    String longQuery = "SELECT " + "a".repeat(300) + " FROM users";
    String result = IssueFingerprintDeduplicator.normalizeQuery(longQuery);
    assertThat(result).hasSize(200);
  }

  @Test
  void normalizeQueryHandlesNullAndBlank() {
    assertThat(IssueFingerprintDeduplicator.normalizeQuery(null)).isEmpty();
    assertThat(IssueFingerprintDeduplicator.normalizeQuery("  ")).isEmpty();
  }

  // -----------------------------------------------------------------------
  // deduplicate()
  // -----------------------------------------------------------------------

  @Test
  void deduplicateGroupsIdenticalIssuesAcrossTests() {
    Issue sameIssue =
        new Issue(
            IssueType.MISSING_JOIN_INDEX,
            Severity.ERROR,
            "SELECT * FROM rooms WHERE owner_id = 1",
            "rooms",
            "owner_id",
            "Missing index",
            "Add index");

    List<QueryAuditReport> reports = new ArrayList<>();
    for (int i = 0; i < 49; i++) {
      reports.add(
          new QueryAuditReport(
              "TestClass",
              "testMethod" + i,
              List.of(sameIssue),
              List.of(),
              List.of(),
              List.of(),
              1,
              1,
              100_000L));
    }

    List<DeduplicatedIssue> result = IssueFingerprintDeduplicator.deduplicate(reports);

    assertThat(result).hasSize(1);
    DeduplicatedIssue dedup = result.get(0);
    assertThat(dedup.occurrenceCount()).isEqualTo(49);
    assertThat(dedup.highestSeverity()).isEqualTo(Severity.ERROR);
    // Max 10 affected tests
    assertThat(dedup.affectedTests()).hasSize(10);
    assertThat(dedup.affectedTests().get(0)).isEqualTo("TestClass#testMethod0");
  }

  @Test
  void deduplicateKeepsDistinctIssuesSeparate() {
    Issue issue1 =
        new Issue(
            IssueType.MISSING_JOIN_INDEX,
            Severity.ERROR,
            "SELECT * FROM rooms WHERE owner_id = 1",
            "rooms",
            "owner_id",
            "detail",
            "suggestion");
    Issue issue2 =
        new Issue(
            IssueType.SELECT_ALL,
            Severity.WARNING,
            "SELECT * FROM users",
            "users",
            null,
            "detail",
            "suggestion");

    QueryAuditReport report =
        new QueryAuditReport(
            "TestClass",
            "testMethod",
            List.of(issue1, issue2),
            List.of(),
            List.of(),
            List.of(),
            2,
            2,
            100_000L);

    List<DeduplicatedIssue> result = IssueFingerprintDeduplicator.deduplicate(List.of(report));

    assertThat(result).hasSize(2);
  }

  @Test
  void deduplicateSortsBySeverityDescThenCountDesc() {
    Issue warning =
        new Issue(
            IssueType.SELECT_ALL, Severity.WARNING, "SELECT * FROM users", "users", null, "d", "s");
    Issue error =
        new Issue(
            IssueType.MISSING_JOIN_INDEX,
            Severity.ERROR,
            "SELECT * FROM rooms WHERE owner_id = 1",
            "rooms",
            "owner_id",
            "d",
            "s");
    Issue info =
        new Issue(
            IssueType.FULL_TABLE_SCAN, Severity.INFO, "SELECT * FROM logs", "logs", null, "d", "s");

    // Warning appears 10 times, error appears 2 times, info 5 times
    List<QueryAuditReport> reports = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      reports.add(
          new QueryAuditReport(
              "C", "t" + i, List.of(warning), List.of(), List.of(), List.of(), 1, 1, 100_000L));
    }
    for (int i = 0; i < 2; i++) {
      reports.add(
          new QueryAuditReport(
              "C", "e" + i, List.of(error), List.of(), List.of(), List.of(), 1, 1, 100_000L));
    }
    for (int i = 0; i < 5; i++) {
      reports.add(
          new QueryAuditReport(
              "C", "i" + i, List.of(), List.of(info), List.of(), List.of(), 1, 1, 100_000L));
    }

    List<DeduplicatedIssue> result = IssueFingerprintDeduplicator.deduplicate(reports);

    assertThat(result).hasSize(3);
    // ERROR first (severity ordinal 0), then WARNING (1), then INFO (2)
    assertThat(result.get(0).highestSeverity()).isEqualTo(Severity.ERROR);
    assertThat(result.get(1).highestSeverity()).isEqualTo(Severity.WARNING);
    assertThat(result.get(2).highestSeverity()).isEqualTo(Severity.INFO);
  }

  @Test
  void deduplicateTracksHighestSeverityPerFingerprint() {
    // Same issue type/table/column but different severity levels
    Issue warningVersion =
        new Issue(
            IssueType.MISSING_JOIN_INDEX,
            Severity.WARNING,
            "SELECT * FROM rooms WHERE owner_id = 1",
            "rooms",
            "owner_id",
            "d",
            "s");
    Issue errorVersion =
        new Issue(
            IssueType.MISSING_JOIN_INDEX,
            Severity.ERROR,
            "SELECT * FROM rooms WHERE owner_id = 1",
            "rooms",
            "owner_id",
            "d",
            "s");

    QueryAuditReport r1 =
        new QueryAuditReport(
            "C", "t1", List.of(warningVersion), List.of(), List.of(), List.of(), 1, 1, 100_000L);
    QueryAuditReport r2 =
        new QueryAuditReport(
            "C", "t2", List.of(errorVersion), List.of(), List.of(), List.of(), 1, 1, 100_000L);

    List<DeduplicatedIssue> result = IssueFingerprintDeduplicator.deduplicate(List.of(r1, r2));

    assertThat(result).hasSize(1);
    assertThat(result.get(0).highestSeverity()).isEqualTo(Severity.ERROR);
    assertThat(result.get(0).occurrenceCount()).isEqualTo(2);
  }

  @Test
  void deduplicateReturnsEmptyForNullOrEmptyInput() {
    assertThat(IssueFingerprintDeduplicator.deduplicate(null)).isEmpty();
    assertThat(IssueFingerprintDeduplicator.deduplicate(List.of())).isEmpty();
  }

  @Test
  void deduplicateIncludesBothConfirmedAndInfoIssues() {
    Issue confirmed =
        new Issue(
            IssueType.N_PLUS_ONE,
            Severity.ERROR,
            "SELECT * FROM users WHERE id = ?",
            "users",
            "id",
            "d",
            "s");
    Issue info =
        new Issue(
            IssueType.FULL_TABLE_SCAN, Severity.INFO, "SELECT * FROM logs", "logs", null, "d", "s");

    QueryAuditReport report =
        new QueryAuditReport(
            "C", "test", List.of(confirmed), List.of(info), List.of(), List.of(), 2, 2, 100_000L);

    List<DeduplicatedIssue> result = IssueFingerprintDeduplicator.deduplicate(List.of(report));

    assertThat(result).hasSize(2);
  }

  @Test
  void deduplicateDoesNotAddDuplicateTestNames() {
    Issue issue =
        new Issue(
            IssueType.SELECT_ALL, Severity.WARNING, "SELECT * FROM users", "users", null, "d", "s");

    // Same test name produces two identical issues (e.g. two SELECT * in one test)
    QueryAuditReport report =
        new QueryAuditReport(
            "C",
            "sameTest",
            List.of(issue, issue),
            List.of(),
            List.of(),
            List.of(),
            1,
            2,
            100_000L);

    List<DeduplicatedIssue> result = IssueFingerprintDeduplicator.deduplicate(List.of(report));

    assertThat(result).hasSize(1);
    assertThat(result.get(0).occurrenceCount()).isEqualTo(2);
    // Same test name should not be duplicated in the list
    assertThat(result.get(0).affectedTests()).hasSize(1);
    assertThat(result.get(0).affectedTests().get(0)).isEqualTo("C#sameTest");
  }
}
