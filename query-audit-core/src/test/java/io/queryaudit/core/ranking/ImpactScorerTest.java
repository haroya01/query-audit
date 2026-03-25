package io.queryaudit.core.ranking;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.Severity;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class ImpactScorerTest {

  @Test
  void emptyListReturnsEmpty() {
    assertThat(ImpactScorer.rank(List.of())).isEmpty();
    assertThat(ImpactScorer.rank(null)).isEmpty();
  }

  @Test
  void singleIssueGetsRankOne() {
    Issue issue =
        new Issue(
            IssueType.N_PLUS_ONE,
            Severity.ERROR,
            "select * from users where id = ?",
            "users",
            null,
            "Executed 5 times",
            "Use JOIN FETCH");

    List<RankedIssue> result = ImpactScorer.rank(List.of(issue));

    assertThat(result).hasSize(1);
    RankedIssue ranked = result.get(0);
    assertThat(ranked.rank()).isEqualTo(1);
    assertThat(ranked.frequency()).isEqualTo(1);
    assertThat(ranked.issue()).isEqualTo(issue);
    // frequencyScore(1) = 15, severityScore(ERROR) = 100, patternScore(N_PLUS_ONE) = 50
    assertThat(ranked.impactScore()).isEqualTo(15 + 100 + 50);
  }

  @Test
  void duplicateIssuesAreGroupedByFingerprint() {
    Issue issue1 =
        new Issue(
            IssueType.MISSING_WHERE_INDEX,
            Severity.ERROR,
            "select * from users where email = 'a@b.com'",
            "users",
            "email",
            "Missing index",
            "Add index");
    Issue issue2 =
        new Issue(
            IssueType.MISSING_WHERE_INDEX,
            Severity.ERROR,
            "select * from users where email = 'c@d.com'",
            "users",
            "email",
            "Missing index",
            "Add index");

    List<RankedIssue> result = ImpactScorer.rank(List.of(issue1, issue2));

    assertThat(result).hasSize(1);
    RankedIssue ranked = result.get(0);
    assertThat(ranked.frequency()).isEqualTo(2);
    // frequencyScore(2) = 30, severityScore(ERROR) = 100, patternScore(MISSING_WHERE_INDEX) = 30
    assertThat(ranked.impactScore()).isEqualTo(30 + 100 + 30);
  }

  @Test
  void differentIssueTypesAreNotGrouped() {
    Issue error =
        new Issue(
            IssueType.N_PLUS_ONE,
            Severity.ERROR,
            "select * from users where id = ?",
            "users",
            null,
            "N+1",
            "Fix");
    Issue warning =
        new Issue(
            IssueType.SELECT_ALL,
            Severity.WARNING,
            "select * from users",
            "users",
            null,
            "SELECT *",
            "Fix");

    List<RankedIssue> result = ImpactScorer.rank(List.of(error, warning));

    assertThat(result).hasSize(2);
  }

  @Test
  void resultsAreSortedByScoreDescending() {
    // CARTESIAN_JOIN + ERROR = 100 + 100 + 15 = 215
    Issue cartesian =
        new Issue(
            IssueType.CARTESIAN_JOIN,
            Severity.ERROR,
            "select * from a, b",
            "a",
            null,
            "Cartesian join",
            "Fix");
    // SELECT_ALL + WARNING = 30 + 10 + 15 = 55
    Issue selectAll =
        new Issue(
            IssueType.SELECT_ALL,
            Severity.WARNING,
            "select * from users",
            "users",
            null,
            "SELECT *",
            "Fix");

    List<RankedIssue> result = ImpactScorer.rank(List.of(selectAll, cartesian));

    assertThat(result.get(0).issue().type()).isEqualTo(IssueType.CARTESIAN_JOIN);
    assertThat(result.get(1).issue().type()).isEqualTo(IssueType.SELECT_ALL);
    assertThat(result.get(0).rank()).isEqualTo(1);
    assertThat(result.get(1).rank()).isEqualTo(2);
  }

  @Test
  void highFrequencyBoostsScore() {
    List<Issue> issues = new ArrayList<>();
    // 10 identical N_PLUS_ONE issues
    for (int i = 0; i < 10; i++) {
      issues.add(
          new Issue(
              IssueType.N_PLUS_ONE,
              Severity.ERROR,
              "select * from users where id = ?",
              "users",
              null,
              "N+1 #" + i,
              "Fix"));
    }
    // 1 CARTESIAN_JOIN issue
    issues.add(
        new Issue(
            IssueType.CARTESIAN_JOIN,
            Severity.ERROR,
            "select * from a, b",
            "a",
            null,
            "Cartesian",
            "Fix"));

    List<RankedIssue> result = ImpactScorer.rank(issues);

    assertThat(result).hasSize(2);
    // N+1 with freq 10: 150 + 100 + 50 = 300
    // CARTESIAN with freq 1: 15 + 100 + 100 = 215
    assertThat(result.get(0).issue().type()).isEqualTo(IssueType.N_PLUS_ONE);
    assertThat(result.get(0).frequency()).isEqualTo(10);
    assertThat(result.get(0).impactScore()).isGreaterThan(result.get(1).impactScore());
  }

  @Test
  void severityScoresAreCorrect() {
    assertThat(ImpactScorer.severityScore(Severity.ERROR)).isEqualTo(100);
    assertThat(ImpactScorer.severityScore(Severity.WARNING)).isEqualTo(30);
    assertThat(ImpactScorer.severityScore(Severity.INFO)).isEqualTo(5);
  }

  @Test
  void patternScoresMatchSpecification() {
    assertThat(ImpactScorer.patternScore(IssueType.N_PLUS_ONE)).isEqualTo(50);
    assertThat(ImpactScorer.patternScore(IssueType.MISSING_JOIN_INDEX)).isEqualTo(40);
    assertThat(ImpactScorer.patternScore(IssueType.FOR_UPDATE_WITHOUT_INDEX)).isEqualTo(40);
    assertThat(ImpactScorer.patternScore(IssueType.MISSING_WHERE_INDEX)).isEqualTo(30);
    assertThat(ImpactScorer.patternScore(IssueType.CORRELATED_SUBQUERY)).isEqualTo(30);
    assertThat(ImpactScorer.patternScore(IssueType.WHERE_FUNCTION)).isEqualTo(25);
    assertThat(ImpactScorer.patternScore(IssueType.NON_SARGABLE_EXPRESSION)).isEqualTo(25);
    assertThat(ImpactScorer.patternScore(IssueType.CARTESIAN_JOIN)).isEqualTo(100);
    assertThat(ImpactScorer.patternScore(IssueType.REDUNDANT_INDEX)).isEqualTo(15);
    assertThat(ImpactScorer.patternScore(IssueType.REDUNDANT_FILTER)).isEqualTo(10);
    assertThat(ImpactScorer.patternScore(IssueType.SELECT_ALL)).isEqualTo(10);
    assertThat(ImpactScorer.patternScore(IssueType.OFFSET_PAGINATION)).isEqualTo(20);
    assertThat(ImpactScorer.patternScore(IssueType.OR_ABUSE)).isEqualTo(15);
  }

  @Test
  void fingerprintGroupsByTypeTableColumn() {
    Issue a = new Issue(IssueType.N_PLUS_ONE, Severity.ERROR, "q1", "users", "id", "d1", "s1");
    Issue b = new Issue(IssueType.N_PLUS_ONE, Severity.ERROR, "q2", "users", "id", "d2", "s2");
    Issue c = new Issue(IssueType.N_PLUS_ONE, Severity.ERROR, "q3", "users", "email", "d3", "s3");

    // a and b have same fingerprint (same type + table + column)
    assertThat(ImpactScorer.fingerprint(a)).isEqualTo(ImpactScorer.fingerprint(b));
    // c has different column
    assertThat(ImpactScorer.fingerprint(a)).isNotEqualTo(ImpactScorer.fingerprint(c));
  }

  @Test
  void frequencyScoreCalculation() {
    // frequency N: N*10 + N*5 = N*15
    assertThat(ImpactScorer.frequencyScore(1)).isEqualTo(15);
    assertThat(ImpactScorer.frequencyScore(5)).isEqualTo(75);
    assertThat(ImpactScorer.frequencyScore(10)).isEqualTo(150);
  }

  @Test
  void nullTableAndColumnHandledInFingerprint() {
    Issue issue = new Issue(IssueType.SELECT_ALL, Severity.WARNING, "q", null, null, "d", "s");
    String fp = ImpactScorer.fingerprint(issue);
    assertThat(fp).isEqualTo("SELECT_ALL||");
  }

  @Test
  void rankedListIsImmutable() {
    Issue issue = new Issue(IssueType.N_PLUS_ONE, Severity.ERROR, "q", "t", "c", "d", "s");
    List<RankedIssue> result = ImpactScorer.rank(List.of(issue));

    org.junit.jupiter.api.Assertions.assertThrows(
        UnsupportedOperationException.class, () -> result.add(new RankedIssue(issue, 0, 0, 0)));
  }
}
