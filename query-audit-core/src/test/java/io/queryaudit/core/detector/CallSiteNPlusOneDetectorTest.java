package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.LifecyclePhase;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.parser.SqlParser;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CallSiteNPlusOneDetectorTest {
  private static final String LOOP =
      "com.example.OrderService.load:42\ncom.example.OrderService.list:17";
  private static final String OTHER = "com.example.OrderService.detail:88";
  private final CallSiteNPlusOneDetector detector = new CallSiteNPlusOneDetector(3);

  @Test
  void reportsTheSameSelectRepeatedFromOneCallSite() {
    List<Issue> issues =
        detector.evaluate(
            repeat("SELECT * FROM customers WHERE id = 1", LOOP, 7, 4), emptyMetadata());

    assertThat(issues)
        .singleElement()
        .satisfies(
            issue -> {
              assertThat(issue.type()).isEqualTo(IssueType.N_PLUS_ONE);
              assertThat(issue.severity()).isEqualTo(Severity.ERROR);
              assertThat(issue.table()).isEqualTo("customers");
              assertThat(issue.detail()).contains("4 times");
              assertThat(issue.sourceLocation()).isEqualTo(LOOP);
            });
  }

  @Test
  void ignoresTheSameSelectSpreadAcrossDifferentCallSites() {
    List<QueryRecord> queries = new ArrayList<>();
    queries.addAll(repeat("SELECT * FROM customers WHERE id = 1", LOOP, 7, 2));
    queries.addAll(repeat("SELECT * FROM customers WHERE id = 1", OTHER, 9, 2));

    assertThat(detector.evaluate(queries, emptyMetadata())).isEmpty();
  }

  @Test
  void separatesCallSitesThatShareTheirVisibleFramesButDifferDeeper() {
    List<QueryRecord> queries = new ArrayList<>();
    queries.addAll(repeat("SELECT * FROM customers WHERE id = 1", LOOP, 7, 2));
    queries.addAll(repeat("SELECT * FROM customers WHERE id = 1", LOOP, 8, 2));

    assertThat(detector.evaluate(queries, emptyMetadata())).isEmpty();
  }

  @Test
  void staysQuietBelowTheThreshold() {
    assertThat(
            detector.evaluate(
                repeat("SELECT * FROM customers WHERE id = 1", LOOP, 7, 2), emptyMetadata()))
        .isEmpty();
  }

  @Test
  void treatsBatchedInListFetchesAsTheFix() {
    assertThat(
            detector.evaluate(
                repeat("SELECT * FROM customers WHERE id IN (?, ?, ?)", LOOP, 7, 5),
                emptyMetadata()))
        .isEmpty();
  }

  @Test
  void ignoresRepeatedWrites() {
    assertThat(
            detector.evaluate(
                repeat("INSERT INTO audit_log (message) VALUES (?)", LOOP, 7, 5), emptyMetadata()))
        .isEmpty();
  }

  @Test
  void ignoresQueriesWithoutACapturedCallSite() {
    assertThat(
            detector.evaluate(
                repeat("SELECT * FROM customers WHERE id = 1", "", 0, 5), emptyMetadata()))
        .isEmpty();
  }

  @Test
  void recognizesOnlyMultiPlaceholderInLists() {
    assertThat(CallSiteNPlusOneDetector.hasBatchedInList("select * from t where id in (?, ?)"))
        .isTrue();
    assertThat(CallSiteNPlusOneDetector.hasBatchedInList("SELECT * FROM t WHERE id IN(?,?,?)"))
        .isTrue();
    assertThat(CallSiteNPlusOneDetector.hasBatchedInList("select * from t where id in (?)"))
        .isFalse();
    assertThat(CallSiteNPlusOneDetector.hasBatchedInList("select * from t where id in (1, 2)"))
        .isFalse();
    assertThat(
            CallSiteNPlusOneDetector.hasBatchedInList(
                "select * from a join (select ?, ?) b on a.id = b.id"))
        .isFalse();
    assertThat(CallSiteNPlusOneDetector.hasBatchedInList("select min(?, ?) from t")).isFalse();
  }

  @Test
  void handlesVeryLongInListsWithoutRecursion() {
    StringBuilder sql = new StringBuilder("select * from t where id in (?");
    for (int i = 0; i < 100_000; i++) sql.append(", ?");
    sql.append(')');

    assertThat(CallSiteNPlusOneDetector.hasBatchedInList(sql.toString())).isTrue();
  }

  @Test
  void reportsALoopThatBindsDifferentValues() {
    assertThat(
            detector.evaluate(
                withValues("SELECT * FROM customers WHERE id = ?", 11, 12, 13, 11),
                emptyMetadata()))
        .singleElement()
        .satisfies(issue -> assertThat(issue.type()).isEqualTo(IssueType.N_PLUS_ONE));
  }

  @Test
  void reportsTheSameValuesRepeatedAsInfo() {
    assertThat(
            detector.evaluate(
                withValues("SELECT * FROM links WHERE code = ?", 21, 21, 21, 21), emptyMetadata()))
        .singleElement()
        .satisfies(
            issue -> {
              assertThat(issue.type()).isEqualTo(IssueType.N_PLUS_ONE);
              assertThat(issue.severity()).isEqualTo(Severity.INFO);
              assertThat(issue.detail()).contains("same values ran 4 times");
            });
    assertThat(
            detector.evaluate(withValues("SELECT count(*) FROM members", 5, 5, 5), emptyMetadata()))
        .singleElement()
        .satisfies(issue -> assertThat(issue.severity()).isEqualTo(Severity.INFO));
  }

  @Test
  void treatsUnknownValuesAsDifferent() {
    assertThat(
            detector.evaluate(
                withValues("SELECT * FROM customers WHERE id = ?", 21, 0, 21), emptyMetadata()))
        .hasSize(1);
  }

  @Test
  void ignoresOffsetPagesButNotSingleRowLimits() {
    for (String paged :
        List.of(
            "SELECT * FROM members ORDER BY id OFFSET ? ROWS FETCH FIRST ? ROWS ONLY",
            "SELECT * FROM members ORDER BY id LIMIT ? OFFSET ?",
            "SELECT * FROM members ORDER BY id LIMIT ?, ?",
            "SELECT * FROM members ORDER BY id LIMIT 3 OFFSET 6")) {
      assertThat(detector.evaluate(withValues(paged, 1, 2, 3, 4), emptyMetadata()))
          .as(paged)
          .isEmpty();
    }
    assertThat(
            detector.evaluate(
                withValues("SELECT * FROM members WHERE team_id = ? LIMIT ?", 1, 2, 3),
                emptyMetadata()))
        .hasSize(1);
  }

  private static List<QueryRecord> withValues(String sql, int... parameterHashes) {
    List<QueryRecord> records = new ArrayList<>();
    for (int i = 0; i < parameterHashes.length; i++) {
      records.add(
          new QueryRecord(
              sql,
              SqlParser.normalize(sql),
              1_000L,
              i,
              LOOP,
              7,
              LifecyclePhase.TEST,
              parameterHashes[i]));
    }
    return records;
  }

  private static List<QueryRecord> repeat(String sql, String stack, int fullStackHash, int count) {
    List<QueryRecord> records = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      records.add(
          new QueryRecord(
              sql, SqlParser.normalize(sql), 1_000L, i, stack, fullStackHash, LifecyclePhase.TEST));
    }
    return records;
  }

  private static IndexMetadata emptyMetadata() {
    return new IndexMetadata(Map.of());
  }
}
