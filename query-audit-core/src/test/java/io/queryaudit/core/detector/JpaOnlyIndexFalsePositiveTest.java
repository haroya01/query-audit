package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.IndexInfo;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Verifies the fix for issue #43: When ddl-auto=none (schema managed by Flyway/Liquibase), index
 * metadata should come from the actual database, not JPA @Index annotations. JPA-only indexes that
 * don't exist in the real DB should not cause false positives in CompositeIndexDetector.
 *
 * <p>The fix ensures that when DB metadata is available, it is used exclusively without merging JPA
 * annotations. JPA metadata is only used as a fallback when no DB provider is available.
 *
 * @see <a href="https://github.com/haroya01/query-guard/issues/43">#43</a>
 */
class JpaOnlyIndexFalsePositiveTest {

  private final CompositeIndexDetector detector = new CompositeIndexDetector();

  private static QueryRecord query(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private static IndexInfo idx(String table, String indexName, String column, int seq) {
    return new IndexInfo(table, indexName, column, seq, true, 100);
  }

  private static IndexInfo pk(String table, String column) {
    return new IndexInfo(table, "PRIMARY", column, 1, false, 1000);
  }

  private static IndexMetadata metadata(IndexInfo... infos) {
    Map<String, List<IndexInfo>> map = new HashMap<>();
    for (IndexInfo info : infos) {
      map.computeIfAbsent(info.tableName(), k -> new ArrayList<>()).add(info);
    }
    return new IndexMetadata(map);
  }

  // ═══════════════════════════════════════════════════════════════
  //  Issue #43: DB 메타데이터만 사용 시 false positive 없음
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("Issue #43 — DB-only metadata prevents JPA-only index false positives")
  class DbOnlyMetadataPreventsFalsePositives {

    /**
     * 이슈에서 보고된 정확한 시나리오:
     * - rooms 테이블에 DB에는 PRIMARY KEY만 존재
     * - JPA @Index(name="idx_room_location_type", columnList="location_id, type") 선언됨
     * - 실제 DB에는 idx_room_location_type 인덱스가 없음 (ddl-auto=none, migration 미작성)
     * - DB 메타데이터만 사용하면 false positive가 발생하지 않음
     */
    @Test
    @DisplayName("DB-only metadata does not trigger warning for non-existent composite index")
    void dbOnlyMetadataDoesNotTriggerWarningForNonExistentIndex() {
      // DB에 실제로 존재하는 인덱스: PRIMARY KEY만
      IndexMetadata dbMetadata = metadata(pk("rooms", "id"));

      // 이슈에서 보고된 쿼리
      String sql = "SELECT r1_0.id FROM rooms r1_0 WHERE r1_0.type = ?";

      List<Issue> issues = detector.evaluate(List.of(query(sql)), dbMetadata);

      // DB에 복합 인덱스가 없으므로 composite-index-leading 경고가 발생하면 안 됨
      assertThat(issues)
          .noneMatch(i -> i.type() == IssueType.COMPOSITE_INDEX_LEADING_COLUMN);
    }

    /**
     * 여러 테이블에 걸쳐 DB에 없는 JPA-only 인덱스가 있어도,
     * DB 메타데이터만 사용하면 false positive가 발생하지 않음.
     */
    @Test
    @DisplayName("Multiple tables — DB-only metadata produces no false positives")
    void multipleTablesDbOnlyMetadataNoFalsePositives() {
      // DB에는 PK만 존재
      IndexMetadata dbMetadata = metadata(pk("rooms", "id"), pk("orders", "id"));

      List<Issue> roomIssues =
          detector.evaluate(
              List.of(query("SELECT * FROM rooms WHERE type = ?")), dbMetadata);
      List<Issue> orderIssues =
          detector.evaluate(
              List.of(query("SELECT * FROM orders WHERE status = ?")), dbMetadata);

      assertThat(roomIssues)
          .noneMatch(i -> i.type() == IssueType.COMPOSITE_INDEX_LEADING_COLUMN);
      assertThat(orderIssues)
          .noneMatch(i -> i.type() == IssueType.COMPOSITE_INDEX_LEADING_COLUMN);
    }
  }

  // ═══════════════════════════════════════════════════════════════
  //  JPA fallback: DB provider 없을 때 JPA 메타데이터 사용
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("JPA fallback — when no DB provider is available")
  class JpaFallback {

    /**
     * DB provider가 없는 경우 (예: H2), JPA 메타데이터를 fallback으로 사용하면
     * 실제 인덱스 정보가 없더라도 JPA 선언 기반으로 경고를 제공.
     * 이 경우 merge가 아니라 JPA 메타데이터 단독 사용.
     */
    @Test
    @DisplayName("JPA-only metadata correctly detects composite index violation as fallback")
    void jpaFallbackDetectsCompositeIndexViolation() {
      // DB provider 없음 → JPA 메타데이터만 사용
      IndexMetadata jpaMetadata =
          metadata(
              pk("rooms", "id"),
              idx("rooms", "idx_room_location_type", "location_id", 1),
              idx("rooms", "idx_room_location_type", "type", 2));

      String sql = "SELECT r1_0.id FROM rooms r1_0 WHERE r1_0.type = ?";
      List<Issue> issues = detector.evaluate(List.of(query(sql)), jpaMetadata);

      // JPA fallback에서는 JPA 선언을 신뢰하므로 경고가 발생 (의도된 동작)
      assertThat(issues)
          .anyMatch(
              i ->
                  i.type() == IssueType.COMPOSITE_INDEX_LEADING_COLUMN
                      && "type".equals(i.column()));
    }
  }

  // ═══════════════════════════════════════════════════════════════
  //  DB에 실제 인덱스가 있는 경우 (true positive)
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("Real DB composite index — true positive")
  class RealDbIndex {

    /**
     * DB에도 인덱스가 있는 경우 이는 정상적인 warning이므로 true positive.
     */
    @Test
    @DisplayName("Real DB composite index correctly triggers warning")
    void realDbCompositeIndexCorrectlyWarns() {
      IndexMetadata dbMetadata =
          metadata(
              pk("rooms", "id"),
              idx("rooms", "idx_room_location_type", "location_id", 1),
              idx("rooms", "idx_room_location_type", "type", 2));

      String sql = "SELECT r1_0.id FROM rooms r1_0 WHERE r1_0.type = ?";
      List<Issue> issues = detector.evaluate(List.of(query(sql)), dbMetadata);

      assertThat(issues)
          .anyMatch(
              i ->
                  i.type() == IssueType.COMPOSITE_INDEX_LEADING_COLUMN
                      && "type".equals(i.column()));
    }
  }

  // ═══════════════════════════════════════════════════════════════
  //  merge()가 여전히 false positive를 유발함을 증명 (회귀 방지)
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("Regression guard — merge() still causes false positives if used")
  class RegressionGuard {

    /**
     * merge()를 직접 호출하면 여전히 JPA-only 인덱스가 포함됨을 증명.
     * IndexMetadataCollector가 merge()를 호출하지 않는 것이 수정의 핵심.
     */
    @Test
    @DisplayName("merge() adds JPA-only indexes — this is why collector must not merge")
    void mergeStillAddsJpaOnlyIndexes() {
      IndexMetadata dbMetadata = metadata(pk("rooms", "id"));
      IndexMetadata jpaMetadata =
          metadata(
              idx("rooms", "idx_room_location_type", "location_id", 1),
              idx("rooms", "idx_room_location_type", "type", 2));

      IndexMetadata merged = dbMetadata.merge(jpaMetadata);

      // merge()를 사용하면 DB에 없는 JPA 인덱스가 포함됨
      assertThat(merged.hasIndexOn("rooms", "location_id")).isTrue();
      assertThat(merged.hasIndexOn("rooms", "type")).isTrue();

      // merge된 메타데이터로 검출하면 false positive 발생
      String sql = "SELECT r1_0.id FROM rooms r1_0 WHERE r1_0.type = ?";
      List<Issue> issues = detector.evaluate(List.of(query(sql)), merged);
      assertThat(issues)
          .as("merge() causes false positive — collector must not call merge when DB metadata exists")
          .anyMatch(i -> i.type() == IssueType.COMPOSITE_INDEX_LEADING_COLUMN);

      // DB 메타데이터만 사용하면 false positive 없음
      List<Issue> dbOnlyIssues = detector.evaluate(List.of(query(sql)), dbMetadata);
      assertThat(dbOnlyIssues)
          .noneMatch(i -> i.type() == IssueType.COMPOSITE_INDEX_LEADING_COLUMN);
    }
  }
}
