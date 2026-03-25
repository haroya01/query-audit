package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class LikeWildcardDetectorTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private final LikeWildcardDetector detector = new LikeWildcardDetector();

  @Test
  void detectsLeadingWildcard() {
    String sql = "SELECT * FROM users WHERE name LIKE '%test'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.LIKE_LEADING_WILDCARD);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
  }

  @Test
  void noIssueForTrailingWildcardOnly() {
    String sql = "SELECT * FROM users WHERE name LIKE 'test%'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForNullSql() {
    QueryRecord nullSqlRecord = new QueryRecord(null, 0L, System.currentTimeMillis(), "");

    List<Issue> issues = detector.evaluate(List.of(nullSqlRecord), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void reportsTableNameFromQuery() {
    // Kills: L47 NegateConditionalsMutator (tables.isEmpty() negated)
    // If negated, table would be null when tables is NOT empty
    String sql = "SELECT * FROM users WHERE name LIKE '%test'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).table()).isEqualTo("users");
  }

  @Test
  void deduplicatesByNormalizedSql() {
    String sql1 = "SELECT * FROM users WHERE name LIKE '%test1'";
    String sql2 = "SELECT * FROM users WHERE name LIKE '%test2'";

    List<Issue> issues = detector.evaluate(List.of(record(sql1), record(sql2)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
  }

  @Test
  void noIssueForQueryWithoutLike() {
    String sql = "SELECT * FROM users WHERE name = 'test'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }
}
