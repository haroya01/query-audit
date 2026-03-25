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

class InsertOnDuplicateKeyDetectorTest {

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private final InsertOnDuplicateKeyDetector detector = new InsertOnDuplicateKeyDetector();
  private final IndexMetadata emptyMetadata = new IndexMetadata(Map.of());

  @Test
  void detectsInsertOnDuplicateKeyUpdate() {
    String sql =
        "INSERT INTO users (id, name) VALUES (1, 'John') ON DUPLICATE KEY UPDATE name = 'John'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.INSERT_ON_DUPLICATE_KEY);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
    assertThat(issues.get(0).detail()).contains("INSERT ON DUPLICATE KEY UPDATE");
  }

  @Test
  void detectsReplaceInto() {
    String sql = "REPLACE INTO users (id, name) VALUES (1, 'John')";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.INSERT_ON_DUPLICATE_KEY);
    assertThat(issues.get(0).detail()).contains("REPLACE INTO");
  }

  @Test
  void noIssueForRegularInsert() {
    String sql = "INSERT INTO users (name) VALUES ('John')";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForSelectQuery() {
    String sql = "SELECT * FROM users WHERE id = 1";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueWithNullSql() {
    QueryRecord nullRecord = new QueryRecord(null, null, 0L, 0L, null, 0);

    List<Issue> issues = detector.evaluate(List.of(nullRecord), emptyMetadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void caseInsensitiveDetection() {
    String sql =
        "insert into users (id, name) values (1, 'John') on duplicate key update name = 'John'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).hasSize(1);
  }

  @Test
  void replaceIntoCaseInsensitive() {
    String sql = "replace into users (id, name) values (1, 'John')";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).hasSize(1);
  }

  @Test
  void deduplicatesSameNormalizedQuery() {
    String sql1 =
        "INSERT INTO users (id, name) VALUES (1, 'John') ON DUPLICATE KEY UPDATE name = 'John'";
    String sql2 =
        "INSERT INTO users (id, name) VALUES (2, 'Jane') ON DUPLICATE KEY UPDATE name = 'Jane'";

    List<Issue> issues = detector.evaluate(List.of(record(sql1), record(sql2)), emptyMetadata);

    assertThat(issues).hasSize(1);
  }

  @Test
  void noIssueForUpdateQuery() {
    String sql = "UPDATE users SET name = 'John' WHERE id = 1";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForDeleteQuery() {
    String sql = "DELETE FROM users WHERE id = 1";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void detectsOnDuplicateKeyWithParameterizedValues() {
    String sql =
        "INSERT INTO users (id, name) VALUES (?, ?) ON DUPLICATE KEY UPDATE name = ?";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.INSERT_ON_DUPLICATE_KEY);
  }

  @Test
  void replaceIntoHandledGracefully() {
    String sql = "REPLACE INTO users (id, name) VALUES (1, 'John')";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.INSERT_ON_DUPLICATE_KEY);
    assertThat(issues.get(0).detail()).contains("REPLACE INTO");
  }
}
