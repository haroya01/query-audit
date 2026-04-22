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

class ForceIndexHintDetectorTest {

  private final ForceIndexHintDetector detector = new ForceIndexHintDetector();
  private final IndexMetadata emptyIndex = new IndexMetadata(Map.of());

  private static QueryRecord q(String sql) {
    return new QueryRecord(sql, 1000L, System.currentTimeMillis(), null);
  }

  @Test
  void detectsForceIndex() {
    List<Issue> issues =
        detector.evaluate(
            List.of(
                q(
                    "SELECT id FROM users FORCE INDEX(idx_users_email) WHERE email = 'test@example.com'")),
            emptyIndex);
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.FORCE_INDEX_HINT);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    assertThat(issues.get(0).detail()).contains("FORCE INDEX");
  }

  @Test
  void detectsUseIndex() {
    List<Issue> issues =
        detector.evaluate(
            List.of(q("SELECT id FROM users USE INDEX(idx_users_status) WHERE status = 'active'")),
            emptyIndex);
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).detail()).contains("USE INDEX");
  }

  @Test
  void detectsIgnoreIndex() {
    List<Issue> issues =
        detector.evaluate(
            List.of(q("SELECT id FROM users IGNORE INDEX(idx_users_name) WHERE name LIKE 'A%'")),
            emptyIndex);
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).detail()).contains("IGNORE INDEX");
  }

  @Test
  void noIssueWithoutHints() {
    List<Issue> issues =
        detector.evaluate(
            List.of(q("SELECT id FROM users WHERE email = 'test@example.com'")), emptyIndex);
    assertThat(issues).isEmpty();
  }

  // ── False-positive reduction tests ─────────────────────────────────

  @Test
  void noIssueForForceIndexInFlywayMigration() {
    // FORCE INDEX in migration scripts is acceptable
    QueryRecord migrationQuery =
        new QueryRecord(
            "SELECT id FROM users FORCE INDEX(idx_users_email) WHERE email = 'test@example.com'",
            1000L,
            System.currentTimeMillis(),
            "org.flywaydb.core.internal.command.DbMigrate.execute(DbMigrate.java:100)");
    List<Issue> issues = detector.evaluate(List.of(migrationQuery), emptyIndex);
    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForForceIndexInLiquibaseMigration() {
    QueryRecord migrationQuery =
        new QueryRecord(
            "SELECT id FROM users FORCE INDEX(idx_users_email) WHERE email = 'test@example.com'",
            1000L,
            System.currentTimeMillis(),
            "liquibase.changelog.ChangeSet.execute(ChangeSet.java:200)");
    List<Issue> issues = detector.evaluate(List.of(migrationQuery), emptyIndex);
    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForForceIndexInMigrationScript() {
    QueryRecord migrationQuery =
        new QueryRecord(
            "SELECT id FROM users FORCE INDEX(idx_users_email) WHERE email = 'test@example.com'",
            1000L,
            System.currentTimeMillis(),
            "com.example.db.migration.V202401__AddIndex.execute(V202401__AddIndex.java:50)");
    List<Issue> issues = detector.evaluate(List.of(migrationQuery), emptyIndex);
    assertThat(issues).isEmpty();
  }

  @Test
  void issueDetectedForForceIndexInApplicationCode() {
    // Non-migration stack trace should still be flagged
    QueryRecord appQuery =
        new QueryRecord(
            "SELECT id FROM users FORCE INDEX(idx_users_email) WHERE email = 'test@example.com'",
            1000L,
            System.currentTimeMillis(),
            "com.example.service.UserService.findByEmail(UserService.java:42)");
    List<Issue> issues = detector.evaluate(List.of(appQuery), emptyIndex);
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.FORCE_INDEX_HINT);
  }

  @Test
  void issueDetectedForForceIndexWithNullStackTrace() {
    // Null stack trace should still be flagged (not migration context)
    List<Issue> issues =
        detector.evaluate(
            List.of(
                q(
                    "SELECT id FROM users FORCE INDEX(idx_users_email) WHERE email = 'test@example.com'")),
            emptyIndex);
    assertThat(issues).hasSize(1);
  }
}
