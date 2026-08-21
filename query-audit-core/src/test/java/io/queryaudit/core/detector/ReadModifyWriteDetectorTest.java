package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.IndexInfo;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Fixtures are the canonical shapes of the four production race conditions that motivated issue
 * #169: find-then-insert without unique backing, read-then-update without a lock, and their safe
 * counterparts (upsert, unique constraint, FOR UPDATE, optimistic version column).
 */
@DisplayName("ReadModifyWriteDetector (issue #169)")
class ReadModifyWriteDetectorTest {

  private final ReadModifyWriteDetector detector = new ReadModifyWriteDetector();

  private static QueryRecord q(String sql) {
    return new QueryRecord(sql, 1_000L, 0L, "at com.example.Service.m(Service.java:10)");
  }

  /** users(email) has only a NON-unique index; accounts(email) has a unique one. */
  private static IndexMetadata metadata() {
    return new IndexMetadata(
        Map.of(
            "users", List.of(new IndexInfo("users", "idx_email", "email", 1, true, 100L)),
            "accounts", List.of(new IndexInfo("accounts", "uk_email", "email", 1, false, 100L))));
  }

  private List<Issue> evaluate(IndexMetadata meta, String... sqls) {
    return detector.evaluate(
        java.util.Arrays.stream(sqls).map(ReadModifyWriteDetectorTest::q).toList(), meta);
  }

  @Nested
  @DisplayName("fires")
  class Fires {

    @Test
    @DisplayName("find-then-insert without unique backing (duplicate-row race)")
    void findThenInsertWithoutUniqueBacking() {
      List<Issue> issues =
          evaluate(
              metadata(),
              "SELECT id FROM users WHERE email = ?",
              "INSERT INTO users (email, name) VALUES (?, ?)");

      assertThat(issues).hasSize(1);
      Issue issue = issues.get(0);
      assertThat(issue.type()).isEqualTo(IssueType.READ_MODIFY_WRITE);
      assertThat(issue.table()).isEqualTo("users");
      assertThat(issue.suggestion()).contains("unique constraint");
      assertThat(issue.sourceLocation()).contains("Service.java:10");
    }

    @Test
    @DisplayName("read-then-update without a lock (lost-update race)")
    void readThenUpdateWithoutLock() {
      List<Issue> issues =
          evaluate(
              metadata(),
              "SELECT balance FROM users WHERE id = ?",
              "UPDATE users SET balance = ? WHERE id = ?");

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).suggestion()).contains("FOR UPDATE");
    }

    @Test
    @DisplayName("the same sequence is reported once, not per repetition")
    void deduplicatesRepeatedSequences() {
      List<Issue> issues =
          evaluate(
              metadata(),
              "SELECT balance FROM users WHERE id = ?",
              "UPDATE users SET balance = ? WHERE id = ?",
              "SELECT balance FROM users WHERE id = ?",
              "UPDATE users SET balance = ? WHERE id = ?");

      assertThat(issues).hasSize(1);
    }
  }

  @Nested
  @DisplayName("stays silent")
  class StaysSilent {

    @Test
    @DisplayName("SELECT ... FOR UPDATE — pessimistic lock taken")
    void forUpdateIsSafe() {
      assertThat(
              evaluate(
                  metadata(),
                  "SELECT balance FROM users WHERE id = ? FOR UPDATE",
                  "UPDATE users SET balance = ? WHERE id = ?"))
          .isEmpty();
    }

    @Test
    @DisplayName("upsert — the constraint resolves the race")
    void upsertIsSafe() {
      assertThat(
              evaluate(
                  metadata(),
                  "SELECT id FROM users WHERE email = ?",
                  "INSERT INTO users (email) VALUES (?) ON DUPLICATE KEY UPDATE name = VALUES(name)"))
          .isEmpty();
    }

    @Test
    @DisplayName("unique index covers the checked predicate — find-then-insert is catchable")
    void uniqueBackedInsertIsSafe() {
      assertThat(
              evaluate(
                  metadata(),
                  "SELECT id FROM accounts WHERE email = ?",
                  "INSERT INTO accounts (email) VALUES (?)"))
          .isEmpty();
    }

    @Test
    @DisplayName("UPDATE whose WHERE carries a version column — optimistic locking")
    void versionColumnUpdateIsSafe() {
      assertThat(
              evaluate(
                  metadata(),
                  "SELECT balance FROM users WHERE id = ?",
                  "UPDATE users SET balance = ? WHERE id = ? AND version = ?"))
          .isEmpty();
    }

    @Test
    @DisplayName("INSERT case without index metadata — no evidence, no finding")
    void insertWithoutMetadataIsSilent() {
      assertThat(
              evaluate(
                  null,
                  "SELECT id FROM users WHERE email = ?",
                  "INSERT INTO users (email) VALUES (?)"))
          .isEmpty();
    }

    @Test
    @DisplayName("self-referential SET (atomic decrement) — already the atomic form")
    void atomicDecrementIsSafe() {
      assertThat(
              evaluate(
                  metadata(),
                  "SELECT quantity FROM users WHERE id = ?",
                  "UPDATE users SET quantity = quantity - ? WHERE id = ?"))
          .isEmpty();
    }

    @Test
    @DisplayName("read and write on different keys — not the RMW pattern")
    void nonOverlappingPredicatesAreSilent() {
      assertThat(
              evaluate(
                  metadata(),
                  "SELECT id FROM users WHERE email = ?",
                  "UPDATE users SET last_login = NOW() WHERE id = ?"))
          .isEmpty();
    }

    @Test
    @DisplayName("write to an unrelated table")
    void unrelatedTableIsSilent() {
      assertThat(
              evaluate(
                  metadata(),
                  "SELECT id FROM users WHERE email = ?",
                  "INSERT INTO audit_log (entry) VALUES (?)"))
          .isEmpty();
    }
  }
}
