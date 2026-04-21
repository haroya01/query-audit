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
import org.junit.jupiter.api.Test;

/**
 * Regression tests for issue #92 — MissingIndexDetector used to skip all LIKE operator columns,
 * which combined with LikeWildcardDetector's leading-wildcard-only scope meant that
 * non-leading-wildcard LIKE and parameterized LIKE fell through both detectors.
 */
@DisplayName("MissingIndexDetector — LIKE handling (issue #92)")
class MissingIndexDetectorLikeTest {

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private final MissingIndexDetector detector = new MissingIndexDetector();

  // users table with NO index on `email`.
  private final IndexMetadata noEmailIndex =
      new IndexMetadata(
          Map.of(
              "users",
              List.of(new IndexInfo("users", "PRIMARY", "id", 1, false, 10000))));

  @Test
  @DisplayName("flags prefix-only LIKE on an unindexed column")
  void flagsPrefixOnlyLikeOnUnindexedColumn() {
    String sql = "SELECT * FROM users WHERE email LIKE 'foo%'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), noEmailIndex);

    assertThat(issues)
        .extracting(Issue::type)
        .contains(IssueType.MISSING_WHERE_INDEX);
    assertThat(issues)
        .extracting(Issue::column)
        .contains("email");
  }

  @Test
  @DisplayName("flags parameterized LIKE on an unindexed column")
  void flagsParameterizedLikeOnUnindexedColumn() {
    String sql = "SELECT * FROM users WHERE email LIKE ?";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), noEmailIndex);

    assertThat(issues)
        .extracting(Issue::column)
        .contains("email");
  }

  @Test
  @DisplayName("skips leading-wildcard LIKE (handled by LikeWildcardDetector)")
  void skipsLeadingWildcardLike() {
    String sql = "SELECT * FROM users WHERE email LIKE '%foo'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), noEmailIndex);

    assertThat(issues)
        .filteredOn(i -> i.column() != null && i.column().equals("email"))
        .isEmpty();
  }
}
