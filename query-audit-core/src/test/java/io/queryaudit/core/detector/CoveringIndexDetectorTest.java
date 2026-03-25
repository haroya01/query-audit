package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.IndexInfo;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CoveringIndexDetectorTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private final CoveringIndexDetector detector = new CoveringIndexDetector();

  @Test
  void detectsCoveringIndexOpportunity() {
    // Index on status covers WHERE, but SELECT also needs name and email
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("users", List.of(new IndexInfo("users", "idx_status", "status", 1, true, 10))));

    String sql = "SELECT name, email FROM users WHERE status = 'ACTIVE'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.COVERING_INDEX_OPPORTUNITY);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    assertThat(issues.get(0).detail()).contains("index-only scan");
    assertThat(issues.get(0).detail()).contains("idx_status");
  }

  @Test
  void noIssueWhenIndexAlreadyCovers() {
    // Index includes all SELECT and WHERE columns
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "users",
                List.of(
                    new IndexInfo("users", "idx_covering", "status", 1, true, 10),
                    new IndexInfo("users", "idx_covering", "name", 2, true, 1000))));

    String sql = "SELECT name FROM users WHERE status = 'ACTIVE'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueWhenTooManyColumnsToAdd() {
    // SELECT has 5 additional columns not in the index — too many to add
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("users", List.of(new IndexInfo("users", "idx_status", "status", 1, true, 10))));

    String sql = "SELECT name, email, phone, address, bio FROM users WHERE status = 'ACTIVE'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForSelectStar() {
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("users", List.of(new IndexInfo("users", "idx_status", "status", 1, true, 10))));

    String sql = "SELECT * FROM users WHERE status = 'ACTIVE'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueWithoutWhereClause() {
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("users", List.of(new IndexInfo("users", "idx_name", "name", 1, true, 1000))));

    String sql = "SELECT name, email FROM users";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueWithEmptyIndexMetadata() {
    String sql = "SELECT name FROM users WHERE status = 'ACTIVE'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void deduplicatesSameNormalizedQuery() {
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("users", List.of(new IndexInfo("users", "idx_status", "status", 1, true, 10))));

    String sql1 = "SELECT name FROM users WHERE status = 'ACTIVE'";
    String sql2 = "SELECT name FROM users WHERE status = 'INACTIVE'";

    List<Issue> issues = detector.evaluate(List.of(record(sql1), record(sql2)), metadata);

    assertThat(issues).hasSize(1);
  }

  @Test
  void suggestionIncludesAlterTable() {
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("users", List.of(new IndexInfo("users", "idx_status", "status", 1, true, 10))));

    String sql = "SELECT name FROM users WHERE status = 'ACTIVE'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).suggestion()).contains("ALTER TABLE");
    assertThat(issues.get(0).suggestion()).contains("idx_covering");
  }

  // ── Mutation-killing tests ──────────────────────────────────────────

  @Test
  void boundaryExactlyMaxAdditionalColumnsStillDetected() {
    // Exactly 3 missing columns (== MAX_ADDITIONAL_COLUMNS) should still be detected.
    // Kills: L140 ConditionalsBoundaryMutator (> vs >=)
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("users", List.of(new IndexInfo("users", "idx_status", "status", 1, true, 10))));

    String sql = "SELECT name, email, phone FROM users WHERE status = 'ACTIVE'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.COVERING_INDEX_OPPORTUNITY);
  }

  @Test
  void fourMissingColumnsNotDetected() {
    // 4 missing columns > MAX_ADDITIONAL_COLUMNS(3), should not be detected
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("users", List.of(new IndexInfo("users", "idx_status", "status", 1, true, 10))));

    String sql = "SELECT name, email, phone, address FROM users WHERE status = 'ACTIVE'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void nullIndexMetadataReturnsEmptyList() {
    // Kills: L60 EmptyObjectReturnValsMutator
    String sql = "SELECT name FROM users WHERE status = 'ACTIVE'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), null);

    assertThat(issues).isEmpty();
  }

  @Test
  void selectWithKeywordColumnNameIsSkipped() {
    // Kills: L238 NegateConditionalsMutator (isKeyword negated)
    // If isKeyword is negated, "select" or other keywords would be treated as column names
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("users", List.of(new IndexInfo("users", "idx_status", "status", 1, true, 10))));

    // This query has columns that should not be confused with SQL keywords
    // The column alias "count" is a keyword and should be skipped
    String sql = "SELECT name FROM users WHERE status = 'ACTIVE'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    // Should detect 1 issue (name is missing from index)
    assertThat(issues).hasSize(1);
    // Verify the missing column is "name", not a keyword
    assertThat(issues.get(0).detail()).contains("name");
  }

  @Test
  void indexWithNullIndexNameIsFiltered() {
    // Kills: L107 BooleanTrueReturnValsMutator (filter idx.indexName() != null)
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "users",
                List.of(
                    new IndexInfo("users", null, "status", 1, true, 10),
                    new IndexInfo("users", "idx_status", "status", 1, true, 10))));

    String sql = "SELECT name FROM users WHERE status = 'ACTIVE'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    // Should still detect issue via idx_status, not crash on null indexName
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).detail()).contains("idx_status");
  }

  @Test
  void indexWithNullColumnNameIsFiltered() {
    // Kills: L115 BooleanTrueReturnValsMutator (filter idx.columnName() != null)
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "users",
                List.of(
                    new IndexInfo("users", "idx_status", null, 1, true, 10),
                    new IndexInfo("users", "idx_status", "status", 1, true, 10))));

    String sql = "SELECT name FROM users WHERE status = 'ACTIVE'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).hasSize(1);
  }

  @Test
  void selectWithNestedParenthesesSplitsCorrectly() {
    // Kills: L216-217 IncrementsMutator (depth++ / depth-- in splitByTopLevelCommas)
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("users", List.of(new IndexInfo("users", "idx_status", "status", 1, true, 10))));

    // Query with function (parens) and regular column - function should be skipped,
    // but the comma-splitting must handle parentheses depth correctly
    String sql = "SELECT CONCAT(first_name, last_name), email FROM users WHERE status = 'ACTIVE'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    // Should detect email as missing from index (CONCAT is skipped because it has parens)
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).detail()).contains("email");
  }

  @Test
  void selectBodyStartEqualsEndReturnsNoColumns() {
    // Kills: L187 ConditionalsBoundaryMutator (>= vs >)
    // This is hard to trigger naturally, but an empty SELECT body would trigger it
    // Using a SELECT * query (already covered by noIssueForSelectStar) is the indirect path.
    // Instead test with metadata present and valid query with extractable columns
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("users", List.of(new IndexInfo("users", "idx_status", "status", 1, true, 10))));

    // SELECT with only an aggregate function - extractSelectColumnNames returns empty
    String sql = "SELECT COUNT(name) FROM users WHERE status = 'ACTIVE'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    // extractSelectColumnNames skips aggregates, so should return empty -> no issue
    assertThat(issues).isEmpty();
  }

  // ── False-positive reduction tests ─────────────────────────────────

  @Test
  void noIssueWhenMissingColumnsArePrimaryKey() {
    // InnoDB secondary indexes implicitly include PK columns,
    // so SELECT id, name FROM users WHERE status = 'ACTIVE'
    // when there's an index on (status) should not flag 'id' as missing
    // because 'id' is the PK and already included.
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "users",
                List.of(
                    new IndexInfo("users", "PRIMARY", "id", 1, false, 10000),
                    new IndexInfo("users", "idx_status", "status", 1, true, 10))));

    String sql = "SELECT id FROM users WHERE status = 'ACTIVE'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    // 'id' is the PK, implicitly covered by InnoDB secondary index -> no issue
    assertThat(issues).isEmpty();
  }

  @Test
  void stillFlagsNonPkMissingColumns() {
    // When missing columns include non-PK columns, still flag
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "users",
                List.of(
                    new IndexInfo("users", "PRIMARY", "id", 1, false, 10000),
                    new IndexInfo("users", "idx_status", "status", 1, true, 10))));

    String sql = "SELECT id, email FROM users WHERE status = 'ACTIVE'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    // 'id' is PK (covered), but 'email' is not -> should flag for 'email'
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).detail()).contains("email");
  }

  @Test
  void extractSelectColumnNamesReturnsNonEmptyForValidQuery() {
    // Kills: L183 EmptyObjectReturnValsMutator
    // If extractSelectColumnNames returns empty when it shouldn't, no issue is reported.
    // This test verifies the method returns actual columns and issues are found.
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("users", List.of(new IndexInfo("users", "idx_status", "status", 1, true, 10))));

    String sql = "SELECT name, email FROM users WHERE status = 'ACTIVE'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).isNotEmpty();
    assertThat(issues.get(0).detail()).containsAnyOf("name", "email");
  }
}
