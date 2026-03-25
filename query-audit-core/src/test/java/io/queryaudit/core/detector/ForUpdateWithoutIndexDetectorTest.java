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

class ForUpdateWithoutIndexDetectorTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private final ForUpdateWithoutIndexDetector detector = new ForUpdateWithoutIndexDetector();

  @Test
  void detectsForUpdateWithoutIndexOnWhereColumn() {
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("orders", List.of(new IndexInfo("orders", "PRIMARY", "id", 1, false, 1000))));

    String sql = "SELECT * FROM orders WHERE status = 'PENDING' FOR UPDATE";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.FOR_UPDATE_WITHOUT_INDEX);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.ERROR);
    assertThat(issues.get(0).column()).isEqualTo("status");
    assertThat(issues.get(0).detail()).contains("FOR UPDATE without index");
  }

  @Test
  void noIssueWhenWhereColumnHasIndex() {
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "orders",
                List.of(
                    new IndexInfo("orders", "PRIMARY", "id", 1, false, 1000),
                    new IndexInfo("orders", "idx_status", "status", 1, true, 5))));

    String sql = "SELECT * FROM orders WHERE status = 'PENDING' FOR UPDATE";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void detectsForUpdateWithoutWhereClause() {
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("orders", List.of(new IndexInfo("orders", "PRIMARY", "id", 1, false, 1000))));

    String sql = "SELECT * FROM orders FOR UPDATE";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).detail()).contains("without WHERE clause");
  }

  @Test
  void detectsForShareWithoutIndex() {
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "accounts", List.of(new IndexInfo("accounts", "PRIMARY", "id", 1, false, 1000))));

    String sql = "SELECT * FROM accounts WHERE balance > 0 FOR SHARE";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.FOR_UPDATE_WITHOUT_INDEX);
    assertThat(issues.get(0).column()).isEqualTo("balance");
  }

  @Test
  void noIssueForSelectWithoutForUpdate() {
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("orders", List.of(new IndexInfo("orders", "PRIMARY", "id", 1, false, 1000))));

    String sql = "SELECT * FROM orders WHERE status = 'PENDING'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueWithEmptyIndexMetadata() {
    String sql = "SELECT * FROM orders WHERE status = 'PENDING' FOR UPDATE";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void caseInsensitiveForUpdateDetection() {
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("orders", List.of(new IndexInfo("orders", "PRIMARY", "id", 1, false, 1000))));

    String sql = "select * from orders where status = 'PENDING' for update";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.FOR_UPDATE_WITHOUT_INDEX);
  }

  // ── Mutation-killing tests ──────────────────────────────────────────

  @Test
  void forUpdateWithoutWhereReportsTableName() {
    // Kills: L72 NegateConditionalsMutator (tables.isEmpty() negated)
    // If negated, table would be null when tables is NOT empty
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("orders", List.of(new IndexInfo("orders", "PRIMARY", "id", 1, false, 1000))));

    String sql = "SELECT * FROM orders FOR UPDATE";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).table()).isEqualTo("orders");
  }

  @Test
  void nullIndexMetadataReturnsEmptyList() {
    // Kills: L45 EmptyObjectReturnValsMutator
    String sql = "SELECT * FROM orders WHERE status = 'PENDING' FOR UPDATE";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), null);

    assertThat(issues).isEmpty();
  }

  @Test
  void forUpdateWithAliasResolvesTable() {
    // Kills: L121 NegateConditionalsMutator (alias != null negated in resolveAliases)
    // If alias != null is negated, aliases would not be registered
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("orders", List.of(new IndexInfo("orders", "PRIMARY", "id", 1, false, 1000))));

    String sql = "SELECT * FROM orders o WHERE o.status = 'PENDING' FOR UPDATE";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).table()).isEqualTo("orders");
    assertThat(issues.get(0).column()).isEqualTo("status");
  }

  @Test
  void resolveTableReturnsResolvedValueWhenFound() {
    // Kills: L142 NegateConditionalsMutator and EmptyObjectReturnValsMutator
    // Tests that when a table alias resolves, the resolved value is returned (not "")
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("orders", List.of(new IndexInfo("orders", "PRIMARY", "id", 1, false, 1000))));

    String sql = "SELECT * FROM orders AS o WHERE o.status = 'PENDING' FOR UPDATE";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).hasSize(1);
    // If resolveTable returned "" instead of the resolved table, hasTable("") would fail
    // and no issue would be reported
    assertThat(issues.get(0).table()).isEqualTo("orders");
  }

  @Test
  void noFalsePositiveForParameterizedWhereOnIndexedColumn() {
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("orders", List.of(new IndexInfo("orders", "PRIMARY", "id", 1, false, 1000))));

    String sql = "SELECT * FROM orders WHERE id = ? FOR UPDATE";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void forUpdateWithNoTableAliasAndDirectColumnRef() {
    // Tests resolveTable path where tableOrAlias is not null but not in alias map
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("orders", List.of(new IndexInfo("orders", "PRIMARY", "id", 1, false, 1000))));

    String sql = "SELECT * FROM orders WHERE orders.status = 'PENDING' FOR UPDATE";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).column()).isEqualTo("status");
  }
}
