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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class UnboundedResultSetDetectorTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private final UnboundedResultSetDetector detector = new UnboundedResultSetDetector();

  // ── existing tests ──────────────────────────────────────────────────

  @Test
  void detectsSelectWithoutLimit() {
    String sql = "SELECT name, email FROM users WHERE active = true";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.UNBOUNDED_RESULT_SET);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
    assertThat(issues.get(0).detail()).contains("without LIMIT");
  }

  @Test
  void noIssueForSelectWithLimit() {
    String sql = "SELECT name FROM users WHERE active = true LIMIT 10";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForCountQuery() {
    String sql = "SELECT COUNT(*) FROM users WHERE active = true";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForExistsQuery() {
    String sql = "SELECT EXISTS(SELECT 1 FROM users WHERE email = 'test@test.com')";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForAggregateMax() {
    String sql = "SELECT MAX(created_at) FROM orders WHERE user_id = 1";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForAggregateMin() {
    String sql = "SELECT MIN(price) FROM products WHERE category = 'electronics'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForAggregateSum() {
    String sql = "SELECT SUM(amount) FROM payments WHERE user_id = 1";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForAggregateAvg() {
    String sql = "SELECT AVG(rating) FROM reviews WHERE product_id = 1";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForSelectWithoutFrom() {
    String sql = "SELECT 1";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForForUpdateQuery() {
    String sql = "SELECT * FROM orders WHERE status = 'PENDING' FOR UPDATE";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForPrimaryKeyLookup() {
    String sql = "SELECT * FROM users WHERE id = ?";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForForeignKeyLookup() {
    String sql = "SELECT * FROM orders WHERE user_id = ?";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForInSubquery() {
    String sql =
        "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE status = 'ACTIVE')";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForNonSelectQuery() {
    String sql = "UPDATE users SET active = false WHERE last_login < '2023-01-01'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void deduplicatesSameNormalizedQuery() {
    List<Issue> issues =
        detector.evaluate(
            List.of(
                record("SELECT name FROM users WHERE active = true"),
                record("SELECT name FROM users WHERE active = true")),
            EMPTY_INDEX);

    assertThat(issues).hasSize(1);
  }

  @Test
  void extractsTableName() {
    String sql = "SELECT name, email FROM orders WHERE status = 'ACTIVE' AND type = 'EXPRESS'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).table()).isEqualTo("orders");
  }

  @Test
  void includesSuggestion() {
    String sql = "SELECT * FROM users WHERE active = true AND role = 'ADMIN'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).suggestion()).contains("LIMIT").contains("Pageable");
  }

  @Test
  void detectsJoinQueryWithoutLimit() {
    String sql =
        "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id WHERE o.status = 'ACTIVE'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
  }

  // ── new tests: expanded PK/unique column patterns ───────────────────

  @Nested
  class ExpandedUniqueLookupPatterns {

    @Test
    void noIssueForEmailLookup() {
      String sql = "SELECT * FROM users WHERE email = ?";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    void noIssueForUuidLookup() {
      String sql = "SELECT * FROM sessions WHERE uuid = ?";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    void noIssueForUsernameLookup() {
      String sql = "SELECT * FROM users WHERE username = ?";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    void noIssueForAliasedEmailLookup() {
      String sql = "SELECT * FROM users u WHERE u.email = ?";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }
  }

  // ── new tests: FOR SHARE ────────────────────────────────────────────

  @Nested
  class ForShareTests {

    @Test
    void noIssueForForShareQuery() {
      String sql = "SELECT * FROM accounts WHERE account_number = 'ACC001' FOR SHARE";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }
  }

  // ── new tests: SELECT INTO ──────────────────────────────────────────

  @Nested
  class SelectIntoTests {

    @Test
    void noIssueForSelectInto() {
      String sql = "SELECT name INTO @user_name FROM users WHERE id = 1";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    void noIssueForSelectIntoMultipleVars() {
      String sql = "SELECT name, email INTO @uname, @uemail FROM users WHERE id = 1";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }
  }

  // ── new tests: index metadata awareness ─────────────────────────────

  @Nested
  class IndexMetadataAwareness {

    @Test
    void noIssueWhenColumnHasUniqueIndex() {
      // "phone" is not in PK_LOOKUP_PATTERN, but has a unique index
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "users",
                  List.of(new IndexInfo("users", "uk_users_phone", "phone", 1, false, 1000))));

      String sql = "SELECT * FROM users WHERE phone = ?";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

      assertThat(issues).isEmpty();
    }

    @Test
    void flagsWhenColumnHasNonUniqueIndex() {
      // "status" has a non-unique index — should still flag
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "users",
                  List.of(new IndexInfo("users", "idx_users_status", "status", 1, true, 5))));

      String sql = "SELECT * FROM users WHERE status = ?";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

      assertThat(issues).hasSize(1);
    }

    @Test
    void flagsWhenCompositeUniqueIndexNotFullyMatched() {
      // Composite unique index on (region, code) — querying only region
      // does not guarantee single row, should still flag
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "warehouses",
                  List.of(
                      new IndexInfo("warehouses", "uk_region_code", "region", 1, false, 100),
                      new IndexInfo("warehouses", "uk_region_code", "code", 2, false, 1000))));

      String sql = "SELECT * FROM warehouses WHERE region = ?";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

      assertThat(issues).hasSize(1);
    }

    @Test
    void noIssueWhenNoIndexMetadataButPkPattern() {
      // Even without metadata, id = ? is still excluded by PK_LOOKUP_PATTERN
      String sql = "SELECT * FROM users WHERE id = ?";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    void flagsUnknownColumnWithoutMetadata() {
      // "phone" is not in PK_LOOKUP_PATTERN and no metadata
      String sql = "SELECT * FROM users WHERE phone = ?";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }

    // ── Issue #33: composite unique index false positives ──────────────

    @Test
    void noIssueWhenAllColumnsOfCompositeUniqueIndexUsed() {
      // UNIQUE(oauth_provider, oauth_sub) — both columns in WHERE → at most 1 row
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "users",
                  List.of(
                      new IndexInfo("users", "uk_oauth", "oauth_provider", 1, false, 100),
                      new IndexInfo("users", "uk_oauth", "oauth_sub", 2, false, 1000))));

      String sql = "SELECT * FROM users WHERE oauth_provider = ? AND oauth_sub = ?";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

      assertThat(issues).isEmpty();
    }

    @Test
    void noIssueWhenCompositeUniqueColumnsInDifferentOrder() {
      // WHERE clause order doesn't need to match index column order
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "users",
                  List.of(
                      new IndexInfo("users", "uk_oauth", "oauth_provider", 1, false, 100),
                      new IndexInfo("users", "uk_oauth", "oauth_sub", 2, false, 1000))));

      String sql = "SELECT * FROM users WHERE oauth_sub = ? AND oauth_provider = ?";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

      assertThat(issues).isEmpty();
    }

    @Test
    void noIssueWhenThreeColumnCompositeUniqueIndexFullyMatched() {
      // UNIQUE(tenant_id, region, code) — all three in WHERE
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "warehouses",
                  List.of(
                      new IndexInfo("warehouses", "uk_tenant_region_code", "tenant_id", 1, false, 10),
                      new IndexInfo("warehouses", "uk_tenant_region_code", "region", 2, false, 100),
                      new IndexInfo("warehouses", "uk_tenant_region_code", "code", 3, false, 1000))));

      String sql =
          "SELECT * FROM warehouses WHERE tenant_id = ? AND region = ? AND code = ?";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

      assertThat(issues).isEmpty();
    }

    @Test
    void flagsWhenCompositeUniqueIndexPartiallyMatched() {
      // UNIQUE(tenant_id, region, code) — only 2 of 3 columns → not guaranteed single row
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "warehouses",
                  List.of(
                      new IndexInfo("warehouses", "uk_tenant_region_code", "tenant_id", 1, false, 10),
                      new IndexInfo("warehouses", "uk_tenant_region_code", "region", 2, false, 100),
                      new IndexInfo("warehouses", "uk_tenant_region_code", "code", 3, false, 1000))));

      String sql = "SELECT * FROM warehouses WHERE tenant_id = ? AND region = ?";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

      assertThat(issues).hasSize(1);
    }

    @Test
    void noIssueWhenCompositeUniqueWithAliasedColumns() {
      // Aliased table: u.oauth_provider, u.oauth_sub
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "users",
                  List.of(
                      new IndexInfo("users", "uk_oauth", "oauth_provider", 1, false, 100),
                      new IndexInfo("users", "uk_oauth", "oauth_sub", 2, false, 1000))));

      String sql = "SELECT * FROM users u WHERE u.oauth_provider = ? AND u.oauth_sub = ?";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

      assertThat(issues).isEmpty();
    }

    @Test
    void doesNotExtractColumnsFromSubqueries() {
      // "user_id = ?" in the scalar subquery should not be counted as a main WHERE equality column.
      // UNIQUE(oauth_provider, user_id) exists, but only oauth_provider is in the outer WHERE.
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "users",
                  List.of(
                      new IndexInfo("users", "uk_provider_uid", "oauth_provider", 1, false, 100),
                      new IndexInfo("users", "uk_provider_uid", "user_id", 2, false, 1000))));

      String sql =
          "SELECT * FROM users WHERE oauth_provider = ? "
              + "AND status = (SELECT status FROM defaults WHERE user_id = ?)";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

      assertThat(issues).hasSize(1);
    }

    @Test
    void flagsWhenOrInWhereClause() {
      // OR breaks uniqueness guarantee even if all composite columns appear
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "users",
                  List.of(
                      new IndexInfo("users", "uk_oauth", "oauth_provider", 1, false, 100),
                      new IndexInfo("users", "uk_oauth", "oauth_sub", 2, false, 1000))));

      String sql =
          "SELECT * FROM users WHERE oauth_provider = ? OR oauth_sub = ?";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

      assertThat(issues).hasSize(1);
    }
  }

  // ── new tests: single equality + LIMIT 1 ───────────────────────────

  @Nested
  class SingleEqualityWithLimit1 {

    @Test
    void noIssueForArbitraryColumnWithLimit1() {
      String sql = "SELECT * FROM users WHERE phone = ? LIMIT 1";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }
  }

  // ── Issue #32: multi-column WHERE false positives ──────────────────

  @Nested
  class MultiColumnWhereFalsePositives {

    @Test
    void falsePositiveOnCompositeUniqueWhere() {
      // Issue #32: WHERE oauth_provider = ? AND oauth_sub = ? forms a composite unique key
      // but is flagged because SINGLE_EQUALITY_PATTERN requires end-of-string after first `= ?`
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "users",
                  List.of(
                      new IndexInfo("users", "uk_oauth", "oauth_provider", 1, false, 100),
                      new IndexInfo("users", "uk_oauth", "oauth_sub", 2, false, 1000))));

      String sql = "SELECT * FROM users WHERE oauth_provider = ? AND oauth_sub = ?";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

      assertThat(issues)
          .as("Composite unique index with all columns matched should not be flagged")
          .isEmpty();
    }

    @Test
    void falsePositiveOnMultiColumnWhereWithoutIndex() {
      // Even without a unique index, multi-column equality with ORDER BY + Optional return
      // pattern from Issue #32: queries returning Optional<T> are bounded
      String sql =
          "SELECT s FROM user_suspensions s "
              + "WHERE s.user_id = ? AND s.unsuspended_at IS NULL "
              + "AND (s.is_permanent = ? OR s.suspended_until > ?) "
              + "ORDER BY s.suspended_at DESC";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      // This should still be flagged (no LIMIT, no unique index match)
      // — included here to document the current behavior
      assertThat(issues).hasSize(1);
    }

    @Test
    void noFalsePositiveOnMultiColumnEqualityWithAllUniqueColumns() {
      // Two equality conditions covering all columns of a composite unique index
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "warehouse_items",
                  List.of(
                      new IndexInfo(
                          "warehouse_items", "uk_wh_item", "warehouse_id", 1, false, 50),
                      new IndexInfo(
                          "warehouse_items", "uk_wh_item", "item_code", 2, false, 500))));

      String sql =
          "SELECT * FROM warehouse_items WHERE warehouse_id = ? AND item_code = ?";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

      assertThat(issues)
          .as("All columns of composite unique index matched → should not flag")
          .isEmpty();
    }

    @Test
    void stillFlagsPartialCompositeUniqueMatch() {
      // Only one column of 2-column composite unique index is used
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "warehouse_items",
                  List.of(
                      new IndexInfo(
                          "warehouse_items", "uk_wh_item", "region", 1, false, 50),
                      new IndexInfo(
                          "warehouse_items", "uk_wh_item", "item_code", 2, false, 500))));

      String sql = "SELECT * FROM warehouse_items WHERE region = ?";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

      assertThat(issues)
          .as("Partial composite unique match does not guarantee single row")
          .hasSize(1);
    }
  }

  // ── false positive fix: EXISTS subquery ────────────────────────────

  @Nested
  class ExistsSubqueryTests {

    @Test
    void noIssueForWhereExistsSubquery() {
      String sql =
          "SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    void noIssueForWhereNotExistsSubquery() {
      String sql =
          "SELECT name FROM users u WHERE NOT EXISTS (SELECT 1 FROM orders WHERE user_id = u.id)";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    void noIssueForExistsWithJoin() {
      String sql =
          "SELECT u.name, u.email FROM users u "
              + "JOIN departments d ON u.dept_id = d.id "
              + "WHERE EXISTS (SELECT 1 FROM active_sessions s WHERE s.user_id = u.id)";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }
  }
}
