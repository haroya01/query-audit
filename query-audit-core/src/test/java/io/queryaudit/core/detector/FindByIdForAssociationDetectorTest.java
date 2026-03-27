package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.interceptor.LazyLoadTracker;
import io.queryaudit.core.interceptor.LazyLoadTracker.ExplicitLoadRecord;
import io.queryaudit.core.interceptor.LazyLoadTracker.LazyLoadRecord;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link FindByIdForAssociationDetector}.
 *
 * <p>This detector identifies cases where {@code findById()} is used only to set a FK association,
 * and {@code getReferenceById()} would avoid the unnecessary SELECT.
 */
class FindByIdForAssociationDetectorTest {

  private final FindByIdForAssociationDetector detector = new FindByIdForAssociationDetector();

  private static ExplicitLoadRecord explicitLoad(String entityType, String id, long timestamp) {
    return new ExplicitLoadRecord(entityType, id, timestamp, "com.example.Service.doSomething");
  }

  private static LazyLoadRecord lazyCollectionRecord(
      String role, String ownerEntity, String ownerId, long timestamp) {
    return new LazyLoadRecord(role, ownerEntity, ownerId, timestamp);
  }

  private static LazyLoadRecord lazyProxyRecord(String entityFqcn, String entityId, long timestamp) {
    return new LazyLoadRecord(
        LazyLoadTracker.PROXY_ROLE_PREFIX + entityFqcn, entityFqcn, entityId, timestamp);
  }

  private static QueryRecord insertQuery(String sql, long timestamp) {
    return new QueryRecord(sql, 1000L, timestamp, "stack");
  }

  private static QueryRecord updateQuery(String sql, long timestamp) {
    return new QueryRecord(sql, 1000L, timestamp, "stack");
  }

  // ====================================================================
  //  Test 1: findById → INSERT with FK → should detect
  // ====================================================================

  @Test
  void findById_followedByInsertWithFk_shouldDetect() {
    List<ExplicitLoadRecord> explicitLoads =
        List.of(explicitLoad("com.example.User", "42", 1000));

    List<QueryRecord> queries =
        List.of(insertQuery("INSERT INTO orders (id, user_id, amount) VALUES (?, ?, ?)", 2000));

    List<Issue> issues = detector.evaluate(explicitLoads, List.of(), queries);

    assertThat(issues).hasSize(1);
    Issue issue = issues.get(0);
    assertThat(issue.type()).isEqualTo(IssueType.FIND_BY_ID_FOR_ASSOCIATION);
    assertThat(issue.severity()).isEqualTo(Severity.INFO);
    assertThat(issue.detail()).contains("User");
    assertThat(issue.detail()).contains("getReferenceById()");
    assertThat(issue.suggestion()).contains("getReferenceById");
    assertThat(issue.table()).isEqualTo("User");
  }

  // ====================================================================
  //  Test 2: findById → UPDATE with FK → should detect
  // ====================================================================

  @Test
  void findById_followedByUpdateWithFk_shouldDetect() {
    List<ExplicitLoadRecord> explicitLoads =
        List.of(explicitLoad("com.example.Category", "7", 1000));

    List<QueryRecord> queries =
        List.of(updateQuery("UPDATE products SET category_id = ? WHERE id = ?", 2000));

    List<Issue> issues = detector.evaluate(explicitLoads, List.of(), queries);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.FIND_BY_ID_FOR_ASSOCIATION);
    assertThat(issues.get(0).table()).isEqualTo("Category");
  }

  // ====================================================================
  //  Test 3: findById + lazy collection access → no issue (entity data used)
  // ====================================================================

  @Test
  void findById_withLazyCollectionAccess_shouldNotDetect() {
    List<ExplicitLoadRecord> explicitLoads =
        List.of(explicitLoad("com.example.User", "42", 1000));

    // Lazy collection init on the same entity → data was used beyond FK
    List<LazyLoadRecord> lazyRecords =
        List.of(
            lazyCollectionRecord(
                "com.example.User.orders", "com.example.User", "42", 1500));

    List<QueryRecord> queries =
        List.of(insertQuery("INSERT INTO audit_log (user_id, action) VALUES (?, ?)", 2000));

    List<Issue> issues = detector.evaluate(explicitLoads, lazyRecords, queries);

    assertThat(issues).isEmpty();
  }

  // ====================================================================
  //  Test 4: findById + lazy proxy resolution → no issue (entity data used)
  // ====================================================================

  @Test
  void findById_withLazyProxyResolution_shouldNotDetect() {
    List<ExplicitLoadRecord> explicitLoads =
        List.of(explicitLoad("com.example.User", "42", 1000));

    // Proxy resolution for the same entity → its fields were accessed
    List<LazyLoadRecord> lazyRecords =
        List.of(lazyProxyRecord("com.example.User", "42", 1500));

    List<QueryRecord> queries =
        List.of(insertQuery("INSERT INTO orders (user_id) VALUES (?)", 2000));

    List<Issue> issues = detector.evaluate(explicitLoads, lazyRecords, queries);

    assertThat(issues).isEmpty();
  }

  // ====================================================================
  //  Test 5: findById but no subsequent DML → no issue
  // ====================================================================

  @Test
  void findById_noSubsequentDml_shouldNotDetect() {
    List<ExplicitLoadRecord> explicitLoads =
        List.of(explicitLoad("com.example.User", "42", 1000));

    // Only SELECT queries, no INSERT/UPDATE
    List<QueryRecord> queries =
        List.of(new QueryRecord("SELECT * FROM users WHERE id = ?", 1000L, 2000, "stack"));

    List<Issue> issues = detector.evaluate(explicitLoads, List.of(), queries);

    assertThat(issues).isEmpty();
  }

  // ====================================================================
  //  Test 6: findById with DML before the load → no issue (temporal ordering)
  // ====================================================================

  @Test
  void findById_dmlBeforeLoad_shouldNotDetect() {
    List<ExplicitLoadRecord> explicitLoads =
        List.of(explicitLoad("com.example.User", "42", 2000));

    // INSERT happened before the findById load
    List<QueryRecord> queries =
        List.of(insertQuery("INSERT INTO orders (user_id) VALUES (?)", 1000));

    List<Issue> issues = detector.evaluate(explicitLoads, List.of(), queries);

    assertThat(issues).isEmpty();
  }

  // ====================================================================
  //  Test 7: Empty explicit loads → no issues
  // ====================================================================

  @Test
  void emptyExplicitLoads_noIssues() {
    List<Issue> issues = detector.evaluate(List.of(), List.of(), List.of());
    assertThat(issues).isEmpty();
  }

  @Test
  void nullExplicitLoads_noIssues() {
    List<Issue> issues = detector.evaluate(null, List.of(), List.of());
    assertThat(issues).isEmpty();
  }

  // ====================================================================
  //  Test 8: Multiple entities, only one is FK-only → one issue
  // ====================================================================

  @Test
  void multipleEntities_onlyFkOnlyOneDetected() {
    List<ExplicitLoadRecord> explicitLoads =
        List.of(
            explicitLoad("com.example.User", "1", 1000),
            explicitLoad("com.example.Category", "5", 1100));

    // User has lazy access (used beyond FK), Category does not
    List<LazyLoadRecord> lazyRecords =
        List.of(
            lazyCollectionRecord(
                "com.example.User.orders", "com.example.User", "1", 1500));

    List<QueryRecord> queries =
        List.of(
            insertQuery("INSERT INTO products (user_id, category_id) VALUES (?, ?)", 2000));

    List<Issue> issues = detector.evaluate(explicitLoads, lazyRecords, queries);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).table()).isEqualTo("Category");
  }

  // ====================================================================
  //  Test 9: CamelCase FK column detection (userId instead of user_id)
  // ====================================================================

  @Test
  void findById_camelCaseFkColumn_shouldDetect() {
    List<ExplicitLoadRecord> explicitLoads =
        List.of(explicitLoad("com.example.User", "42", 1000));

    List<QueryRecord> queries =
        List.of(insertQuery("INSERT INTO orders (id, userId, amount) VALUES (?, ?, ?)", 2000));

    List<Issue> issues = detector.evaluate(explicitLoads, List.of(), queries);

    assertThat(issues).hasSize(1);
  }

  // ====================================================================
  //  Test 10: Entity with multi-word name (UserProfile → user_profile_id)
  // ====================================================================

  @Test
  void findById_multiWordEntityName_snakeCaseFk_shouldDetect() {
    List<ExplicitLoadRecord> explicitLoads =
        List.of(explicitLoad("com.example.UserProfile", "10", 1000));

    List<QueryRecord> queries =
        List.of(
            updateQuery(
                "UPDATE accounts SET user_profile_id = ? WHERE id = ?", 2000));

    List<Issue> issues = detector.evaluate(explicitLoads, List.of(), queries);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).table()).isEqualTo("UserProfile");
  }

  // ====================================================================
  //  Test 11: Lazy init on different entity/id → does not exclude current load
  // ====================================================================

  @Test
  void lazyInitOnDifferentEntity_shouldStillDetect() {
    List<ExplicitLoadRecord> explicitLoads =
        List.of(explicitLoad("com.example.User", "42", 1000));

    // Lazy init on a different entity type
    List<LazyLoadRecord> lazyRecords =
        List.of(
            lazyCollectionRecord(
                "com.example.Order.items", "com.example.Order", "99", 1500));

    List<QueryRecord> queries =
        List.of(insertQuery("INSERT INTO orders (user_id) VALUES (?)", 2000));

    List<Issue> issues = detector.evaluate(explicitLoads, lazyRecords, queries);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).table()).isEqualTo("User");
  }

  // ====================================================================
  //  Test 12: Lazy init on same type but different ID → should still detect
  // ====================================================================

  @Test
  void lazyInitSameTypeDifferentId_shouldStillDetect() {
    List<ExplicitLoadRecord> explicitLoads =
        List.of(explicitLoad("com.example.User", "42", 1000));

    // Lazy init on same entity type but different ID
    List<LazyLoadRecord> lazyRecords =
        List.of(lazyProxyRecord("com.example.User", "99", 1500));

    List<QueryRecord> queries =
        List.of(insertQuery("INSERT INTO orders (user_id) VALUES (?)", 2000));

    List<Issue> issues = detector.evaluate(explicitLoads, lazyRecords, queries);

    assertThat(issues).hasSize(1);
  }

  // ====================================================================
  //  Test 13: Issue contains source location from stack trace
  // ====================================================================

  @Test
  void issue_containsSourceLocation() {
    ExplicitLoadRecord load =
        new ExplicitLoadRecord(
            "com.example.User", "42", 1000, "com.example.OrderService.createOrder(OrderService.java:45)");

    List<QueryRecord> queries =
        List.of(insertQuery("INSERT INTO orders (user_id) VALUES (?)", 2000));

    List<Issue> issues = detector.evaluate(List.of(load), List.of(), queries);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).sourceLocation()).contains("OrderService");
  }

  // ====================================================================
  //  Test 14: Issue query field contains entity info
  // ====================================================================

  @Test
  void issue_queryField_containsEntityInfo() {
    List<ExplicitLoadRecord> explicitLoads =
        List.of(explicitLoad("com.example.User", "42", 1000));

    List<QueryRecord> queries =
        List.of(insertQuery("INSERT INTO orders (user_id) VALUES (?)", 2000));

    List<Issue> issues = detector.evaluate(explicitLoads, List.of(), queries);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).query()).isEqualTo("findById: com.example.User#42");
  }

  // ====================================================================
  //  Test 15: DML at exactly the same timestamp as load → not detected
  // ====================================================================

  @Test
  void dmlAtSameTimestampAsLoad_shouldNotDetect() {
    List<ExplicitLoadRecord> explicitLoads =
        List.of(explicitLoad("com.example.User", "42", 1000));

    // INSERT at same timestamp — not "after" the load
    List<QueryRecord> queries =
        List.of(insertQuery("INSERT INTO orders (user_id) VALUES (?)", 1000));

    List<Issue> issues = detector.evaluate(explicitLoads, List.of(), queries);

    assertThat(issues).isEmpty();
  }

  // ====================================================================
  //  Test 16: Duplicate findById for same entity+id → only one issue
  // ====================================================================

  @Test
  void duplicateFindById_sameEntityAndId_onlyOneIssue() {
    List<ExplicitLoadRecord> explicitLoads =
        List.of(
            explicitLoad("com.example.User", "42", 1000),
            explicitLoad("com.example.User", "42", 1100));

    List<QueryRecord> queries =
        List.of(insertQuery("INSERT INTO orders (user_id) VALUES (?)", 2000));

    List<Issue> issues = detector.evaluate(explicitLoads, List.of(), queries);

    assertThat(issues).hasSize(1);
  }

  // ====================================================================
  //  Test 17: Substring FK name should NOT match (abuser_id ≠ user_id)
  // ====================================================================

  @Test
  void substringFkColumn_shouldNotFalsePositive() {
    List<ExplicitLoadRecord> explicitLoads =
        List.of(explicitLoad("com.example.User", "42", 1000));

    // "abuser_id" contains "user_id" as substring but is a different column
    List<QueryRecord> queries =
        List.of(insertQuery("INSERT INTO reports (abuser_id, accuser_id) VALUES (?, ?)", 2000));

    List<Issue> issues = detector.evaluate(explicitLoads, List.of(), queries);

    assertThat(issues).isEmpty();
  }

  // ====================================================================
  //  Test 18: FK column with backtick/quote quoting → should detect
  // ====================================================================

  @Test
  void findById_quotedFkColumn_shouldDetect() {
    List<ExplicitLoadRecord> explicitLoads =
        List.of(explicitLoad("com.example.User", "42", 1000));

    List<QueryRecord> queries =
        List.of(insertQuery("INSERT INTO orders (`user_id`, `amount`) VALUES (?, ?)", 2000));

    List<Issue> issues = detector.evaluate(explicitLoads, List.of(), queries);

    assertThat(issues).hasSize(1);
  }

  // ====================================================================
  //  Test 19: FK as first column in SQL (no preceding word char)
  // ====================================================================

  @Test
  void findById_fkAsFirstColumnInParens_shouldDetect() {
    List<ExplicitLoadRecord> explicitLoads =
        List.of(explicitLoad("com.example.User", "42", 1000));

    List<QueryRecord> queries =
        List.of(insertQuery("INSERT INTO orders (user_id) VALUES (?)", 2000));

    List<Issue> issues = detector.evaluate(explicitLoads, List.of(), queries);

    assertThat(issues).hasSize(1);
  }

  // ====================================================================
  //  Test 20: UPDATE SET user_id = ? with word boundary
  // ====================================================================

  @Test
  void findById_updateSetFk_shouldDetect() {
    List<ExplicitLoadRecord> explicitLoads =
        List.of(explicitLoad("com.example.User", "42", 1000));

    List<QueryRecord> queries =
        List.of(updateQuery("UPDATE orders SET user_id = ? WHERE id = ?", 2000));

    List<Issue> issues = detector.evaluate(explicitLoads, List.of(), queries);

    assertThat(issues).hasSize(1);
  }
}
