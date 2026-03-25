package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.interceptor.LazyLoadTracker;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.Severity;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link LazyLoadNPlusOneDetector}.
 *
 * <p>This detector analyzes Hibernate lazy collection initialization records. It is the
 * authoritative source for N+1 detection -- it produces ERROR severity issues when the same
 * collection role is initialized for multiple different owner entities, which is a guaranteed N+1
 * pattern.
 */
class LazyLoadNPlusOneDetectorTest {

  private static LazyLoadTracker.LazyLoadRecord record(
      String role, String ownerEntity, String ownerId) {
    return new LazyLoadTracker.LazyLoadRecord(
        role, ownerEntity, ownerId, System.currentTimeMillis());
  }

  // ====================================================================
  //  Test 1: 5 lazy loads of same role with different owner IDs -> ERROR
  // ====================================================================

  @Test
  void fiveLazyLoads_differentOwners_shouldBeError() {
    List<LazyLoadTracker.LazyLoadRecord> records =
        List.of(
            record("com.example.Team.members", "com.example.Team", "1"),
            record("com.example.Team.members", "com.example.Team", "2"),
            record("com.example.Team.members", "com.example.Team", "3"),
            record("com.example.Team.members", "com.example.Team", "4"),
            record("com.example.Team.members", "com.example.Team", "5"));

    LazyLoadNPlusOneDetector detector = new LazyLoadNPlusOneDetector(3);
    List<Issue> issues = detector.evaluate(records);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.N_PLUS_ONE);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.ERROR);
    assertThat(issues.get(0).detail()).contains("members");
    assertThat(issues.get(0).detail()).contains("Team");
    assertThat(issues.get(0).detail()).contains("5 times");
    assertThat(issues.get(0).detail()).contains("5 different entities");
    assertThat(issues.get(0).suggestion()).contains("@EntityGraph");
    assertThat(issues.get(0).suggestion()).contains("JOIN FETCH");
    assertThat(issues.get(0).suggestion()).contains("@BatchSize");
  }

  // ====================================================================
  //  Test 2: 2 lazy loads (below threshold) -> no issue
  // ====================================================================

  @Test
  void twoLazyLoads_belowThreshold_noIssue() {
    List<LazyLoadTracker.LazyLoadRecord> records =
        List.of(
            record("com.example.Team.members", "com.example.Team", "1"),
            record("com.example.Team.members", "com.example.Team", "2"));

    LazyLoadNPlusOneDetector detector = new LazyLoadNPlusOneDetector(3);
    List<Issue> issues = detector.evaluate(records);

    assertThat(issues).isEmpty();
  }

  // ====================================================================
  //  Test 3: Same role loaded 5 times for same owner ID -> no issue
  // ====================================================================

  @Test
  void sameOwerId_reloaded_noIssue() {
    List<LazyLoadTracker.LazyLoadRecord> records =
        List.of(
            record("com.example.Team.members", "com.example.Team", "42"),
            record("com.example.Team.members", "com.example.Team", "42"),
            record("com.example.Team.members", "com.example.Team", "42"),
            record("com.example.Team.members", "com.example.Team", "42"),
            record("com.example.Team.members", "com.example.Team", "42"));

    LazyLoadNPlusOneDetector detector = new LazyLoadNPlusOneDetector(3);
    List<Issue> issues = detector.evaluate(records);

    assertThat(issues).isEmpty();
  }

  // ====================================================================
  //  Test 4: Different roles -> separate detection per role
  // ====================================================================

  @Test
  void differentRoles_separateDetection() {
    List<LazyLoadTracker.LazyLoadRecord> records = new ArrayList<>();

    // Role 1: Team.members - 4 different owners (above threshold)
    for (int i = 1; i <= 4; i++) {
      records.add(record("com.example.Team.members", "com.example.Team", String.valueOf(i)));
    }

    // Role 2: Order.items - 5 different owners (above threshold)
    for (int i = 1; i <= 5; i++) {
      records.add(record("com.example.Order.items", "com.example.Order", String.valueOf(i)));
    }

    // Role 3: User.addresses - 2 different owners (below threshold)
    records.add(record("com.example.User.addresses", "com.example.User", "1"));
    records.add(record("com.example.User.addresses", "com.example.User", "2"));

    LazyLoadNPlusOneDetector detector = new LazyLoadNPlusOneDetector(3);
    List<Issue> issues = detector.evaluate(records);

    assertThat(issues).hasSize(2);
    assertThat(issues).allMatch(i -> i.severity() == Severity.ERROR);
    assertThat(issues).allMatch(i -> i.type() == IssueType.N_PLUS_ONE);

    List<String> tables = issues.stream().map(Issue::table).toList();
    assertThat(tables).containsExactlyInAnyOrder("members", "items");
  }

  // ====================================================================
  //  Test 5: Default threshold (3)
  // ====================================================================

  @Test
  void defaultThreshold_exactlyAtThreshold_shouldDetect() {
    List<LazyLoadTracker.LazyLoadRecord> records =
        List.of(
            record("com.example.Team.members", "com.example.Team", "1"),
            record("com.example.Team.members", "com.example.Team", "2"),
            record("com.example.Team.members", "com.example.Team", "3"));

    LazyLoadNPlusOneDetector detector = new LazyLoadNPlusOneDetector();
    List<Issue> issues = detector.evaluate(records);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.ERROR);
  }

  // ====================================================================
  //  Test 6: Empty records -> no issues
  // ====================================================================

  @Test
  void emptyRecords_noIssues() {
    LazyLoadNPlusOneDetector detector = new LazyLoadNPlusOneDetector();
    List<Issue> issues = detector.evaluate(List.of());

    assertThat(issues).isEmpty();
  }

  // ====================================================================
  //  Test 7: Mixed unique and duplicate owner IDs
  // ====================================================================

  @Test
  void mixedUniqueAndDuplicateOwnerIds() {
    // 3 unique owners + 2 duplicates = 5 total loads, 3 unique
    List<LazyLoadTracker.LazyLoadRecord> records =
        List.of(
            record("com.example.Team.members", "com.example.Team", "1"),
            record("com.example.Team.members", "com.example.Team", "1"), // duplicate
            record("com.example.Team.members", "com.example.Team", "2"),
            record("com.example.Team.members", "com.example.Team", "3"),
            record("com.example.Team.members", "com.example.Team", "3") // duplicate
            );

    LazyLoadNPlusOneDetector detector = new LazyLoadNPlusOneDetector(3);
    List<Issue> issues = detector.evaluate(records);

    // 3 unique owner IDs >= threshold -> detected
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.ERROR);
    assertThat(issues.get(0).detail()).contains("5 times");
    assertThat(issues.get(0).detail()).contains("3 different entities");
  }

  // ====================================================================
  //  Test 8: Query field contains the collection role
  // ====================================================================

  @Test
  void queryField_containsLazyLoadPrefix() {
    List<LazyLoadTracker.LazyLoadRecord> records =
        List.of(
            record("com.example.Team.members", "com.example.Team", "1"),
            record("com.example.Team.members", "com.example.Team", "2"),
            record("com.example.Team.members", "com.example.Team", "3"));

    LazyLoadNPlusOneDetector detector = new LazyLoadNPlusOneDetector();
    List<Issue> issues = detector.evaluate(records);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).query()).isEqualTo("Lazy load: com.example.Team.members");
  }

  // ====================================================================
  //  Test 9: Table name extraction from role
  // ====================================================================

  @Test
  void tableExtraction_usesCollectionName() {
    List<LazyLoadTracker.LazyLoadRecord> records =
        List.of(
            record("com.example.Order.items", "com.example.Order", "1"),
            record("com.example.Order.items", "com.example.Order", "2"),
            record("com.example.Order.items", "com.example.Order", "3"));

    LazyLoadNPlusOneDetector detector = new LazyLoadNPlusOneDetector();
    List<Issue> issues = detector.evaluate(records);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).table()).isEqualTo("items");
  }

  // ====================================================================
  //  @ManyToOne / @OneToOne proxy N+1 tests
  // ====================================================================

  private static LazyLoadTracker.LazyLoadRecord proxyRecord(String entityFqcn, String entityId) {
    return new LazyLoadTracker.LazyLoadRecord(
        LazyLoadTracker.PROXY_ROLE_PREFIX + entityFqcn,
        entityFqcn,
        entityId,
        System.currentTimeMillis());
  }

  // ====================================================================
  //  Test 10: @ManyToOne proxy N+1 - 5 different User entities via proxy
  // ====================================================================

  @Test
  void proxyNPlusOne_fiveDifferentEntities_shouldBeError() {
    List<LazyLoadTracker.LazyLoadRecord> records =
        List.of(
            proxyRecord("com.example.User", "1"),
            proxyRecord("com.example.User", "2"),
            proxyRecord("com.example.User", "3"),
            proxyRecord("com.example.User", "4"),
            proxyRecord("com.example.User", "5"));

    LazyLoadNPlusOneDetector detector = new LazyLoadNPlusOneDetector(3);
    List<Issue> issues = detector.evaluate(records);

    assertThat(issues).hasSize(1);
    Issue issue = issues.get(0);
    assertThat(issue.type()).isEqualTo(IssueType.N_PLUS_ONE);
    assertThat(issue.severity()).isEqualTo(Severity.ERROR);
    assertThat(issue.detail()).contains("@ManyToOne/@OneToOne proxy");
    assertThat(issue.detail()).contains("User");
    assertThat(issue.detail()).contains("5 times");
    assertThat(issue.detail()).contains("5 different IDs");
    assertThat(issue.query()).isEqualTo("Lazy load: proxy:com.example.User");
    assertThat(issue.table()).isEqualTo("User");
    assertThat(issue.suggestion()).contains("@EntityGraph");
    assertThat(issue.suggestion()).contains("JOIN FETCH");
    assertThat(issue.suggestion()).contains("@BatchSize");
  }

  // ====================================================================
  //  Test 11: Proxy below threshold -> no issue
  // ====================================================================

  @Test
  void proxyLoads_belowThreshold_noIssue() {
    List<LazyLoadTracker.LazyLoadRecord> records =
        List.of(proxyRecord("com.example.User", "1"), proxyRecord("com.example.User", "2"));

    LazyLoadNPlusOneDetector detector = new LazyLoadNPlusOneDetector(3);
    List<Issue> issues = detector.evaluate(records);

    assertThat(issues).isEmpty();
  }

  // ====================================================================
  //  Test 12: Same proxy entity loaded multiple times for same ID -> no issue
  // ====================================================================

  @Test
  void proxyLoads_sameId_noIssue() {
    List<LazyLoadTracker.LazyLoadRecord> records =
        List.of(
            proxyRecord("com.example.User", "42"),
            proxyRecord("com.example.User", "42"),
            proxyRecord("com.example.User", "42"),
            proxyRecord("com.example.User", "42"),
            proxyRecord("com.example.User", "42"));

    LazyLoadNPlusOneDetector detector = new LazyLoadNPlusOneDetector(3);
    List<Issue> issues = detector.evaluate(records);

    assertThat(issues).isEmpty();
  }

  // ====================================================================
  //  Test 13: @OneToOne proxy N+1 - same pattern as @ManyToOne
  // ====================================================================

  @Test
  void oneToOneProxy_NPlusOne_shouldBeError() {
    List<LazyLoadTracker.LazyLoadRecord> records =
        List.of(
            proxyRecord("com.example.UserProfile", "10"),
            proxyRecord("com.example.UserProfile", "20"),
            proxyRecord("com.example.UserProfile", "30"));

    LazyLoadNPlusOneDetector detector = new LazyLoadNPlusOneDetector(3);
    List<Issue> issues = detector.evaluate(records);

    assertThat(issues).hasSize(1);
    Issue issue = issues.get(0);
    assertThat(issue.type()).isEqualTo(IssueType.N_PLUS_ONE);
    assertThat(issue.severity()).isEqualTo(Severity.ERROR);
    assertThat(issue.detail()).contains("UserProfile");
    assertThat(issue.detail()).contains("@ManyToOne/@OneToOne proxy");
  }

  // ====================================================================
  //  Test 14: Mixed proxy and collection N+1 -> both detected separately
  // ====================================================================

  @Test
  void mixedProxyAndCollection_bothDetected() {
    List<LazyLoadTracker.LazyLoadRecord> records = new ArrayList<>();

    // Collection N+1: Team.members
    for (int i = 1; i <= 4; i++) {
      records.add(record("com.example.Team.members", "com.example.Team", String.valueOf(i)));
    }

    // Proxy N+1: User loaded via @ManyToOne
    for (int i = 1; i <= 5; i++) {
      records.add(proxyRecord("com.example.User", String.valueOf(i)));
    }

    LazyLoadNPlusOneDetector detector = new LazyLoadNPlusOneDetector(3);
    List<Issue> issues = detector.evaluate(records);

    assertThat(issues).hasSize(2);
    assertThat(issues).allMatch(i -> i.severity() == Severity.ERROR);
    assertThat(issues).allMatch(i -> i.type() == IssueType.N_PLUS_ONE);

    List<String> tables = issues.stream().map(Issue::table).toList();
    assertThat(tables).containsExactlyInAnyOrder("members", "User");

    // Verify proxy issue has proxy-specific detail
    Issue proxyIssue =
        issues.stream().filter(i -> i.query().contains("proxy:")).findFirst().orElseThrow();
    assertThat(proxyIssue.detail()).contains("@ManyToOne/@OneToOne proxy");

    // Verify collection issue has collection-specific detail
    Issue collectionIssue =
        issues.stream().filter(i -> !i.query().contains("proxy:")).findFirst().orElseThrow();
    assertThat(collectionIssue.detail()).contains("Lazy collection");
  }

  // ====================================================================
  //  Test 15: Multiple proxy entity types -> separate detection per type
  // ====================================================================

  @Test
  void multipleProxyEntityTypes_separateDetection() {
    List<LazyLoadTracker.LazyLoadRecord> records = new ArrayList<>();

    // User proxy N+1 (above threshold)
    for (int i = 1; i <= 4; i++) {
      records.add(proxyRecord("com.example.User", String.valueOf(i)));
    }

    // Category proxy (below threshold)
    records.add(proxyRecord("com.example.Category", "1"));
    records.add(proxyRecord("com.example.Category", "2"));

    // Address proxy N+1 (above threshold)
    for (int i = 1; i <= 3; i++) {
      records.add(proxyRecord("com.example.Address", String.valueOf(i)));
    }

    LazyLoadNPlusOneDetector detector = new LazyLoadNPlusOneDetector(3);
    List<Issue> issues = detector.evaluate(records);

    assertThat(issues).hasSize(2);
    List<String> tables = issues.stream().map(Issue::table).toList();
    assertThat(tables).containsExactlyInAnyOrder("User", "Address");
  }

  // ====================================================================
  //  Test 16: Proxy suggestion uses lowercase field name convention
  // ====================================================================

  @Test
  void proxySuggestion_usesLowercaseFieldName() {
    List<LazyLoadTracker.LazyLoadRecord> records =
        List.of(
            proxyRecord("com.example.User", "1"),
            proxyRecord("com.example.User", "2"),
            proxyRecord("com.example.User", "3"));

    LazyLoadNPlusOneDetector detector = new LazyLoadNPlusOneDetector(3);
    List<Issue> issues = detector.evaluate(records);

    assertThat(issues).hasSize(1);
    // Suggestion should reference "e.user" (lowercase) in JOIN FETCH
    assertThat(issues.get(0).suggestion()).contains("e.user");
  }

  // ====================================================================
  //  Test 17: Role without dots -> collectionName is the entire role
  // ====================================================================

  @Test
  void roleWithoutDots_collectionNameIsEntireRole() {
    List<LazyLoadTracker.LazyLoadRecord> records =
        List.of(
            record("members", "SomeEntity", "1"),
            record("members", "SomeEntity", "2"),
            record("members", "SomeEntity", "3"));

    LazyLoadNPlusOneDetector detector = new LazyLoadNPlusOneDetector(3);
    List<Issue> issues = detector.evaluate(records);

    assertThat(issues).hasSize(1);
    Issue issue = issues.get(0);
    // When role has no dots, collectionName == role, table == role
    assertThat(issue.table()).isEqualTo("members");
    assertThat(issue.detail()).contains("members");
    // simpleClassName for ownerEntity without dots should be "SomeEntity"
    assertThat(issue.detail()).contains("SomeEntity");
  }

  // ====================================================================
  //  Test 18: Verify BatchSize suggestion value for collection issue
  // ====================================================================

  @Test
  void collectionIssue_batchSizeSuggestion_usesDoubleUniqueOwnerIds() {
    // 5 unique owners -> BatchSize should suggest min(5*2, 100) = 10
    List<LazyLoadTracker.LazyLoadRecord> records =
        List.of(
            record("com.example.Order.items", "com.example.Order", "1"),
            record("com.example.Order.items", "com.example.Order", "2"),
            record("com.example.Order.items", "com.example.Order", "3"),
            record("com.example.Order.items", "com.example.Order", "4"),
            record("com.example.Order.items", "com.example.Order", "5"));

    LazyLoadNPlusOneDetector detector = new LazyLoadNPlusOneDetector(3);
    List<Issue> issues = detector.evaluate(records);

    assertThat(issues).hasSize(1);
    // @BatchSize(size=10) -- 5 unique * 2 = 10
    assertThat(issues.get(0).suggestion()).contains("@BatchSize(size=10)");
  }

  // ====================================================================
  //  Test 19: Verify BatchSize suggestion value for proxy issue
  // ====================================================================

  @Test
  void proxyIssue_batchSizeSuggestion_usesDoubleUniqueOwnerIds() {
    // 4 unique IDs -> BatchSize should suggest min(4*2, 100) = 8
    List<LazyLoadTracker.LazyLoadRecord> records =
        List.of(
            proxyRecord("com.example.User", "1"),
            proxyRecord("com.example.User", "2"),
            proxyRecord("com.example.User", "3"),
            proxyRecord("com.example.User", "4"));

    LazyLoadNPlusOneDetector detector = new LazyLoadNPlusOneDetector(3);
    List<Issue> issues = detector.evaluate(records);

    assertThat(issues).hasSize(1);
    // @BatchSize(size=8) -- 4 unique * 2 = 8
    assertThat(issues.get(0).suggestion()).contains("@BatchSize(size=8)");
  }

  // ====================================================================
  //  Test 20: Verify exact collectionName extraction from dotted role
  // ====================================================================

  @Test
  void collectionName_extractedCorrectlyFromDottedRole() {
    List<LazyLoadTracker.LazyLoadRecord> records =
        List.of(
            record(
                "com.example.deep.nested.Entity.myCollection",
                "com.example.deep.nested.Entity",
                "1"),
            record(
                "com.example.deep.nested.Entity.myCollection",
                "com.example.deep.nested.Entity",
                "2"),
            record(
                "com.example.deep.nested.Entity.myCollection",
                "com.example.deep.nested.Entity",
                "3"));

    LazyLoadNPlusOneDetector detector = new LazyLoadNPlusOneDetector(3);
    List<Issue> issues = detector.evaluate(records);

    assertThat(issues).hasSize(1);
    Issue issue = issues.get(0);
    // collectionName should be "myCollection", not ".myCollection" or anything else
    assertThat(issue.detail()).startsWith("Lazy collection 'myCollection'");
    // EntityGraph suggestion should use "myCollection"
    assertThat(issue.suggestion()).contains("@EntityGraph(attributePaths = {\"myCollection\"})");
    // table extracted from role should be "myCollection"
    assertThat(issue.table()).isEqualTo("myCollection");
  }
}
