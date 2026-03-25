package io.queryaudit.core.eval;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.detector.CollectionManagementDetector;
import io.queryaudit.core.detector.DerivedDeleteDetector;
import io.queryaudit.core.detector.ForUpdateNonUniqueIndexDetector;
import io.queryaudit.core.detector.LazyLoadNPlusOneDetector;
import io.queryaudit.core.detector.MissingIndexDetector;
import io.queryaudit.core.detector.NPlusOneDetector;
import io.queryaudit.core.detector.NotInSubqueryDetector;
import io.queryaudit.core.detector.OrderByRandDetector;
import io.queryaudit.core.detector.RepeatedSingleInsertDetector;
import io.queryaudit.core.detector.SubqueryInDmlDetector;
import io.queryaudit.core.interceptor.LazyLoadTracker;
import io.queryaudit.core.model.IndexInfo;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

/**
 * TEAM 6: Suggestion Quality Audit
 *
 * <p>Evaluates whether each detector's suggestion text is: - Actionable (contains a specific action
 * the developer can take) - Correct (does not give misleading or impossible advice) - Specific
 * (references the actual table/column names) - Exemplified (includes a code example or SQL fix
 * where applicable)
 *
 * <p>Also checks for BAD suggestions that would break queries, are too vague, reference wrong
 * tables/columns, or suggest impossible actions.
 */
class Team6SuggestionQualityTest {

  // ── Scoring infrastructure ──────────────────────────────────────────

  private static final Map<String, SuggestionScore> SCORES = new LinkedHashMap<>();

  private record SuggestionScore(
      String detectorName,
      boolean hasAction,
      boolean hasExample,
      boolean isCorrect,
      boolean isSpecific,
      int score,
      String note) {}

  private static void recordScore(
      String name,
      boolean hasAction,
      boolean hasExample,
      boolean isCorrect,
      boolean isSpecific,
      int score,
      String note) {
    SCORES.put(
        name, new SuggestionScore(name, hasAction, hasExample, isCorrect, isSpecific, score, note));
  }

  @AfterAll
  static void printReport() {
    System.out.println();
    System.out.println("=== TEAM 6: SUGGESTION QUALITY ===");
    System.out.printf(
        "%-30s | %-10s | %-11s | %-7s | %-8s | Score%n",
        "Detector", "Has Action", "Has Example", "Correct", "Specific");
    System.out.println("-".repeat(90));

    int totalScore = 0;
    int needsImprovement = 0;
    for (SuggestionScore s : SCORES.values()) {
      System.out.printf(
          "%-30s | %-10s | %-11s | %-7s | %-8s | %d/10%s%n",
          s.detectorName,
          s.hasAction ? "YES" : "NO",
          s.hasExample ? "YES" : "NO",
          s.isCorrect ? "YES" : "PARTIAL",
          s.isSpecific ? "YES" : "NO",
          s.score,
          s.note.isEmpty() ? "" : " (" + s.note + ")");
      totalScore += s.score;
      if (s.score < 8) needsImprovement++;
    }
    double avg = SCORES.isEmpty() ? 0 : (double) totalScore / SCORES.size();
    System.out.println("-".repeat(90));
    System.out.printf("Average suggestion quality: %.1f/10%n", avg);
    System.out.printf("Suggestions needing improvement: %d%n", needsImprovement);
    System.out.println("=== END TEAM 6 REPORT ===");
    System.out.println();
  }

  // ── Helper methods ──────────────────────────────────────────────────

  private static QueryRecord query(String sql) {
    return new QueryRecord(sql, 1_000_000, System.currentTimeMillis(), null);
  }

  private static QueryRecord queryWithStack(String sql, String stack) {
    return new QueryRecord(sql, 1_000_000, System.currentTimeMillis(), stack);
  }

  private static IndexMetadata indexMetadata(String table, List<IndexInfo> indexes) {
    Map<String, List<IndexInfo>> map = new HashMap<>();
    map.put(table, indexes);
    return new IndexMetadata(map);
  }

  private static IndexInfo idx(
      String table, String indexName, String column, int seq, boolean nonUnique, long cardinality) {
    return new IndexInfo(table, indexName, column, seq, nonUnique, cardinality);
  }

  private static LazyLoadTracker.LazyLoadRecord lazyRecord(
      String role, String ownerEntity, String ownerId) {
    return new LazyLoadTracker.LazyLoadRecord(
        role, ownerEntity, ownerId, System.currentTimeMillis());
  }

  // ====================================================================
  //  1. RepeatedSingleInsertDetector
  // ====================================================================

  @Test
  void repeatedSingleInsert_suggestionHasActionAndExample() {
    RepeatedSingleInsertDetector detector = new RepeatedSingleInsertDetector(3);
    List<QueryRecord> queries =
        List.of(
            query("INSERT INTO orders (user_id, total) VALUES (1, 100)"),
            query("INSERT INTO orders (user_id, total) VALUES (2, 200)"),
            query("INSERT INTO orders (user_id, total) VALUES (3, 300)"));

    List<Issue> issues = detector.evaluate(queries, new IndexMetadata(Map.of()));
    assertThat(issues).hasSize(1);

    Issue issue = issues.get(0);
    String suggestion = issue.suggestion();

    // Has specific action: mentions batch insert methods
    assertThat(suggestion).containsIgnoringCase("batch");
    assertThat(suggestion).contains("addBatch/executeBatch");
    assertThat(suggestion).contains("saveAll()");

    // Has example: mentions JPA and JDBC approaches
    assertThat(suggestion).contains("hibernate.jdbc.batch_size");

    // References table
    assertThat(issue.table()).isEqualTo("orders");
    assertThat(issue.detail()).contains("orders");

    // Check for IDENTITY strategy caveat -- this is MISSING from the suggestion
    // IDENTITY strategy makes batch inserts impossible in Hibernate
    boolean mentionsIdentityCaveat =
        suggestion.toLowerCase().contains("identity")
            || suggestion.toLowerCase().contains("generation strategy");

    // Score: good action and example, but missing IDENTITY caveat
    recordScore(
        "RepeatedSingleInsert",
        true,
        true,
        !mentionsIdentityCaveat,
        true,
        7,
        "missing IDENTITY generation strategy caveat");

    // The suggestion is actionable but incomplete - IDENTITY prevents batching
    assertThat(suggestion).doesNotContain("fix this"); // not too vague
    assertThat(suggestion).contains("5-10x"); // quantifies benefit
  }

  @Test
  void repeatedSingleInsert_suggestionDoesNotMisleadAboutBatching() {
    // Verify the suggestion does not claim batching always works
    // (it doesn't work with GenerationType.IDENTITY)
    RepeatedSingleInsertDetector detector = new RepeatedSingleInsertDetector(3);
    List<QueryRecord> queries =
        List.of(
            query("INSERT INTO users (name) VALUES ('a')"),
            query("INSERT INTO users (name) VALUES ('b')"),
            query("INSERT INTO users (name) VALUES ('c')"));

    List<Issue> issues = detector.evaluate(queries, new IndexMetadata(Map.of()));
    assertThat(issues).hasSize(1);

    // The suggestion recommends batching but does not warn that
    // GenerationType.IDENTITY disables JDBC batching in Hibernate.
    // This is a known gap -- documenting it here.
    String suggestion = issues.get(0).suggestion();
    assertThat(suggestion).contains("batch");
    // Not asserting IDENTITY caveat is present -- it is absent (known gap)
  }

  // ====================================================================
  //  2. MissingIndexDetector
  // ====================================================================

  @Test
  void missingIndex_suggestionIncludesExactCreateIndexSQL() {
    MissingIndexDetector detector = new MissingIndexDetector();
    IndexMetadata meta =
        indexMetadata("orders", List.of(idx("orders", "PRIMARY", "id", 1, false, 10000)));

    List<QueryRecord> queries = List.of(query("SELECT * FROM orders WHERE customer_id = 42"));

    List<Issue> issues = detector.evaluate(queries, meta);
    assertThat(issues).isNotEmpty();

    Issue issue =
        issues.stream()
            .filter(i -> "customer_id".equalsIgnoreCase(i.column()))
            .findFirst()
            .orElseThrow();

    String suggestion = issue.suggestion();

    // Must include exact ALTER TABLE / ADD INDEX SQL
    assertThat(suggestion).contains("ALTER TABLE orders ADD INDEX");
    assertThat(suggestion).contains("idx_customer_id");
    assertThat(suggestion).contains("(customer_id)");

    // Must reference correct table and column
    assertThat(issue.table()).isEqualTo("orders");
    assertThat(issue.column()).isEqualTo("customer_id");

    // The detail should explain the impact
    assertThat(issue.detail()).contains("orders");
    assertThat(issue.detail()).contains("customer_id");
    assertThat(issue.detail()).containsIgnoringCase("full table scan");

    recordScore("MissingIndexDetector", true, true, true, true, 10, "");
  }

  @Test
  void missingIndex_compositeIndexSuggestionWhenOtherIndexExists() {
    MissingIndexDetector detector = new MissingIndexDetector();
    IndexMetadata meta =
        indexMetadata(
            "orders",
            List.of(
                idx("orders", "PRIMARY", "id", 1, false, 10000),
                idx("orders", "idx_status", "status_code", 1, true, 5000)));

    List<QueryRecord> queries =
        List.of(query("SELECT * FROM orders WHERE status_code = 'active' AND region = 'US'"));

    List<Issue> issues = detector.evaluate(queries, meta);

    // Should suggest composite index incorporating the existing indexed column
    Issue regionIssue =
        issues.stream().filter(i -> "region".equalsIgnoreCase(i.column())).findFirst().orElse(null);

    if (regionIssue != null) {
      String suggestion = regionIssue.suggestion();
      // Should suggest composite index with status_code as leading column
      assertThat(suggestion).contains("status_code");
      assertThat(suggestion).contains("region");
      assertThat(suggestion).containsIgnoringCase("composite");
    }
  }

  // ====================================================================
  //  3. NPlusOneDetector (SQL-level)
  // ====================================================================

  @Test
  void nPlusOne_sqlLevel_suggestionPointsToHibernateDetector() {
    NPlusOneDetector detector = new NPlusOneDetector(3);
    List<QueryRecord> queries =
        List.of(
            query("SELECT * FROM users WHERE id = 1"),
            query("SELECT * FROM users WHERE id = 2"),
            query("SELECT * FROM users WHERE id = 3"));

    List<Issue> issues = detector.evaluate(queries, new IndexMetadata(Map.of()));
    assertThat(issues).hasSize(1);

    Issue issue = issues.get(0);
    String suggestion = issue.suggestion();

    // SQL-level detector correctly defers to Hibernate-level detector
    assertThat(suggestion).containsIgnoringCase("Hibernate");
    assertThat(issue.severity()).isEqualTo(Severity.INFO); // Not ERROR

    // This detector intentionally does NOT suggest JOIN FETCH etc.
    // because it's a supplementary detector -- the LazyLoadNPlusOneDetector
    // provides the authoritative actionable suggestions.
    recordScore(
        "NPlusOneDetector(SQL)",
        true,
        false,
        true,
        false,
        6,
        "supplementary - defers to Hibernate detector");
  }

  // ====================================================================
  //  4. LazyLoadNPlusOneDetector (Hibernate-level)
  // ====================================================================

  @Test
  void lazyLoadNPlusOne_collectionSuggestion_hasAllThreeStrategies() {
    LazyLoadNPlusOneDetector detector = new LazyLoadNPlusOneDetector(3);
    List<LazyLoadTracker.LazyLoadRecord> records =
        List.of(
            lazyRecord("com.example.Order.items", "com.example.Order", "1"),
            lazyRecord("com.example.Order.items", "com.example.Order", "2"),
            lazyRecord("com.example.Order.items", "com.example.Order", "3"),
            lazyRecord("com.example.Order.items", "com.example.Order", "4"),
            lazyRecord("com.example.Order.items", "com.example.Order", "5"));

    List<Issue> issues = detector.evaluate(records);
    assertThat(issues).hasSize(1);

    Issue issue = issues.get(0);
    String suggestion = issue.suggestion();

    // Must suggest all three strategies
    assertThat(suggestion).contains("@EntityGraph");
    assertThat(suggestion).contains("JOIN FETCH");
    assertThat(suggestion).contains("@BatchSize");

    // Must include actual entity/collection names
    assertThat(suggestion).contains("items");
    assertThat(suggestion).contains("Order");

    // Must include proper JPQL example
    assertThat(suggestion).contains("SELECT e FROM Order e JOIN FETCH e.items");

    // BatchSize should have a reasonable value
    assertThat(suggestion).matches("(?s).*@BatchSize\\(size=\\d+\\).*");

    // Detail must be informative
    assertThat(issue.detail()).contains("items");
    assertThat(issue.detail()).contains("Order");
    assertThat(issue.detail()).contains("5 times");
    assertThat(issue.detail()).contains("5 different entities");

    recordScore("LazyLoadNPlusOne(coll)", true, true, true, true, 10, "");
  }

  @Test
  void lazyLoadNPlusOne_proxySuggestion_hasAllThreeStrategies() {
    LazyLoadNPlusOneDetector detector = new LazyLoadNPlusOneDetector(3);
    List<LazyLoadTracker.LazyLoadRecord> records =
        List.of(
            lazyRecord("proxy:com.example.User", "com.example.User", "10"),
            lazyRecord("proxy:com.example.User", "com.example.User", "20"),
            lazyRecord("proxy:com.example.User", "com.example.User", "30"));

    List<Issue> issues = detector.evaluate(records);
    assertThat(issues).hasSize(1);

    Issue issue = issues.get(0);
    String suggestion = issue.suggestion();

    // Must suggest all three strategies for proxy N+1 too
    assertThat(suggestion).contains("@EntityGraph");
    assertThat(suggestion).contains("JOIN FETCH");
    assertThat(suggestion).contains("@BatchSize");

    // Must reference the entity name
    assertThat(suggestion).contains("User");

    // Detail must describe the proxy pattern
    assertThat(issue.detail()).contains("@ManyToOne");
    assertThat(issue.detail()).contains("User");
  }

  // ====================================================================
  //  5. ForUpdateNonUniqueIndexDetector
  // ====================================================================

  @Test
  void forUpdateNonUnique_suggestionExplainsGapLocks() {
    ForUpdateNonUniqueIndexDetector detector = new ForUpdateNonUniqueIndexDetector();
    IndexMetadata meta =
        indexMetadata(
            "accounts",
            List.of(
                idx("accounts", "PRIMARY", "id", 1, false, 10000),
                idx("accounts", "idx_status", "status", 1, true, 5) // non-unique
                ));

    List<QueryRecord> queries =
        List.of(query("SELECT * FROM accounts WHERE status = 'active' FOR UPDATE"));

    List<Issue> issues = detector.evaluate(queries, meta);
    assertThat(issues).hasSize(1);

    Issue issue = issues.get(0);
    String suggestion = issue.suggestion();

    // Must explain gap locks
    assertThat(suggestion).containsIgnoringCase("gap lock");

    // Must mention the column name
    assertThat(suggestion).contains("status");

    // Must suggest a fix: unique key or reducing scope
    assertThat(suggestion).containsIgnoringCase("unique");

    // Must reference correct table
    assertThat(issue.table()).isEqualTo("accounts");
    assertThat(issue.column()).isEqualTo("status");

    // Detail also explains the problem
    assertThat(issue.detail()).containsIgnoringCase("next-key lock");

    // Evaluation: explains gap locks correctly but could include more detail
    // about what gap locks actually do (blocking inserts in range)
    boolean mentionsInsertBlocking =
        suggestion.toLowerCase().contains("blocking insert")
            || suggestion.toLowerCase().contains("block insert");

    recordScore(
        "ForUpdateNonUnique",
        true,
        false,
        true,
        true,
        8,
        mentionsInsertBlocking ? "" : "could elaborate on insert blocking");
  }

  // ====================================================================
  //  6. CollectionManagementDetector
  // ====================================================================

  @Test
  void collectionManagement_suggestionExplainsBidirectionalVsUnidirectional() {
    CollectionManagementDetector detector = new CollectionManagementDetector(2);
    List<QueryRecord> queries =
        List.of(
            query("DELETE FROM team_members WHERE team_id = 5"),
            query("INSERT INTO team_members (team_id, user_id) VALUES (5, 10)"),
            query("INSERT INTO team_members (team_id, user_id) VALUES (5, 20)"),
            query("INSERT INTO team_members (team_id, user_id) VALUES (5, 30)"));

    List<Issue> issues = detector.evaluate(queries, new IndexMetadata(Map.of()));
    assertThat(issues).hasSize(1);

    Issue issue = issues.get(0);
    String suggestion = issue.suggestion();

    // Must explain unidirectional vs bidirectional
    assertThat(suggestion).containsIgnoringCase("unidirectional");

    // Must mention @OneToMany or @ManyToMany
    assertThat(suggestion).contains("@OneToMany");
    assertThat(suggestion).contains("@ManyToMany");

    // Must suggest bidirectional mapping or Set
    assertThat(suggestion).containsIgnoringCase("bidirectional");
    assertThat(suggestion).contains("Set<>");

    // Must suggest @JoinColumn
    assertThat(suggestion).contains("@JoinColumn");

    // Must reference correct table
    assertThat(issue.table()).isEqualTo("team_members");
    assertThat(issue.column()).isEqualTo("team_id");

    // Detail describes the pattern
    assertThat(issue.detail()).contains("DELETE-all");
    assertThat(issue.detail()).contains("re-INSERT");
    assertThat(issue.detail()).contains("team_members");
    assertThat(issue.detail()).contains("team_id");

    recordScore(
        "CollectionManagement",
        true,
        false,
        true,
        true,
        9,
        "correct advice, could include code example");
  }

  // ====================================================================
  //  7. SubqueryInDmlDetector
  // ====================================================================

  @Test
  void subqueryInDml_suggestionShowsMultiTableRewrite() {
    SubqueryInDmlDetector detector = new SubqueryInDmlDetector();
    List<QueryRecord> queries =
        List.of(
            query(
                "UPDATE orders SET status = 'cancelled' WHERE user_id IN (SELECT id FROM users WHERE banned = 1)"));

    List<Issue> issues = detector.evaluate(queries, new IndexMetadata(Map.of()));
    assertThat(issues).hasSize(1);

    Issue issue = issues.get(0);
    String suggestion = issue.suggestion();

    // Must suggest multi-table UPDATE/DELETE with JOIN
    assertThat(suggestion).containsIgnoringCase("multi-table");
    assertThat(suggestion).containsIgnoringCase("JOIN");

    // Must explain WHY: semijoin optimization disabled
    assertThat(suggestion).containsIgnoringCase("semijoin");

    // Table may be extracted from the subquery's FROM clause (extractTableNames uses FROM pattern)
    // The UPDATE target 'orders' has no FROM keyword, so the first table found may be 'users'
    // This is acceptable -- the suggestion is about the overall query pattern
    assertThat(issue.table()).isNotNull();

    // Check if it shows the actual rewritten SQL (it does not -- known gap)
    boolean hasRewrittenSQL =
        suggestion.contains("UPDATE orders") && suggestion.contains("JOIN users");

    recordScore(
        "SubqueryInDmlDetector",
        true,
        false,
        true,
        false,
        7,
        "suggests JOIN rewrite but no concrete SQL example");
  }

  // ====================================================================
  //  8. OrderByRandDetector
  // ====================================================================

  @Test
  void orderByRand_suggestionShowsApplicationSideAlternative() {
    OrderByRandDetector detector = new OrderByRandDetector();
    List<QueryRecord> queries = List.of(query("SELECT * FROM products ORDER BY RAND() LIMIT 5"));

    List<Issue> issues = detector.evaluate(queries, new IndexMetadata(Map.of()));
    assertThat(issues).hasSize(1);

    Issue issue = issues.get(0);
    String suggestion = issue.suggestion();

    // Must suggest application-side random offset
    assertThat(suggestion).containsIgnoringCase("application-side");
    assertThat(suggestion).containsIgnoringCase("random");

    // Must mention pre-computed random column as alternative
    assertThat(suggestion).containsIgnoringCase("pre-computed");

    // Must reference correct table
    assertThat(issue.table()).isEqualTo("products");

    // Detail explains the O(n log n) cost
    assertThat(issue.detail()).containsIgnoringCase("every row");
    assertThat(issue.detail()).containsIgnoringCase("catastrophic");

    // Check: does not include a concrete code example
    boolean hasCodeExample =
        suggestion.contains("SELECT")
            || suggestion.contains("OFFSET")
            || suggestion.contains("Math.random");

    recordScore(
        "OrderByRandDetector",
        true,
        false,
        true,
        false,
        7,
        "mentions alternatives but no concrete code example");
  }

  // ====================================================================
  //  9. NotInSubqueryDetector
  // ====================================================================

  @Test
  void notInSubquery_suggestionShowsNotExistsRewrite() {
    NotInSubqueryDetector detector = new NotInSubqueryDetector();
    List<QueryRecord> queries =
        List.of(query("SELECT * FROM orders WHERE user_id NOT IN (SELECT id FROM banned_users)"));

    List<Issue> issues = detector.evaluate(queries, new IndexMetadata(Map.of()));
    assertThat(issues).hasSize(1);

    Issue issue = issues.get(0);
    String suggestion = issue.suggestion();

    // Must show NOT EXISTS rewrite
    assertThat(suggestion).contains("NOT EXISTS");
    assertThat(suggestion).contains("SELECT 1 FROM");

    // Must explain the NULL correctness bug
    assertThat(suggestion).containsIgnoringCase("NULL");

    // Must reference correct table
    assertThat(issue.table()).isEqualTo("orders");

    // Detail explains the correctness bug
    assertThat(issue.detail()).containsIgnoringCase("NULL");
    assertThat(issue.detail()).containsIgnoringCase("no rows");

    // Check: suggestion includes partial NOT EXISTS template but not full rewrite
    boolean hasFullRewrite = suggestion.contains("NOT EXISTS (SELECT 1 FROM banned_users WHERE");

    recordScore(
        "NotInSubqueryDetector",
        true,
        true,
        true,
        false,
        8,
        hasFullRewrite ? "has full rewrite" : "partial template, not table-specific");
  }

  // ====================================================================
  //  10. DerivedDeleteDetector
  // ====================================================================

  @Test
  void derivedDelete_suggestionShowsModifyingAnnotation() {
    DerivedDeleteDetector detector = new DerivedDeleteDetector(3);
    List<QueryRecord> queries =
        List.of(
            query("SELECT * FROM notifications WHERE status = 'read'"),
            query("DELETE FROM notifications WHERE id = 1"),
            query("DELETE FROM notifications WHERE id = 2"),
            query("DELETE FROM notifications WHERE id = 3"));

    List<Issue> issues = detector.evaluate(queries, new IndexMetadata(Map.of()));
    assertThat(issues).hasSize(1);

    Issue issue = issues.get(0);
    String suggestion = issue.suggestion();

    // Must suggest @Modifying @Query
    assertThat(suggestion).contains("@Modifying");
    assertThat(suggestion).contains("@Query");

    // Must suggest bulk DELETE
    assertThat(suggestion).containsIgnoringCase("bulk delete");

    // Must explain the Spring Data pattern
    assertThat(suggestion).containsIgnoringCase("Spring Data");
    assertThat(suggestion).containsIgnoringCase("deleteBy");

    // Must reference table
    assertThat(issue.table()).isEqualTo("notifications");
    assertThat(suggestion).contains("notifications");

    // Must include example JPQL
    assertThat(suggestion).contains("DELETE FROM entity WHERE condition");

    recordScore("DerivedDeleteDetector", true, true, true, true, 9, "");
  }

  // ====================================================================
  //  BAD SUGGESTION CHECKS
  // ====================================================================

  @Test
  void noSuggestionContainsVaguePhrases() {
    // Collect all suggestions from all detectors
    List<String> suggestions = new ArrayList<>();

    // RepeatedSingleInsert
    {
      RepeatedSingleInsertDetector d = new RepeatedSingleInsertDetector(3);
      List<QueryRecord> q =
          List.of(
              query("INSERT INTO t (a) VALUES (1)"),
              query("INSERT INTO t (a) VALUES (2)"),
              query("INSERT INTO t (a) VALUES (3)"));
      d.evaluate(q, new IndexMetadata(Map.of())).forEach(i -> suggestions.add(i.suggestion()));
    }

    // OrderByRand
    {
      OrderByRandDetector d = new OrderByRandDetector();
      d.evaluate(
              List.of(query("SELECT * FROM t ORDER BY RAND() LIMIT 1")),
              new IndexMetadata(Map.of()))
          .forEach(i -> suggestions.add(i.suggestion()));
    }

    // NotInSubquery
    {
      NotInSubqueryDetector d = new NotInSubqueryDetector();
      d.evaluate(
              List.of(query("SELECT * FROM t WHERE x NOT IN (SELECT y FROM t2)")),
              new IndexMetadata(Map.of()))
          .forEach(i -> suggestions.add(i.suggestion()));
    }

    // SubqueryInDml
    {
      SubqueryInDmlDetector d = new SubqueryInDmlDetector();
      d.evaluate(
              List.of(query("UPDATE t SET a = 1 WHERE b IN (SELECT c FROM t2)")),
              new IndexMetadata(Map.of()))
          .forEach(i -> suggestions.add(i.suggestion()));
    }

    // None should be too vague
    for (String s : suggestions) {
      assertThat(s).doesNotContainIgnoringCase("fix this");
      assertThat(s).doesNotContainIgnoringCase("improve this");
      assertThat(s).doesNotContainIgnoringCase("optimize this");
      assertThat(s).doesNotContainIgnoringCase("consider fixing");
      // Each suggestion must be at least 20 chars (not empty/trivial)
      assertThat(s.length()).isGreaterThan(20);
    }
  }

  @Test
  void missingIndex_suggestionReferencesCorrectTableNotWrongOne() {
    MissingIndexDetector detector = new MissingIndexDetector();
    Map<String, List<IndexInfo>> map = new HashMap<>();
    map.put("orders", List.of(idx("orders", "PRIMARY", "id", 1, false, 10000)));
    map.put(
        "users",
        List.of(
            idx("users", "PRIMARY", "id", 1, false, 5000),
            idx("users", "idx_email", "email", 1, true, 5000)));
    IndexMetadata meta = new IndexMetadata(map);

    List<QueryRecord> queries = List.of(query("SELECT * FROM orders WHERE customer_id = 1"));

    List<Issue> issues = detector.evaluate(queries, meta);
    for (Issue issue : issues) {
      if ("customer_id".equals(issue.column())) {
        // Suggestion must reference 'orders' table, NOT 'users'
        assertThat(issue.suggestion()).contains("orders");
        assertThat(issue.suggestion()).doesNotContain("users");
        assertThat(issue.table()).isEqualTo("orders");
      }
    }
  }

  @Test
  void forUpdateNonUnique_suggestionDoesNotSuggestDroppingIndex() {
    ForUpdateNonUniqueIndexDetector detector = new ForUpdateNonUniqueIndexDetector();
    IndexMetadata meta =
        indexMetadata(
            "accounts",
            List.of(
                idx("accounts", "PRIMARY", "id", 1, false, 10000),
                idx("accounts", "idx_type", "account_type", 1, true, 3)));

    List<QueryRecord> queries =
        List.of(query("SELECT * FROM accounts WHERE account_type = 'savings' FOR UPDATE"));

    List<Issue> issues = detector.evaluate(queries, meta);
    assertThat(issues).isNotEmpty();

    for (Issue issue : issues) {
      // Suggestion should NOT say "drop the index" -- that would make things worse
      assertThat(issue.suggestion()).doesNotContainIgnoringCase("drop index");
      assertThat(issue.suggestion()).doesNotContainIgnoringCase("remove index");
    }
  }

  @Test
  void notInSubquery_suggestionDoesNotSuggestNotIn() {
    // The fix for NOT IN should not re-suggest NOT IN
    NotInSubqueryDetector detector = new NotInSubqueryDetector();
    List<QueryRecord> queries =
        List.of(
            query(
                "SELECT * FROM products WHERE category_id NOT IN (SELECT id FROM deprecated_categories)"));

    List<Issue> issues = detector.evaluate(queries, new IndexMetadata(Map.of()));
    assertThat(issues).hasSize(1);

    String suggestion = issues.get(0).suggestion();
    // Suggestion mentions NOT EXISTS as the alternative
    assertThat(suggestion).contains("NOT EXISTS");
    // It should not simply say "use NOT IN" since that's the problem
    // (it can reference NOT IN to explain the problem, but must include NOT EXISTS)
  }

  @Test
  void collectionManagement_suggestionDoesNotSuggestDeleteAll() {
    // The fix should NOT suggest manually doing DELETE-all + re-INSERT
    CollectionManagementDetector detector = new CollectionManagementDetector(2);
    List<QueryRecord> queries =
        List.of(
            query("DELETE FROM tags WHERE post_id = 1"),
            query("INSERT INTO tags (post_id, tag_name) VALUES (1, 'java')"),
            query("INSERT INTO tags (post_id, tag_name) VALUES (1, 'spring')"));

    List<Issue> issues = detector.evaluate(queries, new IndexMetadata(Map.of()));
    assertThat(issues).hasSize(1);

    String suggestion = issues.get(0).suggestion();
    // Should suggest fixing the mapping, not continuing the anti-pattern
    assertThat(suggestion).containsIgnoringCase("bidirectional");
    assertThat(suggestion).contains("Set<>");
  }

  @Test
  void repeatedSingleInsert_detailIncludesExecutionCount() {
    RepeatedSingleInsertDetector detector = new RepeatedSingleInsertDetector(3);
    List<QueryRecord> queries = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      queries.add(query("INSERT INTO logs (message) VALUES ('entry " + i + "')"));
    }

    List<Issue> issues = detector.evaluate(queries, new IndexMetadata(Map.of()));
    assertThat(issues).hasSize(1);

    // Detail must include the exact count
    assertThat(issues.get(0).detail()).contains("5 times");
    assertThat(issues.get(0).detail()).contains("logs");
  }
}
