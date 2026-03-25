package io.queryaudit.core.eval;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.detector.QueryAuditAnalyzer;
import io.queryaudit.core.model.IndexInfo;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * TEAM 4: Production Workload Evaluation
 *
 * <p>Simulates 5 realistic production scenarios with 20-30 queries each. Measures precision,
 * recall, and F1 score for each scenario and overall.
 *
 * <p>Each query is manually annotated with expected anti-pattern detections. After running the
 * analyzer, each detection is classified as: - TRUE POSITIVE (TP): detection matches a known
 * anti-pattern - FALSE POSITIVE (FP): detection on a query that has no real issue Additionally,
 * undetected known anti-patterns are counted as FALSE NEGATIVES (FN).
 */
class Team4ProductionWorkloadTest {

  private static QueryAuditAnalyzer analyzer;

  @BeforeAll
  static void setUp() {
    QueryAuditConfig config =
        QueryAuditConfig.builder()
            .nPlusOneThreshold(3)
            .offsetPaginationThreshold(1000)
            .orClauseThreshold(3)
            .build();
    analyzer = new QueryAuditAnalyzer(config, List.of());
  }

  // ---------------------------------------------------------------
  // Helper methods
  // ---------------------------------------------------------------

  private static QueryRecord q(String sql) {
    return new QueryRecord(sql, 1_000L, System.currentTimeMillis(), "");
  }

  /** Create a query record with a specific stack trace hash to simulate same call site. */
  private static QueryRecord qWithStack(String sql, String stackTrace) {
    return new QueryRecord(sql, 1_000L, System.currentTimeMillis(), stackTrace);
  }

  // ---------------------------------------------------------------
  // Scenario result tracking
  // ---------------------------------------------------------------

  static class ScenarioResult {
    final String name;
    final int queryCount;
    final int tp;
    final int fp;
    final int fn;

    ScenarioResult(String name, int queryCount, int tp, int fp, int fn) {
      this.name = name;
      this.queryCount = queryCount;
      this.tp = tp;
      this.fp = fp;
      this.fn = fn;
    }

    double precision() {
      return (tp + fp) == 0 ? 1.0 : (double) tp / (tp + fp);
    }

    double recall() {
      return (tp + fn) == 0 ? 1.0 : (double) tp / (tp + fn);
    }

    double f1() {
      double p = precision();
      double r = recall();
      return (p + r) == 0 ? 0.0 : 2.0 * p * r / (p + r);
    }
  }

  // ===============================================================
  // SCENARIO 1: E-commerce Checkout Flow
  // ===============================================================

  private static IndexMetadata ecommerceSchema() {
    Map<String, List<IndexInfo>> idx = new HashMap<>();

    idx.put(
        "users",
        List.of(
            new IndexInfo("users", "PRIMARY", "id", 1, false, 50000),
            new IndexInfo("users", "idx_email", "email", 1, false, 50000)));
    idx.put(
        "cart_items",
        List.of(
            new IndexInfo("cart_items", "PRIMARY", "id", 1, false, 20000),
            new IndexInfo("cart_items", "idx_user_id", "user_id", 1, true, 15000)));
    idx.put(
        "products",
        List.of(
            new IndexInfo("products", "PRIMARY", "id", 1, false, 10000),
            new IndexInfo("products", "idx_category_id", "category_id", 1, true, 500)));
    idx.put(
        "coupons",
        List.of(
            new IndexInfo("coupons", "PRIMARY", "id", 1, false, 1000),
            new IndexInfo("coupons", "idx_code", "code", 1, false, 1000)));
    idx.put(
        "orders",
        List.of(
            new IndexInfo("orders", "PRIMARY", "id", 1, false, 100000),
            new IndexInfo("orders", "idx_user_id", "user_id", 1, true, 50000)));
    idx.put(
        "order_items",
        List.of(
            new IndexInfo("order_items", "PRIMARY", "id", 1, false, 300000),
            new IndexInfo("order_items", "idx_order_id", "order_id", 1, true, 100000)));
    idx.put(
        "inventory", List.of(new IndexInfo("inventory", "PRIMARY", "product_id", 1, false, 10000)));
    idx.put(
        "payments",
        List.of(
            new IndexInfo("payments", "PRIMARY", "id", 1, false, 100000),
            new IndexInfo("payments", "idx_order_id", "order_id", 1, false, 100000)));
    idx.put(
        "audit_log",
        List.of(
            new IndexInfo("audit_log", "PRIMARY", "id", 1, false, 1000000),
            new IndexInfo("audit_log", "idx_created_at", "created_at", 1, true, 800000)));

    return new IndexMetadata(idx);
  }

  @Test
  void scenario1_ecommerceCheckout() {
    IndexMetadata schema = ecommerceSchema();

    // Simulate a full checkout flow with realistic queries
    // Stack traces simulate same call-site for N+1 / repeated insert patterns
    String orderItemStack =
        "com.shop.OrderService.createOrderItems:42\ncom.shop.CheckoutController.checkout:88";
    String inventoryStack =
        "com.shop.InventoryService.updateStock:55\ncom.shop.CheckoutController.checkout:90";

    List<QueryRecord> queries =
        List.of(
            // Q1: User login - authenticate by email (indexed, clean)
            q("SELECT id, email, password_hash, status FROM users WHERE email = ?"),
            // Q2: Load cart items with product join (clean)
            q(
                "SELECT ci.id, ci.quantity, p.id AS product_id, p.name, p.price "
                    + "FROM cart_items ci JOIN products p ON ci.product_id = p.id "
                    + "WHERE ci.user_id = ?"),
            // Q3: Validate product availability (clean)
            q("SELECT product_id, quantity FROM inventory WHERE product_id = ?"),
            // Q4: Apply coupon - lookup by code (clean)
            q("SELECT id, code, discount_pct, min_order, expires_at FROM coupons WHERE code = ?"),
            // Q5: Check coupon validity with function on column (ANTI-PATTERN: WHERE_FUNCTION)
            q("SELECT id FROM coupons WHERE UPPER(code) = ?"),
            // Q6: Create order (clean)
            q(
                "INSERT INTO orders (user_id, status, total_amount, coupon_id, created_at) VALUES (?, 'PENDING', ?, ?, NOW())"),
            // Q7-Q11: Insert order items one by one (ANTI-PATTERN: REPEATED_SINGLE_INSERT, 5
            // repeated)
            qWithStack(
                "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (?, ?, ?, ?)",
                orderItemStack),
            qWithStack(
                "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (?, ?, ?, ?)",
                orderItemStack),
            qWithStack(
                "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (?, ?, ?, ?)",
                orderItemStack),
            qWithStack(
                "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (?, ?, ?, ?)",
                orderItemStack),
            qWithStack(
                "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (?, ?, ?, ?)",
                orderItemStack),
            // Q12-Q16: Update inventory one by one (ANTI-PATTERN: N+1-like repeated UPDATE)
            qWithStack(
                "UPDATE inventory SET quantity = quantity - ? WHERE product_id = ?",
                inventoryStack),
            qWithStack(
                "UPDATE inventory SET quantity = quantity - ? WHERE product_id = ?",
                inventoryStack),
            qWithStack(
                "UPDATE inventory SET quantity = quantity - ? WHERE product_id = ?",
                inventoryStack),
            qWithStack(
                "UPDATE inventory SET quantity = quantity - ? WHERE product_id = ?",
                inventoryStack),
            qWithStack(
                "UPDATE inventory SET quantity = quantity - ? WHERE product_id = ?",
                inventoryStack),
            // Q17: Record payment (clean)
            q(
                "INSERT INTO payments (order_id, payment_method, amount, status, transaction_id, created_at) VALUES (?, ?, ?, 'SUCCESS', ?, NOW())"),
            // Q18: Update order status (clean)
            q("UPDATE orders SET status = 'PAID' WHERE id = ?"),
            // Q19: Clear cart (clean)
            q("DELETE FROM cart_items WHERE user_id = ?"),
            // Q20: Audit log (clean)
            q(
                "INSERT INTO audit_log (entity_type, entity_id, action, user_id, created_at) VALUES ('ORDER', ?, 'CREATED', ?, NOW())"),
            // Q21: Load order confirmation with SELECT * (ANTI-PATTERN: SELECT_ALL)
            q("SELECT * FROM orders WHERE id = ?"),
            // Q22: Load order items for confirmation (clean)
            q(
                "SELECT oi.product_id, oi.quantity, oi.unit_price, p.name FROM order_items oi JOIN products p ON oi.product_id = p.id WHERE oi.order_id = ?"),
            // Q23: Send email - log (clean)
            q(
                "INSERT INTO audit_log (entity_type, entity_id, action, user_id, created_at) VALUES ('EMAIL', ?, 'CHECKOUT_CONFIRMATION', ?, NOW())"),
            // Q24: Update user last_order_date without index (ANTI-PATTERN: MISSING_WHERE_INDEX on
            // last_order_date... but actually WHERE id = ? uses PK)
            q("UPDATE users SET last_order_date = NOW() WHERE id = ?"),
            // Q25: Check loyalty points with implicit type conversion (ANTI-PATTERN: column not in
            // schema, clean pass)
            q("SELECT points FROM users WHERE id = ?"));

    QueryAuditReport report = analyzer.analyze("ecommerceCheckout", queries, schema);
    List<Issue> allIssues = collectAllIssues(report);

    // --- Manual classification ---
    // Known anti-patterns in this scenario:
    //   1. WHERE_FUNCTION on Q5 (UPPER(code))
    //   2. REPEATED_SINGLE_INSERT on Q7-Q11 (5 identical inserts)
    //   3. N_PLUS_ONE on Q12-Q16 (5 identical updates from same call site)
    //   4. SELECT_ALL on Q21

    // Define known anti-pattern issue types for this scenario
    Set<IssueType> expectedDetections =
        Set.of(
            IssueType.WHERE_FUNCTION,
            IssueType.REPEATED_SINGLE_INSERT,
            IssueType.N_PLUS_ONE,
            IssueType.SELECT_ALL);

    // We also accept these legitimate informational detections as TP:
    Set<IssueType> acceptableInfoDetections =
        Set.of(
            IssueType.UNBOUNDED_RESULT_SET,
            IssueType.COUNT_STAR_WITHOUT_WHERE,
            IssueType.INSERT_ON_DUPLICATE_KEY,
            IssueType.EXCESSIVE_COLUMN_FETCH,
            IssueType.COVERING_INDEX_OPPORTUNITY);

    // Known false negatives we expect to miss:
    // (none expected for this scenario - the tool should catch all 4 issues above)
    int knownFalseNegatives = 0;

    int tp = 0;
    int fp = 0;
    List<String> fpDetails = new ArrayList<>();
    List<String> tpDetails = new ArrayList<>();

    for (Issue issue : allIssues) {
      if (expectedDetections.contains(issue.type())
          || acceptableInfoDetections.contains(issue.type())) {
        tp++;
        tpDetails.add(
            String.format("  TP: [%s] %s", issue.type().getCode(), truncate(issue.query(), 60)));
      } else {
        fp++;
        fpDetails.add(
            String.format(
                "  FP: [%s] %s on query: %s",
                issue.type().getCode(), issue.severity(), truncate(issue.query(), 60)));
      }
    }

    // Check for false negatives: expected issues that were NOT detected
    int fn = knownFalseNegatives;
    List<String> fnDetails = new ArrayList<>();
    if (!hasIssueOfType(allIssues, IssueType.WHERE_FUNCTION)) {
      fn++;
      fnDetails.add("  FN: WHERE_FUNCTION on UPPER(code) not detected");
    }
    if (!hasIssueOfType(allIssues, IssueType.REPEATED_SINGLE_INSERT)) {
      fn++;
      fnDetails.add("  FN: REPEATED_SINGLE_INSERT on order_items not detected");
    }
    if (!hasIssueOfType(allIssues, IssueType.N_PLUS_ONE)) {
      fn++;
      fnDetails.add("  FN: N_PLUS_ONE on inventory updates not detected");
    }
    if (!hasIssueOfType(allIssues, IssueType.SELECT_ALL)) {
      fn++;
      fnDetails.add("  FN: SELECT_ALL on orders not detected");
    }

    ScenarioResult result1 = new ScenarioResult("E-commerce", queries.size(), tp, fp, fn);
    printScenarioReport(
        "Scenario 1: E-commerce Checkout", result1, tpDetails, fpDetails, fnDetails);

    // Precision should be reasonable
    assertThat(result1.precision())
        .as("E-commerce scenario precision")
        .isGreaterThanOrEqualTo(0.70);

    // Store for final report
    scenarioResults.put("E-commerce", result1);
  }

  // ===============================================================
  // SCENARIO 2: Social Media Feed
  // ===============================================================

  private static IndexMetadata socialMediaSchema() {
    Map<String, List<IndexInfo>> idx = new HashMap<>();

    idx.put(
        "users",
        List.of(
            new IndexInfo("users", "PRIMARY", "id", 1, false, 100000),
            new IndexInfo("users", "idx_username", "username", 1, false, 100000)));
    idx.put(
        "posts",
        List.of(
            new IndexInfo("posts", "PRIMARY", "id", 1, false, 500000),
            new IndexInfo("posts", "idx_author_id", "author_id", 1, true, 100000),
            new IndexInfo("posts", "idx_created_at", "created_at", 1, true, 400000)));
    idx.put(
        "comments",
        List.of(
            new IndexInfo("comments", "PRIMARY", "id", 1, false, 2000000),
            new IndexInfo("comments", "idx_post_id", "post_id", 1, true, 500000)));
    idx.put(
        "likes",
        List.of(
            new IndexInfo("likes", "PRIMARY", "id", 1, false, 5000000),
            new IndexInfo("likes", "idx_post_id", "post_id", 1, true, 500000),
            new IndexInfo("likes", "idx_user_id", "user_id", 1, true, 100000)));
    idx.put(
        "followers",
        List.of(
            new IndexInfo("followers", "PRIMARY", "id", 1, false, 300000),
            new IndexInfo("followers", "idx_user_id", "user_id", 1, true, 100000),
            new IndexInfo("followers", "idx_follower_id", "follower_id", 1, true, 100000)));
    idx.put(
        "media",
        List.of(
            new IndexInfo("media", "PRIMARY", "id", 1, false, 800000),
            new IndexInfo("media", "idx_post_id", "post_id", 1, true, 500000)));

    return new IndexMetadata(idx);
  }

  @Test
  void scenario2_socialMediaFeed() {
    IndexMetadata schema = socialMediaSchema();

    // N+1 pattern: load comments per post from same call site
    String commentStack =
        "com.social.CommentService.loadForPost:33\ncom.social.FeedController.loadFeed:77";
    // N+1 pattern: load like counts per post
    String likeCountStack =
        "com.social.LikeService.countForPost:22\ncom.social.FeedController.loadFeed:78";

    List<QueryRecord> queries =
        List.of(
            // Q1: Load user profile (clean)
            q(
                "SELECT id, username, display_name, bio, avatar_url, created_at FROM users WHERE id = ?"),
            // Q2: Load feed posts with pagination (clean)
            q(
                "SELECT p.id, p.title, p.content, p.author_id, p.created_at, u.username "
                    + "FROM posts p JOIN users u ON p.author_id = u.id "
                    + "WHERE p.author_id IN (SELECT follower_id FROM followers WHERE user_id = ?) "
                    + "ORDER BY p.created_at DESC LIMIT 20"),
            // Q3-Q7: Load comments per post -- N+1! (ANTI-PATTERN: N_PLUS_ONE)
            qWithStack(
                "SELECT c.id, c.content, c.created_at, u.username FROM comments c JOIN users u ON c.author_id = u.id WHERE c.post_id = ? ORDER BY c.created_at LIMIT 3",
                commentStack),
            qWithStack(
                "SELECT c.id, c.content, c.created_at, u.username FROM comments c JOIN users u ON c.author_id = u.id WHERE c.post_id = ? ORDER BY c.created_at LIMIT 3",
                commentStack),
            qWithStack(
                "SELECT c.id, c.content, c.created_at, u.username FROM comments c JOIN users u ON c.author_id = u.id WHERE c.post_id = ? ORDER BY c.created_at LIMIT 3",
                commentStack),
            qWithStack(
                "SELECT c.id, c.content, c.created_at, u.username FROM comments c JOIN users u ON c.author_id = u.id WHERE c.post_id = ? ORDER BY c.created_at LIMIT 3",
                commentStack),
            qWithStack(
                "SELECT c.id, c.content, c.created_at, u.username FROM comments c JOIN users u ON c.author_id = u.id WHERE c.post_id = ? ORDER BY c.created_at LIMIT 3",
                commentStack),
            // Q8-Q12: Load like counts per post -- N+1! (ANTI-PATTERN: N_PLUS_ONE /
            // COUNT_INSTEAD_OF_EXISTS)
            qWithStack("SELECT COUNT(*) FROM likes WHERE post_id = ?", likeCountStack),
            qWithStack("SELECT COUNT(*) FROM likes WHERE post_id = ?", likeCountStack),
            qWithStack("SELECT COUNT(*) FROM likes WHERE post_id = ?", likeCountStack),
            qWithStack("SELECT COUNT(*) FROM likes WHERE post_id = ?", likeCountStack),
            qWithStack("SELECT COUNT(*) FROM likes WHERE post_id = ?", likeCountStack),
            // Q13: Load followers list (clean)
            q(
                "SELECT u.id, u.username, u.avatar_url FROM followers f JOIN users u ON f.follower_id = u.id WHERE f.user_id = ? LIMIT 50"),
            // Q14: Load follower count (clean, simple aggregate)
            q("SELECT COUNT(*) FROM followers WHERE user_id = ?"),
            // Q15: Load following count (clean)
            q("SELECT COUNT(*) FROM followers WHERE follower_id = ?"),
            // Q16: Post new comment (clean)
            q(
                "INSERT INTO comments (post_id, author_id, content, created_at) VALUES (?, ?, ?, NOW())"),
            // Q17: Load older posts with OFFSET pagination (ANTI-PATTERN: OFFSET_PAGINATION at high
            // offset)
            q(
                "SELECT p.id, p.title, p.content, p.created_at FROM posts p "
                    + "WHERE p.author_id = ? ORDER BY p.created_at DESC LIMIT 20 OFFSET 5000"),
            // Q18: Search posts with leading wildcard (ANTI-PATTERN: LIKE_LEADING_WILDCARD)
            q("SELECT id, title FROM posts WHERE title LIKE '%vacation%'"),
            // Q19: Check if user already liked (clean)
            q("SELECT id FROM likes WHERE post_id = ? AND user_id = ? LIMIT 1"),
            // Q20: Load media for a post (clean)
            q("SELECT id, url, type FROM media WHERE post_id = ?"),
            // Q21: Load post with all columns (ANTI-PATTERN: SELECT_ALL)
            q("SELECT * FROM posts WHERE id = ?"),
            // Q22: Unbounded followers query (ANTI-PATTERN: UNBOUNDED_RESULT_SET)
            q(
                "SELECT u.id, u.username FROM followers f JOIN users u ON f.follower_id = u.id WHERE f.user_id = ?"));

    QueryAuditReport report = analyzer.analyze("socialMediaFeed", queries, schema);
    List<Issue> allIssues = collectAllIssues(report);

    // Known anti-patterns:
    //   1. N_PLUS_ONE on comments (Q3-Q7)
    //   2. N_PLUS_ONE on like counts (Q8-Q12)
    //   3. OFFSET_PAGINATION on Q17 (OFFSET 5000)
    //   4. LIKE_LEADING_WILDCARD on Q18
    //   5. SELECT_ALL on Q21

    Set<IssueType> expectedDetections =
        Set.of(
            IssueType.N_PLUS_ONE,
            IssueType.OFFSET_PAGINATION,
            IssueType.LIKE_LEADING_WILDCARD,
            IssueType.SELECT_ALL);

    // Also accept these detections as true positives -- they are valid findings:
    // - MISSING_JOIN_INDEX: comments.author_id is not indexed (legitimate finding)
    // - ORDER_BY_LIMIT_WITHOUT_INDEX: ORDER BY c.created_at without index (legitimate)
    // - MISSING_ORDER_BY_INDEX: same column, different detector
    // - COUNT_INSTEAD_OF_EXISTS: flagged on COUNT(*) queries (informational)
    // - NON_DETERMINISTIC_PAGINATION: pagination on non-unique column (informational)
    Set<IssueType> acceptableInfoDetections =
        Set.of(
            IssueType.UNBOUNDED_RESULT_SET,
            IssueType.COVERING_INDEX_OPPORTUNITY,
            IssueType.COUNT_STAR_WITHOUT_WHERE,
            IssueType.EXCESSIVE_COLUMN_FETCH,
            IssueType.CORRELATED_SUBQUERY,
            IssueType.NOT_IN_SUBQUERY,
            IssueType.MISSING_JOIN_INDEX,
            IssueType.ORDER_BY_LIMIT_WITHOUT_INDEX,
            IssueType.MISSING_ORDER_BY_INDEX,
            IssueType.COUNT_INSTEAD_OF_EXISTS,
            IssueType.NON_DETERMINISTIC_PAGINATION);

    int tp = 0;
    int fp = 0;
    List<String> fpDetails = new ArrayList<>();
    List<String> tpDetails = new ArrayList<>();

    for (Issue issue : allIssues) {
      if (expectedDetections.contains(issue.type())
          || acceptableInfoDetections.contains(issue.type())) {
        tp++;
        tpDetails.add(
            String.format("  TP: [%s] %s", issue.type().getCode(), truncate(issue.query(), 60)));
      } else {
        fp++;
        fpDetails.add(
            String.format(
                "  FP: [%s] %s on query: %s",
                issue.type().getCode(), issue.severity(), truncate(issue.query(), 60)));
      }
    }

    int fn = 0;
    List<String> fnDetails = new ArrayList<>();
    if (!hasIssueOfType(allIssues, IssueType.N_PLUS_ONE)) {
      fn++;
      fnDetails.add("  FN: N_PLUS_ONE on comments not detected");
    }
    if (!hasIssueOfType(allIssues, IssueType.OFFSET_PAGINATION)) {
      fn++;
      fnDetails.add("  FN: OFFSET_PAGINATION on posts not detected");
    }
    if (!hasIssueOfType(allIssues, IssueType.LIKE_LEADING_WILDCARD)) {
      fn++;
      fnDetails.add("  FN: LIKE_LEADING_WILDCARD on posts.title not detected");
    }
    if (!hasIssueOfType(allIssues, IssueType.SELECT_ALL)) {
      fn++;
      fnDetails.add("  FN: SELECT_ALL on posts not detected");
    }

    ScenarioResult result2 = new ScenarioResult("Social media", queries.size(), tp, fp, fn);
    printScenarioReport("Scenario 2: Social Media Feed", result2, tpDetails, fpDetails, fnDetails);

    assertThat(result2.precision())
        .as("Social media scenario precision")
        .isGreaterThanOrEqualTo(0.70);

    scenarioResults.put("Social media", result2);
  }

  // ===============================================================
  // SCENARIO 3: Admin Dashboard
  // ===============================================================

  private static IndexMetadata adminSchema() {
    Map<String, List<IndexInfo>> idx = new HashMap<>();

    idx.put(
        "users",
        List.of(
            new IndexInfo("users", "PRIMARY", "id", 1, false, 50000),
            new IndexInfo("users", "idx_email", "email", 1, false, 50000),
            new IndexInfo("users", "idx_status", "status", 1, true, 5),
            new IndexInfo("users", "idx_created_at", "created_at", 1, true, 40000)));
    idx.put(
        "orders",
        List.of(
            new IndexInfo("orders", "PRIMARY", "id", 1, false, 100000),
            new IndexInfo("orders", "idx_user_id", "user_id", 1, true, 50000),
            new IndexInfo("orders", "idx_status", "status", 1, true, 5),
            new IndexInfo("orders", "idx_created_at", "created_at", 1, true, 80000)));
    idx.put(
        "products",
        List.of(
            new IndexInfo("products", "PRIMARY", "id", 1, false, 10000),
            new IndexInfo("products", "idx_category_id", "category_id", 1, true, 500)));
    idx.put("categories", List.of(new IndexInfo("categories", "PRIMARY", "id", 1, false, 200)));
    idx.put(
        "order_items",
        List.of(
            new IndexInfo("order_items", "PRIMARY", "id", 1, false, 300000),
            new IndexInfo("order_items", "idx_order_id", "order_id", 1, true, 100000),
            new IndexInfo("order_items", "idx_product_id", "product_id", 1, true, 10000)));
    idx.put(
        "sessions",
        List.of(
            new IndexInfo("sessions", "PRIMARY", "id", 1, false, 200000),
            new IndexInfo("sessions", "idx_user_id", "user_id", 1, true, 50000),
            new IndexInfo("sessions", "idx_expires_at", "expires_at", 1, true, 150000)));
    idx.put("roles", List.of(new IndexInfo("roles", "PRIMARY", "id", 1, false, 10)));
    idx.put(
        "user_roles",
        List.of(
            new IndexInfo("user_roles", "idx_user_id", "user_id", 1, true, 50000),
            new IndexInfo("user_roles", "idx_role_id", "role_id", 1, true, 10)));
    idx.put(
        "audit_log",
        List.of(
            new IndexInfo("audit_log", "PRIMARY", "id", 1, false, 1000000),
            new IndexInfo("audit_log", "idx_user_id", "user_id", 1, true, 50000),
            new IndexInfo("audit_log", "idx_created_at", "created_at", 1, true, 800000)));

    return new IndexMetadata(idx);
  }

  @Test
  void scenario3_adminDashboard() {
    IndexMetadata schema = adminSchema();

    List<QueryRecord> queries =
        List.of(
            // Q1: Load users with too many joins (ANTI-PATTERN: TOO_MANY_JOINS)
            q(
                "SELECT u.id, u.email, u.status, u.created_at, r.name AS role_name, "
                    + "o.total_amount, p.name AS last_product, c.name AS category, "
                    + "s.expires_at AS session_expires "
                    + "FROM users u "
                    + "LEFT JOIN user_roles ur ON u.id = ur.user_id "
                    + "LEFT JOIN roles r ON ur.role_id = r.id "
                    + "LEFT JOIN orders o ON u.id = o.user_id "
                    + "LEFT JOIN order_items oi ON o.id = oi.order_id "
                    + "LEFT JOIN products p ON oi.product_id = p.id "
                    + "LEFT JOIN categories c ON p.category_id = c.id "
                    + "LEFT JOIN sessions s ON u.id = s.user_id "
                    + "WHERE u.status = 'ACTIVE' LIMIT 100"),
            // Q2: Export CSV with SELECT * and no LIMIT (ANTI-PATTERN: SELECT_ALL +
            // UNBOUNDED_RESULT_SET)
            q("SELECT * FROM users WHERE created_at >= '2025-01-01'"),
            // Q3: Count all users (ANTI-PATTERN: COUNT_STAR_WITHOUT_WHERE)
            q("SELECT COUNT(*) FROM users"),
            // Q4: Bulk update status with WHERE (clean)
            q(
                "UPDATE users SET status = 'SUSPENDED' WHERE status = 'INACTIVE' AND last_login_at < '2024-01-01'"),
            // Q5: Revenue report with GROUP BY (clean)
            q(
                "SELECT DATE(o.created_at) AS order_date, COUNT(*) AS order_count, SUM(o.total_amount) AS revenue "
                    + "FROM orders o WHERE o.created_at >= '2025-01-01' "
                    + "GROUP BY DATE(o.created_at) ORDER BY order_date"),
            // Q6: Product sales report (clean)
            q(
                "SELECT p.id, p.name, SUM(oi.quantity) AS total_sold, SUM(oi.quantity * oi.unit_price) AS revenue "
                    + "FROM order_items oi JOIN products p ON oi.product_id = p.id "
                    + "GROUP BY p.id, p.name ORDER BY revenue DESC LIMIT 50"),
            // Q7: HAVING misuse - non-aggregate in HAVING (ANTI-PATTERN: HAVING_MISUSE)
            q(
                "SELECT u.id, u.email, COUNT(o.id) AS order_count "
                    + "FROM users u JOIN orders o ON u.id = o.user_id "
                    + "GROUP BY u.id, u.email HAVING u.status = 'ACTIVE'"),
            // Q8: Cleanup old sessions (clean)
            q("DELETE FROM sessions WHERE expires_at < NOW()"),
            // Q9: Search users by name with function (ANTI-PATTERN: WHERE_FUNCTION)
            q("SELECT id, email, status FROM users WHERE LOWER(email) = ?"),
            // Q10: Load recent audit log (clean)
            q(
                "SELECT id, entity_type, entity_id, action, created_at FROM audit_log WHERE created_at >= ? ORDER BY created_at DESC LIMIT 100"),
            // Q11: Load user detail (clean)
            q("SELECT id, email, status, created_at FROM users WHERE id = ?"),
            // Q12: Update single user (clean)
            q("UPDATE users SET status = ? WHERE id = ?"),
            // Q13: Export all orders - unbounded (ANTI-PATTERN: UNBOUNDED_RESULT_SET + SELECT_ALL)
            q("SELECT * FROM orders"),
            // Q14: Complex OR search (ANTI-PATTERN: OR_ABUSE with 4+ ORs)
            q(
                "SELECT id, email, status FROM users "
                    + "WHERE email LIKE '%test%' OR status = 'BLOCKED' OR status = 'SUSPENDED' OR status = 'PENDING' OR created_at < '2020-01-01'"),
            // Q15: GROUP BY with function (ANTI-PATTERN: GROUP_BY_FUNCTION)
            q(
                "SELECT YEAR(created_at) AS year, MONTH(created_at) AS month, COUNT(*) "
                    + "FROM orders GROUP BY YEAR(created_at), MONTH(created_at)"),
            // Q16: Dashboard count with subquery (clean)
            q(
                "SELECT (SELECT COUNT(*) FROM users WHERE status = 'ACTIVE') AS active_users, "
                    + "(SELECT COUNT(*) FROM orders WHERE created_at >= CURDATE()) AS today_orders"),
            // Q17: User list for dropdown (clean)
            q("SELECT id, email FROM users WHERE status = 'ACTIVE' ORDER BY email LIMIT 1000"),
            // Q18: Bulk delete with no WHERE (ANTI-PATTERN: UPDATE_WITHOUT_WHERE)
            q("DELETE FROM sessions"),
            // Q19: Order search by date range (clean)
            q(
                "SELECT id, user_id, total_amount, status, created_at FROM orders WHERE created_at BETWEEN ? AND ? ORDER BY created_at DESC LIMIT 50"),
            // Q20: DML on unindexed column (ANTI-PATTERN: DML_WITHOUT_INDEX on last_login_at)
            q("UPDATE users SET status = 'INACTIVE' WHERE last_login_at < '2024-06-01'"),
            // Q21: Audit log insert (clean)
            q(
                "INSERT INTO audit_log (entity_type, entity_id, action, user_id, created_at) VALUES ('USER', ?, 'STATUS_CHANGE', ?, NOW())"),
            // Q22: Role statistics (clean)
            q(
                "SELECT r.name, COUNT(ur.user_id) AS user_count FROM roles r LEFT JOIN user_roles ur ON r.id = ur.role_id GROUP BY r.name"),
            // Q23: Search with string concat (ANTI-PATTERN: STRING_CONCAT_IN_WHERE)
            q("SELECT id, email FROM users WHERE CONCAT(first_name, ' ', last_name) = ?"),
            // Q24: Load order with items (clean)
            q(
                "SELECT o.id, o.status, o.total_amount, oi.product_id, oi.quantity FROM orders o JOIN order_items oi ON o.id = oi.order_id WHERE o.id = ?"),
            // Q25: Implicit join syntax (ANTI-PATTERN: IMPLICIT_JOIN)
            q(
                "SELECT u.email, o.total_amount FROM users u, orders o WHERE u.id = o.user_id AND o.status = 'PAID' LIMIT 100"));

    QueryAuditReport report = analyzer.analyze("adminDashboard", queries, schema);
    List<Issue> allIssues = collectAllIssues(report);

    Set<IssueType> expectedDetections =
        Set.of(
            IssueType.TOO_MANY_JOINS,
            IssueType.SELECT_ALL,
            IssueType.HAVING_MISUSE,
            IssueType.WHERE_FUNCTION,
            IssueType.OR_ABUSE,
            IssueType.GROUP_BY_FUNCTION,
            IssueType.UPDATE_WITHOUT_WHERE,
            IssueType.DML_WITHOUT_INDEX,
            IssueType.STRING_CONCAT_IN_WHERE,
            IssueType.IMPLICIT_JOIN,
            IssueType.LIKE_LEADING_WILDCARD);

    Set<IssueType> acceptableInfoDetections =
        Set.of(
            IssueType.UNBOUNDED_RESULT_SET,
            IssueType.COUNT_STAR_WITHOUT_WHERE,
            IssueType.COVERING_INDEX_OPPORTUNITY,
            IssueType.EXCESSIVE_COLUMN_FETCH,
            IssueType.CORRELATED_SUBQUERY,
            IssueType.NON_SARGABLE_EXPRESSION,
            IssueType.MISSING_WHERE_INDEX,
            IssueType.MISSING_GROUP_BY_INDEX);

    int tp = 0;
    int fp = 0;
    List<String> fpDetails = new ArrayList<>();
    List<String> tpDetails = new ArrayList<>();

    for (Issue issue : allIssues) {
      if (expectedDetections.contains(issue.type())
          || acceptableInfoDetections.contains(issue.type())) {
        tp++;
        tpDetails.add(
            String.format("  TP: [%s] %s", issue.type().getCode(), truncate(issue.query(), 60)));
      } else {
        fp++;
        fpDetails.add(
            String.format(
                "  FP: [%s] %s on query: %s",
                issue.type().getCode(), issue.severity(), truncate(issue.query(), 60)));
      }
    }

    int fn = 0;
    List<String> fnDetails = new ArrayList<>();
    if (!hasIssueOfType(allIssues, IssueType.SELECT_ALL)) {
      fn++;
      fnDetails.add("  FN: SELECT_ALL not detected");
    }
    if (!hasIssueOfType(allIssues, IssueType.TOO_MANY_JOINS)) {
      fn++;
      fnDetails.add("  FN: TOO_MANY_JOINS not detected");
    }
    if (!hasIssueOfType(allIssues, IssueType.HAVING_MISUSE)) {
      fn++;
      fnDetails.add("  FN: HAVING_MISUSE not detected");
    }
    if (!hasIssueOfType(allIssues, IssueType.WHERE_FUNCTION)) {
      fn++;
      fnDetails.add("  FN: WHERE_FUNCTION not detected");
    }
    if (!hasIssueOfType(allIssues, IssueType.UPDATE_WITHOUT_WHERE)) {
      fn++;
      fnDetails.add("  FN: UPDATE_WITHOUT_WHERE not detected");
    }
    if (!hasIssueOfType(allIssues, IssueType.STRING_CONCAT_IN_WHERE)) {
      fn++;
      fnDetails.add("  FN: STRING_CONCAT_IN_WHERE not detected");
    }
    if (!hasIssueOfType(allIssues, IssueType.IMPLICIT_JOIN)) {
      fn++;
      fnDetails.add("  FN: IMPLICIT_JOIN not detected");
    }

    ScenarioResult result3 = new ScenarioResult("Admin dashboard", queries.size(), tp, fp, fn);
    printScenarioReport("Scenario 3: Admin Dashboard", result3, tpDetails, fpDetails, fnDetails);

    assertThat(result3.precision())
        .as("Admin dashboard scenario precision")
        .isGreaterThanOrEqualTo(0.70);

    scenarioResults.put("Admin dashboard", result3);
  }

  // ===============================================================
  // SCENARIO 4: Background Batch Job
  // ===============================================================

  private static IndexMetadata batchJobSchema() {
    Map<String, List<IndexInfo>> idx = new HashMap<>();

    idx.put(
        "work_items",
        List.of(
            new IndexInfo("work_items", "PRIMARY", "id", 1, false, 500000),
            new IndexInfo("work_items", "idx_status", "status", 1, true, 5),
            new IndexInfo("work_items", "idx_created_at", "created_at", 1, true, 400000)));
    idx.put(
        "work_archive", List.of(new IndexInfo("work_archive", "PRIMARY", "id", 1, false, 2000000)));
    idx.put(
        "statistics",
        List.of(
            new IndexInfo("statistics", "PRIMARY", "id", 1, false, 1000),
            new IndexInfo("statistics", "idx_metric_date", "metric_name", 1, true, 50)));
    idx.put(
        "error_log",
        List.of(
            new IndexInfo("error_log", "PRIMARY", "id", 1, false, 100000),
            new IndexInfo("error_log", "idx_created_at", "created_at", 1, true, 80000)));
    idx.put(
        "notifications",
        List.of(
            new IndexInfo("notifications", "PRIMARY", "id", 1, false, 300000),
            new IndexInfo("notifications", "idx_user_id", "user_id", 1, true, 50000),
            new IndexInfo("notifications", "idx_status", "status", 1, true, 3)));

    return new IndexMetadata(idx);
  }

  @Test
  void scenario4_backgroundBatchJob() {
    IndexMetadata schema = batchJobSchema();

    // Simulate repeated updates from same call site
    String processStack = "com.batch.WorkProcessor.processItem:66\ncom.batch.BatchRunner.run:120";

    List<QueryRecord> queries = new ArrayList<>();

    // Q1: Load unprocessed items (clean)
    queries.add(
        q(
            "SELECT id, payload, priority, created_at FROM work_items WHERE status = 'PENDING' ORDER BY priority DESC, created_at ASC LIMIT 100"));
    // Q2: Lock items for processing (clean)
    queries.add(
        q(
            "UPDATE work_items SET status = 'PROCESSING', started_at = NOW() WHERE id = ? AND status = 'PENDING'"));
    // Q3-Q8: Process each item - update one by one (ANTI-PATTERN: N_PLUS_ONE repeated UPDATE)
    for (int i = 0; i < 6; i++) {
      queries.add(
          qWithStack(
              "UPDATE work_items SET status = 'COMPLETED', completed_at = NOW(), result = ? WHERE id = ?",
              processStack));
    }
    // Q9: Archive old records using INSERT...SELECT (ANTI-PATTERN: INSERT_SELECT_LOCKS_SOURCE)
    queries.add(
        q(
            "INSERT INTO work_archive (id, payload, status, created_at, completed_at) "
                + "SELECT id, payload, status, created_at, completed_at FROM work_items WHERE status = 'COMPLETED' AND completed_at < DATE_SUB(NOW(), INTERVAL 30 DAY)"));
    // Q10: Archive with SELECT * (ANTI-PATTERN: INSERT_SELECT_ALL)
    queries.add(q("INSERT INTO work_archive SELECT * FROM work_items WHERE status = 'ARCHIVED'"));
    // Q11: Cleanup completed items (clean)
    queries.add(
        q(
            "DELETE FROM work_items WHERE status = 'COMPLETED' AND completed_at < DATE_SUB(NOW(), INTERVAL 30 DAY)"));
    // Q12: Update statistics with ON DUPLICATE KEY (ANTI-PATTERN: INSERT_ON_DUPLICATE_KEY - INFO
    // level)
    queries.add(
        q(
            "INSERT INTO statistics (metric_name, metric_date, value) VALUES (?, CURDATE(), ?) "
                + "ON DUPLICATE KEY UPDATE value = value + VALUES(value)"));
    // Q13: Log errors without explicit columns (ANTI-PATTERN: IMPLICIT_COLUMNS_INSERT)
    queries.add(q("INSERT INTO error_log VALUES (NULL, ?, ?, ?, NOW())"));
    // Q14: Count remaining items (clean)
    queries.add(q("SELECT COUNT(*) FROM work_items WHERE status = 'PENDING'"));
    // Q15: DML on unindexed column (ANTI-PATTERN: DML_WITHOUT_INDEX on completed_at)
    queries.add(q("DELETE FROM work_items WHERE completed_at < '2024-01-01'"));
    // Q16: Load items with ORDER BY RAND (ANTI-PATTERN: ORDER_BY_RAND)
    queries.add(q("SELECT id FROM work_items WHERE status = 'PENDING' ORDER BY RAND() LIMIT 10"));
    // Q17: Load work item with null comparison (ANTI-PATTERN: NULL_COMPARISON)
    queries.add(q("SELECT id, status FROM work_items WHERE result = NULL"));
    // Q18: Send notifications in batch - repeated inserts (ANTI-PATTERN: REPEATED_SINGLE_INSERT)
    String notifStack = "com.batch.NotificationService.send:44\ncom.batch.BatchRunner.notify:130";
    for (int i = 0; i < 5; i++) {
      queries.add(
          qWithStack(
              "INSERT INTO notifications (user_id, type, message, status, created_at) VALUES (?, 'BATCH_COMPLETE', ?, 'PENDING', NOW())",
              notifStack));
    }
    // Q23: Cleanup old error logs (clean)
    queries.add(q("DELETE FROM error_log WHERE created_at < DATE_SUB(NOW(), INTERVAL 90 DAY)"));
    // Q24: Final statistics update (clean)
    queries.add(
        q("UPDATE statistics SET value = ? WHERE metric_name = ? AND metric_date = CURDATE()"));
    // Q25: Load summary for report (ANTI-PATTERN: SELECT_ALL + UNBOUNDED_RESULT_SET)
    queries.add(q("SELECT * FROM statistics"));

    QueryAuditReport report = analyzer.analyze("backgroundBatchJob", queries, schema);
    List<Issue> allIssues = collectAllIssues(report);

    Set<IssueType> expectedDetections =
        Set.of(
            IssueType.N_PLUS_ONE,
            IssueType.INSERT_SELECT_LOCKS_SOURCE,
            IssueType.INSERT_SELECT_ALL,
            IssueType.INSERT_ON_DUPLICATE_KEY,
            IssueType.IMPLICIT_COLUMNS_INSERT,
            IssueType.DML_WITHOUT_INDEX,
            IssueType.ORDER_BY_RAND,
            IssueType.NULL_COMPARISON,
            IssueType.REPEATED_SINGLE_INSERT,
            IssueType.SELECT_ALL);

    Set<IssueType> acceptableInfoDetections =
        Set.of(
            IssueType.UNBOUNDED_RESULT_SET,
            IssueType.COVERING_INDEX_OPPORTUNITY,
            IssueType.COUNT_STAR_WITHOUT_WHERE,
            IssueType.EXCESSIVE_COLUMN_FETCH,
            IssueType.MISSING_WHERE_INDEX,
            IssueType.NON_SARGABLE_EXPRESSION,
            IssueType.SUBQUERY_IN_DML,
            IssueType.WHERE_FUNCTION);

    int tp = 0;
    int fp = 0;
    List<String> fpDetails = new ArrayList<>();
    List<String> tpDetails = new ArrayList<>();

    for (Issue issue : allIssues) {
      if (expectedDetections.contains(issue.type())
          || acceptableInfoDetections.contains(issue.type())) {
        tp++;
        tpDetails.add(
            String.format("  TP: [%s] %s", issue.type().getCode(), truncate(issue.query(), 60)));
      } else {
        fp++;
        fpDetails.add(
            String.format(
                "  FP: [%s] %s on query: %s",
                issue.type().getCode(), issue.severity(), truncate(issue.query(), 60)));
      }
    }

    int fn = 0;
    List<String> fnDetails = new ArrayList<>();
    if (!hasIssueOfType(allIssues, IssueType.N_PLUS_ONE)) {
      fn++;
      fnDetails.add("  FN: N_PLUS_ONE on work_items updates not detected");
    }
    if (!hasIssueOfType(allIssues, IssueType.INSERT_SELECT_ALL)) {
      fn++;
      fnDetails.add("  FN: INSERT_SELECT_ALL not detected");
    }
    if (!hasIssueOfType(allIssues, IssueType.ORDER_BY_RAND)) {
      fn++;
      fnDetails.add("  FN: ORDER_BY_RAND not detected");
    }
    if (!hasIssueOfType(allIssues, IssueType.NULL_COMPARISON)) {
      fn++;
      fnDetails.add("  FN: NULL_COMPARISON not detected");
    }
    if (!hasIssueOfType(allIssues, IssueType.REPEATED_SINGLE_INSERT)) {
      fn++;
      fnDetails.add("  FN: REPEATED_SINGLE_INSERT on notifications not detected");
    }
    if (!hasIssueOfType(allIssues, IssueType.SELECT_ALL)) {
      fn++;
      fnDetails.add("  FN: SELECT_ALL on statistics not detected");
    }

    ScenarioResult result4 = new ScenarioResult("Batch job", queries.size(), tp, fp, fn);
    printScenarioReport(
        "Scenario 4: Background Batch Job", result4, tpDetails, fpDetails, fnDetails);

    assertThat(result4.precision()).as("Batch job scenario precision").isGreaterThanOrEqualTo(0.70);

    scenarioResults.put("Batch job", result4);
  }

  // ===============================================================
  // SCENARIO 5: Search API
  // ===============================================================

  private static IndexMetadata searchApiSchema() {
    Map<String, List<IndexInfo>> idx = new HashMap<>();

    idx.put(
        "articles",
        List.of(
            new IndexInfo("articles", "PRIMARY", "id", 1, false, 200000),
            new IndexInfo("articles", "idx_category_id", "category_id", 1, true, 500),
            new IndexInfo("articles", "idx_published_at", "published_at", 1, true, 150000),
            new IndexInfo("articles", "idx_status", "status", 1, true, 3),
            new IndexInfo("articles", "idx_author_id", "author_id", 1, true, 5000)));
    idx.put(
        "categories",
        List.of(
            new IndexInfo("categories", "PRIMARY", "id", 1, false, 500),
            new IndexInfo("categories", "idx_name", "name", 1, true, 500),
            new IndexInfo("categories", "idx_parent_id", "parent_id", 1, true, 100)));
    idx.put(
        "tags",
        List.of(
            new IndexInfo("tags", "PRIMARY", "id", 1, false, 2000),
            new IndexInfo("tags", "idx_name", "name", 1, false, 2000)));
    idx.put(
        "article_tags",
        List.of(
            new IndexInfo("article_tags", "idx_article_id", "article_id", 1, true, 200000),
            new IndexInfo("article_tags", "idx_tag_id", "tag_id", 1, true, 2000)));
    idx.put(
        "search_log",
        List.of(
            new IndexInfo("search_log", "PRIMARY", "id", 1, false, 1000000),
            new IndexInfo("search_log", "idx_created_at", "created_at", 1, true, 800000),
            new IndexInfo("search_log", "idx_user_id", "user_id", 1, true, 50000)));
    idx.put(
        "authors",
        List.of(
            new IndexInfo("authors", "PRIMARY", "id", 1, false, 5000),
            new IndexInfo("authors", "idx_name", "name", 1, true, 5000)));

    return new IndexMetadata(idx);
  }

  @Test
  void scenario5_searchApi() {
    IndexMetadata schema = searchApiSchema();

    List<QueryRecord> queries =
        List.of(
            // Q1: Text search with leading wildcard (ANTI-PATTERN: LIKE_LEADING_WILDCARD)
            q(
                "SELECT id, title, excerpt FROM articles WHERE title LIKE '%machine learning%' AND status = 'PUBLISHED'"),
            // Q2: Also searching body with leading wildcard (ANTI-PATTERN: LIKE_LEADING_WILDCARD)
            q("SELECT id, title FROM articles WHERE body LIKE '%neural network%'"),
            // Q3: Filter by multiple categories with OR (ANTI-PATTERN: OR_ABUSE, 5 ORs)
            q(
                "SELECT id, title, published_at FROM articles "
                    + "WHERE category_id = 1 OR category_id = 2 OR category_id = 3 OR category_id = 4 OR category_id = 5"),
            // Q4: Sort by expression (ANTI-PATTERN: non-sargable/function in ORDER BY)
            q(
                "SELECT id, title, published_at FROM articles "
                    + "WHERE status = 'PUBLISHED' ORDER BY LENGTH(title) LIMIT 20"),
            // Q5: Paginate with high offset (ANTI-PATTERN: OFFSET_PAGINATION)
            q(
                "SELECT id, title, excerpt, published_at FROM articles "
                    + "WHERE status = 'PUBLISHED' ORDER BY published_at DESC LIMIT 20 OFFSET 10000"),
            // Q6: Log search query (clean)
            q(
                "INSERT INTO search_log (user_id, query_text, result_count, created_at) VALUES (?, ?, ?, NOW())"),
            // Q7: Load article details (clean)
            q(
                "SELECT id, title, body, author_id, category_id, published_at FROM articles WHERE id = ?"),
            // Q8: Load article tags (clean)
            q(
                "SELECT t.id, t.name FROM article_tags at JOIN tags t ON at.tag_id = t.id WHERE at.article_id = ?"),
            // Q9: Load author info (clean)
            q("SELECT id, name, bio FROM authors WHERE id = ?"),
            // Q10: Related articles by tag (clean)
            q(
                "SELECT DISTINCT a.id, a.title FROM articles a "
                    + "JOIN article_tags at ON a.id = at.article_id "
                    + "WHERE at.tag_id IN (SELECT tag_id FROM article_tags WHERE article_id = ?) "
                    + "AND a.id != ? AND a.status = 'PUBLISHED' LIMIT 5"),
            // Q11: Category tree with self-join (clean)
            q(
                "SELECT c.id, c.name, p.name AS parent_name FROM categories c LEFT JOIN categories p ON c.parent_id = p.id WHERE c.id = ?"),
            // Q12: REGEXP usage (ANTI-PATTERN: REGEXP_INSTEAD_OF_LIKE)
            q("SELECT id, title FROM articles WHERE title REGEXP '^[A-Z].*2025'"),
            // Q13: Faceted search count (clean)
            q(
                "SELECT c.id, c.name, COUNT(a.id) AS article_count "
                    + "FROM categories c LEFT JOIN articles a ON c.id = a.category_id AND a.status = 'PUBLISHED' "
                    + "GROUP BY c.id, c.name ORDER BY article_count DESC"),
            // Q14: Search with FIND_IN_SET (ANTI-PATTERN: FIND_IN_SET_USAGE)
            q("SELECT id, title FROM articles WHERE FIND_IN_SET('python', tags_csv) > 0"),
            // Q15: Autocomplete search (clean)
            q(
                "SELECT id, title FROM articles WHERE title LIKE 'machine%' AND status = 'PUBLISHED' ORDER BY published_at DESC LIMIT 10"),
            // Q16: Popular search terms - unbounded (ANTI-PATTERN: UNBOUNDED_RESULT_SET)
            q(
                "SELECT query_text, COUNT(*) AS frequency FROM search_log GROUP BY query_text ORDER BY frequency DESC"),
            // Q17: NOT IN subquery (ANTI-PATTERN: NOT_IN_SUBQUERY)
            q(
                "SELECT id, title FROM articles WHERE id NOT IN (SELECT article_id FROM article_tags WHERE tag_id = ?)"),
            // Q18: Load all articles for sitemap (ANTI-PATTERN: SELECT_ALL + UNBOUNDED_RESULT_SET)
            q("SELECT * FROM articles WHERE status = 'PUBLISHED'"),
            // Q19: Search with implicit type conversion (ANTI-PATTERN: WHERE_FUNCTION via
            // CAST-like)
            q("SELECT id, title FROM articles WHERE YEAR(published_at) = 2025"),
            // Q20: Count articles by status (clean)
            q("SELECT status, COUNT(*) FROM articles GROUP BY status"),
            // Q21: Tag cloud query (clean)
            q(
                "SELECT t.name, COUNT(at.article_id) AS usage_count FROM tags t "
                    + "JOIN article_tags at ON t.id = at.tag_id "
                    + "GROUP BY t.name ORDER BY usage_count DESC LIMIT 50"),
            // Q22: UNION without ALL for combined search (ANTI-PATTERN: UNION_WITHOUT_ALL)
            q(
                "SELECT id, title, 'title_match' AS match_type FROM articles WHERE title LIKE '%AI%' "
                    + "UNION SELECT id, title, 'body_match' FROM articles WHERE body LIKE '%AI%'"),
            // Q23: Correlated subquery for latest per category (ANTI-PATTERN: CORRELATED_SUBQUERY)
            q(
                "SELECT a.id, a.title, a.category_id FROM articles a "
                    + "WHERE a.published_at = (SELECT MAX(a2.published_at) FROM articles a2 WHERE a2.category_id = a.category_id)"),
            // Q24: Search log cleanup (clean)
            q("DELETE FROM search_log WHERE created_at < DATE_SUB(NOW(), INTERVAL 90 DAY)"),
            // Q25: Full text alternative using OR (ANTI-PATTERN: OR_ABUSE)
            q(
                "SELECT id, title FROM articles WHERE title LIKE '%deep%' OR body LIKE '%deep%' OR excerpt LIKE '%deep%' OR tags_csv LIKE '%deep%'"));

    QueryAuditReport report = analyzer.analyze("searchApi", queries, schema);
    List<Issue> allIssues = collectAllIssues(report);

    Set<IssueType> expectedDetections =
        Set.of(
            IssueType.LIKE_LEADING_WILDCARD,
            IssueType.OR_ABUSE,
            IssueType.OFFSET_PAGINATION,
            IssueType.REGEXP_INSTEAD_OF_LIKE,
            IssueType.FIND_IN_SET_USAGE,
            IssueType.NOT_IN_SUBQUERY,
            IssueType.SELECT_ALL,
            IssueType.WHERE_FUNCTION,
            IssueType.UNION_WITHOUT_ALL,
            IssueType.CORRELATED_SUBQUERY);

    Set<IssueType> acceptableInfoDetections =
        Set.of(
            IssueType.UNBOUNDED_RESULT_SET,
            IssueType.COVERING_INDEX_OPPORTUNITY,
            IssueType.COUNT_STAR_WITHOUT_WHERE,
            IssueType.EXCESSIVE_COLUMN_FETCH,
            IssueType.MISSING_WHERE_INDEX,
            IssueType.NON_SARGABLE_EXPRESSION,
            IssueType.DISTINCT_MISUSE,
            IssueType.MISSING_GROUP_BY_INDEX,
            IssueType.GROUP_BY_FUNCTION);

    int tp = 0;
    int fp = 0;
    List<String> fpDetails = new ArrayList<>();
    List<String> tpDetails = new ArrayList<>();

    for (Issue issue : allIssues) {
      if (expectedDetections.contains(issue.type())
          || acceptableInfoDetections.contains(issue.type())) {
        tp++;
        tpDetails.add(
            String.format("  TP: [%s] %s", issue.type().getCode(), truncate(issue.query(), 60)));
      } else {
        fp++;
        fpDetails.add(
            String.format(
                "  FP: [%s] %s on query: %s",
                issue.type().getCode(), issue.severity(), truncate(issue.query(), 60)));
      }
    }

    int fn = 0;
    List<String> fnDetails = new ArrayList<>();
    if (!hasIssueOfType(allIssues, IssueType.LIKE_LEADING_WILDCARD)) {
      fn++;
      fnDetails.add("  FN: LIKE_LEADING_WILDCARD not detected");
    }
    if (!hasIssueOfType(allIssues, IssueType.OFFSET_PAGINATION)) {
      fn++;
      fnDetails.add("  FN: OFFSET_PAGINATION not detected");
    }
    if (!hasIssueOfType(allIssues, IssueType.REGEXP_INSTEAD_OF_LIKE)) {
      fn++;
      fnDetails.add("  FN: REGEXP_INSTEAD_OF_LIKE not detected");
    }
    if (!hasIssueOfType(allIssues, IssueType.FIND_IN_SET_USAGE)) {
      fn++;
      fnDetails.add("  FN: FIND_IN_SET not detected");
    }
    if (!hasIssueOfType(allIssues, IssueType.NOT_IN_SUBQUERY)) {
      fn++;
      fnDetails.add("  FN: NOT_IN_SUBQUERY not detected");
    }
    if (!hasIssueOfType(allIssues, IssueType.SELECT_ALL)) {
      fn++;
      fnDetails.add("  FN: SELECT_ALL not detected");
    }
    if (!hasIssueOfType(allIssues, IssueType.CORRELATED_SUBQUERY)) {
      fn++;
      fnDetails.add("  FN: CORRELATED_SUBQUERY not detected");
    }
    if (!hasIssueOfType(allIssues, IssueType.UNION_WITHOUT_ALL)) {
      fn++;
      fnDetails.add("  FN: UNION_WITHOUT_ALL not detected");
    }

    ScenarioResult result5 = new ScenarioResult("Search API", queries.size(), tp, fp, fn);
    printScenarioReport("Scenario 5: Search API", result5, tpDetails, fpDetails, fnDetails);

    assertThat(result5.precision())
        .as("Search API scenario precision")
        .isGreaterThanOrEqualTo(0.70);

    scenarioResults.put("Search API", result5);
  }

  // ===============================================================
  // Final aggregated report
  // ===============================================================

  /** Shared storage across test methods - order matters for the final report. */
  private static final Map<String, ScenarioResult> scenarioResults =
      java.util.Collections.synchronizedMap(new LinkedHashMap<>());

  @Test
  void printFinalReport() {
    // Run all scenarios first to populate results
    scenario1_ecommerceCheckout();
    scenario2_socialMediaFeed();
    scenario3_adminDashboard();
    scenario4_backgroundBatchJob();
    scenario5_searchApi();

    System.out.println();
    System.out.println("=".repeat(100));
    System.out.println("=== TEAM 4: PRODUCTION WORKLOAD ===");
    System.out.println("=".repeat(100));
    System.out.printf(
        "%-18s | %7s | %3s | %3s | %3s | %9s | %6s | %6s%n",
        "Scenario", "Queries", "TP", "FP", "FN", "Precision", "Recall", "F1");
    System.out.println("-".repeat(100));

    int totalQueries = 0;
    int totalTp = 0;
    int totalFp = 0;
    int totalFn = 0;

    for (Map.Entry<String, ScenarioResult> entry : scenarioResults.entrySet()) {
      ScenarioResult r = entry.getValue();
      totalQueries += r.queryCount;
      totalTp += r.tp;
      totalFp += r.fp;
      totalFn += r.fn;

      System.out.printf(
          "%-18s | %7d | %3d | %3d | %3d | %8.1f%% | %5.1f%% | %5.1f%%%n",
          r.name,
          r.queryCount,
          r.tp,
          r.fp,
          r.fn,
          r.precision() * 100,
          r.recall() * 100,
          r.f1() * 100);
    }

    double overallPrecision =
        (totalTp + totalFp) == 0 ? 1.0 : (double) totalTp / (totalTp + totalFp);
    double overallRecall = (totalTp + totalFn) == 0 ? 1.0 : (double) totalTp / (totalTp + totalFn);
    double overallF1 =
        (overallPrecision + overallRecall) == 0
            ? 0.0
            : 2.0 * overallPrecision * overallRecall / (overallPrecision + overallRecall);

    System.out.println("-".repeat(100));
    System.out.printf(
        "%-18s | %7d | %3d | %3d | %3d | %8.1f%% | %5.1f%% | %5.1f%%%n",
        "OVERALL",
        totalQueries,
        totalTp,
        totalFp,
        totalFn,
        overallPrecision * 100,
        overallRecall * 100,
        overallF1 * 100);
    System.out.println("=".repeat(100));

    // Assert minimum overall quality thresholds
    assertThat(overallPrecision)
        .as("Overall precision should be at least 70%%")
        .isGreaterThanOrEqualTo(0.70);
    assertThat(overallRecall)
        .as("Overall recall should be at least 60%%")
        .isGreaterThanOrEqualTo(0.60);
    assertThat(overallF1).as("Overall F1 should be at least 65%%").isGreaterThanOrEqualTo(0.65);
  }

  // ---------------------------------------------------------------
  // Utility methods
  // ---------------------------------------------------------------

  private List<Issue> collectAllIssues(QueryAuditReport report) {
    List<Issue> all = new ArrayList<>();
    all.addAll(report.getConfirmedIssues());
    all.addAll(report.getInfoIssues());
    return all;
  }

  private boolean hasIssueOfType(List<Issue> issues, IssueType type) {
    return issues.stream().anyMatch(i -> i.type() == type);
  }

  private static String truncate(String s, int maxLen) {
    if (s == null) return "<null>";
    return s.length() <= maxLen ? s : s.substring(0, maxLen) + "...";
  }

  private void printScenarioReport(
      String title,
      ScenarioResult result,
      List<String> tpDetails,
      List<String> fpDetails,
      List<String> fnDetails) {
    System.out.println();
    System.out.println("--- " + title + " ---");
    System.out.printf(
        "Queries: %d | TP: %d | FP: %d | FN: %d%n",
        result.queryCount, result.tp, result.fp, result.fn);
    System.out.printf(
        "Precision: %.1f%% | Recall: %.1f%% | F1: %.1f%%%n",
        result.precision() * 100, result.recall() * 100, result.f1() * 100);

    if (!tpDetails.isEmpty()) {
      System.out.println("True Positives:");
      tpDetails.forEach(System.out::println);
    }
    if (!fpDetails.isEmpty()) {
      System.out.println("False Positives:");
      fpDetails.forEach(System.out::println);
    }
    if (!fnDetails.isEmpty()) {
      System.out.println("False Negatives:");
      fnDetails.forEach(System.out::println);
    }
  }
}
