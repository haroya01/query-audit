package io.queryaudit.core.eval;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.detector.*;
import io.queryaudit.core.model.IndexInfo;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

/**
 * FALSE POSITIVE AUDIT
 *
 * <p>Tests every detector against realistic, legitimate SQL queries that should NOT be flagged.
 * Uses Hibernate-style generated SQL patterns (aliases like u1_0, qualified columns, etc.). Target:
 * 200+ legitimate queries, zero false positives.
 */
class FalsePositiveAuditTest {

  static int totalQueries = 0;
  static int falsePositives = 0;
  static final List<String> falsePositiveDetails = new ArrayList<>();

  @AfterAll
  static void printReport() {
    System.out.println("=== FALSE POSITIVE AUDIT ===");
    System.out.println("Total legitimate queries tested: " + totalQueries);
    System.out.println("False positives found: " + falsePositives);
    System.out.printf("False positive rate: %.2f%%%n", (falsePositives * 100.0 / totalQueries));
    if (!falsePositiveDetails.isEmpty()) {
      System.out.println("\n--- FALSE POSITIVE DETAILS ---");
      for (String detail : falsePositiveDetails) {
        System.out.println(detail);
      }
    }
    System.out.println("=== END ===");
  }

  // ── Helpers ──────────────────────────────────────────────────────────

  private static QueryRecord q(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private static final IndexMetadata RICH_INDEX =
      new IndexMetadata(
          Map.of(
              "users",
                  List.of(
                      new IndexInfo("users", "PRIMARY", "id", 1, false, 10000),
                      new IndexInfo("users", "idx_users_email", "email", 1, true, 9500),
                      new IndexInfo("users", "idx_users_status", "status", 1, true, 5)),
              "orders",
                  List.of(
                      new IndexInfo("orders", "PRIMARY", "id", 1, false, 50000),
                      new IndexInfo("orders", "idx_orders_user_id", "user_id", 1, true, 10000),
                      new IndexInfo("orders", "idx_orders_status", "status", 1, true, 10)),
              "products",
                  List.of(
                      new IndexInfo("products", "PRIMARY", "id", 1, false, 20000),
                      new IndexInfo(
                          "products", "idx_products_category", "category_id", 1, true, 100)),
              "order_items",
                  List.of(
                      new IndexInfo("order_items", "PRIMARY", "id", 1, false, 100000),
                      new IndexInfo(
                          "order_items", "idx_items_order_id", "order_id", 1, true, 50000))));

  private List<Issue> evaluate(DetectionRule detector, List<String> sqls) {
    List<QueryRecord> records = sqls.stream().map(FalsePositiveAuditTest::q).toList();
    totalQueries += sqls.size();
    List<Issue> issues = detector.evaluate(records, RICH_INDEX);
    if (!issues.isEmpty()) {
      falsePositives += issues.size();
      for (Issue issue : issues) {
        falsePositiveDetails.add(
            String.format(
                "  [%s] %s -> %s",
                detector.getClass().getSimpleName(), issue.type(), issue.query()));
      }
    }
    return issues;
  }

  // ── SelectAllDetector ────────────────────────────────────────────────

  @Test
  void selectAllDetector_legitimateQueries() {
    List<String> sqls =
        List.of(
            "select u1_0.id,u1_0.name,u1_0.email from users u1_0 where u1_0.id=?",
            "select o1_0.id,o1_0.total,o1_0.status from orders o1_0 where o1_0.user_id=?",
            "select count(*) from users where status=?",
            "select u1_0.id from users u1_0 where u1_0.email=?",
            "insert into users (id,name,email) values (?,?,?)",
            "update users set name=?,version=? where id=? and version=?",
            "delete from sessions where expired_at<?",
            "select p1_0.id,p1_0.name,p1_0.price from products p1_0 where p1_0.category_id=?",
            "select distinct u1_0.status from users u1_0",
            "select u1_0.id,u1_0.name from users u1_0 inner join orders o1_0 on u1_0.id=o1_0.user_id where o1_0.status=?");
    List<Issue> issues = evaluate(new SelectAllDetector(), sqls);
    assertThat(issues).as("SelectAllDetector false positives").isEmpty();
  }

  // ── LikeWildcardDetector ─────────────────────────────────────────────

  @Test
  void likeWildcardDetector_legitimateQueries() {
    // Audit focus: WARNING-level issues only. Post-#91, parameterized LIKE (`LIKE ?`) emits
    // an INFO-level suggestive heads-up because the runtime binding could begin with '%';
    // those are not false positives for this audit.
    List<String> sqls =
        List.of(
            "select u1_0.id,u1_0.name from users u1_0 where u1_0.name like 'John%'",
            "select u1_0.id from users u1_0 where u1_0.email like ?",
            "select p1_0.id,p1_0.name from products p1_0 where p1_0.name like 'Apple%'",
            "select u1_0.id from users u1_0 where u1_0.name=?",
            "select o1_0.id from orders o1_0 where o1_0.tracking_code like 'TRK-%'",
            "insert into audit_log (action,detail) values (?,?)",
            "select u1_0.id from users u1_0 where u1_0.status='active'",
            "select p1_0.id from products p1_0 where p1_0.sku like 'SKU-2024%'",
            "update users set last_login=? where id=?",
            "select u1_0.id from users u1_0 where u1_0.name like ?");
    List<Issue> issues = evaluate(new LikeWildcardDetector(), sqls);
    assertThat(issues)
        .as("LikeWildcardDetector WARNING-level false positives")
        .filteredOn(i -> i.severity() == Severity.WARNING)
        .isEmpty();
  }

  // ── UnboundedResultSetDetector ───────────────────────────────────────

  @Test
  void unboundedResultSetDetector_legitimateQueries() {
    List<String> sqls =
        List.of(
            "select u1_0.id,u1_0.name from users u1_0 where u1_0.id=?",
            "select count(*) from users where status=?",
            "select max(u1_0.created_at) from users u1_0",
            "select u1_0.id,u1_0.name from users u1_0 where u1_0.status=? limit 20",
            "select 1",
            "select u1_0.id from users u1_0 where u1_0.id=? for update",
            "select exists(select 1 from users where email=?)",
            "select sum(o1_0.total) from orders o1_0 where o1_0.user_id=?",
            "select avg(o1_0.total) from orders o1_0",
            "select min(p1_0.price) from products p1_0 where p1_0.category_id=?");
    List<Issue> issues = evaluate(new UnboundedResultSetDetector(), sqls);
    assertThat(issues).as("UnboundedResultSetDetector false positives").isEmpty();
  }

  // ── UpdateWithoutWhereDetector ───────────────────────────────────────

  @Test
  void updateWithoutWhereDetector_legitimateQueries() {
    List<String> sqls =
        List.of(
            "update users set name=?,version=? where id=? and version=?",
            "update orders set status=? where id=?",
            "delete from sessions where expired_at<?",
            "delete from order_items where order_id=?",
            "update products set price=? where id=? and version=?",
            "update users set last_login=? where id=?",
            "delete from notifications where user_id=? and read_at is not null",
            "update orders set shipped_at=? where id=? and status=?",
            "delete from temp_tokens where created_at<? and used=?",
            "update users set password_hash=? where id=? and email=?");
    List<Issue> issues = evaluate(new UpdateWithoutWhereDetector(), sqls);
    assertThat(issues).as("UpdateWithoutWhereDetector false positives").isEmpty();
  }

  // ── CartesianJoinDetector ────────────────────────────────────────────

  @Test
  void cartesianJoinDetector_legitimateQueries() {
    List<String> sqls =
        List.of(
            "select u1_0.id,o1_0.id from users u1_0 inner join orders o1_0 on u1_0.id=o1_0.user_id",
            "select u1_0.id from users u1_0 left join orders o1_0 on u1_0.id=o1_0.user_id where o1_0.id is null",
            "select o1_0.id,i1_0.id from orders o1_0 join order_items i1_0 on o1_0.id=i1_0.order_id",
            "select u1_0.id from users u1_0 cross join (select count(*) cnt from orders) sub1",
            "select p1_0.id from products p1_0 natural join categories c1_0",
            "select u1_0.id from users u1_0 join orders o1_0 on u1_0.id=o1_0.user_id join order_items i1_0 on o1_0.id=i1_0.order_id",
            "select u1_0.id from users u1_0 left join orders o1_0 on u1_0.id=o1_0.user_id left join payments p1_0 on o1_0.id=p1_0.order_id",
            "select u1_0.id from users u1_0 right join orders o1_0 on u1_0.id=o1_0.user_id",
            "insert into users (id,name) values (?,?)",
            "update orders set status=? where id=?");
    List<Issue> issues = evaluate(new CartesianJoinDetector(), sqls);
    assertThat(issues).as("CartesianJoinDetector false positives").isEmpty();
  }

  // ── NullComparisonDetector ───────────────────────────────────────────

  @Test
  void nullComparisonDetector_legitimateQueries() {
    List<String> sqls =
        List.of(
            "select u1_0.id from users u1_0 where u1_0.deleted_at is null",
            "select u1_0.id from users u1_0 where u1_0.email is not null",
            "select o1_0.id from orders o1_0 where o1_0.shipped_at is null and o1_0.status=?",
            "update users set deleted_at=? where id=? and deleted_at is null",
            "select u1_0.id from users u1_0 where u1_0.name=? and u1_0.deleted_at is null",
            "select count(*) from users where phone is null",
            "select u1_0.id from users u1_0 where u1_0.verified_at is not null",
            "delete from sessions where user_id=? and revoked_at is not null",
            "select p1_0.id from products p1_0 where p1_0.discontinued_at is null",
            "select o1_0.id from orders o1_0 where o1_0.cancelled_at is null and o1_0.user_id=?");
    List<Issue> issues = evaluate(new NullComparisonDetector(), sqls);
    assertThat(issues).as("NullComparisonDetector false positives").isEmpty();
  }

  // ── OrderByRandDetector ──────────────────────────────────────────────

  @Test
  void orderByRandDetector_legitimateQueries() {
    List<String> sqls =
        List.of(
            "select u1_0.id,u1_0.name from users u1_0 order by u1_0.name asc",
            "select u1_0.id from users u1_0 order by u1_0.created_at desc limit 10",
            "select o1_0.id from orders o1_0 order by o1_0.total desc limit 5",
            "select p1_0.id from products p1_0 order by p1_0.name asc limit 100",
            "select u1_0.id from users u1_0 order by u1_0.id",
            "select o1_0.id,o1_0.total from orders o1_0 where o1_0.user_id=? order by o1_0.created_at desc",
            "select p1_0.id from products p1_0 where p1_0.category_id=? order by p1_0.price asc",
            "insert into users (id,name) values (?,?)",
            "select u1_0.id from users u1_0 order by u1_0.last_login desc limit 20",
            "select o1_0.id from orders o1_0 order by o1_0.id desc limit 50");
    List<Issue> issues = evaluate(new OrderByRandDetector(), sqls);
    assertThat(issues).as("OrderByRandDetector false positives").isEmpty();
  }

  // ── TooManyJoinsDetector ─────────────────────────────────────────────

  @Test
  void tooManyJoinsDetector_legitimateQueries() {
    List<String> sqls =
        List.of(
            "select u1_0.id from users u1_0 join orders o1_0 on u1_0.id=o1_0.user_id",
            "select u1_0.id from users u1_0 join orders o1_0 on u1_0.id=o1_0.user_id join order_items i1_0 on o1_0.id=i1_0.order_id",
            "select u1_0.id from users u1_0 left join profiles p1_0 on u1_0.id=p1_0.user_id",
            "select u1_0.id from users u1_0 join orders o1_0 on u1_0.id=o1_0.user_id join order_items i1_0 on o1_0.id=i1_0.order_id join products p1_0 on i1_0.product_id=p1_0.id",
            "select u1_0.id from users u1_0 join roles r1_0 on u1_0.role_id=r1_0.id join departments d1_0 on u1_0.dept_id=d1_0.id join teams t1_0 on d1_0.team_id=t1_0.id join locations l1_0 on t1_0.loc_id=l1_0.id",
            "select u1_0.id from users u1_0 where u1_0.status=?",
            "select o1_0.id from orders o1_0 join payments p1_0 on o1_0.id=p1_0.order_id",
            "insert into users (id,name) values (?,?)",
            "select u1_0.id from users u1_0 left join addresses a1_0 on u1_0.id=a1_0.user_id left join countries c1_0 on a1_0.country_id=c1_0.id",
            "update orders set status=? where id=?");
    List<Issue> issues = evaluate(new TooManyJoinsDetector(), sqls);
    assertThat(issues).as("TooManyJoinsDetector false positives").isEmpty();
  }

  // ── OrAbuseDetector ──────────────────────────────────────────────────

  @Test
  void orAbuseDetector_legitimateQueries() {
    List<String> sqls =
        List.of(
            "select u1_0.id from users u1_0 where u1_0.status=? or u1_0.status=?",
            "select u1_0.id from users u1_0 where u1_0.id=? or u1_0.id=?",
            "select u1_0.id from users u1_0 where u1_0.name=?",
            "select o1_0.id from orders o1_0 where o1_0.status in (?,?,?)",
            "select u1_0.id from users u1_0 where u1_0.active=?",
            "select p1_0.id from products p1_0 where p1_0.category_id=? or p1_0.category_id=?",
            "insert into users (id,name) values (?,?)",
            "update users set status=? where id=?",
            "select u1_0.id from users u1_0 where u1_0.role=? and u1_0.active=?",
            "select o1_0.id from orders o1_0 where o1_0.user_id=?");
    List<Issue> issues = evaluate(new OrAbuseDetector(), sqls);
    assertThat(issues).as("OrAbuseDetector false positives").isEmpty();
  }

  // ── WhereFunctionDetector ────────────────────────────────────────────

  @Test
  void whereFunctionDetector_legitimateQueries() {
    List<String> sqls =
        List.of(
            "select u1_0.id from users u1_0 where u1_0.created_at>=? and u1_0.created_at<?",
            "select u1_0.id from users u1_0 where u1_0.status=?",
            "select u1_0.id from users u1_0 where u1_0.email=? and u1_0.deleted_at is null",
            "select count(*) from orders where total>?",
            "select o1_0.id from orders o1_0 where o1_0.user_id=? and o1_0.created_at>=?",
            "select u1_0.id,u1_0.name from users u1_0 where u1_0.id in (?,?,?)",
            "select p1_0.id from products p1_0 where p1_0.price between ? and ?",
            "update users set name=? where id=?",
            "select u1_0.id from users u1_0 where u1_0.id=?",
            "select o1_0.id from orders o1_0 where o1_0.status=? and o1_0.shipped_at is not null");
    List<Issue> issues = evaluate(new WhereFunctionDetector(), sqls);
    assertThat(issues).as("WhereFunctionDetector false positives").isEmpty();
  }

  // ── OffsetPaginationDetector ─────────────────────────────────────────

  @Test
  void offsetPaginationDetector_legitimateQueries() {
    List<String> sqls =
        List.of(
            "select u1_0.id,u1_0.name from users u1_0 order by u1_0.id limit 20",
            "select u1_0.id from users u1_0 where u1_0.status=? limit 10",
            "select o1_0.id from orders o1_0 order by o1_0.created_at desc limit 50",
            "select p1_0.id from products p1_0 limit 100",
            "select u1_0.id from users u1_0 where u1_0.id>? order by u1_0.id limit 20",
            "insert into users (id,name) values (?,?)",
            "update users set name=? where id=?",
            "select count(*) from users",
            "select u1_0.id from users u1_0 order by u1_0.name limit 10 offset 0",
            "select o1_0.id from orders o1_0 order by o1_0.id limit 20 offset 100");
    List<Issue> issues = evaluate(new OffsetPaginationDetector(), sqls);
    assertThat(issues).as("OffsetPaginationDetector false positives").isEmpty();
  }

  // ── LargeInListDetector ──────────────────────────────────────────────

  @Test
  void largeInListDetector_legitimateQueries() {
    List<String> sqls =
        List.of(
            "select u1_0.id from users u1_0 where u1_0.id in (?,?,?)",
            "select o1_0.id from orders o1_0 where o1_0.status in ('pending','shipped','delivered')",
            "select u1_0.id from users u1_0 where u1_0.role in (?,?)",
            "select p1_0.id from products p1_0 where p1_0.category_id in (?,?,?,?,?)",
            "select u1_0.id from users u1_0 where u1_0.id in (?,?,?,?,?,?,?,?,?,?)",
            "select o1_0.id from orders o1_0 where o1_0.id in (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            "select u1_0.id from users u1_0 where u1_0.status=?",
            "insert into users (id,name) values (?,?)",
            "select p1_0.id from products p1_0 where p1_0.id in (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            "update users set status=? where id in (?,?,?,?,?)");
    List<Issue> issues = evaluate(new LargeInListDetector(), sqls);
    assertThat(issues).as("LargeInListDetector false positives").isEmpty();
  }

  // ── DistinctMisuseDetector ───────────────────────────────────────────

  @Test
  void distinctMisuseDetector_legitimateQueries() {
    List<String> sqls =
        List.of(
            "select distinct u1_0.status from users u1_0",
            "select distinct u1_0.role from users u1_0 where u1_0.active=?",
            "select u1_0.id,u1_0.name from users u1_0 where u1_0.id=?",
            "select distinct o1_0.status from orders o1_0",
            "select distinct p1_0.category_id from products p1_0",
            "insert into users (id,name) values (?,?)",
            "update users set name=? where id=?",
            "select count(distinct u1_0.email) from users u1_0",
            "select distinct u1_0.country from users u1_0 where u1_0.active=?",
            "select u1_0.id from users u1_0 where u1_0.status=? limit 10");
    List<Issue> issues = evaluate(new DistinctMisuseDetector(), sqls);
    assertThat(issues).as("DistinctMisuseDetector false positives").isEmpty();
  }

  // ── CountInsteadOfExistsDetector ─────────────────────────────────────

  @Test
  void countInsteadOfExistsDetector_legitimateQueries() {
    List<String> sqls =
        List.of(
            "select count(*) from users",
            "select count(*) from users group by status",
            "select count(distinct email) from users where status=?",
            "select u1_0.id, (select count(*) from orders o where o.user_id=u1_0.id) from users u1_0",
            "select count(*) from orders group by user_id having count(*)>?",
            "select u1_0.id from users u1_0 where exists (select 1 from orders o1_0 where o1_0.user_id=u1_0.id)",
            "insert into users (id,name) values (?,?)",
            "update users set name=? where id=?",
            "select sum(total) from orders where user_id=?",
            "select max(created_at) from users where status=?");
    List<Issue> issues = evaluate(new CountInsteadOfExistsDetector(), sqls);
    assertThat(issues).as("CountInsteadOfExistsDetector false positives").isEmpty();
  }

  // ── SelectCountStarWithoutWhereDetector ──────────────────────────────

  @Test
  void selectCountStarWithoutWhereDetector_legitimateQueries() {
    List<String> sqls =
        List.of(
            "select count(*) from users where status=?",
            "select count(1) from orders where user_id=?",
            "select count(*) from products where category_id=?",
            "select count(*) from users group by status",
            "select u1_0.id from users u1_0 where u1_0.id=?",
            "insert into users (id,name) values (?,?)",
            "select count(*) from orders where created_at>=? and created_at<?",
            "update users set name=? where id=?",
            "select count(1) from sessions where user_id=? and active=?",
            "select count(*) from order_items where order_id=?");
    List<Issue> issues = evaluate(new SelectCountStarWithoutWhereDetector(), sqls);
    assertThat(issues).as("SelectCountStarWithoutWhereDetector false positives").isEmpty();
  }

  // ── ImplicitJoinDetector ─────────────────────────────────────────────

  @Test
  void implicitJoinDetector_legitimateQueries() {
    List<String> sqls =
        List.of(
            "select u1_0.id from users u1_0 join orders o1_0 on u1_0.id=o1_0.user_id",
            "select u1_0.id from users u1_0 inner join orders o1_0 on u1_0.id=o1_0.user_id",
            "select u1_0.id from users u1_0 left join orders o1_0 on u1_0.id=o1_0.user_id",
            "select u1_0.id from users u1_0 right join orders o1_0 on u1_0.id=o1_0.user_id",
            "select u1_0.id from users u1_0 where u1_0.status=?",
            "select o1_0.id from orders o1_0 join order_items i1_0 on o1_0.id=i1_0.order_id join products p1_0 on i1_0.product_id=p1_0.id",
            "insert into users (id,name,email) values (?,?,?)",
            "update users set status=? where id=?",
            "delete from sessions where expired_at<?",
            "select p1_0.id from products p1_0 left join categories c1_0 on p1_0.category_id=c1_0.id");
    List<Issue> issues = evaluate(new ImplicitJoinDetector(), sqls);
    assertThat(issues).as("ImplicitJoinDetector false positives").isEmpty();
  }

  // ── UnionWithoutAllDetector ──────────────────────────────────────────

  @Test
  void unionWithoutAllDetector_legitimateQueries() {
    List<String> sqls =
        List.of(
            "select u1_0.id from users u1_0 where u1_0.status=? union all select u2_0.id from users u2_0 where u2_0.role=?",
            "select u1_0.id from users u1_0 where u1_0.active=?",
            "insert into users (id,name) values (?,?)",
            "update users set name=? where id=?",
            "select o1_0.id from orders o1_0 where o1_0.status=? union all select o2_0.id from orders o2_0 where o2_0.status=?",
            "select u1_0.id from users u1_0 limit 10",
            "select count(*) from users",
            "select p1_0.id from products p1_0 union all select p2_0.id from archived_products p2_0",
            "delete from sessions where expired_at<?",
            "select u1_0.id from users u1_0 where u1_0.id=?");
    List<Issue> issues = evaluate(new UnionWithoutAllDetector(), sqls);
    assertThat(issues).as("UnionWithoutAllDetector false positives").isEmpty();
  }

  // ── NotInSubqueryDetector ────────────────────────────────────────────

  @Test
  void notInSubqueryDetector_legitimateQueries() {
    List<String> sqls =
        List.of(
            "select u1_0.id from users u1_0 where u1_0.id not in (?,?,?)",
            "select u1_0.id from users u1_0 where not exists (select 1 from blocked_users b where b.user_id=u1_0.id)",
            "select o1_0.id from orders o1_0 where o1_0.status not in ('cancelled','refunded')",
            "select u1_0.id from users u1_0 where u1_0.role in (select r.name from roles r where r.active=?)",
            "select u1_0.id from users u1_0 where u1_0.status=?",
            "insert into users (id,name) values (?,?)",
            "update users set status=? where id=?",
            "select p1_0.id from products p1_0 where p1_0.id in (select i.product_id from order_items i)",
            "select u1_0.id from users u1_0 where u1_0.email is not null",
            "delete from sessions where user_id=?");
    List<Issue> issues = evaluate(new NotInSubqueryDetector(), sqls);
    assertThat(issues).as("NotInSubqueryDetector false positives").isEmpty();
  }

  // ── HavingMisuseDetector ─────────────────────────────────────────────

  @Test
  void havingMisuseDetector_legitimateQueries() {
    List<String> sqls =
        List.of(
            "select u1_0.status,count(*) cnt from users u1_0 group by u1_0.status having count(*)>?",
            "select o1_0.user_id,sum(o1_0.total) total_sum from orders o1_0 group by o1_0.user_id having sum(o1_0.total)>?",
            "select p1_0.category_id,avg(p1_0.price) from products p1_0 group by p1_0.category_id having avg(p1_0.price)>?",
            "select u1_0.role,count(*) from users u1_0 group by u1_0.role having count(*)>=?",
            "select o1_0.status,max(o1_0.total) from orders o1_0 group by o1_0.status having max(o1_0.total)>?",
            "select u1_0.id from users u1_0 where u1_0.status=?",
            "insert into users (id,name) values (?,?)",
            "select u1_0.dept_id,count(*) cnt from users u1_0 group by u1_0.dept_id having cnt>?",
            "select o1_0.user_id,count(*) order_count from orders o1_0 group by o1_0.user_id having order_count>=?",
            "update users set name=? where id=?");
    List<Issue> issues = evaluate(new HavingMisuseDetector(), sqls);
    assertThat(issues).as("HavingMisuseDetector false positives").isEmpty();
  }

  // ── StringConcatInWhereDetector ──────────────────────────────────────

  @Test
  void stringConcatInWhereDetector_legitimateQueries() {
    List<String> sqls =
        List.of(
            "select u1_0.id from users u1_0 where u1_0.name=?",
            "select u1_0.id from users u1_0 where u1_0.first_name=? and u1_0.last_name=?",
            "select u1_0.id from users u1_0 where u1_0.email like ?",
            "select o1_0.id from orders o1_0 where o1_0.status=? and o1_0.user_id=?",
            "update users set name=? where id=?",
            "insert into users (id,name) values (?,?)",
            "select p1_0.id from products p1_0 where p1_0.name=? and p1_0.sku=?",
            "select u1_0.id from users u1_0 where u1_0.id=?",
            "delete from sessions where user_id=?",
            "select u1_0.id from users u1_0 where u1_0.country=? and u1_0.city=?");
    List<Issue> issues = evaluate(new StringConcatInWhereDetector(), sqls);
    assertThat(issues).as("StringConcatInWhereDetector false positives").isEmpty();
  }

  // ── CorrelatedSubqueryDetector ───────────────────────────────────────

  @Test
  void correlatedSubqueryDetector_legitimateQueries() {
    List<String> sqls =
        List.of(
            "select u1_0.id,u1_0.name from users u1_0 where u1_0.status=?",
            "select u1_0.id from users u1_0 join orders o1_0 on u1_0.id=o1_0.user_id",
            "select u1_0.id from users u1_0 where u1_0.id in (select o.user_id from orders o where o.status=?)",
            "select u1_0.id from users u1_0 where exists (select 1 from orders o where o.user_id=u1_0.id)",
            "select count(*) from users where status=?",
            "insert into users (id,name) values (?,?)",
            "update users set name=? where id=?",
            "select u1_0.id,(select max(created_at) from system_config) as last_updated from users u1_0 where u1_0.active=?",
            "select o1_0.id,o1_0.total from orders o1_0 where o1_0.user_id=?",
            "select p1_0.id,p1_0.name from products p1_0 left join categories c1_0 on p1_0.category_id=c1_0.id");
    List<Issue> issues = evaluate(new CorrelatedSubqueryDetector(), sqls);
    assertThat(issues).as("CorrelatedSubqueryDetector false positives").isEmpty();
  }

  // ── InsertSelectAllDetector ──────────────────────────────────────────

  @Test
  void insertSelectAllDetector_legitimateQueries() {
    List<String> sqls =
        List.of(
            "insert into users (id,name,email) values (?,?,?)",
            "insert into archive_orders (id,user_id,total) select id,user_id,total from orders where status=?",
            "insert into users (id,name) values (?,?)",
            "insert into order_items (order_id,product_id,qty) values (?,?,?)",
            "insert into audit_log (action,entity_id,timestamp) values (?,?,?)",
            "insert into notifications (user_id,message,created_at) values (?,?,?)",
            "update users set name=? where id=?",
            "select u1_0.id from users u1_0 where u1_0.status=?",
            "insert into temp_results (id,score) select id,score from candidates where round=?",
            "delete from sessions where expired_at<?");
    List<Issue> issues = evaluate(new InsertSelectAllDetector(), sqls);
    assertThat(issues).as("InsertSelectAllDetector false positives").isEmpty();
  }

  // ── GroupByFunctionDetector ──────────────────────────────────────────

  @Test
  void groupByFunctionDetector_legitimateQueries() {
    List<String> sqls =
        List.of(
            "select u1_0.status,count(*) from users u1_0 group by u1_0.status",
            "select o1_0.user_id,sum(o1_0.total) from orders o1_0 group by o1_0.user_id",
            "select p1_0.category_id,count(*) from products p1_0 group by p1_0.category_id",
            "select u1_0.role,u1_0.dept_id,count(*) from users u1_0 group by u1_0.role,u1_0.dept_id",
            "select o1_0.status,avg(o1_0.total) from orders o1_0 group by o1_0.status",
            "select u1_0.country,count(*) from users u1_0 group by u1_0.country",
            "insert into users (id,name) values (?,?)",
            "select u1_0.id from users u1_0 where u1_0.status=?",
            "select p1_0.brand,count(*) from products p1_0 group by p1_0.brand",
            "update users set name=? where id=?");
    List<Issue> issues = evaluate(new GroupByFunctionDetector(), sqls);
    assertThat(issues).as("GroupByFunctionDetector false positives").isEmpty();
  }

  // ── SargabilityDetector ──────────────────────────────────────────────

  @Test
  void sargabilityDetector_legitimateQueries() {
    List<String> sqls =
        List.of(
            "select u1_0.id from users u1_0 where u1_0.age=?",
            "select u1_0.id from users u1_0 where u1_0.age>? and u1_0.age<?",
            "select o1_0.id from orders o1_0 where o1_0.total>=?",
            "select u1_0.id from users u1_0 where u1_0.created_at>=? and u1_0.created_at<?",
            "select p1_0.id from products p1_0 where p1_0.price between ? and ?",
            "select u1_0.id from users u1_0 where u1_0.status=? and u1_0.role=?",
            "insert into users (id,name) values (?,?)",
            "update users set name=? where id=?",
            "select o1_0.id from orders o1_0 where o1_0.user_id=? and o1_0.status=?",
            "delete from sessions where expired_at<?");
    List<Issue> issues = evaluate(new SargabilityDetector(), sqls);
    assertThat(issues).as("SargabilityDetector false positives").isEmpty();
  }

  // ── RedundantFilterDetector ──────────────────────────────────────────

  @Test
  void redundantFilterDetector_legitimateQueries() {
    List<String> sqls =
        List.of(
            "select u1_0.id from users u1_0 where u1_0.status=? and u1_0.active=?",
            "select u1_0.id from users u1_0 where u1_0.email=? and u1_0.deleted_at is null",
            "select o1_0.id from orders o1_0 where o1_0.user_id=? and o1_0.status=?",
            "select u1_0.id from users u1_0 where u1_0.age>? and u1_0.country=?",
            "select p1_0.id from products p1_0 where p1_0.category_id=? and p1_0.active=?",
            "select u1_0.id from users u1_0 where u1_0.role=? and u1_0.dept_id=? and u1_0.active=?",
            "insert into users (id,name) values (?,?)",
            "update users set name=? where id=?",
            "select o1_0.id from orders o1_0 where o1_0.created_at>=? and o1_0.status=?",
            "delete from sessions where user_id=? and expired_at<?");
    List<Issue> issues = evaluate(new RedundantFilterDetector(), sqls);
    assertThat(issues).as("RedundantFilterDetector false positives").isEmpty();
  }

  // ── ImplicitColumnsInsertDetector ────────────────────────────────────

  @Test
  void implicitColumnsInsertDetector_legitimateQueries() {
    List<String> sqls =
        List.of(
            "insert into users (id,name,email) values (?,?,?)",
            "insert into orders (id,user_id,total,status) values (?,?,?,?)",
            "insert into order_items (order_id,product_id,quantity,price) values (?,?,?,?)",
            "insert into products (id,name,price,category_id) values (?,?,?,?)",
            "insert into sessions (id,user_id,token,created_at) values (?,?,?,?)",
            "insert into audit_log (action,entity_type,entity_id,timestamp) values (?,?,?,?)",
            "insert into notifications (user_id,message,type,created_at) values (?,?,?,?)",
            "update users set name=? where id=?",
            "select u1_0.id from users u1_0 where u1_0.status=?",
            "delete from sessions where expired_at<?");
    List<Issue> issues = evaluate(new ImplicitColumnsInsertDetector(), sqls);
    assertThat(issues).as("ImplicitColumnsInsertDetector false positives").isEmpty();
  }

  // ── InsertOnDuplicateKeyDetector ─────────────────────────────────────

  @Test
  void insertOnDuplicateKeyDetector_legitimateQueries() {
    List<String> sqls =
        List.of(
            "insert into users (id,name,email) values (?,?,?)",
            "insert into orders (id,user_id,total) values (?,?,?)",
            "insert into order_items (order_id,product_id,qty) values (?,?,?)",
            "update users set name=? where id=?",
            "select u1_0.id from users u1_0 where u1_0.status=?",
            "delete from sessions where expired_at<?",
            "insert into products (id,name,price) values (?,?,?)",
            "insert into sessions (id,user_id,token) values (?,?,?)",
            "insert into audit_log (action,entity_id) values (?,?)",
            "insert into notifications (user_id,message) values (?,?)");
    List<Issue> issues = evaluate(new InsertOnDuplicateKeyDetector(), sqls);
    assertThat(issues).as("InsertOnDuplicateKeyDetector false positives").isEmpty();
  }

  // ── RegexpInsteadOfLikeDetector ──────────────────────────────────────

  @Test
  void regexpInsteadOfLikeDetector_legitimateQueries() {
    List<String> sqls =
        List.of(
            "select u1_0.id from users u1_0 where u1_0.name like ?",
            "select u1_0.id from users u1_0 where u1_0.email like 'test%'",
            "select p1_0.id from products p1_0 where p1_0.name like '%widget%'",
            "select u1_0.id from users u1_0 where u1_0.status=?",
            "insert into users (id,name) values (?,?)",
            "update users set name=? where id=?",
            "select o1_0.id from orders o1_0 where o1_0.tracking like 'TRK%'",
            "select u1_0.id from users u1_0 where u1_0.phone like '+1%'",
            "delete from sessions where token like 'expired_%'",
            "select p1_0.id from products p1_0 where p1_0.sku like 'SKU-%'");
    List<Issue> issues = evaluate(new RegexpInsteadOfLikeDetector(), sqls);
    assertThat(issues).as("RegexpInsteadOfLikeDetector false positives").isEmpty();
  }

  // ── FindInSetDetector ────────────────────────────────────────────────

  @Test
  void findInSetDetector_legitimateQueries() {
    List<String> sqls =
        List.of(
            "select u1_0.id from users u1_0 where u1_0.role in (?,?,?)",
            "select u1_0.id from users u1_0 where u1_0.status=?",
            "select p1_0.id from products p1_0 where p1_0.category_id in (?,?)",
            "select o1_0.id from orders o1_0 where o1_0.status in ('pending','active')",
            "insert into users (id,name) values (?,?)",
            "update users set name=? where id=?",
            "select u1_0.id from users u1_0 where u1_0.id=?",
            "select u1_0.id from users u1_0 join user_roles ur on u1_0.id=ur.user_id where ur.role_id=?",
            "delete from sessions where user_id=?",
            "select p1_0.id from products p1_0 where p1_0.active=?");
    List<Issue> issues = evaluate(new FindInSetDetector(), sqls);
    assertThat(issues).as("FindInSetDetector false positives").isEmpty();
  }

  // ── Hibernate-style complex queries (cross-detector) ────────────────

  @Test
  void hibernateRealisticPatterns_noFalsePositives() {
    // These are realistic Hibernate 6+ generated queries
    List<String> hibernateQueries =
        List.of(
            // Typical entity load by PK
            "select u1_0.id,u1_0.name,u1_0.email,u1_0.status,u1_0.created_at,u1_0.version from users u1_0 where u1_0.id=?",
            // Eager fetch with join
            "select u1_0.id,u1_0.name,o1_0.id,o1_0.total,o1_0.status from users u1_0 left join orders o1_0 on u1_0.id=o1_0.user_id where u1_0.id=?",
            // Paginated query
            "select u1_0.id,u1_0.name,u1_0.email from users u1_0 where u1_0.status=? order by u1_0.created_at desc limit ? offset ?",
            // Optimistic locking update
            "update users set name=?,email=?,status=?,version=? where id=? and version=?",
            // Soft delete check
            "select u1_0.id,u1_0.name from users u1_0 where u1_0.deleted_at is null and u1_0.status=?",
            // Batch insert
            "insert into order_items (id,order_id,product_id,quantity,unit_price) values (?,?,?,?,?)",
            // Collection fetch
            "select o1_0.id,o1_0.user_id,o1_0.total,o1_0.status,o1_0.created_at from orders o1_0 where o1_0.user_id=? order by o1_0.created_at desc",
            // Exists check
            "select case when count(*)>0 then 1 else 0 end from users u1_0 where u1_0.email=?",
            // Multi-column WHERE
            "select u1_0.id from users u1_0 where u1_0.tenant_id=? and u1_0.status=? and u1_0.role=? order by u1_0.name limit ?",
            // Delete by FK
            "delete from order_items where order_id=?");

    // Run through all "syntax/pattern" detectors that don't need index metadata
    List<DetectionRule> patternDetectors =
        List.of(
            new SelectAllDetector(),
            new LikeWildcardDetector(),
            new UpdateWithoutWhereDetector(),
            new CartesianJoinDetector(),
            new NullComparisonDetector(),
            new OrderByRandDetector(),
            new TooManyJoinsDetector(),
            new InsertSelectAllDetector(),
            new ImplicitJoinDetector(),
            new NotInSubqueryDetector(),
            new StringConcatInWhereDetector(),
            new InsertOnDuplicateKeyDetector(),
            new RegexpInsteadOfLikeDetector(),
            new FindInSetDetector(),
            new ImplicitColumnsInsertDetector(),
            new SargabilityDetector(),
            new GroupByFunctionDetector(),
            new CorrelatedSubqueryDetector(),
            new RedundantFilterDetector());

    List<QueryRecord> records = hibernateQueries.stream().map(FalsePositiveAuditTest::q).toList();
    totalQueries += hibernateQueries.size() * patternDetectors.size();

    for (DetectionRule detector : patternDetectors) {
      List<Issue> issues = detector.evaluate(records, RICH_INDEX);
      if (!issues.isEmpty()) {
        falsePositives += issues.size();
        for (Issue issue : issues) {
          falsePositiveDetails.add(
              String.format(
                  "  [%s] %s -> %s",
                  detector.getClass().getSimpleName(), issue.type(), issue.query()));
        }
      }
      assertThat(issues)
          .as("False positives from %s on Hibernate patterns", detector.getClass().getSimpleName())
          .isEmpty();
    }
  }

  // ── Additional realistic patterns for broader coverage ──────────────

  @Test
  void additionalRealisticPatterns_selectAllDetector() {
    // Edge cases: column names containing "star" or asterisk in strings
    List<String> sqls =
        List.of(
            "select u1_0.id,u1_0.rating from users u1_0 where u1_0.rating>=?",
            "select p1_0.id,p1_0.star_count from products p1_0 where p1_0.active=?",
            "select u1_0.id from users u1_0 where u1_0.bio like '%star%'");
    List<Issue> issues = evaluate(new SelectAllDetector(), sqls);
    assertThat(issues).as("SelectAllDetector edge cases").isEmpty();
  }

  @Test
  void additionalRealisticPatterns_unboundedWithPkLookup() {
    // Queries with PK-like patterns that should be excluded
    List<String> sqls =
        List.of(
            "select u1_0.id,u1_0.name from users u1_0 where u1_0.user_id=?",
            "select o1_0.id,o1_0.total from orders o1_0 where o1_0.order_id=?");
    List<Issue> issues = evaluate(new UnboundedResultSetDetector(), sqls);
    assertThat(issues).as("UnboundedResultSetDetector PK lookup").isEmpty();
  }

  @Test
  void additionalRealisticPatterns_whereFunction_aggregatesInSelect() {
    // Aggregate functions in SELECT should not trigger WhereFunctionDetector
    List<String> sqls =
        List.of(
            "select count(*),max(u1_0.created_at) from users u1_0 where u1_0.status=?",
            "select sum(o1_0.total),avg(o1_0.total) from orders o1_0 where o1_0.user_id=?",
            "select min(p1_0.price),max(p1_0.price) from products p1_0 where p1_0.category_id=?");
    List<Issue> issues = evaluate(new WhereFunctionDetector(), sqls);
    assertThat(issues).as("WhereFunctionDetector aggregate in SELECT").isEmpty();
  }

  @Test
  void additionalRealisticPatterns_cartesianJoin_subqueries() {
    // Subqueries in FROM clause should not trigger Cartesian detection
    List<String> sqls =
        List.of(
            "select u1_0.id from users u1_0 where u1_0.id in (select o.user_id from orders o where o.total>?)",
            "select u1_0.id from users u1_0 where u1_0.email=? and u1_0.status=?");
    List<Issue> issues = evaluate(new CartesianJoinDetector(), sqls);
    assertThat(issues).as("CartesianJoinDetector subquery").isEmpty();
  }

  @Test
  void additionalRealisticPatterns_offsetSmall() {
    // Small literal offsets should not be flagged (threshold is 1000)
    List<String> sqls =
        List.of(
            "select u1_0.id from users u1_0 order by u1_0.id limit 20 offset 20",
            "select u1_0.id from users u1_0 order by u1_0.id limit 20 offset 40",
            "select u1_0.id from users u1_0 order by u1_0.id limit 20 offset 500",
            "select u1_0.id from users u1_0 order by u1_0.id limit 20 offset 999");
    List<Issue> issues = evaluate(new OffsetPaginationDetector(), sqls);
    assertThat(issues).as("OffsetPaginationDetector small offsets").isEmpty();
  }

  @Test
  void additionalRealisticPatterns_nullComparison_caseWhen() {
    // CASE WHEN with IS NULL should be fine
    List<String> sqls =
        List.of(
            "select u1_0.id,case when u1_0.phone is null then 'no phone' else u1_0.phone end from users u1_0 where u1_0.id=?",
            "select coalesce(u1_0.nickname, u1_0.name) from users u1_0 where u1_0.id=?");
    List<Issue> issues = evaluate(new NullComparisonDetector(), sqls);
    assertThat(issues).as("NullComparisonDetector CASE/COALESCE").isEmpty();
  }

  @Test
  void additionalRealisticPatterns_orAbuse_sameColumn() {
    // Multiple ORs on the same column should NOT be flagged (can use IN)
    List<String> sqls =
        List.of(
            "select u1_0.id from users u1_0 where u1_0.status='active' or u1_0.status='pending' or u1_0.status='review' or u1_0.status='new'");
    List<Issue> issues = evaluate(new OrAbuseDetector(), sqls);
    assertThat(issues).as("OrAbuseDetector same-column ORs").isEmpty();
  }

  @Test
  void additionalRealisticPatterns_distinctWithoutJoinOrGroupBy() {
    // DISTINCT on a simple query without JOIN or GROUP BY is legitimate
    List<String> sqls =
        List.of(
            "select distinct u1_0.country from users u1_0 where u1_0.active=?",
            "select distinct p1_0.brand from products p1_0 where p1_0.category_id=?");
    List<Issue> issues = evaluate(new DistinctMisuseDetector(), sqls);
    assertThat(issues).as("DistinctMisuseDetector simple distinct").isEmpty();
  }

  @Test
  void additionalRealisticPatterns_largeInList_boundary() {
    // IN list with exactly 100 values -- should NOT trigger (threshold is >100)
    StringBuilder sb = new StringBuilder("select u1_0.id from users u1_0 where u1_0.id in (");
    for (int i = 0; i < 100; i++) {
      if (i > 0) sb.append(",");
      sb.append("?");
    }
    sb.append(")");
    List<String> sqls = List.of(sb.toString());
    List<Issue> issues = evaluate(new LargeInListDetector(), sqls);
    assertThat(issues).as("LargeInListDetector at boundary=100").isEmpty();
  }

  @Test
  void additionalRealisticPatterns_tooManyJoins_exactly5() {
    // Exactly 5 joins should NOT trigger (threshold is >5)
    String sql =
        "select u1_0.id from users u1_0 "
            + "join orders o1_0 on u1_0.id=o1_0.user_id "
            + "join order_items i1_0 on o1_0.id=i1_0.order_id "
            + "join products p1_0 on i1_0.product_id=p1_0.id "
            + "join categories c1_0 on p1_0.category_id=c1_0.id "
            + "join brands b1_0 on p1_0.brand_id=b1_0.id";
    List<String> sqls = List.of(sql);
    List<Issue> issues = evaluate(new TooManyJoinsDetector(), sqls);
    assertThat(issues).as("TooManyJoinsDetector at threshold=5").isEmpty();
  }

  @Test
  void additionalRealisticPatterns_havingWithAggregateAlias() {
    // HAVING referencing an alias of an aggregate function should be fine
    List<String> sqls =
        List.of(
            "select u1_0.status,count(*) as cnt from users u1_0 group by u1_0.status having cnt>?",
            "select o1_0.user_id,sum(o1_0.total) as total_sum from orders o1_0 group by o1_0.user_id having total_sum>?");
    List<Issue> issues = evaluate(new HavingMisuseDetector(), sqls);
    assertThat(issues).as("HavingMisuseDetector aggregate alias").isEmpty();
  }

  @Test
  void additionalRealisticPatterns_updateWithWhereSubquery() {
    // UPDATE with WHERE clause containing subquery
    List<String> sqls =
        List.of(
            "update orders set status='cancelled' where id=? and user_id=?",
            "delete from notifications where user_id=? and created_at<?");
    List<Issue> issues = evaluate(new UpdateWithoutWhereDetector(), sqls);
    assertThat(issues).as("UpdateWithoutWhereDetector with complex WHERE").isEmpty();
  }

  @Test
  void additionalRealisticPatterns_countWithGroupBy() {
    // COUNT(*) with GROUP BY is a legitimate aggregation, not an existence check
    List<String> sqls =
        List.of(
            "select status,count(*) from orders group by status",
            "select user_id,count(1) from orders group by user_id");
    List<Issue> issues = evaluate(new CountInsteadOfExistsDetector(), sqls);
    assertThat(issues).as("CountInsteadOfExistsDetector with GROUP BY").isEmpty();
  }

  @Test
  void additionalRealisticPatterns_insertWithColumns() {
    // INSERT with explicit column list should never be flagged by ImplicitColumnsInsertDetector
    List<String> sqls =
        List.of(
            "insert into users (id, name, email, created_at) values (?, ?, ?, ?)",
            "insert into orders (id, user_id, total, status, created_at) values (?, ?, ?, ?, ?)");
    List<Issue> issues = evaluate(new ImplicitColumnsInsertDetector(), sqls);
    assertThat(issues).as("ImplicitColumnsInsertDetector explicit columns").isEmpty();
  }
}
