package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.QueryRecord;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the {@link ExpectQueries} budget verification implemented in {@link
 * QueryAuditExtension#buildExpectQueriesFailureMessage}.
 */
class ExpectQueriesMessageTest {

  private static final String CALL_SITE = "com.example.OrderService.createOrder:42";

  /** Annotation fixtures — budgets are read from these methods via reflection. */
  @SuppressWarnings("unused")
  private static class Fixtures {

    @ExpectQueries(select = 1)
    void selectBudgetOne() {}

    @ExpectQueries(insert = 2)
    void insertBudgetTwo() {}

    @ExpectQueries(select = 0, update = 0)
    void selectAndUpdateForbidden() {}

    @ExpectQueries(insert = 0, update = 0, delete = 0)
    void readOnly() {}

    @ExpectQueries
    void noBudgets() {}

    @ExpectQueries(total = 2)
    void totalBudgetTwo() {}

    @ExpectQueries(select = 5, total = 2)
    void typeAndTotalBudgets() {}

    @ExpectQueries(select = 2, insert = 0, exact = true)
    void exactSelectsNoInserts() {}

    @ExpectQueries(total = 3, exact = true)
    void exactTotal() {}
  }

  private static ExpectQueries budget(String fixtureMethod) throws NoSuchMethodException {
    return Fixtures.class.getDeclaredMethod(fixtureMethod).getAnnotation(ExpectQueries.class);
  }

  private static QueryRecord query(String sql) {
    return new QueryRecord(sql, 0, 0, CALL_SITE);
  }

  @Test
  void totalBudgetCountsEveryStatementType() throws NoSuchMethodException {
    List<QueryRecord> queries =
        List.of(
            query("select * from orders"),
            query("insert into orders values (?)"),
            query("update orders set status = ?"));

    String message =
        QueryAuditExtension.buildExpectQueriesFailureMessage(
            budget("totalBudgetTwo"), queries, "createOrder()");

    assertThat(message)
        .contains("TOTAL: executed 3, expected at most 2.")
        .contains("update orders set status = ?");
  }

  @Test
  void totalBudgetAppliesAlongsideTypeBudgets() throws NoSuchMethodException {
    List<QueryRecord> queries = List.of(query("select 1"), query("select 2"), query("select 3"));

    String message =
        QueryAuditExtension.buildExpectQueriesFailureMessage(
            budget("typeAndTotalBudgets"), queries, "createOrder()");

    assertThat(message).contains("TOTAL: executed 3").doesNotContain("SELECT: executed");
    assertThat(
            QueryAuditExtension.buildExpectQueriesFailureMessage(
                budget("totalBudgetTwo"), List.of(query("select 1")), "createOrder()"))
        .isNull();
  }

  @Test
  void exactCountFailsWhenFewerQueriesRun() throws NoSuchMethodException {
    String message =
        QueryAuditExtension.buildExpectQueriesFailureMessage(
            budget("exactSelectsNoInserts"),
            List.of(query("select * from orders")),
            "listOrders()");

    assertThat(message)
        .startsWith("QueryAudit: listOrders() did not match its exact query counts.")
        .contains("SELECT: executed 1, expected exactly 2.")
        .contains("select * from orders")
        .contains("at " + CALL_SITE)
        .doesNotContain("INSERT:");
  }

  @Test
  void exactCountFailsWhenMoreQueriesRun() throws NoSuchMethodException {
    List<QueryRecord> queries = List.of(query("select 1"), query("select 2"), query("select 3"));

    assertThat(
            QueryAuditExtension.buildExpectQueriesFailureMessage(
                budget("exactSelectsNoInserts"), queries, "listOrders()"))
        .contains("SELECT: executed 3, expected exactly 2.");
  }

  @Test
  void exactCountPassesOnlyAtTheDeclaredCount() throws NoSuchMethodException {
    assertThat(
            QueryAuditExtension.buildExpectQueriesFailureMessage(
                budget("exactSelectsNoInserts"),
                List.of(query("select 1"), query("select 2")),
                "listOrders()"))
        .isNull();
    assertThat(
            QueryAuditExtension.buildExpectQueriesFailureMessage(
                budget("exactSelectsNoInserts"),
                List.of(query("select 1"), query("select 2"), query("insert into t values (1)")),
                "listOrders()"))
        .contains("INSERT: executed 1, expected exactly 0.");
  }

  @Test
  void exactAppliesToTheTotalAndLeavesUndeclaredTypesFree() throws NoSuchMethodException {
    List<QueryRecord> threeMixed =
        List.of(query("select 1"), query("insert into t values (1)"), query("delete from t"));

    assertThat(
            QueryAuditExtension.buildExpectQueriesFailureMessage(
                budget("exactTotal"), threeMixed, "sync()"))
        .isNull();
    assertThat(
            QueryAuditExtension.buildExpectQueriesFailureMessage(
                budget("exactTotal"), threeMixed.subList(0, 2), "sync()"))
        .contains("TOTAL: executed 2, expected exactly 3.");
  }

  @Test
  @DisplayName("Fails when the SELECT budget is exceeded, listing SQL and call site")
  void failsWhenSelectBudgetExceeded() throws NoSuchMethodException {
    List<QueryRecord> queries =
        List.of(query("select * from orders"), query("select * from members"));

    String message =
        QueryAuditExtension.buildExpectQueriesFailureMessage(
            budget("selectBudgetOne"), queries, "createOrder()");

    assertThat(message)
        .startsWith("QueryAudit: createOrder() exceeded its query budget.")
        .contains("SELECT: executed 2, expected at most 1")
        .contains("select * from orders")
        .contains("select * from members")
        .contains("at " + CALL_SITE);
  }

  @Test
  @DisplayName("Passes when the count equals the budget exactly")
  void passesAtExactBudget() throws NoSuchMethodException {
    List<QueryRecord> queries = List.of(query("select * from orders"));

    String message =
        QueryAuditExtension.buildExpectQueriesFailureMessage(
            budget("selectBudgetOne"), queries, "createOrder()");

    assertThat(message).isNull();
  }

  @Test
  @DisplayName("Ignores query types left at the default -1")
  void ignoresTypesLeftAtDefault() throws NoSuchMethodException {
    List<QueryRecord> queries =
        List.of(
            query("select * from orders"),
            query("insert into orders (id) values (1)"),
            query("update orders set status = 'PAID'"),
            query("delete from carts where id = 1"));

    String message =
        QueryAuditExtension.buildExpectQueriesFailureMessage(
            budget("noBudgets"), queries, "createOrder()");

    assertThat(message).isNull();
  }

  @Test
  @DisplayName("Counts only queries of the budgeted type")
  void countsOnlyMatchingType() throws NoSuchMethodException {
    List<QueryRecord> queries =
        List.of(
            query("select * from orders"),
            query("select * from members"),
            query("select * from teams"),
            query("insert into orders (id) values (1)"),
            query("insert into orders (id) values (2)"));

    String message =
        QueryAuditExtension.buildExpectQueriesFailureMessage(
            budget("insertBudgetTwo"), queries, "createOrder()");

    assertThat(message).isNull();
  }

  @Test
  @DisplayName("Reports every violated type in a single failure")
  void reportsEveryViolatedType() throws NoSuchMethodException {
    List<QueryRecord> queries =
        List.of(
            query("select * from orders"),
            query("update orders set status = 'PAID'"),
            query("insert into audit_log (id) values (1)"));

    String message =
        QueryAuditExtension.buildExpectQueriesFailureMessage(
            budget("selectAndUpdateForbidden"), queries, "createOrder()");

    assertThat(message)
        .contains("SELECT: executed 1, expected at most 0")
        .contains("UPDATE: executed 1, expected at most 0")
        .doesNotContain("INSERT:");
  }

  @Test
  @DisplayName("A zero budget blocks all DML on a read-only path")
  void zeroBudgetBlocksDml() throws NoSuchMethodException {
    List<QueryRecord> queries =
        List.of(query("select * from orders"), query("delete from orders where id = 1"));

    String message =
        QueryAuditExtension.buildExpectQueriesFailureMessage(
            budget("readOnly"), queries, "listOrders()");

    assertThat(message)
        .contains("DELETE: executed 1, expected at most 0")
        .doesNotContain("SELECT:");
  }

  @Test
  @DisplayName("Truncates long SQL to 100 characters in the failure message")
  void truncatesLongSql() throws NoSuchMethodException {
    String longSql =
        "select column_name_padding from a_very_long_table_name where " + "x".repeat(80);
    List<QueryRecord> queries = List.of(query(longSql), query("select 1"));

    String message =
        QueryAuditExtension.buildExpectQueriesFailureMessage(
            budget("selectBudgetOne"), queries, "createOrder()");

    assertThat(message).contains(longSql.substring(0, 100) + "...").doesNotContain(longSql);
  }
}
