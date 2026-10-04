package io.queryaudit.core.regression;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.QueryRecord;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class QueryCountEvidenceTest {

  private static final String ORDERS = "select o.id, o.customer_id from orders o";
  private static final String CUSTOMER = "select c.id, c.name from customers c where c.id = ?";
  private static final String ITEMS = "select i.id, i.sku from items i where i.order_id = ?";

  private static QueryRecord query(String sql, String... frames) {
    return new QueryRecord(sql, 0, 0, String.join("\n", frames));
  }

  private static String grouped(List<QueryRecord> queries) {
    StringBuilder sb = new StringBuilder();
    QueryCountEvidence.appendGrouped(sb, queries, "  ");
    return sb.toString();
  }

  @Test
  void groupsRepeatedStatementsByCallSiteMostRepeatedFirst() {
    List<QueryRecord> queries = new ArrayList<>();
    queries.add(query(ORDERS, "demo.$Proxy136.findAll:-1", "demo.OrderService.summaries:39"));
    for (int i = 0; i < 5; i++) {
      queries.add(
          query(
              CUSTOMER,
              "demo.Customer$HibernateProxy.getName:-1",
              "demo.OrderService.lambda$summaries$0:40"));
    }
    queries.add(query(ORDERS, "demo.$Proxy136.findAll:-1", "demo.OrderService.itemCount:58"));
    for (int i = 0; i < 3; i++) {
      queries.add(query(ITEMS, "demo.OrderService.lambda$itemCount$1:58"));
    }

    assertThat(grouped(queries))
        .isEqualTo(
            String.join(
                "\n",
                "  5x " + CUSTOMER,
                "     at demo.OrderService.lambda$summaries$0:40",
                "  3x " + ITEMS,
                "     at demo.OrderService.lambda$itemCount$1:58",
                "  1x " + ORDERS,
                "     at demo.OrderService.summaries:39",
                "  1x " + ORDERS,
                "     at demo.OrderService.itemCount:58",
                ""));
  }

  @Test
  void statementsDifferingOnlyInLiteralsShareAGroup() {
    String caller = "demo.Report.build:12";
    List<QueryRecord> queries =
        List.of(
            query("select * from orders where id = 1", caller),
            query("select * from orders where id = 2", caller));

    assertThat(grouped(queries)).startsWith("  2x select * from orders where id = 1\n");
  }

  @Test
  void alignsTheCallSiteUnderTheStatementForWideCounts() {
    List<QueryRecord> queries = new ArrayList<>();
    for (int i = 0; i < 12; i++) {
      queries.add(query(CUSTOMER, "demo.Loop.run:7"));
    }

    assertThat(grouped(queries))
        .isEqualTo("  12x " + CUSTOMER + "\n" + "      at demo.Loop.run:7\n");
  }

  @Test
  void truncatesLongStatementsAndOmitsAMissingCallSite() {
    String longSql = "select " + "x, ".repeat(60) + "y from wide";

    assertThat(grouped(List.of(new QueryRecord(longSql, 0, 0, null))))
        .isEqualTo("  1x " + longSql.substring(0, 100) + "...\n");
  }
}
