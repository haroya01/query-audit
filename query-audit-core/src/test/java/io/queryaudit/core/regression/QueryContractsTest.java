package io.queryaudit.core.regression;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.QueryRecord;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("QueryContracts (issue #166)")
class QueryContractsTest {

  private static final String KEY_CLASS = "OrderServiceTest";
  private static final String KEY_TEST = "findOrders";

  private static Map<String, QueryCounts> contracts(QueryCounts counts) {
    return Map.of(QueryCountBaseline.key(KEY_CLASS, KEY_TEST), counts);
  }

  @Test
  @DisplayName("no contract for the test — not enforced")
  void missingContractIsNotEnforced() {
    String failure =
        QueryContracts.verify(
            KEY_CLASS,
            "someOtherTest",
            new QueryCounts(5, 0, 0, 0, 5),
            contracts(new QueryCounts(1, 0, 0, 0, 1)),
            List.of());
    assertThat(failure).isNull();
  }

  @Test
  @DisplayName("matching counts — contract met")
  void matchingCountsPass() {
    QueryCounts counts = new QueryCounts(3, 1, 0, 0, 4);
    String failure =
        QueryContracts.verify(KEY_CLASS, KEY_TEST, counts, contracts(counts), List.of());
    assertThat(failure).isNull();
  }

  @Test
  @DisplayName("stable ID keeps the contract when the display name changes")
  void stableIdSurvivesDisplayNameChanges() {
    String testId = "[engine:junit-jupiter]/[class:example.OrderTest]/[method:findOrders()]";
    QueryCounts counts = new QueryCounts(3, 0, 0, 0, 3);
    Map<String, QueryCounts> contracts = Map.of(QueryCountBaseline.key(testId), counts);

    String failure =
        QueryContracts.verify(testId, KEY_CLASS, "renamed display", counts, contracts, List.of());

    assertThat(failure).isNull();
  }

  @Test
  @DisplayName("an increase fails with the delta and the offending SQL + call site")
  void increaseFailsWithDelta() {
    QueryRecord select =
        new QueryRecord(
            "SELECT * FROM orders",
            1_000L,
            0L,
            "com.example.OrderService.load(OrderService.java:42)");
    String failure =
        QueryContracts.verify(
            KEY_CLASS,
            KEY_TEST,
            new QueryCounts(2, 0, 0, 0, 2),
            contracts(new QueryCounts(1, 0, 0, 0, 1)),
            List.of(select, select));

    assertThat(failure).contains("deviates from its recorded query contract");
    assertThat(failure).contains("SELECT: contract 1, executed 2 (+1)");
    assertThat(failure).contains("SELECT * FROM orders");
    assertThat(failure).contains("at com.example.OrderService.load(OrderService.java:42)");
    assertThat(failure).contains("-DqueryAudit.contracts.record=true");
  }

  @Test
  @DisplayName("an increase groups the statements behind it by call site")
  void increaseGroupsRepeatedStatements() {
    QueryRecord list =
        new QueryRecord(
            "select * from orders", 0, 0, "demo.$Proxy136.findAll:-1\ndemo.OrderService.list:30");
    QueryRecord customer =
        new QueryRecord(
            "select * from customers where id = ?",
            0,
            0,
            "demo.Customer$HibernateProxy.getName:-1\ndemo.OrderService.lambda$list$0:31");
    String failure =
        QueryContracts.verify(
            KEY_CLASS,
            KEY_TEST,
            new QueryCounts(4, 0, 0, 0, 4),
            contracts(new QueryCounts(1, 0, 0, 0, 1)),
            List.of(list, customer, customer, customer));

    assertThat(failure)
        .contains(
            "  SELECT: contract 1, executed 4 (+3)\n"
                + "    3x select * from customers where id = ?\n"
                + "       at demo.OrderService.lambda$list$0:31\n"
                + "    1x select * from orders\n"
                + "       at demo.OrderService.list:30\n")
        .doesNotContain("$Proxy136")
        .doesNotContain("HibernateProxy");
  }

  @Test
  @DisplayName("a decrease also fails — snapshot semantics, both directions")
  void decreaseAlsoFails() {
    String failure =
        QueryContracts.verify(
            KEY_CLASS,
            KEY_TEST,
            new QueryCounts(1, 0, 0, 0, 1),
            contracts(new QueryCounts(2, 1, 0, 0, 3)),
            List.of());

    assertThat(failure).contains("SELECT: contract 2, executed 1 (-1)");
    assertThat(failure).contains("INSERT: contract 1, executed 0 (-1)");
  }
}
