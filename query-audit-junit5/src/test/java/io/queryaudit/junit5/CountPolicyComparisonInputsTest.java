package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.detector.QueryAuditAnalyzer;
import io.queryaudit.core.provenance.AuditCapability;
import io.queryaudit.core.provenance.ComparisonInputs;
import io.queryaudit.core.regression.QueryCountBaseline;
import io.queryaudit.core.regression.QueryCounts;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
class CountPolicyComparisonInputsTest {
  private static final String TEST_ID = "[engine:junit-jupiter]/[class:Fixture]/[method:query()]";

  @Test
  void budgetsAndContractCountsAreNotComparisonInputs() throws Exception {
    String unbounded = fingerprint("unbounded", Map.of());
    String contracted = fingerprint("unbounded", contract(new QueryCounts(1, 0, 0, 0, 1)));
    String reRecorded = fingerprint("unbounded", contract(new QueryCounts(2, 1, 0, 0, 3)));

    assertThat(contracted).isEqualTo(unbounded).isEqualTo(reRecorded);
    assertThat(fingerprint("selectBudget", Map.of()))
        .isEqualTo(fingerprint("looserBudget", Map.of()))
        .isEqualTo(fingerprint("totalBudget", Map.of()))
        .isEqualTo(unbounded);
  }

  @Test
  void contractRecordModeStillChangesTheInputs() throws Exception {
    String enforced = fingerprint("unbounded", Map.of());
    String previous = System.getProperty("queryAudit.contracts.record");
    try {
      System.setProperty("queryAudit.contracts.record", "true");
      assertThat(fingerprint("unbounded", Map.of())).isNotEqualTo(enforced);
    } finally {
      if (previous == null) System.clearProperty("queryAudit.contracts.record");
      else System.setProperty("queryAudit.contracts.record", previous);
    }
  }

  private static Map<String, QueryCounts> contract(QueryCounts counts) {
    return Map.of(QueryCountBaseline.key(TEST_ID), counts);
  }

  private static String fingerprint(String method, Map<String, QueryCounts> contracts)
      throws Exception {
    AuditScope scope = mock(AuditScope.class);
    ExtensionContext context = mock(ExtensionContext.class);
    Method testMethod = Fixtures.class.getDeclaredMethod(method);
    doReturn(Fixtures.class).when(context).getRequiredTestClass();
    when(context.getTestMethod()).thenReturn(Optional.of(testMethod));
    when(context.getRequiredTestMethod()).thenReturn(testMethod);
    QueryAuditExtension.AuditRunState state = new QueryAuditExtension.AuditRunState();
    AuditCapability absent = AuditCapability.absent();
    when(scope.context()).thenReturn(context);
    when(scope.runState()).thenReturn(state);
    when(scope.inputContext())
        .thenReturn(new AuditInputContext("h2", absent, absent, absent, null));
    when(scope.explainCapability()).thenReturn(absent);
    when(scope.contracts()).thenReturn(contracts);
    when(scope.countBaseline()).thenReturn(Map.of());
    new ComparisonInputRecorder(new AuditSettingsResolver(ignored -> QueryAuditConfig.defaults()))
        .record(
            scope,
            new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of()),
            TEST_ID,
            "Fixture",
            "query()");
    var run = state.result(List.of());
    assertThat(run.incompleteReasons()).isEmpty();
    ComparisonInputs inputs = run.comparisonInputs().get(TEST_ID);
    return inputs.fingerprints().queryContracts();
  }

  static class Fixtures {
    void unbounded() {}

    @ExpectQueries(select = 1, insert = 0)
    void selectBudget() {}

    @ExpectQueries(select = 5)
    void looserBudget() {}

    @ExpectQueries(total = 3)
    void totalBudget() {}
  }
}
