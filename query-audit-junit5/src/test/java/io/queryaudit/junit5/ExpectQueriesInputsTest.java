package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.detector.QueryAuditAnalyzer;
import io.queryaudit.core.provenance.AuditCapability;
import io.queryaudit.core.provenance.ComparisonInputs;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
class ExpectQueriesInputsTest {

  @Test
  void switchingABudgetToExactCountsChangesTheComparisonInputs() throws Exception {
    ComparisonInputs upperBound = recordedInputs("upperBound");
    ComparisonInputs exact = recordedInputs("exact");

    assertThat(exact.fingerprints().queryContracts())
        .isNotEqualTo(upperBound.fingerprints().queryContracts());
    assertThat(recordedInputs("upperBoundAgain").fingerprints().queryContracts())
        .isEqualTo(upperBound.fingerprints().queryContracts());
  }

  private static ComparisonInputs recordedInputs(String method) throws Exception {
    AuditScope scope = mock(AuditScope.class);
    ExtensionContext context = context(method);
    QueryAuditExtension.AuditRunState state = new QueryAuditExtension.AuditRunState();
    AuditCapability absent = AuditCapability.absent();
    when(scope.context()).thenReturn(context);
    when(scope.runState()).thenReturn(state);
    when(scope.inputContext())
        .thenReturn(new AuditInputContext("h2", absent, absent, absent, null));
    when(scope.explainCapability()).thenReturn(absent);
    when(scope.contracts()).thenReturn(Map.of());
    when(scope.countBaseline()).thenReturn(Map.of());
    new ComparisonInputRecorder(new AuditSettingsResolver(ignored -> QueryAuditConfig.defaults()))
        .record(
            scope,
            new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of()),
            "test-id",
            "Fixture",
            method);
    var run = state.result(List.of());
    assertThat(run.incompleteReasons()).isEmpty();
    return run.comparisonInputs().get("test-id");
  }

  private static ExtensionContext context(String name) throws Exception {
    ExtensionContext context = mock(ExtensionContext.class);
    Method method = Fixtures.class.getDeclaredMethod(name);
    doReturn(Fixtures.class).when(context).getRequiredTestClass();
    when(context.getTestMethod()).thenReturn(Optional.of(method));
    when(context.getRequiredTestMethod()).thenReturn(method);
    return context;
  }

  static class Fixtures {
    @ExpectQueries(select = 2)
    void upperBound() {}

    @ExpectQueries(select = 2)
    void upperBoundAgain() {}

    @ExpectQueries(select = 2, exact = true)
    void exact() {}
  }
}
