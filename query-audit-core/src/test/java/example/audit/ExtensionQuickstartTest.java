package example.audit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.detector.QueryAuditAnalyzer;
import io.queryaudit.core.extension.AuditExtensions;
import io.queryaudit.core.extension.AuditRuleTestKit;
import io.queryaudit.core.extension.RuleContext;
import io.queryaudit.core.model.LifecyclePhase;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Exercises the documented customer workflow without access to QueryAudit internals. */
class ExtensionQuickstartTest {
  @TempDir Path temporary;

  @Test
  void thresholdBoundaryAndEmptyCaptureAreExplicit() {
    var rule = new ApplicationBudgetRule(75);
    var output =
        AuditRuleTestKit.verifyDeterministic(
            rule, new RuleContext(List.of(query(75), query(76)), null));
    assertThat(output).hasSize(1);
    assertThat(output.get(0).kindId()).isEqualTo(ApplicationBudgetRule.KIND);
    assertThat(AuditRuleTestKit.verify(rule, new RuleContext(List.of(), null))).isEmpty();
    assertThatThrownBy(() -> new ApplicationBudgetRule(0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void registersRuleAndReadsAllKindsThroughTheRecommendedApi() {
    var extensions =
        AuditExtensions.builder()
            .auditRule("application-budget", new ApplicationBudgetRule(75))
            .build();
    var analyzer =
        QueryAuditAnalyzer.withExtensions(
            QueryAuditConfig.defaults(), temporary.resolve("no-baseline"), extensions);
    var report = analyzer.analyze("OrderTest", "listOrders", List.of(query(80)), null);
    var budget =
        report.getFindings().confirmed().stream()
            .filter(finding -> finding.kindId().equals(ApplicationBudgetRule.KIND))
            .toList();

    assertThat(budget).hasSize(1);
    assertThat(report.getFindings().warnings()).containsAll(budget);
    assertThat(report.getTestClass()).isEqualTo("OrderTest");
    assertThat(report.getTotalQueryCount()).isEqualTo(1);
    assertThat(report.getFindings().acknowledged()).isEmpty();
  }

  @Test
  void applicationConfigurationControlsPolicyWithoutEditingTheRule() {
    var extensions =
        AuditExtensions.builder()
            .auditRule("application-budget", new ApplicationBudgetRule(75))
            .build();
    var config =
        QueryAuditConfig.builder()
            .severityOverrides(Map.of(ApplicationBudgetRule.KIND.value(), Severity.ERROR))
            .build();
    var analyzer =
        QueryAuditAnalyzer.withExtensions(config, temporary.resolve("no-baseline"), extensions);
    var findings = analyzer.analyze("listOrders", List.of(query(80)), null).getFindings();

    assertThat(findings.errors())
        .anyMatch(finding -> finding.kindId().equals(ApplicationBudgetRule.KIND));
    assertThat(findings.warnings())
        .noneMatch(finding -> finding.kindId().equals(ApplicationBudgetRule.KIND));
  }

  private static QueryRecord query(long milliseconds) {
    return new QueryRecord(
        "select id from orders",
        "select id from orders",
        TimeUnit.MILLISECONDS.toNanos(milliseconds),
        1,
        null,
        0,
        LifecyclePhase.TEST);
  }
}
