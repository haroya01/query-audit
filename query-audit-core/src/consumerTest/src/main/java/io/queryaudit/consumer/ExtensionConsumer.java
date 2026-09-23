package io.queryaudit.consumer;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.detector.QueryAuditAnalyzer;
import io.queryaudit.core.extension.AuditExtensions;
import io.queryaudit.core.extension.AuditRule;
import io.queryaudit.core.extension.AuditRuleTestKit;
import io.queryaudit.core.extension.FindingKindId;
import io.queryaudit.core.extension.RuleContext;
import io.queryaudit.core.extension.RuleDescriptor;
import io.queryaudit.core.extension.RuleId;
import io.queryaudit.core.model.AuditOutcome;
import io.queryaudit.core.model.AuditRunResult;
import io.queryaudit.core.model.Finding;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.reporter.FindingId;
import io.queryaudit.core.reporter.delivery.AuditReportPublisher;
import io.queryaudit.core.reporter.delivery.PublicationResult;
import io.queryaudit.core.reporter.delivery.PublishedAuditRun;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** A framework-free user's complete custom rule and sink workflow against the published JAR. */
public final class ExtensionConsumer {
  private static final FindingKindId KIND = FindingKindId.of("consumer:latency-budget");

  private ExtensionConsumer() {}

  public static void verify() {
    BudgetRule rule = new BudgetRule(75);
    require(
        AuditRuleTestKit.verifyDeterministic(rule, new RuleContext(List.of(), null)).isEmpty(),
        "Empty evidence must not produce findings");
    require(
        AuditRuleTestKit.verifyDeterministic(rule, new RuleContext(List.of(query(75)), null))
            .isEmpty(),
        "The exact budget is permitted");
    List<Finding> checked =
        AuditRuleTestKit.verifyDeterministic(rule, new RuleContext(List.of(query(76)), null));
    require(checked.size() == 1, "The budget boundary must produce one finding");
    requireImmutable(checked);

    List<PublishedAuditRun> received = new ArrayList<>();
    AuditExtensions extensions =
        AuditExtensions.builder()
            .auditRule("consumer:budget-registration", rule)
            .reportSink(
                "consumer:optional",
                false,
                run -> {
                  throw new IllegalStateException("private-exception-payload");
                })
            .reportSink("consumer:archive", true, received::add)
            .build();
    QueryAuditConfig config =
        QueryAuditConfig.builder()
            .slowQueryWarningMs(1)
            .slowQueryErrorMs(2)
            .disabledRules(Set.of("slow-query"))
            .severityOverrides(Map.of(KIND.value(), Severity.ERROR))
            .build();
    Path directory;
    try {
      directory = Files.createTempDirectory("query-audit-consumer-");
    } catch (java.io.IOException failure) {
      throw new AssertionError("Cannot isolate the consumer baseline", failure);
    }
    try {
      Path absentBaseline = directory.resolve("absent-baseline.json");
      var enabledBaseline =
          QueryAuditAnalyzer.withExtensions(
                  QueryAuditConfig.Builder.from(config).disabledRules(Set.of()).build(),
                  absentBaseline,
                  extensions)
              .analyze("ConsumerTest", "enabled-control", List.of(query(80)), null);
      require(
          enabledBaseline.getFindings().all().stream()
              .anyMatch(f -> f.kindId().value().equals("slow-query")),
          "The control must detect a slow query before it is disabled");
      var report =
          QueryAuditAnalyzer.withExtensions(config, absentBaseline, extensions)
              .analyze("ConsumerTest", "budget", List.of(query(80)), null);
      List<Finding> custom =
          report.getFindings().confirmed().stream().filter(f -> f.kindId().equals(KIND)).toList();
      require(custom.size() == 1, "Explicit registration must execute the custom rule");
      Finding finding = custom.get(0);
      require(
          finding.severity() == Severity.ERROR, "Host severity policy must apply to custom kinds");
      requireImmutable(report.getFindings().confirmed());
      require(
          report.getFindings().all().stream()
              .noneMatch(f -> f.kindId().value().equals("slow-query")),
          "Built-in rules remain configurable alongside custom rules");

      var disabled =
          QueryAuditAnalyzer.withExtensions(
                  QueryAuditConfig.builder().disabledRules(Set.of(KIND.value())).build(),
                  absentBaseline,
                  extensions)
              .analyze("ConsumerTest", "disabled", List.of(query(80)), null);
      require(
          disabled.getFindings().all().stream().noneMatch(f -> f.kindId().equals(KIND)),
          "Custom kinds can be disabled without editing an enum");

      String findingId = FindingId.of(report.getTestId(), finding);
      require(
          findingId.equals(
              FindingId.of(report.getTestId(), finding.withSeverity(Severity.WARNING))),
          "Finding identity must survive a severity policy change");
      require(
          !findingId.equals(FindingId.of("different-test", finding)),
          "Finding identity must retain test scope");
      AuditRunResult run = new AuditRunResult(List.of(report), AuditOutcome.FAIL, List.of());
      PublicationResult publication =
          new AuditReportPublisher(extensions.reportSinks()).publish(run);
      require(
          publication.analysisOutcome() == AuditOutcome.FAIL, "Delivery cannot change analysis");
      require(publication.hasOptionalFailures(), "The optional delivery failure must be visible");
      require(
          !publication.hasRequiredFailures(), "An ordinary failure must not block the next sink");
      require(
          publication.deliveries().get(0).registrationId().equals("consumer:optional"),
          "Delivery failures must be traceable by registration ID");
      require(received.size() == 1, "The required sink must receive the run exactly once");
      String json = received.get(0).json();
      require(
          json.contains(findingId) && json.contains(KIND.value()), "Published IDs must correlate");
      require(
          !json.contains("private-tenant")
              && !json.contains("private-exception-payload")
              && !json.contains("ConsumerTest.java")
              && !json.contains("SELECT"),
          "The publication boundary must omit SQL, source locations and exception payloads");
    } finally {
      try {
        Files.delete(directory);
      } catch (java.io.IOException failure) {
        throw new AssertionError("Cannot remove the empty consumer directory", failure);
      }
    }
    System.out.println(
        "Published public APIs support custom rules, policy, stable IDs and isolated sinks");
  }

  private static QueryRecord query(long milliseconds) {
    return new QueryRecord(
        "SELECT id FROM accounts WHERE tenant = 'private-tenant'",
        milliseconds * 1_000_000L,
        1L,
        "ConsumerTest.java:42");
  }

  private static void requireImmutable(List<Finding> findings) {
    try {
      findings.clear();
      throw new AssertionError("Public finding collections must be immutable");
    } catch (UnsupportedOperationException expected) {
      // Contract satisfied.
    }
  }

  private static void require(boolean condition, String message) {
    if (!condition) throw new AssertionError(message);
  }

  private static final class BudgetRule implements AuditRule {
    private final long milliseconds;

    private BudgetRule(long milliseconds) {
      this.milliseconds = milliseconds;
    }

    @Override
    public RuleDescriptor descriptor() {
      return new RuleDescriptor(
          new RuleId("consumer:budget"),
          "1",
          Set.of(KIND),
          Map.of("milliseconds", Long.toString(milliseconds)));
    }

    @Override
    public List<Finding> evaluate(RuleContext context) {
      return context.queries().stream()
          .filter(query -> query.executionTimeNanos() > milliseconds * 1_000_000L)
          .map(
              query ->
                  new Finding(
                      KIND,
                      Severity.WARNING,
                      query.sql(),
                      null,
                      null,
                      "Query exceeded the application budget",
                      "Reduce latency or configure the budget",
                      query.stackTrace()))
          .toList();
    }
  }
}
