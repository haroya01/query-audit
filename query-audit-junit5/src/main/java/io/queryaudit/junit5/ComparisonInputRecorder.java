package io.queryaudit.junit5;

import io.queryaudit.core.detector.QueryAuditAnalyzer;
import io.queryaudit.core.model.IncompleteReasonCode;
import io.queryaudit.core.provenance.AuditCapability;
import io.queryaudit.core.provenance.AuditPolicyInputs;
import io.queryaudit.core.regression.QueryCountBaseline;
import io.queryaudit.core.regression.QueryCounts;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.extension.ExtensionContext;

/** Captures effective comparison inputs from this test's policy and initialized capabilities. */
final class ComparisonInputRecorder {
  private final AuditSettingsResolver settings;

  ComparisonInputRecorder(AuditSettingsResolver settings) {
    this.settings = settings;
  }

  void record(
      AuditScope scope,
      QueryAuditAnalyzer analyzer,
      String testId,
      String testClass,
      String testName) {
    ExtensionContext context = scope.context();
    AuditInputContext inputs = scope.inputContext();
    if (inputs == null) {
      return;
    }
    try {
      Map<String, Integer> inlineLimits = new TreeMap<>();
      QueryAudit annotation = settings.findAnnotation(context);
      FindingFailurePolicy.from(annotation).recordInto(inlineLimits);
      Method method = context.getRequiredTestMethod();
      ExpectQueries queries = AuditAnnotations.onMethod(method, ExpectQueries.class);
      if (queries != null) {
        inlineLimits.put("expectQueriesPresent", 1);
        inlineLimits.put("select", queries.select());
        inlineLimits.put("insert", queries.insert());
        inlineLimits.put("update", queries.update());
        inlineLimits.put("delete", queries.delete());
        if (queries.total() >= 0) inlineLimits.put("total", queries.total());
        if (queries.exact()) inlineLimits.put("exact", 1);
      }
      ExpectMaxQueryCount maximum = AuditAnnotations.onMethod(method, ExpectMaxQueryCount.class);
      if (maximum != null) {
        inlineLimits.put("maximumQueries", maximum.value());
      }
      if (settings.findDetectNPlusOne(context) != null) {
        inlineLimits.put("detectNPlusOne", analyzer.getConfig().getNPlusOneThreshold());
      }
      AuditPolicyInputs policy =
          new AuditPolicyInputs(
              queries == null
                  ? effectiveCounts(scope.contracts(), testId, testClass, testName)
                  : Map.of(),
              effectiveCounts(scope.countBaseline(), testId, testClass, testName),
              inlineLimits,
              AuditSettingsResolver.isContractRecordMode(),
              AuditSettingsResolver.isCountRecordMode());
      AuditCapability explain = scope.explainCapability();
      if (explain == null) {
        throw new IllegalStateException("EXPLAIN capability was not identified");
      }
      scope.runState().recordInputs(testId, inputs.describe(analyzer, policy, explain));
    } catch (RuntimeException | LinkageError failure) {
      AuditDiagnostics.incomplete(
          scope,
          IncompleteReasonCode.COMPARISON_INPUTS_UNAVAILABLE,
          "Effective audit inputs could not be identified for " + testId);
    }
  }

  private static Map<String, QueryCounts> effectiveCounts(
      Map<String, QueryCounts> values, String testId, String testClass, String testName) {
    QueryCounts selected = QueryCountBaseline.find(values, testId, testClass, testName);
    return selected == null ? Map.of() : Map.of(testId, selected);
  }
}
