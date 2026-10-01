package io.queryaudit.junit5;

import io.queryaudit.core.detector.QueryAuditAnalyzer;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.regression.ContractFiles;
import io.queryaudit.core.regression.QueryCountBaseline;
import io.queryaudit.core.regression.QueryCountRegressionDetector;
import io.queryaudit.core.regression.QueryCounts;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Count-based rules and direct snapshot contracts, independent of their file representation. */
final class QueryCountPolicies {
  private static final QueryCountRegressionDetector REGRESSION_DETECTOR =
      new QueryCountRegressionDetector();

  QueryAuditReport detectRegression(
      AuditScope scope,
      QueryAuditReport report,
      List<QueryRecord> queries,
      String testId,
      String testClass,
      String testName,
      QueryAuditAnalyzer analyzer) {

    QueryCounts current = QueryCounts.from(queries);

    Map<String, QueryCounts> currentCounts = scope.currentCounts();
    if (currentCounts != null) {
      currentCounts.put(QueryCountBaseline.key(testId), current);
    }

    Map<String, QueryCounts> countBaseline = scope.countBaseline();
    if (countBaseline == null || countBaseline.isEmpty()) {
      return report;
    }

    LegacyPolicyIdentities.track(
        scope, "count baseline", countBaseline, testId, testClass, testName, true);
    QueryCounts baselineCounts =
        QueryCountBaseline.find(countBaseline, testId, testClass, testName);

    List<Issue> regressionIssues =
        REGRESSION_DETECTOR.detect(testClass, testName, current, baselineCounts);
    return analyzer.mergeDetectedIssues(report, regressionIssues);
  }

  String contractFailure(
      AuditScope scope,
      List<QueryRecord> queries,
      String testId,
      String testClass,
      String testName) {
    if (AuditSettingsResolver.isContractRecordMode()) {
      return null;
    }
    Map<String, QueryCounts> contracts = scope.contracts();
    if (contracts == null || contracts.isEmpty()) {
      return null;
    }
    Optional<Method> method = scope.context().getTestMethod();
    boolean inlineContract =
        method.isPresent() && AuditAnnotations.onMethod(method.get(), ExpectQueries.class) != null;
    LegacyPolicyIdentities.track(
        scope, "query contract", contracts, testId, testClass, testName, !inlineContract);
    if (inlineContract) {
      return null;
    }
    Path location = AuditSettingsResolver.resolveContractsPath(scope.context());
    String source =
        Files.isDirectory(location)
            ? ContractFiles.load(location).fileFor(QueryCountBaseline.key(testId)).toString()
            : location.toString();
    return AuditAssertions.contractFailure(testId, testClass, testName, queries, contracts, source);
  }
}
