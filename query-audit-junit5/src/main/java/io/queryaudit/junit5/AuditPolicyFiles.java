package io.queryaudit.junit5;

import io.queryaudit.core.model.IncompleteReasonCode;
import io.queryaudit.core.regression.QueryCountBaseline;
import io.queryaudit.core.regression.QueryCounts;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;

/** Loads and merge-writes policy files; it does not decide whether a query violates a policy. */
final class AuditPolicyFiles {
  private final AuditSettingsResolver settings;

  AuditPolicyFiles(AuditSettingsResolver settings) {
    this.settings = settings;
  }

  record Loaded(Map<String, QueryCounts> countBaseline, Map<String, QueryCounts> contracts) {}

  Loaded load(AuditScope scope) {
    return new Loaded(
        QueryCountBaseline.load(settings.resolveCountBaselinePath(scope.context())),
        QueryCountBaseline.load(AuditSettingsResolver.resolveContractsPath()));
  }

  void writeCountBaselineIfRequested(AuditScope scope) {
    boolean updateBaseline =
        Boolean.parseBoolean(
            AuditSettingsResolver.resolveSystemProperty(
                "queryAudit.updateBaseline", "queryGuard.updateBaseline", "false"));
    if (!updateBaseline) {
      return;
    }

    Map<String, QueryCounts> currentCounts = scope.currentCounts();
    if (currentCounts == null || currentCounts.isEmpty()) {
      return;
    }

    try {
      Path countBaselinePath = settings.resolveCountBaselinePath(scope.context());

      mergeAndSave(countBaselinePath, currentCounts, null);
      System.out.println(
          "[QueryAudit] Count baseline updated: "
              + countBaselinePath.toAbsolutePath()
              + " ("
              + currentCounts.size()
              + " test(s))");
    } catch (Exception e) {
      throw policyWriteFailure(scope, "count baseline", e);
    }
  }

  void writeContractsIfRequested(AuditScope scope) {
    if (!AuditSettingsResolver.isContractRecordMode()) {
      return;
    }
    Map<String, QueryCounts> currentCounts = scope.currentCounts();
    if (currentCounts == null || currentCounts.isEmpty()) {
      return;
    }
    try {
      Path contractsPath = AuditSettingsResolver.resolveContractsPath();
      mergeAndSave(contractsPath, currentCounts, "QueryAudit Query Contracts");
      System.out.println(
          "[QueryAudit] Query contracts recorded: "
              + contractsPath.toAbsolutePath()
              + " ("
              + currentCounts.size()
              + " test(s))");
    } catch (Exception e) {
      throw policyWriteFailure(scope, "query contracts", e);
    }
  }

  static synchronized void mergeAndSave(
      Path path, Map<String, QueryCounts> currentCounts, String header) throws Exception {
    // Classes can finish concurrently. Keep read/merge/write atomic within this test JVM.
    Map<String, QueryCounts> merged = new LinkedHashMap<>(QueryCountBaseline.load(path));
    merged.putAll(currentCounts);
    if (header == null) QueryCountBaseline.save(path, merged);
    else QueryCountBaseline.save(path, merged, header);
  }

  private static ExtensionConfigurationException policyWriteFailure(
      AuditScope scope, String policy, Exception cause) {
    String detail = "Could not write " + policy + "; the requested recording did not complete.";
    AuditDiagnostics.incomplete(scope, IncompleteReasonCode.POLICY_WRITE_FAILED, detail);
    return new ExtensionConfigurationException("QueryAudit: " + detail, cause);
  }
}
