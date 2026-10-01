package io.queryaudit.core.detector;

import io.queryaudit.core.extension.AuditRuleException;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.QueryRecord;
import java.util.List;

/** A registration keeps its identity even when the same rule object appears more than once. */
record DetectionRuleRegistration(DetectionRule rule, String registrationId) {
  List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata metadata) {
    if (registrationId == null) return rule.evaluate(queries, metadata);
    List<Issue> findings;
    try {
      findings = rule.evaluate(queries, metadata);
    } catch (RuntimeException | LinkageError failure) {
      throw failure(AuditRuleException.Reason.EXECUTION_FAILED);
    }
    try {
      List<Issue> snapshot = List.copyOf(findings);
      if (snapshot.stream().anyMatch(issue -> issue.type() == null || issue.severity() == null))
        throw failure(AuditRuleException.Reason.INVALID_RESULT);
      return snapshot;
    } catch (RuntimeException | LinkageError failure) {
      throw failure(AuditRuleException.Reason.INVALID_RESULT);
    }
  }

  AuditRuleException failure(AuditRuleException.Reason reason) {
    return new AuditRuleException(registrationId, null, reason);
  }
}
