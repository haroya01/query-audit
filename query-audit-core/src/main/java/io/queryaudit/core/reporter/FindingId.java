package io.queryaudit.core.reporter;

import io.queryaudit.core.identity.FindingIdentity;
import io.queryaudit.core.model.Finding;
import io.queryaudit.core.model.Issue;

/** Report-facing facade; identity canonicalization is owned by the framework-neutral core. */
public final class FindingId {
  static final String PREFIX = FindingIdentity.PREFIX;

  private FindingId() {}

  public static String of(String testId, Issue issue) {
    return FindingIdentity.of(testId, issue);
  }

  public static String of(String testId, Finding finding) {
    return FindingIdentity.of(testId, finding);
  }

  static String legacyKey(
      String testIdentity, String type, String query, String source, String table, String column) {
    return FindingIdentity.legacyKey(testIdentity, type, query, source, table, column);
  }

  static String sourceMethod(String value) {
    return FindingIdentity.sourceMethod(value);
  }
}
