package io.queryaudit.core.extension.internal;

/** Host-only diagnostic identities; malformed catalog strings are not safe diagnostic text. */
public final class RuleRegistrationIds {
  private RuleRegistrationIds() {}

  public static String safe(String id, int ordinal) {
    return id != null && id.matches("[A-Za-z0-9][A-Za-z0-9._:/-]{0,199}")
        ? id
        : "unidentified-rule:" + ordinal;
  }
}
