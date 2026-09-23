package io.queryaudit.core.extension;

import java.util.Objects;

/**
 * A catalog rule failed its host contract. This diagnostic contains only validated identities and a
 * host-owned reason, never the implementation's exception message, cause, SQL or configuration.
 * List-based legacy analyzer constructors retain their original exception behavior.
 */
public final class AuditRuleException extends IllegalArgumentException {
  public enum Reason {
    DECLARATION_FAILED("rule declaration failed"),
    INVALID_DECLARATION("invalid rule declaration: reserved finding kinds or namespaces"),
    EXECUTION_FAILED("rule execution failed"),
    INVALID_RESULT("rule returned null findings, a null finding or an undeclared finding kind"),
    DESCRIPTOR_CHANGED("rule descriptor changed");

    private final String description;

    Reason(String description) {
      this.description = description;
    }
  }

  private final String registrationId;
  private final RuleId ruleId;
  private final Reason reason;

  /** Creates a safe diagnostic; invalid registration IDs are replaced rather than echoed. */
  public AuditRuleException(String registrationId, RuleId ruleId, Reason reason) {
    super(
        "Audit rule registration "
            + safeId(registrationId)
            + (ruleId == null ? "" : " (" + ruleId.value() + ")")
            + ": "
            + Objects.requireNonNull(reason, "reason").name()
            + " — "
            + reason.description);
    this.registrationId = safeId(registrationId);
    this.ruleId = ruleId;
    this.reason = reason;
  }

  public String registrationId() {
    return registrationId;
  }

  /** The declared identity, or null if the declaration could not be read or for legacy rules. */
  public RuleId ruleId() {
    return ruleId;
  }

  public Reason reason() {
    return reason;
  }

  private static String safeId(String value) {
    return value != null && value.matches("[A-Za-z0-9][A-Za-z0-9._:/-]{0,199}")
        ? value
        : "unidentified-rule";
  }
}
