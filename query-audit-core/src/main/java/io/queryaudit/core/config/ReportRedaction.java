package io.queryaudit.core.config;

import java.util.Locale;

/** Detail policy for machine-readable artifacts. It does not change analysis or enforcement. */
public enum ReportRedaction {
  REDACTED,
  FULL;

  public static ReportRedaction parse(String value) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("Report redaction must be 'redacted' or 'full'");
    }
    try {
      return valueOf(value.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Report redaction must be 'redacted' or 'full'", e);
    }
  }
}
