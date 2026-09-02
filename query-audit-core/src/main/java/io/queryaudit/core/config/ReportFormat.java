package io.queryaudit.core.config;

import java.util.Locale;

/**
 * Suite artifact selected by {@code query-audit.report.format}.
 *
 * @since 0.6.0
 */
public enum ReportFormat {
  CONSOLE,
  JSON,
  HTML;

  /**
   * Parses a configured format name.
   *
   * @param value configured format name
   * @return the matching format
   * @throws IllegalArgumentException when the value is blank or unsupported
   * @since 0.6.0
   */
  public static ReportFormat parse(String value) {
    if (value == null || value.isBlank()) {
      throw invalidFormat(value);
    }
    try {
      return valueOf(value.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw invalidFormat(value);
    }
  }

  private static IllegalArgumentException invalidFormat(String value) {
    return new IllegalArgumentException(
        "Unknown query-audit report format '"
            + value
            + "' — expected 'console', 'json', or 'html'");
  }
}
