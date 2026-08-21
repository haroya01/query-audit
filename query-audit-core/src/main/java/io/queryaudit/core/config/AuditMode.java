package io.queryaudit.core.config;

import java.util.Locale;

/**
 * Controls which tests the JUnit extension audits.
 *
 * <ul>
 *   <li>{@link #ANNOTATED} — only tests that opt in via {@code @QueryAudit} (or a related
 *       annotation / direct {@code @ExtendWith}). The default, and the only behavior before 0.5.0.
 *   <li>{@link #ALL} — every test in the suite is audited unless it opts out with
 *       {@code @QueryAuditExclude}. Requires the extension to be registered globally, typically via
 *       JUnit's extension autodetection ({@code junit.jupiter.extensions.autodetection.enabled}).
 * </ul>
 *
 * @author haroya
 * @since 0.5.0
 */
public enum AuditMode {
  ANNOTATED,
  ALL;

  /**
   * Parses a configuration value into an {@code AuditMode}. Case-insensitive; {@code null} and
   * blank map to {@link #ANNOTATED} so absent configuration keeps the pre-0.5.0 behavior.
   *
   * @throws IllegalArgumentException on any other value, naming the accepted ones
   */
  public static AuditMode parse(String value) {
    if (value == null || value.isBlank()) {
      return ANNOTATED;
    }
    return switch (value.trim().toLowerCase(Locale.ROOT)) {
      case "annotated" -> ANNOTATED;
      case "all" -> ALL;
      default ->
          throw new IllegalArgumentException(
              "Unknown query-audit mode '" + value + "' — expected 'annotated' or 'all'");
    };
  }
}
