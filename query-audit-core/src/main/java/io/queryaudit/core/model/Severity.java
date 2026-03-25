package io.queryaudit.core.model;

/**
 * Defines the severity levels for detected issues: {@code ERROR} for critical problems that
 * should fail the build, {@code WARNING} for potential problems worth investigating, and
 * {@code INFO} for informational hints.
 *
 * @author haroya
 * @since 0.2.0
 */
public enum Severity {
  ERROR,
  WARNING,
  INFO
}
