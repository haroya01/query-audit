/**
 * The built-in detection rules and the analyzer that runs them.
 *
 * <p>Only {@link io.queryaudit.core.detector.DetectionRule} and {@link
 * io.queryaudit.core.detector.QueryAuditAnalyzer} are supported. Every other public type here is a
 * built-in rule or registry detail: the rules are implementation, their class names and severity
 * choices are not an API, and a new release may add, rename, merge, or remove them. Enable a
 * built-in rule by its documented rule code rather than by importing its class.
 *
 * <p>To add a rule, implement {@code DetectionRule} or the {@code AuditRule} SPI in {@link
 * io.queryaudit.core.extension}.
 */
package io.queryaudit.core.detector;
