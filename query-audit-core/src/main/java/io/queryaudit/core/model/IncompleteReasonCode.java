package io.queryaudit.core.model;

/**
 * Machine-readable reason codes for an inconclusive QueryAudit run.
 *
 * @since 0.6.0
 */
public enum IncompleteReasonCode {
  /** Query capture stopped after reaching the configured maximum. */
  QUERY_LIMIT_REACHED,

  /** An active audit could not resolve a DataSource. */
  DATASOURCE_UNAVAILABLE,

  /** QueryAudit could not initialize reliable query capture for an active test. */
  AUDIT_INITIALIZATION_FAILED,

  /** QueryAudit could not complete analysis for an active test. */
  AUDIT_ANALYSIS_FAILED,

  /** A configured query contract or count baseline could not be read. */
  CONTRACT_UNREADABLE,

  /** Requested query contract or count-baseline recording did not complete. */
  POLICY_WRITE_FAILED,

  /** A test required by the audit coverage manifest did not run. */
  EXPECTED_TEST_MISSING,

  /** The expected-test manifest was missing, unreadable, empty, or malformed. */
  COVERAGE_MANIFEST_UNREADABLE,

  /** Reports were produced with different expected-test manifests. */
  COVERAGE_MANIFEST_MISMATCH,

  /** The selected suite report could not be written. */
  REPORT_WRITE_FAILED,

  /** Comparison inputs used different artifact detail policies. */
  REPORT_REDACTION_MISMATCH,

  /** A report input used an unsupported schema version. */
  UNSUPPORTED_SCHEMA
}
