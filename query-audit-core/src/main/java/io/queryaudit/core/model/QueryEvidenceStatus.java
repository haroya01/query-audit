package io.queryaudit.core.model;

/** Availability of captured query records in a report, separate from the audit outcome. */
public enum QueryEvidenceStatus {
  /** All captured query records are retained, including tests that executed no queries. */
  COMPLETE,
  /** Some captured query records are retained. */
  PARTIAL,
  /** The test executed queries, but none of their records are retained. */
  OMITTED
}
