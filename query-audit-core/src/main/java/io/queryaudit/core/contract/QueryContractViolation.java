package io.queryaudit.core.contract;

/**
 * A {@link QueryContractScope} rejected the captured work: a count differed from its recorded
 * contract, no contract was recorded for the scope, or the capture was incomplete. A test that ends
 * with this failure makes the audit run {@code FAIL}, even when the test itself is not audited.
 *
 * @since 0.7.2
 */
public final class QueryContractViolation extends AssertionError {
  QueryContractViolation(String message) {
    super(message);
  }
}
