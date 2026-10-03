/**
 * Recorded query count contracts.
 *
 * <p>{@link io.queryaudit.core.regression.QueryContracts}, {@link
 * io.queryaudit.core.regression.QueryCountBaseline}, and {@link
 * io.queryaudit.core.regression.QueryCounts} are supported. The remaining public types here, {@code
 * ContractFiles} and {@code QueryCountRegressionDetector}, are host internals whose signatures may
 * change in any release.
 *
 * <p>The on-disk contract format is not covered by the Java API floor. A release may add fields,
 * and an older library may refuse a newer contract file. Keep the library version that recorded a
 * contract.
 */
package io.queryaudit.core.regression;
