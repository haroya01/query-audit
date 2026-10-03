/**
 * Internal query-count baseline storage.
 *
 * <p>Not a supported extension point. {@link io.queryaudit.core.baseline.Baseline} and {@link
 * io.queryaudit.core.baseline.BaselineEntry} read and write the deprecated count-baseline file
 * format. Use recorded query contracts through {@link io.queryaudit.core.regression.QueryContracts}
 * instead; the file layout here may change or be removed in any release.
 */
package io.queryaudit.core.baseline;
