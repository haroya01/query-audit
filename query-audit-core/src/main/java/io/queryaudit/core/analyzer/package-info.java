/**
 * Database and plan analysis SPIs.
 *
 * <p>{@link io.queryaudit.core.analyzer.IndexMetadataProvider}, {@link
 * io.queryaudit.core.analyzer.ExplainAnalyzer}, and {@link
 * io.queryaudit.core.analyzer.ExplainAnalysisException} are supported; they are how an optional
 * database module contributes index metadata and plan analysis. {@link
 * io.queryaudit.core.analyzer.JpaIndexScanner} is an internal fallback used when no provider
 * matches, and its signatures may change in any release.
 */
package io.queryaudit.core.analyzer;
