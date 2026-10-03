/**
 * The JUnit 5 entry points: audit annotations and the extension that runs them.
 *
 * <p>The supported types are the annotations {@link io.queryaudit.junit5.QueryAudit}, {@link
 * io.queryaudit.junit5.ExpectQueries}, {@link io.queryaudit.junit5.ExpectMaxQueryCount}, {@link
 * io.queryaudit.junit5.DetectNPlusOne}, {@link io.queryaudit.junit5.QueryAuditExclude}, {@link
 * io.queryaudit.junit5.EnableQueryInspector}, the enum {@link io.queryaudit.junit5.BooleanOverride},
 * and the runtime types {@link io.queryaudit.junit5.QueryAuditExtension} and {@link
 * io.queryaudit.junit5.QueryAuditDataSourceStore}.
 *
 * <p>Everything else in this package is internal. {@link
 * io.queryaudit.junit5.AuditCoverageListener} in particular is public only because the JUnit
 * Platform loads it through the service loader; it is not a type to reference or implement.
 */
package io.queryaudit.junit5;
