/**
 * Internal SQL parsing support shared by the built-in rules.
 *
 * <p>Not a supported extension point. The public types here, including {@link
 * io.queryaudit.core.parser.SqlParser}, exist so rules and their tests can share one
 * literal-aware parse; their signatures may change in any release. A custom rule should use the
 * SQL text handed to it in {@code RuleContext} and report findings, not depend on this package.
 *
 * @see io.queryaudit.core.extension.AuditRule
 */
package io.queryaudit.core.parser;
