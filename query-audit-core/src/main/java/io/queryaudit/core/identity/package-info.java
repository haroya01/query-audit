/**
 * Internal finding identity computation.
 *
 * <p>Not a supported extension point. {@link io.queryaudit.core.identity.FindingIdentity} derives
 * the stable identity used to match findings across runs. The published identity is the {@code
 * findingId} in the report; this class is the host's derivation of it and may change in any
 * release.
 */
package io.queryaudit.core.identity;
