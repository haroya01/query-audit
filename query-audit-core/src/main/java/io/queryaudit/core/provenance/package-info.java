/**
 * Query capture capability and comparison-input provenance.
 *
 * <p>Not a supported extension point. These types describe what the host observed while collecting
 * evidence, including why a run is incomplete and whether two runs may be compared. Their shape
 * tracks report schema versions, so it can change with any release. Read the machine-readable
 * report and the comparison verdict instead of depending on these classes.
 */
package io.queryaudit.core.provenance;
