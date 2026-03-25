package io.queryaudit.core.baseline;

/**
 * Represents a single acknowledged issue in the baseline file.
 *
 * <p>An entry matches an issue when the {@code issueCode} is equal and the optional {@code
 * table}/{@code column} fields either match or are {@code null} (acting as a wildcard).
 *
 * @author haroya
 * @since 0.2.0
 */
public record BaselineEntry(
    String issueCode, // e.g. "n-plus-one", "missing-where-index"
    String table, // nullable, e.g. "users"
    String column, // nullable, e.g. "deleted_at"
    String queryPattern, // nullable, normalized SQL pattern
    String acknowledgedBy, // who acknowledged it
    String reason // why it's OK
    ) {

  /**
   * Returns {@code true} if this baseline entry matches the given issue coordinates. The {@code
   * issueCode} must match exactly; {@code table} and {@code column} in this entry act as wildcards
   * when {@code null}.
   */
  public boolean matches(String otherCode, String otherTable, String otherColumn) {
    if (!issueCode.equals(otherCode)) return false;
    if (table != null && !table.equalsIgnoreCase(otherTable)) return false;
    if (column != null && !column.equalsIgnoreCase(otherColumn)) return false;
    return true;
  }
}
