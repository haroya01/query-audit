package io.queryaudit.core.detector;

/**
 * Categorizes Spring Data repository method return types for unbounded-result-set
 * false positive reduction.
 *
 * @author haroya
 * @since 0.3.0
 */
public enum RepositoryReturnType {

  /** {@code Optional<T>} — returns 0 or 1 row. */
  OPTIONAL,

  /** Single entity ({@code T}) — returns 0 or 1 row. */
  SINGLE_ENTITY,

  /** {@code List<T>}, {@code Collection<T>}, {@code Set<T>}, {@code Stream<T>}. */
  COLLECTION,

  /** {@code Page<T>} or {@code Slice<T>} — framework adds LIMIT internally. */
  PAGE_OR_SLICE,

  /** Return type could not be resolved from the stack trace. */
  UNKNOWN
}
