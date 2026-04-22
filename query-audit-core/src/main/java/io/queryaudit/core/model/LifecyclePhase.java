package io.queryaudit.core.model;

/**
 * Represents the JUnit 5 test lifecycle phase during which a query was captured.
 *
 * <p>Used to distinguish test infrastructure queries (fixture setup/teardown) from production code
 * path queries executed during the actual test method.
 *
 * @author haroya
 * @since 0.2.0
 */
public enum LifecyclePhase {

  /** Queries captured during {@code @BeforeEach} user methods. */
  SETUP,

  /** Queries captured during the actual {@code @Test} method execution. */
  TEST,

  /** Queries captured during {@code @AfterEach} user methods. */
  TEARDOWN
}
