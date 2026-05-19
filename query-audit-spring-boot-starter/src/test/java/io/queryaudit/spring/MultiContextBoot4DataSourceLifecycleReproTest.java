package io.queryaudit.spring;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Placeholder reproducer for <a href="https://github.com/haroya01/query-audit/issues/153">issue
 * #153</a> — the root-cause cascade-close on Spring Boot 4.x left open after <a
 * href="https://github.com/haroya01/query-audit/issues/134">#134</a> / <a
 * href="https://github.com/haroya01/query-audit/pull/142">#142</a>.
 *
 * <p>This class is intentionally empty and {@link Disabled} on the Spring Boot 3.4.x classpath
 * this project compiles against — {@link MultiContextDataSourceLifecycleTest} already pins the
 * good 3.4.x behavior, so a copy of that shape here would just duplicate a passing test. The
 * scenario this placeholder is reserving is the failing one on Boot 4.x.
 *
 * <h3>Scenario to reproduce in the 0.3.2 fixture</h3>
 *
 * <ul>
 *   <li>Spring Boot <b>4.0.6</b> (Framework 7) on the test classpath of a dedicated subproject
 *       (e.g. {@code query-audit-spring-boot-4-it}) so the main starter keeps its 3.4.x baseline.
 *   <li>At least three coexisting {@code @SpringBootTest} contexts each backed by a real
 *       {@code HikariDataSource}. Above {@code ContextCache.DEFAULT_MAX_CONTEXT_CACHE_SIZE = 32}
 *       it should also exercise eviction.
 *   <li>Per-context BPP wrap via the existing {@link QueryAuditAutoConfiguration}.
 *   <li>Expected before the fix: closing/evicting one context surfaces {@code HikariDataSource has
 *       been closed} on a still-active context's pool — symptomatically identical to the external
 *       reproducer (short-link, 35 failures in {@code user.application}).
 *   <li>Expected after the fix: every still-cached context's {@code DataSource} continues to hand
 *       out valid connections, regardless of how the BPP layer wraps it.
 * </ul>
 *
 * <h3>Why a placeholder rather than a Boot-version-conditional test</h3>
 *
 * <p>Bringing Spring Boot 4.x into this module's test classpath would force a {@code @ConditionalOn}
 * dance that obscures the lifecycle the test is supposed to demonstrate. A dedicated subproject
 * with a pinned Boot 4 dependency reads cleaner and lets the test fail loudly when run, which is
 * the point of a reproducer.
 */
@Disabled("Requires Spring Boot 4.x classpath; tracked in #153 — fixture to be wired in 0.3.2 PR.")
class MultiContextBoot4DataSourceLifecycleReproTest {

  @Test
  @DisplayName("Closing one context does not close other contexts' DataSources (regression #153)")
  void closingOneContextDoesNotAffectOthers() {
    // Skeleton to be filled when the Boot 4 fixture lands. See class-level Javadoc for the
    // scenario this is reserving — shape mirrors MultiContextDataSourceLifecycleTest.
  }
}
