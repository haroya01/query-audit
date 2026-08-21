package io.queryaudit.junit5;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Opts a test class or method out of QueryAudit analysis.
 *
 * <p>Primarily for {@code mode: all} suites, where every test is audited by default and this is the
 * escape hatch for tests that intentionally violate a rule (load-shape fixtures, migration replays,
 * etc.). Also honored in the default {@code annotated} mode: a method carrying this annotation is
 * skipped even when its class is annotated with {@code @QueryAudit}.
 *
 * @author haroya
 * @since 0.5.0
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface QueryAuditExclude {}
