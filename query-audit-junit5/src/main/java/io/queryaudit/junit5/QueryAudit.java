package io.queryaudit.junit5;

import io.queryaudit.core.model.IssueType;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Enables QueryAudit analysis for a test class or method.
 *
 * <p>When applied, the {@link QueryAuditExtension} intercepts SQL queries executed during each test
 * and reports potential performance issues.
 *
 * @author haroya
 * @since 0.2.0
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(QueryAuditExtension.class)
public @interface QueryAudit {

  /** Issue type codes to suppress (e.g., {@code "select-all"}, {@code "n-plus-one"}). */
  String[] suppress() default {};

  /**
   * Issue types that should cause the test to fail. Defaults to all confirmed issues when empty.
   */
  IssueType[] failOn() default {};

  /** N+1 detection threshold. A value of {@code -1} means use the default. */
  int nPlusOneThreshold() default -1;

  /** Whether to fail on any confirmed detection. */
  boolean failOnDetection() default true;

  /**
   * Path to the baseline file. An empty string means use the default ({@code .query-audit-baseline}
   * in the working directory).
   */
  String baselinePath() default "";

  /**
   * Whether to automatically open the HTML report in a browser after tests complete. Defaults to
   * false (must be opted in).
   */
  boolean autoOpenReport() default false;
}
