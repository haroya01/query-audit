package io.queryaudit.junit5;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Fails the test if N+1 queries are detected.
 *
 * <pre>{@code
 * @Test
 * @DetectNPlusOne
 * void getOrders() {
 *     orderService.getOrders();
 *     // fails if any N+1 pattern is detected
 * }
 * }</pre>
 *
 * @author haroya
 * @since 0.2.0
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(QueryAuditExtension.class)
public @interface DetectNPlusOne {

  /** Same pattern must appear this many times to be considered N+1. Default: 3. */
  int threshold() default 3;
}
