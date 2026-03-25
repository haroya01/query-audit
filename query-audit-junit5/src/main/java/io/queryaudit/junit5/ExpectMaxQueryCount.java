package io.queryaudit.junit5;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Fails the test if the number of executed queries exceeds the given value. All query types
 * (SELECT, INSERT, UPDATE, DELETE) are counted.
 *
 * <pre>{@code
 * @Test
 * @ExpectMaxQueryCount(10)
 * void createOrder() {
 *     orderService.createOrder(request);
 *     // fails if more than 10 queries are executed
 * }
 * }</pre>
 *
 * @author haroya
 * @since 0.2.0
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(QueryAuditExtension.class)
public @interface ExpectMaxQueryCount {

  /** Maximum number of queries allowed (all types: SELECT, INSERT, UPDATE, DELETE). */
  int value();
}
