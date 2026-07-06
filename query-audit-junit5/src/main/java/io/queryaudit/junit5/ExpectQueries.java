package io.queryaudit.junit5;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Fails the test when the number of executed queries of a given type exceeds its declared budget.
 * Each attribute limits one query type independently; attributes left at {@code -1} are not
 * verified.
 *
 * <p>Complements {@link ExpectMaxQueryCount}, which limits the total query count regardless of
 * type. Both annotations can be combined on the same test method.
 *
 * <pre>{@code
 * @Test
 * @ExpectQueries(select = 2, insert = 1)
 * void createOrder() {
 *     orderService.createOrder(request);
 *     // fails if more than 2 SELECTs or more than 1 INSERT are executed
 * }
 * }</pre>
 *
 * <p>A budget of {@code 0} forbids that query type entirely. For example
 * {@code @ExpectQueries(insert = 0, update = 0, delete = 0)} asserts that a read-only path performs
 * no writes.
 *
 * <p>When a budget is exceeded, the failure message lists every query of the violated type together
 * with its call site.
 *
 * @author haroya
 * @since 0.4.0
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(QueryAuditExtension.class)
public @interface ExpectQueries {

  /** Maximum number of SELECT queries allowed, or {@code -1} to skip verification. */
  int select() default -1;

  /** Maximum number of INSERT queries allowed, or {@code -1} to skip verification. */
  int insert() default -1;

  /** Maximum number of UPDATE queries allowed, or {@code -1} to skip verification. */
  int update() default -1;

  /** Maximum number of DELETE queries allowed, or {@code -1} to skip verification. */
  int delete() default -1;
}
