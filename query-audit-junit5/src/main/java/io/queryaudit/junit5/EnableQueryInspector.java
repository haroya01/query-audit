package io.queryaudit.junit5;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * One-line annotation to enable query inspection on a test class. Reports all detected issues
 * without failing the test.
 *
 * <pre>{@code
 * @SpringBootTest
 * @EnableQueryInspector
 * class OrderServiceTest { ... }
 * }</pre>
 *
 * Equivalent to {@code @QueryAudit(failOnDetection = BooleanOverride.FALSE)}.
 *
 * @author haroya
 * @since 0.2.0
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(QueryAuditExtension.class)
public @interface EnableQueryInspector {}
