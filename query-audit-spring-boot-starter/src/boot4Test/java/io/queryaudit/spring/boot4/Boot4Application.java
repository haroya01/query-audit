package io.queryaudit.spring.boot4;

import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Minimal JPA + web application used to reproduce the multi-context DataSource lifecycle scenario
 * from issue #134 on an actual Spring Boot 4.x classpath. The QueryAudit autoconfiguration is
 * picked up from the starter's {@code AutoConfiguration.imports} exactly as in a user project — no
 * {@code @QueryAudit} annotation anywhere, matching the report.
 */
@SpringBootApplication
public class Boot4Application {}
