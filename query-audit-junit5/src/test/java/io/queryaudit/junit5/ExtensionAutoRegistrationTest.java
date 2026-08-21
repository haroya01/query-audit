package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Pins the ServiceLoader registration that {@code mode: all} depends on: JUnit's extension
 * autodetection can only pick the extension up for unannotated tests if this resource names it.
 */
@DisplayName("Issue #163: ServiceLoader registration for extension autodetection")
class ExtensionAutoRegistrationTest {

  @Test
  @DisplayName("META-INF/services registers QueryAuditExtension")
  void serviceFileRegistersExtension() throws Exception {
    try (InputStream in =
        getClass()
            .getResourceAsStream("/META-INF/services/org.junit.jupiter.api.extension.Extension")) {
      assertThat(in).as("ServiceLoader registration file must be on the classpath").isNotNull();
      String content = new String(in.readAllBytes(), StandardCharsets.UTF_8).trim();
      assertThat(content).isEqualTo(QueryAuditExtension.class.getName());
    }
  }
}
