package io.queryaudit.spring.slice;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import io.queryaudit.spring.QueryAuditAutoConfiguration;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class SliceRegistrationTest {

  static final List<String> BOOT_3_SLICES =
      List.of(
          "org.springframework.boot.test.autoconfigure.orm.jpa.AutoConfigureDataJpa",
          "org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureJdbc",
          "org.springframework.boot.test.autoconfigure.data.jdbc.AutoConfigureDataJdbc",
          "org.springframework.boot.test.autoconfigure.jooq.AutoConfigureJooq");

  static final List<String> BOOT_4_SLICES =
      List.of(
          "org.springframework.boot.data.jpa.test.autoconfigure.AutoConfigureDataJpa",
          "org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureJdbc",
          "org.springframework.boot.data.jdbc.test.autoconfigure.AutoConfigureDataJdbc",
          "org.springframework.boot.jooq.test.autoconfigure.AutoConfigureJooq");

  static Stream<String> allSlices() {
    return Stream.concat(BOOT_3_SLICES.stream(), BOOT_4_SLICES.stream());
  }

  @ParameterizedTest
  @MethodSource("allSlices")
  void dataSourceSliceImportsTheAutoConfiguration(String slice) throws IOException {
    List<String> candidates = new ArrayList<>();
    for (URL resource :
        Collections.list(
            getClass().getClassLoader().getResources("META-INF/spring/" + slice + ".imports"))) {
      try (InputStream in = resource.openStream()) {
        new String(in.readAllBytes(), StandardCharsets.UTF_8).lines().forEach(candidates::add);
      }
    }
    assertThat(candidates).contains(QueryAuditAutoConfiguration.class.getName());
  }

  @Test
  void boot3SliceAnnotationsExistOnTheBaselineClasspath() {
    for (String slice : BOOT_3_SLICES) {
      assertThatCode(() -> Class.forName(slice)).as(slice).doesNotThrowAnyException();
    }
  }
}
