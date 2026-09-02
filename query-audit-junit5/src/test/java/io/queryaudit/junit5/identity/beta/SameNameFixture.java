package io.queryaudit.junit5.identity.beta;

import io.queryaudit.junit5.IdentityCaptureExtension;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(IdentityCaptureExtension.class)
public class SameNameFixture {

  @Test
  @DisplayName("duplicate")
  void work() {}
}
