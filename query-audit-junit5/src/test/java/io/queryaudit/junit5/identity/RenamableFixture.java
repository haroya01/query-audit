package io.queryaudit.junit5.identity;

import io.queryaudit.junit5.IdentityCaptureExtension;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(IdentityCaptureExtension.class)
@DisplayNameGeneration(IdentityCaptureExtension.MutableDisplayNames.class)
public class RenamableFixture {

  @Test
  void stableMethod() {}
}
