package io.queryaudit.junit5;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public final class IdentityCaptureExtension implements AfterEachCallback {

  private static final List<CapturedIdentity> CAPTURED = new CopyOnWriteArrayList<>();

  @Override
  public void afterEach(ExtensionContext context) {
    JUnitTestIdentity identity = JUnitTestIdentity.from(context);
    CAPTURED.add(
        new CapturedIdentity(
            context.getRequiredTestClass(),
            context.getDisplayName(),
            identity.testId(),
            identity.selector().type(),
            identity.selector().value()));
  }

  static void reset() {
    CAPTURED.clear();
  }

  static List<CapturedIdentity> captured() {
    return List.copyOf(CAPTURED);
  }

  record CapturedIdentity(
      Class<?> testClass,
      String displayName,
      String testId,
      String selectorType,
      String selectorValue) {}

  public static final class MutableDisplayNames extends DisplayNameGenerator.Standard {

    private static volatile String prefix = "first: ";

    public static void usePrefix(String value) {
      prefix = value;
    }

    @Override
    public String generateDisplayNameForMethod(Class<?> testClass, Method testMethod) {
      return prefix + testMethod.getName();
    }
  }
}
