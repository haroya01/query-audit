package io.queryaudit.junit5;

import io.queryaudit.core.config.AuditMode;
import io.queryaudit.core.model.IncompleteReasonCode;
import java.lang.reflect.Method;
import java.util.Optional;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.support.AnnotationSupport;

/** Opt-in and lifecycle-boundary decisions; it does not allocate resources or run detectors. */
final class AuditActivation {
  private final AuditSettingsResolver settings;
  private final boolean explicitRegistration;

  AuditActivation(AuditSettingsResolver settings, boolean explicitRegistration) {
    this.settings = settings;
    this.explicitRegistration = explicitRegistration;
  }

  boolean callbacksAllowed(AuditScope scope) {
    return !AuditDiagnostics.initializationFailed(scope) && isActive(scope);
  }

  boolean computeActive(AuditScope scope) {
    ExtensionContext context = scope.context();
    if (settings.isClassExcluded(context.getRequiredTestClass())) {
      return false;
    }
    if (settings.resolveAuditMode(context) == AuditMode.ALL) {
      return true;
    }
    return explicitRegistration
        || settings.findAnnotation(context) != null
        || settings.hasEnableQueryInspector(context)
        || settings.hasFocusedAuditAnnotation(context)
        || settings.hasDirectExtendWith(context);
  }

  boolean isActive(AuditScope scope) {
    ExtensionContext context = scope.context();
    Optional<Method> method = context.getTestMethod();
    if (method.isPresent() && method.get().isAnnotationPresent(QueryAuditExclude.class)) {
      return false;
    }
    if (settings.isClassExcluded(context.getRequiredTestClass())) {
      return false;
    }

    // A method-level opt-in registers the extension too late for beforeAll. It must therefore opt
    // the method in even if suite-wide autodetection cached the otherwise plain class as false.
    if (method.isPresent() && settings.isMethodLevelOptIn(method.get())) {
      Boolean methodActive = scope.flag(AuditScope.Flag.METHOD_ACTIVE);
      if (methodActive != null) {
        return methodActive;
      }
      boolean active = settings.buildConfig(context, scope.returnTypeResolver()).isEnabled();
      scope.flag(AuditScope.Flag.METHOD_ACTIVE, active);
      return active;
    }

    Boolean inherited = scope.inheritedActive();
    if (inherited != null) return inherited;
    boolean active = computeActive(scope);
    if (active) {
      active = settings.buildConfig(context, scope.returnTypeResolver()).isEnabled();
    }
    scope.flag(AuditScope.Flag.ACTIVE, active);
    return active;
  }

  static void rejectDynamicTestFactoryAudit(AuditScope scope) {
    ExtensionContext context = scope.context();
    Method method = context.getRequiredTestMethod();
    if (!AnnotationSupport.isAnnotated(method, TestFactory.class)) {
      return;
    }

    String target = scope.target();
    String detail =
        "Dynamic-test audit rejected for "
            + target
            + ": JUnit lifecycle callbacks do not expose a separate audit boundary for each"
            + " DynamicTest child.";
    AuditDiagnostics.initializationFailure(
        scope, IncompleteReasonCode.AUDIT_INITIALIZATION_FAILED, detail);
    throw new ExtensionConfigurationException(
        "QueryAudit: cannot audit @TestFactory method "
            + target
            + ". JUnit lifecycle callbacks surround the factory method, but do not expose a"
            + " separate audit boundary for each DynamicTest child. Use ordinary @Test or"
            + " @ParameterizedTest methods for audited cases, or add @QueryAuditExclude to this"
            + " factory.");
  }
}
