package io.queryaudit.junit5;

import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * The wait for off-thread work that the Spring starter registers from query-audit.await-executors.
 */
final class BackgroundWork {
  static final String BEAN_NAME = "queryAuditBackgroundWork";

  private BackgroundWork() {}

  static Runnable lookup(ExtensionContext context) {
    Object applicationContext = AuditSettingsResolver.resolveApplicationContext(context);
    if (applicationContext == null) return null;
    try {
      Object present =
          applicationContext
              .getClass()
              .getMethod("containsBean", String.class)
              .invoke(applicationContext, BEAN_NAME);
      if (!Boolean.TRUE.equals(present)) return null;
      Object bean =
          applicationContext
              .getClass()
              .getMethod("getBean", String.class)
              .invoke(applicationContext, BEAN_NAME);
      return bean instanceof Runnable await ? await : null;
    } catch (ReflectiveOperationException | RuntimeException unavailable) {
      return null;
    }
  }
}
