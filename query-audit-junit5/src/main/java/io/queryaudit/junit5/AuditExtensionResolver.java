package io.queryaudit.junit5;

import io.queryaudit.core.extension.AuditExtensions;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.function.Supplier;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;

/** Reads the optional Spring catalog without making Spring a JUnit runtime dependency. */
final class AuditExtensionResolver {
  private static final String KEY = AuditExtensions.class.getName();
  private final ExtensionContext.Namespace namespace;
  private final AuditExtensions supplied;

  AuditExtensionResolver(ExtensionContext.Namespace namespace, AuditExtensions supplied) {
    this.namespace = namespace;
    this.supplied = supplied;
  }

  boolean explicitlyRegistered() {
    return supplied != null;
  }

  void requireStaticRegistration(ExtensionContext context) {
    if (supplied != null && findRegistered(context) != supplied) {
      throw new ExtensionConfigurationException(
          "QueryAudit: use a static @RegisterExtension field for a supplied AuditExtensions catalog");
    }
  }

  void register(ExtensionContext context, boolean captureStarted) {
    if (supplied == null) {
      return;
    }
    ExtensionContext.Store store = context.getStore(namespace);
    AuditExtensions existing = findRegistered(context);
    if (captureStarted && existing != supplied) {
      throw new ExtensionConfigurationException(
          "QueryAudit: extensions must be registered before capture starts. Use the static "
              + "@RegisterExtension field instead of also registering @QueryAudit or @ExtendWith.");
    }
    if (existing != null && existing != supplied) {
      throw new ExtensionConfigurationException("QueryAudit: conflicting extension catalogs");
    }
    store.put(KEY, supplied);
  }

  AuditExtensions resolve(ExtensionContext context, Supplier<Object> applicationContext) {
    AuditExtensions registered = findRegistered(context);
    if (registered != null) {
      return registered;
    }
    AuditExtensions resolved =
        supplied != null ? supplied : fromApplicationContext(applicationContext.get());
    context.getStore(namespace).put(KEY, resolved);
    return resolved;
  }

  private AuditExtensions findRegistered(ExtensionContext context) {
    ExtensionContext current = context;
    while (current != null) {
      AuditExtensions registered = current.getStore(namespace).get(KEY, AuditExtensions.class);
      if (registered != null) {
        return registered;
      }
      current = current.getParent().orElse(null);
    }
    return null;
  }

  static AuditExtensions fromApplicationContext(Object applicationContext) {
    if (applicationContext == null) {
      return AuditExtensions.empty();
    }
    try {
      Object result =
          applicationContext
              .getClass()
              .getMethod("getBeansOfType", Class.class)
              .invoke(applicationContext, AuditExtensions.class);
      if (!(result instanceof Map<?, ?> beans)) {
        throw new ExtensionConfigurationException("QueryAudit: invalid extension catalog lookup");
      }
      if (beans.isEmpty()) {
        return AuditExtensions.empty();
      }
      if (beans.size() != 1) {
        throw new ExtensionConfigurationException(
            "QueryAudit: expected one AuditExtensions bean, found " + beans.keySet());
      }
      return AuditExtensions.class.cast(beans.values().iterator().next());
    } catch (InvocationTargetException failure) {
      throw new ExtensionConfigurationException(
          "QueryAudit: could not initialize the extension catalog", failure.getCause());
    } catch (ReflectiveOperationException | ClassCastException failure) {
      throw new ExtensionConfigurationException(
          "QueryAudit: could not read the extension catalog", failure);
    }
  }
}
