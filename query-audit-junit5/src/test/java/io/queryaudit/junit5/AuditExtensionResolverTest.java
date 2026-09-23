package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.queryaudit.core.extension.AuditExtensions;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;

class AuditExtensionResolverTest {
  @Test
  void nestedCallbacksCanReuseTheirInheritedCatalogAfterCaptureStarted() {
    var namespace = ExtensionContext.Namespace.create(getClass());
    var extensions = AuditExtensions.empty();
    var parent = mock(ExtensionContext.class);
    var child = mock(ExtensionContext.class);
    var parentStore = mock(ExtensionContext.Store.class);
    var childStore = mock(ExtensionContext.Store.class);
    when(parent.getStore(namespace)).thenReturn(parentStore);
    when(child.getStore(namespace)).thenReturn(childStore);
    when(child.getParent()).thenReturn(Optional.of(parent));
    when(parentStore.get(AuditExtensions.class.getName(), AuditExtensions.class))
        .thenReturn(extensions);
    var resolver = new AuditExtensionResolver(namespace, extensions);

    resolver.register(child, true);
    resolver.requireStaticRegistration(child);
    assertThat(
            resolver.resolve(
                child,
                () -> {
                  throw new AssertionError("unexpected Spring lookup");
                }))
        .isSameAs(extensions);
  }

  @Test
  void noSpringOrNoCatalogPreservesDefaultDiscovery() {
    assertThat(AuditExtensionResolver.fromApplicationContext(null).rules()).isEmpty();
    assertThat(AuditExtensionResolver.fromApplicationContext(new FakeContext(Map.of())).rules())
        .isEmpty();
  }

  @Test
  void findsExactlyOneCatalogWithoutDependingOnBeanName() {
    AuditExtensions extensions = AuditExtensions.empty();
    assertThat(
            AuditExtensionResolver.fromApplicationContext(
                new FakeContext(Map.of("company", extensions))))
        .isSameAs(extensions);
  }

  @Test
  void multipleCatalogsFailInsteadOfSilentlyUsingDefaults() {
    assertThatThrownBy(
            () ->
                AuditExtensionResolver.fromApplicationContext(
                    new FakeContext(
                        Map.of("one", AuditExtensions.empty(), "two", AuditExtensions.empty()))))
        .isInstanceOf(ExtensionConfigurationException.class)
        .hasMessageContaining("expected one AuditExtensions bean");
  }

  @Test
  void beanInitializationFailureIsNotMistakenForAnAbsentExtension() {
    assertThatThrownBy(() -> AuditExtensionResolver.fromApplicationContext(new BrokenContext()))
        .isInstanceOf(ExtensionConfigurationException.class)
        .hasCauseInstanceOf(IllegalStateException.class);
  }

  public static class FakeContext {
    private final Map<String, AuditExtensions> beans;

    FakeContext(Map<String, AuditExtensions> beans) {
      this.beans = beans;
    }

    public Map<String, AuditExtensions> getBeansOfType(Class<?> type) {
      return beans;
    }
  }

  public static class BrokenContext {
    public Map<String, AuditExtensions> getBeansOfType(Class<?> type) {
      throw new IllegalStateException("extension could not initialize");
    }
  }
}
