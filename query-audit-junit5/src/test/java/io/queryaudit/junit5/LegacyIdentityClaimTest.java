package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;

class LegacyIdentityClaimTest {

  @Test
  void oneLegacyEntryCannotApplyToTwoStableTests() {
    ExtensionContext context = contextWithRegistry();
    String claimKey = "query contract|OrderTest|duplicate";

    QueryAuditExtension.claimLegacyIdentity(
        context, claimKey, "junit:first", true, "OrderTest", "duplicate");

    assertThatCode(
            () ->
                QueryAuditExtension.claimLegacyIdentity(
                    context, claimKey, "junit:first", true, "OrderTest", "duplicate"))
        .doesNotThrowAnyException();
    assertThatThrownBy(
            () ->
                QueryAuditExtension.claimLegacyIdentity(
                    context, claimKey, "junit:second", true, "OrderTest", "duplicate"))
        .isInstanceOf(ExtensionConfigurationException.class)
        .hasMessageContaining("ambiguous 0.5 identity")
        .hasMessageContaining("junit:first")
        .hasMessageContaining("junit:second")
        .hasMessageContaining("Re-record the policy file");
  }

  @Test
  void aStableOwnerPreventsAnotherTestFromUsingItsPreservedLegacyEntry() {
    ExtensionContext context = contextWithRegistry();
    String claimKey = "count baseline|OrderTest|duplicate";

    QueryAuditExtension.claimLegacyIdentity(
        context, claimKey, "junit:stable", false, "OrderTest", "duplicate");

    assertThatThrownBy(
            () ->
                QueryAuditExtension.claimLegacyIdentity(
                    context, claimKey, "junit:fallback", true, "OrderTest", "duplicate"))
        .isInstanceOf(ExtensionConfigurationException.class)
        .hasMessageContaining("junit:stable")
        .hasMessageContaining("junit:fallback");
  }

  @Test
  void aFallbackIsRejectedWhenTheStableOwnerRunsLater() {
    ExtensionContext context = contextWithRegistry();
    String claimKey = "count baseline|OrderTest|duplicate";

    QueryAuditExtension.claimLegacyIdentity(
        context, claimKey, "junit:fallback", true, "OrderTest", "duplicate");

    assertThatThrownBy(
            () ->
                QueryAuditExtension.claimLegacyIdentity(
                    context, claimKey, "junit:stable", false, "OrderTest", "duplicate"))
        .isInstanceOf(ExtensionConfigurationException.class)
        .hasMessageContaining("junit:fallback")
        .hasMessageContaining("junit:stable");
  }

  @Test
  void fullyMigratedTestsCanIgnoreTheSamePreservedLegacyEntry() {
    ExtensionContext context = contextWithRegistry();
    String claimKey = "query contract|OrderTest|duplicate";

    assertThatCode(
            () -> {
              QueryAuditExtension.claimLegacyIdentity(
                  context, claimKey, "junit:first", false, "OrderTest", "duplicate");
              QueryAuditExtension.claimLegacyIdentity(
                  context, claimKey, "junit:second", false, "OrderTest", "duplicate");
            })
        .doesNotThrowAnyException();
  }

  @Test
  void warningDeduplicationIsScopedToOneRegistry() {
    QueryAuditExtension.LegacyIdentityRegistry first =
        new QueryAuditExtension.LegacyIdentityRegistry();
    QueryAuditExtension.LegacyIdentityRegistry second =
        new QueryAuditExtension.LegacyIdentityRegistry();

    assertThat(first.markWarning("query contract|OrderTest|duplicate")).isTrue();
    assertThat(first.markWarning("query contract|OrderTest|duplicate")).isFalse();
    assertThat(second.markWarning("query contract|OrderTest|duplicate")).isTrue();
  }

  private static ExtensionContext contextWithRegistry() {
    QueryAuditExtension.LegacyIdentityRegistry registry =
        new QueryAuditExtension.LegacyIdentityRegistry();
    ExtensionContext.Store rootStore = mock(ExtensionContext.Store.class);
    when(rootStore.getOrComputeIfAbsent(
            any(), any(), eq(QueryAuditExtension.LegacyIdentityRegistry.class)))
        .thenReturn(registry);
    ExtensionContext root = mock(ExtensionContext.class);
    when(root.getStore(any(ExtensionContext.Namespace.class))).thenReturn(rootStore);
    ExtensionContext context = mock(ExtensionContext.class);
    when(context.getRoot()).thenReturn(root);
    return context;
  }
}
