package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class DatabaseProvidersTest {
  @Test
  void explicitProviderTakesPrecedenceWithoutInitializingDiscovery() {
    Iterable<String> brokenDiscovery =
        () -> {
          throw new AssertionError("discovery was used");
        };
    assertThat(
            DatabaseProviders.select("H2 2.3", List.of("h2"), brokenDiscovery, Function.identity()))
        .isEqualTo("h2");
  }

  @Test
  void unrelatedExplicitProviderPreservesLegacyFallback() {
    assertThat(
            DatabaseProviders.select(
                "PostgreSQL", List.of("mysql"), List.of("postgresql"), Function.identity()))
        .isEqualTo("postgresql");
  }

  @Test
  void overlappingExplicitProvidersAreNotChosenByRegistrationOrder() {
    assertThatThrownBy(
            () ->
                DatabaseProviders.select(
                    "MySQL", List.of("mysql", "sql"), List.of(), Function.identity()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Multiple explicitly registered");
  }

  @Test
  void absentProviderIsOptionalButBlankProviderNameIsInvalid() {
    assertThat(DatabaseProviders.select("H2", List.of(), List.of("mysql"), Function.identity()))
        .isNull();
    assertThatThrownBy(
            () -> DatabaseProviders.select("H2", List.of(" "), List.of(), Function.identity()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("non-blank");
  }

  @Test
  void ambiguousNamedProvidersRetainSafeIdsAndAReasonWithoutTheDatabaseProduct() {
    Map<String, String> providers = new LinkedHashMap<>();
    providers.put("index-metadata:applicationIndexes", "h2");
    providers.put("index-metadata:duplicateIndexes", "h2");

    assertThatThrownBy(
            () ->
                DatabaseProviders.selectRegistered(
                    "H2 private_database_name", providers, List.of(), Function.identity()))
        .isInstanceOf(DatabaseProviders.SelectionException.class)
        .hasMessageContaining("AMBIGUOUS_EXPLICIT_PROVIDERS")
        .hasMessageContaining("index-metadata:applicationIndexes")
        .hasMessageContaining("index-metadata:duplicateIndexes")
        .hasMessageNotContaining("private_database_name");
  }

  @Test
  void unsafeAndOversizedRegistrationIdsAreReplacedRatherThanEchoed() {
    Map<String, String> providers = new LinkedHashMap<>();
    providers.put("private-token\nSELECT secret FROM customer", "h2");
    providers.put("oversized-" + "x".repeat(200), "h2");

    assertThatThrownBy(
            () ->
                DatabaseProviders.selectRegistered("H2", providers, List.of(), Function.identity()))
        .isInstanceOf(DatabaseProviders.SelectionException.class)
        .hasMessageContaining("unidentified-provider:1")
        .hasMessageContaining("unidentified-provider:2")
        .hasMessageNotContaining("private-token")
        .hasMessageNotContaining("SELECT")
        .hasMessageNotContaining("oversized-");
  }

  @Test
  void throwingDeclarationsExposeOnlyTheRegistrationAndHostReason() {
    assertThatThrownBy(
            () ->
                DatabaseProviders.selectRegistered(
                    "h2",
                    Map.of("company:broken", "provider"),
                    List.of(),
                    ignored -> {
                      throw new IllegalStateException("private-token SELECT secret");
                    }))
        .isInstanceOf(DatabaseProviders.SelectionException.class)
        .hasMessageContaining("PROVIDER_DECLARATION_FAILED")
        .hasMessageContaining("company:broken")
        .hasMessageNotContaining("private-token")
        .hasNoCause();
  }

  @Test
  void declarationServiceErrorsRetainTheRegistrationWithoutLeakingPayloads() {
    assertThatThrownBy(
            () ->
                DatabaseProviders.selectRegistered(
                    "h2",
                    Map.of("company:broken-service", "provider"),
                    List.of(),
                    ignored -> {
                      throw new ServiceConfigurationError("private-token SELECT secret");
                    }))
        .isInstanceOf(DatabaseProviders.SelectionException.class)
        .hasMessageContaining("PROVIDER_DECLARATION_FAILED")
        .hasMessageContaining("company:broken-service")
        .hasMessageNotContaining("private-token")
        .hasNoCause();
  }

  @Test
  void invalidNamedProviderDeclarationRetainsItsSafeRegistrationId() {
    assertThatThrownBy(
            () ->
                DatabaseProviders.selectRegistered(
                    "H2", Map.of("index-metadata:broken", " "), List.of(), Function.identity()))
        .isInstanceOf(DatabaseProviders.SelectionException.class)
        .hasMessageContaining("INVALID_PROVIDER_DATABASE_NAME")
        .hasMessageContaining("index-metadata:broken");
  }
}
