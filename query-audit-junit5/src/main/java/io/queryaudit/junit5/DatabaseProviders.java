package io.queryaudit.junit5;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.function.Function;

/** Explicit providers take precedence; legacy discovery remains the fallback. */
final class DatabaseProviders {
  private DatabaseProviders() {}

  enum FailureReason {
    DATABASE_NAME_UNAVAILABLE("Database product name is unavailable"),
    INVALID_PROVIDER_DATABASE_NAME("A database provider must declare a non-blank database name"),
    AMBIGUOUS_EXPLICIT_PROVIDERS("Multiple explicitly registered providers support this database"),
    PROVIDER_DECLARATION_FAILED("A provider could not declare its supported database"),
    PROVIDER_EXECUTION_FAILED("The selected provider did not complete");

    private final String description;

    FailureReason(String description) {
      this.description = description;
    }
  }

  /** Only host-owned reason text and restricted registration identifiers may leave the host. */
  record Diagnostic(FailureReason reason, List<String> registrationIds) {
    Diagnostic {
      List<String> safeIds = new ArrayList<>();
      for (int index = 0; index < registrationIds.size(); index++) {
        String id = registrationIds.get(index);
        safeIds.add(
            id != null && id.matches("[A-Za-z0-9][A-Za-z0-9._:/-]{0,199}")
                ? id
                : "unidentified-provider:" + (index + 1));
      }
      registrationIds = List.copyOf(safeIds);
    }

    String description() {
      return reason.name() + ": " + reason.description + "; registrations=" + registrationIds;
    }
  }

  static final class SelectionException extends IllegalStateException {
    private final Diagnostic diagnostic;

    SelectionException(FailureReason reason, List<String> registrationIds) {
      this(new Diagnostic(reason, registrationIds));
    }

    private SelectionException(Diagnostic diagnostic) {
      super(diagnostic.description());
      this.diagnostic = diagnostic;
    }

    Diagnostic diagnostic() {
      return diagnostic;
    }
  }

  /** Assembly identity stays separate from the provider's comparison/provenance fingerprint. */
  record Selected<T>(String registrationId, T provider) {
    Diagnostic executionFailure() {
      return new Diagnostic(FailureReason.PROVIDER_EXECUTION_FAILED, List.of(registrationId));
    }
  }

  static <T> T select(
      String database,
      List<T> explicit,
      Iterable<T> discovered,
      Function<T, String> supportedDatabase) {
    return selectRegistered(
        database, anonymousRegistrations(explicit), discovered, supportedDatabase);
  }

  static <T> Map<String, T> anonymousRegistrations(List<T> providers) {
    Map<String, T> registrations = new LinkedHashMap<>();
    for (int index = 0; index < providers.size(); index++) {
      registrations.put("explicit-provider:" + (index + 1), providers.get(index));
    }
    return registrations;
  }

  static <T> T selectRegistered(
      String database,
      Map<String, T> explicit,
      Iterable<T> discovered,
      Function<T, String> supportedDatabase) {
    Selected<T> selected = selectWithIdentity(database, explicit, discovered, supportedDatabase);
    return selected == null ? null : selected.provider();
  }

  static <T> Selected<T> selectWithIdentity(
      String database,
      Map<String, T> explicit,
      Iterable<T> discovered,
      Function<T, String> supportedDatabase) {
    if (database == null || database.isBlank()) {
      throw new SelectionException(FailureReason.DATABASE_NAME_UNAVAILABLE, List.of());
    }
    String product = database.toLowerCase(Locale.ROOT);
    T selected = null;
    String selectedId = null;
    for (Map.Entry<String, T> registration : explicit.entrySet()) {
      T provider = registration.getValue();
      if (matches(
          product,
          declaredDatabase(provider, supportedDatabase, registration.getKey()),
          registration.getKey())) {
        if (selected != null) {
          throw new SelectionException(
              FailureReason.AMBIGUOUS_EXPLICIT_PROVIDERS,
              List.of(selectedId, registration.getKey()));
        }
        selected = provider;
        selectedId = registration.getKey();
      }
    }
    if (selected != null) {
      return new Selected<>(selectedId, selected);
    }
    int discoveredIndex = 0;
    for (T provider : discovered) {
      String id = "discovered-provider:" + (++discoveredIndex);
      if (matches(product, declaredDatabase(provider, supportedDatabase, id), id)) {
        return new Selected<>(id, provider);
      }
    }
    return null;
  }

  private static <T> String declaredDatabase(
      T provider, Function<T, String> declaration, String id) {
    try {
      return declaration.apply(provider);
    } catch (RuntimeException | LinkageError | ServiceConfigurationError failure) {
      throw new SelectionException(FailureReason.PROVIDER_DECLARATION_FAILED, List.of(id));
    }
  }

  private static boolean matches(String product, String supported, String registrationId) {
    if (supported == null || supported.isBlank()) {
      throw new SelectionException(
          FailureReason.INVALID_PROVIDER_DATABASE_NAME, List.of(registrationId));
    }
    return product.contains(supported.toLowerCase(Locale.ROOT));
  }
}
