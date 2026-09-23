package io.queryaudit.junit5;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.detector.RepositoryReturnTypeResolver;
import io.queryaudit.core.extension.AuditExtensions;
import io.queryaudit.core.interceptor.LazyLoadTracker;
import io.queryaudit.core.interceptor.QueryInterceptor;
import io.queryaudit.core.model.IncompleteReasonCode;
import io.queryaudit.core.provenance.AuditCapability;
import io.queryaudit.core.provenance.AuditRuntimeIdentity;
import java.nio.file.InvalidPathException;
import javax.sql.DataSource;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;

/** Acquires one scope's resources transactionally and releases only what that scope installed. */
final class AuditScopeInitializer {
  enum Boundary {
    CLASS,
    METHOD
  }

  private final DataSourceResolver dataSources;
  private final IndexMetadataCollector metadata;
  private final HibernateIntegration hibernate;
  private final AuditPolicyFiles policyFiles;

  AuditScopeInitializer(
      DataSourceResolver dataSources,
      IndexMetadataCollector metadata,
      HibernateIntegration hibernate,
      AuditPolicyFiles policyFiles) {
    this.dataSources = dataSources;
    this.metadata = metadata;
    this.hibernate = hibernate;
    this.policyFiles = policyFiles;
  }

  void initialize(
      AuditScope scope, QueryAuditConfig config, AuditExtensions extensions, Boundary boundary)
      throws Exception {
    if (scope.interceptor() != null && boundary == Boundary.CLASS) return;
    DataSourceResolver.ResolvedDataSource resolved = dataSources.resolve(scope.context());
    if (scope.interceptor() != null && !replacesInheritedDataSource(scope, resolved)) return;
    if (resolved == null) {
      if (boundary == Boundary.METHOD) missingDataSource(scope);
      return;
    }

    AuditPolicyFiles.Loaded policies;
    try {
      policies = policyFiles.load(scope);
    } catch (IllegalStateException | InvalidPathException failure) {
      AuditDiagnostics.initializationFailure(
          scope, IncompleteReasonCode.CONTRACT_UNREADABLE, failure.getMessage());
      throw failure;
    }

    QueryInterceptor interceptor = new QueryInterceptor();
    Runnable hookCleanup;
    try {
      interceptor.setMaxQueries(config.getMaxQueries());
      hookCleanup = dataSources.hookInterceptor(resolved, interceptor);
    } catch (RuntimeException | Error failure) {
      AuditDiagnostics.initializationFailure(
          scope,
          IncompleteReasonCode.AUDIT_INITIALIZATION_FAILED,
          "Could not initialize query capture for "
              + scope.target()
              + ": "
              + AuditDiagnostics.failureMessage(failure));
      throw failure;
    }

    AuditResources resources =
        new AuditResources(scope.identity(), interceptor, resolved.dataSource(), hookCleanup);
    try {
      scope.installCapture(resources);
      initializeCapabilities(scope, resources, resolved.dataSource(), extensions, policies);
      if (boundary == Boundary.METHOD) scope.methodCleanup(() -> close(scope));
    } catch (RuntimeException | Error failure) {
      resources.close(failure);
      scope.clearInitialization();
      throw failure;
    }
  }

  private static boolean replacesInheritedDataSource(
      AuditScope scope, DataSourceResolver.ResolvedDataSource resolved) {
    DataSource inherited = scope.dataSource();
    return resolved != null
        && resolved.staticField() == null
        && inherited != null
        && resolved.dataSource() != inherited;
  }

  private void initializeCapabilities(
      AuditScope scope,
      AuditResources resources,
      DataSource dataSource,
      AuditExtensions extensions,
      AuditPolicyFiles.Loaded policies) {
    IndexMetadataCollector.Result indexes =
        extensions.indexMetadataProviders().isEmpty()
            ? metadata.collectWithCapabilities(dataSource)
            : metadata.collectWithCapabilities(dataSource, extensions.indexMetadataProvidersById());
    if (indexes.diagnostic() == null) {
      AuditDiagnostics.capabilityFailure(
          scope, indexes.capability(), IncompleteReasonCode.CAPABILITY_INITIALIZATION_FAILED);
    } else {
      AuditDiagnostics.incomplete(
          scope,
          IncompleteReasonCode.CAPABILITY_INITIALIZATION_FAILED,
          "Index metadata provider failed: " + indexes.diagnostic().description());
    }
    scope.metadata(indexes.metadata());
    RepositoryCapability repository = initializeRepositoryTypes(scope);
    scope.countPolicies(policies.countBaseline(), policies.contracts());

    HibernateIntegration.Registration registration =
        hibernate.registerWithCapabilities(scope.context());
    LazyLoadTracker tracker = registration.tracker();
    if (tracker != null) {
      resources.attachTracker(tracker, () -> hibernate.unregisterTracker(scope.context(), tracker));
    }
    AuditDiagnostics.capabilityFailure(
        scope, registration.capability(), IncompleteReasonCode.CAPABILITY_INITIALIZATION_FAILED);
    scope.inputContext(
        new AuditInputContext(
            indexes.dialect(),
            indexes.capability(),
            registration.capability(),
            repository.capability(),
            repository.resolver()));
  }

  private RepositoryCapability initializeRepositoryTypes(AuditScope scope) {
    try {
      Object applicationContext = AuditSettingsResolver.resolveApplicationContext(scope.context());
      if (applicationContext != null && SpringDataReturnTypeResolver.isAvailable()) {
        SpringDataReturnTypeResolver resolver =
            new SpringDataReturnTypeResolver(applicationContext);
        scope.returnTypeResolver(resolver);
        return new RepositoryCapability(
            AuditCapability.available(AuditRuntimeIdentity.implementation(resolver.getClass())),
            resolver);
      }
    } catch (Exception | NoClassDefFoundError failure) {
      AuditCapability capability = AuditCapability.failed("spring-data-return-types");
      AuditDiagnostics.capabilityFailure(
          scope, capability, IncompleteReasonCode.CAPABILITY_INITIALIZATION_FAILED);
      return new RepositoryCapability(capability, null);
    }
    return new RepositoryCapability(AuditCapability.absent(), null);
  }

  void close(AuditScope scope) {
    AuditResources resources = scope.resourcesForCleanup(hibernate);
    if (resources.isClosed()) return;
    Throwable failure = resources.close(null);
    if (failure != null) {
      AuditDiagnostics.incomplete(
          scope,
          IncompleteReasonCode.AUDIT_ANALYSIS_FAILED,
          "Could not clean up QueryAudit for "
              + scope.target()
              + ": "
              + AuditDiagnostics.failureMessage(failure));
    }
    failure =
        AuditResources.runCleanup(failure, () -> policyFiles.writeCountBaselineIfRequested(scope));
    failure =
        AuditResources.runCleanup(failure, () -> policyFiles.writeContractsIfRequested(scope));
    if (failure instanceof RuntimeException runtimeFailure) throw runtimeFailure;
    if (failure instanceof Error error) throw error;
  }

  private static void missingDataSource(AuditScope scope) {
    AuditDiagnostics.initializationFailure(
        scope,
        IncompleteReasonCode.DATASOURCE_UNAVAILABLE,
        "No DataSource was available for " + scope.target());
    throw new ExtensionConfigurationException(
        "QueryAudit: DataSource unavailable for active audit of "
            + scope.target()
            + ". Register a DataSource bean in the Spring ApplicationContext or expose a"
            + " non-null static DataSource field on the test class.");
  }

  private record RepositoryCapability(
      AuditCapability capability, RepositoryReturnTypeResolver resolver) {}
}
