package io.queryaudit.junit5;

import io.queryaudit.core.detector.RepositoryReturnTypeResolver;
import io.queryaudit.core.interceptor.LazyLoadTracker;
import io.queryaudit.core.interceptor.QueryCaptureSnapshot;
import io.queryaudit.core.interceptor.QueryInterceptor;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.provenance.AuditCapability;
import io.queryaudit.core.regression.QueryCounts;
import io.queryaudit.junit5.QueryAuditExtension.AuditRunState;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import javax.sql.DataSource;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Typed access to a JUnit audit scope. Store keys and parent lookup live here, so analysis and
 * lifecycle collaborators depend on resources and policies rather than their storage layout.
 */
final class AuditScope {
  private static final String KEY_INPUT_CONTEXT = "auditInputContext";
  private static final String KEY_EXPLAIN_CAPABILITY = "explainCapability";
  private static final String KEY_CAPTURE = InvocationCapture.class.getName();
  private static final String KEY_SUPPRESSION = InvocationSuppression.class.getName();

  static final ExtensionContext.Namespace NAMESPACE =
      ExtensionContext.Namespace.create(QueryAuditExtension.class);

  private static final String KEY_INDEX_METADATA = "indexMetadata";
  private static final String KEY_COUNT_BASELINE = "countBaseline";
  private static final String KEY_CURRENT_COUNTS = "currentCounts";
  private static final String KEY_RETURN_TYPE_RESOLVER = "returnTypeResolver";
  private static final String KEY_METHOD_SCOPED_CLEANUP = "methodScopedCleanup";
  private static final String KEY_AUDIT_RESOURCES = AuditResources.class.getName();
  private static final String KEY_ACTIVE = "auditActive";
  private static final String KEY_METHOD_ACTIVE = "methodAuditActive";
  private static final String KEY_AFTER_EACH_DONE = "afterEachDone";
  private static final String KEY_INITIALIZATION_FAILURE_RECORDED = "initializationFailureRecorded";
  private static final String KEY_CONTRACTS = "queryContracts";
  private static final String KEY_RUN_STATE = AuditRunState.class.getName();
  private static final String KEY_LEGACY_IDENTITY_CLAIMS = "legacyIdentityClaims";

  enum Flag {
    ACTIVE(KEY_ACTIVE),
    METHOD_ACTIVE(KEY_METHOD_ACTIVE),
    ANALYSIS_DONE(KEY_AFTER_EACH_DONE),
    INITIALIZATION_FAILED(KEY_INITIALIZATION_FAILURE_RECORDED);
    private final String key;

    Flag(String key) {
      this.key = key;
    }
  }

  private final ExtensionContext context;

  private AuditScope(ExtensionContext context) {
    this.context = Objects.requireNonNull(context);
  }

  static AuditScope of(ExtensionContext context) {
    return new AuditScope(context);
  }

  ExtensionContext context() {
    return context;
  }

  private ExtensionContext.Store store() {
    return context.getStore(NAMESPACE);
  }

  Boolean flag(Flag flag) {
    return store().get(flag.key, Boolean.class);
  }

  void flag(Flag flag, boolean value) {
    store().put(flag.key, value);
  }

  Boolean inheritedActive() {
    for (ExtensionContext current = context;
        current != null;
        current = current.getParent().orElse(null)) {
      Boolean active = current.getStore(NAMESPACE).get(KEY_ACTIVE, Boolean.class);
      if (active != null) return active;
    }
    return null;
  }

  void installCapture(AuditResources resources) {
    store().put(KEY_AUDIT_RESOURCES, resources);
  }

  void metadata(IndexMetadata metadata) {
    if (metadata != null) store().put(KEY_INDEX_METADATA, metadata);
  }

  void returnTypeResolver(RepositoryReturnTypeResolver resolver) {
    store().put(KEY_RETURN_TYPE_RESOLVER, resolver);
  }

  void countPolicies(Map<String, QueryCounts> baseline, Map<String, QueryCounts> contracts) {
    store().put(KEY_COUNT_BASELINE, baseline);
    store().put(KEY_CURRENT_COUNTS, new ConcurrentHashMap<String, QueryCounts>());
    store().put(KEY_CONTRACTS, contracts);
  }

  void inputContext(AuditInputContext inputs) {
    store().put(KEY_INPUT_CONTEXT, inputs);
  }

  AuditCapability explainCapability() {
    return store().get(KEY_EXPLAIN_CAPABILITY, AuditCapability.class);
  }

  void explainCapability(AuditCapability capability) {
    store().put(KEY_EXPLAIN_CAPABILITY, capability);
  }

  void methodCleanup(Runnable cleanup) {
    store().put(KEY_METHOD_SCOPED_CLEANUP, (ExtensionContext.Store.CloseableResource) cleanup::run);
  }

  void clearInitialization() {
    LegacyAuditResourceStore.clear(store());
    for (String key :
        List.of(
            KEY_AUDIT_RESOURCES,
            KEY_INDEX_METADATA,
            KEY_RETURN_TYPE_RESOLVER,
            KEY_COUNT_BASELINE,
            KEY_CURRENT_COUNTS,
            KEY_CONTRACTS,
            KEY_METHOD_SCOPED_CLEANUP)) {
      store().remove(key);
    }
  }

  QueryAuditExtension.ReportFinalizer finalizer(
      Supplier<QueryAuditExtension.ReportFinalizer> factory) {
    return (QueryAuditExtension.ReportFinalizer)
        context
            .getRoot()
            .getStore(NAMESPACE)
            .getOrComputeIfAbsent(
                QueryAuditExtension.ReportFinalizer.class.getName(), key -> factory.get());
  }

  SuiteReportFinalizer finalizer() {
    Object value =
        context
            .getRoot()
            .getStore(NAMESPACE)
            .get(QueryAuditExtension.ReportFinalizer.class.getName());
    return value instanceof SuiteReportFinalizer finalizer ? finalizer : null;
  }

  QueryAuditExtension.LegacyIdentityRegistry identityClaims() {
    return context
        .getRoot()
        .getStore(NAMESPACE)
        .getOrComputeIfAbsent(
            KEY_LEGACY_IDENTITY_CLAIMS,
            ignored -> new QueryAuditExtension.LegacyIdentityRegistry(),
            QueryAuditExtension.LegacyIdentityRegistry.class);
  }

  String target() {
    String target = context.getRequiredTestClass().getName();
    return context.getTestMethod().map(method -> target + "#" + method.getName()).orElse(target);
  }

  String identity() {
    String uniqueId = context.getUniqueId();
    if (uniqueId != null && !uniqueId.isBlank()) {
      return uniqueId;
    }
    String className = context.getRequiredTestClass().getName();
    return context
        .getTestMethod()
        .map(method -> className + "#" + method.toGenericString())
        .orElse(className);
  }

  boolean ownsResources() {
    AuditResources resources = initializedResources();
    return resources == null
        ? LegacyAuditResourceStore.isOwnedBy(context, identity())
        : resources.isOwnedBy(identity());
  }

  RepositoryReturnTypeResolver returnTypeResolver() {
    ExtensionContext.Store store = context.getStore(NAMESPACE);
    Object resolver = store.get(KEY_RETURN_TYPE_RESOLVER);
    if (resolver instanceof RepositoryReturnTypeResolver r) {
      return r;
    }
    ExtensionContext parent = context.getParent().orElse(null);
    if (parent != null) {
      resolver = parent.getStore(NAMESPACE).get(KEY_RETURN_TYPE_RESOLVER);
      if (resolver instanceof RepositoryReturnTypeResolver r) {
        return r;
      }
    }
    return null;
  }

  QueryInterceptor interceptor() {
    InvocationCapture capture = invocationCapture();
    if (capture != null) return capture.session().interceptor();
    AuditResources resources = initializedResources();
    return resources == null
        ? LegacyAuditResourceStore.interceptor(context)
        : resources.interceptor();
  }

  LazyLoadTracker tracker() {
    InvocationCapture capture = invocationCapture();
    if (capture != null) return capture.session().tracker();
    AuditResources resources = initializedResources();
    return resources == null ? LegacyAuditResourceStore.tracker(context) : resources.tracker();
  }

  DataSource dataSource() {
    AuditResources resources = initializedResources();
    return resources == null
        ? LegacyAuditResourceStore.dataSource(context)
        : resources.dataSource();
  }

  IndexMetadata metadata() {
    ExtensionContext.Store store = context.getStore(NAMESPACE);
    IndexMetadata metadata = store.get(KEY_INDEX_METADATA, IndexMetadata.class);
    if (metadata == null) {
      ExtensionContext parent = context.getParent().orElse(null);
      if (parent != null) {
        metadata = parent.getStore(NAMESPACE).get(KEY_INDEX_METADATA, IndexMetadata.class);
      }
    }
    return metadata;
  }

  @SuppressWarnings("unchecked") // Legacy Store representation; writes are typed in countPolicies.
  Map<String, QueryCounts> countBaseline() {
    ExtensionContext.Store store = context.getStore(NAMESPACE);
    Object obj = store.get(KEY_COUNT_BASELINE);
    if (obj instanceof Map<?, ?> map) {
      return (Map<String, QueryCounts>) map;
    }
    ExtensionContext parent = context.getParent().orElse(null);
    if (parent != null) {
      obj = parent.getStore(NAMESPACE).get(KEY_COUNT_BASELINE);
      if (obj instanceof Map<?, ?> map) {
        return (Map<String, QueryCounts>) map;
      }
    }
    return null;
  }

  @SuppressWarnings("unchecked") // Legacy Store representation; writes are typed in countPolicies.
  Map<String, QueryCounts> currentCounts() {
    ExtensionContext.Store store = context.getStore(NAMESPACE);
    Object obj = store.get(KEY_CURRENT_COUNTS);
    if (obj instanceof Map<?, ?> map) {
      return (Map<String, QueryCounts>) map;
    }
    ExtensionContext parent = context.getParent().orElse(null);
    if (parent != null) {
      obj = parent.getStore(NAMESPACE).get(KEY_CURRENT_COUNTS);
      if (obj instanceof Map<?, ?> map) {
        return (Map<String, QueryCounts>) map;
      }
    }
    return null;
  }

  @SuppressWarnings("unchecked") // Legacy Store representation; writes are typed in countPolicies.
  Map<String, QueryCounts> contracts() {
    ExtensionContext current = context;
    while (current != null) {
      Object obj = current.getStore(NAMESPACE).get(KEY_CONTRACTS);
      if (obj instanceof Map<?, ?> map) {
        return (Map<String, QueryCounts>) map;
      }
      current = current.getParent().orElse(null);
    }
    return null;
  }

  AuditInputContext inputContext() {
    for (ExtensionContext current = context;
        current != null;
        current = current.getParent().orElse(null)) {
      AuditInputContext inputs =
          current.getStore(NAMESPACE).get(KEY_INPUT_CONTEXT, AuditInputContext.class);
      if (inputs != null) {
        return inputs;
      }
    }
    return null;
  }

  AuditResources captureResources() {
    AuditResources resources = initializedResources();
    return resources == null ? LegacyAuditResourceStore.capture(context, identity()) : resources;
  }

  void startCapture(int maxQueries) {
    AuditResources resources = initializedResources();
    if (resources == null) {
      // Compatibility for integrations that seeded the former internal Store layout.
      captureResources().startCapture();
      return;
    }
    if (invocationCapture() != null) {
      // Auto-detection and static registration may invoke the same callback; the Store owns it
      // once.
      return;
    }
    InvocationCapture capture = resources.openCapture(this, maxQueries);
    try {
      Runnable backgroundWork = BackgroundWork.lookup(context);
      if (backgroundWork != null) capture.awaitBackgroundWorkWith(backgroundWork);
      store().put(KEY_CAPTURE, capture);
    } catch (RuntimeException | Error failure) {
      capture.close();
      throw failure;
    }
  }

  void suppressCaptureForInvocation() {
    InvocationSuppression existing = store().get(KEY_SUPPRESSION, InvocationSuppression.class);
    if (existing != null && existing.belongsTo(identity())) return;
    InvocationSuppression suppression = new InvocationSuppression(identity());
    try {
      store().put(KEY_SUPPRESSION, suppression);
    } catch (RuntimeException | Error failure) {
      suppression.close();
      throw failure;
    }
  }

  void transitionCapture(io.queryaudit.core.model.LifecyclePhase phase) {
    InvocationCapture capture = invocationCapture();
    if (capture != null) capture.session().setPhase(phase);
    else if (interceptor() != null) interceptor().setPhase(phase);
  }

  void finishTestPhase() {
    InvocationCapture capture = invocationCapture();
    try {
      if (capture != null) capture.awaitBackgroundWork();
    } finally {
      transitionCapture(io.queryaudit.core.model.LifecyclePhase.TEARDOWN);
    }
  }

  QueryCaptureSnapshot stopCapture() {
    InvocationCapture capture = invocationCapture();
    return capture == null ? captureResources().stopCapture() : capture.stop();
  }

  void closeCapture() {
    InvocationCapture capture = invocationCapture();
    if (capture != null) capture.close();
  }

  private InvocationCapture invocationCapture() {
    ExtensionContext.Store store = store();
    InvocationCapture capture =
        store == null ? null : store.get(KEY_CAPTURE, InvocationCapture.class);
    return capture != null && capture.belongsTo(identity()) ? capture : null;
  }

  AuditResources resourcesForCleanup(HibernateIntegration hibernateIntegration) {
    AuditResources resources = initializedResources();
    if (resources == null) {
      resources = LegacyAuditResourceStore.cleanup(context, identity(), hibernateIntegration);
      store().put(KEY_AUDIT_RESOURCES, resources);
    }
    return resources;
  }

  private AuditResources initializedResources() {
    for (ExtensionContext current = context;
        current != null;
        current = current.getParent().orElse(null)) {
      ExtensionContext.Store store = current.getStore(NAMESPACE);
      AuditResources resources =
          store == null ? null : store.get(KEY_AUDIT_RESOURCES, AuditResources.class);
      if (resources != null) return resources;
    }
    return null;
  }

  AuditRunState runState() {
    return (AuditRunState)
        context
            .getRoot()
            .getStore(NAMESPACE)
            .getOrComputeIfAbsent(KEY_RUN_STATE, key -> new AuditRunState());
  }
}
