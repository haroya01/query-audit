package io.queryaudit.core.contract;

import io.queryaudit.core.interceptor.QueryCaptureSession;
import io.queryaudit.core.interceptor.QueryCaptureSnapshot;
import io.queryaudit.core.interceptor.QueryInterceptor;
import io.queryaudit.core.regression.ContractFiles;
import io.queryaudit.core.regression.QueryContracts;
import io.queryaudit.core.regression.QueryCountBaseline;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

public final class QueryContractScope {
  public static final String RECORD_PROPERTY = "queryAudit.contracts.record";
  private static final String LEGACY_RECORD_PROPERTY = "queryGuard.contracts.record";
  private static final Map<QueryInterceptor, String> OPEN = new ConcurrentHashMap<>();

  private final QueryInterceptor interceptor;
  private final Path location;
  private final Runnable awaitCompletion;

  private QueryContractScope(
      QueryInterceptor interceptor, Path location, Runnable awaitCompletion) {
    this.interceptor = Objects.requireNonNull(interceptor, "interceptor");
    this.location = Objects.requireNonNull(location, "location");
    this.awaitCompletion = Objects.requireNonNull(awaitCompletion, "awaitCompletion");
  }

  public static QueryContractScope of(QueryInterceptor interceptor, Path location) {
    return new QueryContractScope(interceptor, location, () -> {});
  }

  public QueryContractScope awaitingCompletion(Runnable awaitCompletion) {
    return new QueryContractScope(interceptor, location, awaitCompletion);
  }

  public Path location() {
    return location;
  }

  public <T> T verify(String contractId, Callable<T> work) throws Exception {
    CapturedQueries<T> captured = capture(contractId, work);
    verify(captured);
    return captured.value();
  }

  public <T> CapturedQueries<T> capture(String contractId, Callable<T> work) throws Exception {
    Objects.requireNonNull(work, "work");
    Window window = new Window(requireId(contractId));
    T value;
    try {
      value = work.call();
    } catch (Throwable failure) {
      window.abandon();
      throw failure;
    }
    return new CapturedQueries<>(contractId, value, window.finish());
  }

  public ContractCapture open(String contractId) {
    return new ContractCapture(new Window(requireId(contractId)));
  }

  public void verify(CapturedQueries<?> captured) {
    Objects.requireNonNull(captured, "captured");
    String key = QueryCountBaseline.key(captured.contractId());
    if (isRecordMode()) {
      try {
        ContractFiles.record(location, Map.of(key, captured.counts()));
      } catch (IOException failure) {
        throw new UncheckedIOException("Cannot record query contract in " + location, failure);
      }
      System.out.println(
          "[QueryAudit] Query contract recorded: "
              + captured.contractId()
              + " -> "
              + ContractFiles.load(location).fileFor(key));
      return;
    }
    ContractFiles files = ContractFiles.load(location);
    if (!files.contracts().containsKey(key)) {
      throw new QueryContractViolation(
          "Missing query contract: "
              + captured.contractId()
              + "\n  Measured: "
              + QueryCountBaseline.line(key, captured.counts())
              + "\n  Add this line to "
              + files.fileFor(key)
              + ", or rerun with -D"
              + RECORD_PROPERTY
              + "=true and review the diff.");
    }
    String failure =
        QueryContracts.verify(
            captured.contractId(),
            captured.contractId(),
            captured.contractId(),
            captured.counts(),
            files.contracts(),
            captured.queries(),
            files.fileFor(key).toString());
    if (failure != null) {
      throw new QueryContractViolation(failure);
    }
  }

  static boolean isRecordMode() {
    String value = System.getProperty(RECORD_PROPERTY, System.getProperty(LEGACY_RECORD_PROPERTY));
    return Boolean.parseBoolean(value);
  }

  private static String requireId(String contractId) {
    if (contractId == null || contractId.isBlank()) {
      throw new IllegalArgumentException("contractId must not be blank");
    }
    return contractId;
  }

  public final class ContractCapture implements AutoCloseable {
    private final Window window;
    private boolean closed;

    private ContractCapture(Window window) {
      this.window = window;
    }

    @Override
    public void close() {
      if (closed) return;
      closed = true;
      verify(new CapturedQueries<>(window.contractId, null, window.finish()));
    }
  }

  private final class Window {
    private final String contractId;
    private final QueryCaptureSession.Scope claim;

    private Window(String contractId) {
      String open = OPEN.putIfAbsent(interceptor, contractId);
      if (open != null) {
        throw new IllegalStateException(
            "Query contract scope \""
                + open
                + "\" is still open. Close it with try-with-resources before opening \""
                + contractId
                + "\".");
      }
      if (interceptor.isActive()) {
        OPEN.remove(interceptor, contractId);
        throw new IllegalStateException(
            "Another query capture is already running on this interceptor; scoped contracts run"
                + " one at a time");
      }
      this.contractId = contractId;
      this.claim = QueryCaptureSession.claimUnboundWork();
      interceptor.start();
    }

    private QueryCaptureSnapshot finish() {
      try {
        awaitCompletion.run();
      } finally {
        stop();
      }
      QueryCaptureSnapshot snapshot = interceptor.snapshot();
      if (snapshot.truncated()) {
        throw new QueryContractViolation(
            "Query capture for "
                + contractId
                + " discarded "
                + snapshot.droppedCount()
                + " queries; an incomplete capture cannot verify a contract");
      }
      return snapshot;
    }

    private void abandon() {
      stop();
    }

    private void stop() {
      interceptor.stop();
      claim.close();
      OPEN.remove(interceptor, contractId);
    }
  }
}
