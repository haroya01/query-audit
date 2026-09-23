package io.queryaudit.core.contract;

import io.queryaudit.core.interceptor.QueryInterceptor;
import io.queryaudit.core.regression.QueryContracts;
import io.queryaudit.core.regression.QueryCountBaseline;
import io.queryaudit.core.regression.QueryCounts;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

public final class QueryContractScope {
  public static final String RECORD_PROPERTY = "queryAudit.contracts.record";

  private final QueryInterceptor interceptor;
  private final Path directory;
  private final Runnable awaitCompletion;

  private QueryContractScope(
      QueryInterceptor interceptor, Path directory, Runnable awaitCompletion) {
    this.interceptor = Objects.requireNonNull(interceptor, "interceptor");
    this.directory = Objects.requireNonNull(directory, "directory");
    this.awaitCompletion = Objects.requireNonNull(awaitCompletion, "awaitCompletion");
  }

  public static QueryContractScope forDirectory(QueryInterceptor interceptor, Path directory) {
    return new QueryContractScope(interceptor, directory, () -> {});
  }

  public QueryContractScope awaitingCompletion(Runnable awaitCompletion) {
    return new QueryContractScope(interceptor, directory, awaitCompletion);
  }

  public <T> T verify(String contractId, Callable<T> work) throws Exception {
    CapturedQueries<T> captured = capture(contractId, work);
    verify(captured);
    return captured.value();
  }

  public <T> CapturedQueries<T> capture(String contractId, Callable<T> work) throws Exception {
    if (contractId == null || contractId.isBlank()) {
      throw new IllegalArgumentException("contractId must not be blank");
    }
    Objects.requireNonNull(work, "work");
    if (interceptor.isActive()) {
      throw new IllegalStateException(
          "Another query capture is already running; scoped contracts run one at a time");
    }
    T value;
    interceptor.start();
    try {
      value = work.call();
      awaitCompletion.run();
    } finally {
      interceptor.stop();
    }
    CapturedQueries<T> captured = new CapturedQueries<>(contractId, value, interceptor.snapshot());
    if (captured.snapshot().truncated()) {
      throw new AssertionError(
          "Query capture for "
              + contractId
              + " discarded "
              + captured.snapshot().droppedCount()
              + " queries; an incomplete capture cannot verify a contract");
    }
    return captured;
  }

  public void verify(CapturedQueries<?> captured) {
    Objects.requireNonNull(captured, "captured");
    String key = QueryCountBaseline.key(captured.contractId());
    Map<Path, Map<String, QueryCounts>> files = loadFiles();
    if (Boolean.getBoolean(RECORD_PROPERTY)) {
      record(files, key, captured.counts());
      return;
    }
    Map<String, QueryCounts> contracts = new LinkedHashMap<>();
    files.values().forEach(contracts::putAll);
    if (!contracts.containsKey(key)) {
      throw new AssertionError(
          "Missing query contract: "
              + captured.contractId()
              + " in "
              + directory
              + ". Record it with -D"
              + RECORD_PROPERTY
              + "=true and review the file diff.");
    }
    String failure =
        QueryContracts.verify(
            captured.contractId(),
            captured.contractId(),
            captured.contractId(),
            captured.counts(),
            contracts,
            captured.queries());
    if (failure != null) {
      throw new AssertionError(failure);
    }
  }

  private void record(Map<Path, Map<String, QueryCounts>> files, String key, QueryCounts counts) {
    Path target =
        files.entrySet().stream()
            .filter(entry -> entry.getValue().containsKey(key))
            .map(Map.Entry::getKey)
            .findFirst()
            .orElse(directory.resolve(QueryContracts.DEFAULT_FILE_NAME));
    Map<String, QueryCounts> updated = new LinkedHashMap<>(files.getOrDefault(target, Map.of()));
    updated.put(key, counts);
    try {
      QueryCountBaseline.save(target, updated, "QueryAudit Query Contracts");
    } catch (IOException failure) {
      throw new UncheckedIOException("Cannot record query contract in " + target, failure);
    }
  }

  private Map<Path, Map<String, QueryCounts>> loadFiles() {
    Map<Path, Map<String, QueryCounts>> files = new LinkedHashMap<>();
    if (!Files.isDirectory(directory)) {
      return files;
    }
    List<Path> paths;
    try (Stream<Path> listing = Files.list(directory)) {
      paths = listing.filter(QueryContractScope::isContractFile).sorted().toList();
    } catch (IOException failure) {
      throw new UncheckedIOException("Cannot read query contracts in " + directory, failure);
    }
    Map<String, Path> owners = new LinkedHashMap<>();
    for (Path path : paths) {
      Map<String, QueryCounts> contracts = QueryCountBaseline.load(path);
      for (String key : contracts.keySet()) {
        Path previous = owners.putIfAbsent(key, path);
        if (previous != null) {
          throw new IllegalStateException(
              "Duplicate query contract " + key + " in " + previous + " and " + path);
        }
      }
      files.put(path, contracts);
    }
    return files;
  }

  private static boolean isContractFile(Path path) {
    String name = path.getFileName().toString();
    return Files.isRegularFile(path)
        && (name.endsWith(".contracts") || name.equals(QueryContracts.DEFAULT_FILE_NAME));
  }
}
