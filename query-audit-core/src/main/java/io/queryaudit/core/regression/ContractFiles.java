package io.queryaudit.core.regression;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public final class ContractFiles {
  private final Path location;
  private final boolean directory;
  private final Map<String, QueryCounts> contracts;
  private final Map<String, Path> owners;

  private ContractFiles(
      Path location,
      boolean directory,
      Map<String, QueryCounts> contracts,
      Map<String, Path> owners) {
    this.location = location;
    this.directory = directory;
    this.contracts = Collections.unmodifiableMap(contracts);
    this.owners = Collections.unmodifiableMap(owners);
  }

  public static ContractFiles load(Path location) {
    boolean directory = Files.isDirectory(location);
    Map<String, QueryCounts> contracts = new LinkedHashMap<>();
    Map<String, Path> owners = new LinkedHashMap<>();
    for (Path file : directory ? contractFilesIn(location) : List.of(location)) {
      for (Map.Entry<String, QueryCounts> entry : QueryCountBaseline.load(file).entrySet()) {
        Path previous = owners.putIfAbsent(entry.getKey(), file);
        if (previous != null) {
          throw new IllegalStateException(
              "Duplicate query contract " + entry.getKey() + " in " + previous + " and " + file);
        }
        contracts.put(entry.getKey(), entry.getValue());
      }
    }
    return new ContractFiles(location, directory, contracts, owners);
  }

  public Map<String, QueryCounts> contracts() {
    return contracts;
  }

  public Path location() {
    return location;
  }

  public Path fileFor(String key) {
    Path owner = owners.get(key);
    if (owner != null) return owner;
    return directory ? location.resolve(QueryContracts.DEFAULT_FILE_NAME) : location;
  }

  public static synchronized void record(Path location, Map<String, QueryCounts> updates)
      throws IOException {
    ContractFiles current = load(location);
    Map<Path, Map<String, QueryCounts>> byFile = new LinkedHashMap<>();
    for (Map.Entry<String, QueryCounts> update : updates.entrySet()) {
      Path file = current.fileFor(update.getKey());
      byFile
          .computeIfAbsent(file, path -> new LinkedHashMap<>(QueryCountBaseline.load(path)))
          .put(update.getKey(), update.getValue());
    }
    for (Map.Entry<Path, Map<String, QueryCounts>> file : byFile.entrySet()) {
      QueryCountBaseline.save(file.getKey(), file.getValue(), "QueryAudit Query Contracts");
    }
  }

  private static List<Path> contractFilesIn(Path directory) {
    try (Stream<Path> listing = Files.list(directory)) {
      return listing
          .filter(Files::isRegularFile)
          .filter(
              path -> {
                String name = path.getFileName().toString();
                return name.endsWith(".contracts") || name.equals(QueryContracts.DEFAULT_FILE_NAME);
              })
          .sorted()
          .toList();
    } catch (IOException failure) {
      throw new UncheckedIOException("Cannot read query contracts in " + directory, failure);
    }
  }
}
