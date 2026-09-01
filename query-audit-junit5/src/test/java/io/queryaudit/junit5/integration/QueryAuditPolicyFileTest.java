package io.queryaudit.junit5.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import io.queryaudit.junit5.QueryAudit;
import io.queryaudit.junit5.QueryAuditDataSourceStore;
import io.queryaudit.junit5.QueryAuditExtension;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import javax.sql.DataSource;
import net.ttddyy.dsproxy.support.ProxyDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
class QueryAuditPolicyFileTest {

  @TempDir Path tempDir;

  private String countBaselinePath;
  private String contractsPath;

  @BeforeEach
  void setUp() {
    countBaselinePath = System.getProperty("queryAudit.countBaselinePath");
    contractsPath = System.getProperty("queryAudit.contractsPath");
    System.clearProperty("queryAudit.countBaselinePath");
    System.clearProperty("queryAudit.contractsPath");
    AuditedFixture.dataSource = new ProxyDataSource(mock(DataSource.class));
    QueryAuditDataSourceStore.clear();
  }

  @AfterEach
  void tearDown() {
    restoreProperty("queryAudit.countBaselinePath", countBaselinePath);
    restoreProperty("queryAudit.contractsPath", contractsPath);
    AuditedFixture.dataSource = null;
    QueryAuditDataSourceStore.clear();
  }

  @Test
  void malformedCountBaselineStopsAuditInitialization() throws Exception {
    Path file = tempDir.resolve(".query-audit-counts");
    Files.writeString(file, "OrderServiceTest | loadsOrders | invalid | 0 | 0 | 0 | 1\n");
    System.setProperty("queryAudit.countBaselinePath", file.toString());

    assertInvalidPolicyStopsInitialization(file);
  }

  @Test
  void malformedContractsFileStopsAuditInitialization() throws Exception {
    Path file = tempDir.resolve(".query-audit-contracts");
    Files.writeString(file, "OrderServiceTest | loadsOrders | 1 | 0 | 0\n");
    System.setProperty("queryAudit.contractsPath", file.toString());

    assertInvalidPolicyStopsInitialization(file);
  }

  private static void assertInvalidPolicyStopsInitialization(Path file) throws Exception {
    int listenersBefore = listenerCount();
    ExtensionContext context = new StubWithMethod(AuditedFixture.class, "audited");

    assertThatThrownBy(() -> new QueryAuditExtension().beforeEach(context))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(file.toAbsolutePath().toString())
        .hasMessageContaining("line 1");

    assertThat(listenerCount()).isEqualTo(listenersBefore);
    assertThat(QueryAuditDataSourceStore.get()).isNull();
  }

  private static int listenerCount() {
    return AuditedFixture.dataSource.getProxyConfig().getQueryListener().getListeners().size();
  }

  private static void restoreProperty(String key, String value) {
    if (value == null) {
      System.clearProperty(key);
    } else {
      System.setProperty(key, value);
    }
  }

  @QueryAudit
  static class AuditedFixture {
    static ProxyDataSource dataSource;

    void audited() {}
  }

  private static final class StubWithMethod extends StubExtensionContext {
    private final Method method;

    StubWithMethod(Class<?> testClass, String methodName) throws NoSuchMethodException {
      super(testClass);
      method = testClass.getDeclaredMethod(methodName);
    }

    @Override
    public Optional<Method> getTestMethod() {
      return Optional.of(method);
    }
  }
}
