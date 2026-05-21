package io.queryaudit.core.interceptor;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.Closeable;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import javax.sql.DataSource;
import net.ttddyy.dsproxy.support.ProxyDataSource;
import org.junit.jupiter.api.Test;

/**
 * Regression coverage for issue #153 — the post-BPP {@link DataSource} returned by {@link
 * DataSourceProxyFactory#wrap} must not implement {@link Closeable}/{@link AutoCloseable} so that
 * Spring's destroy-method inference cannot register a close-cascade against it.
 */
class NonClosingDataSourceTest {

  @Test
  void wrap_returnsNonCloseableNonAutoCloseableInstance() {
    CountingCloseableDataSource underlying = new CountingCloseableDataSource();

    DataSource wrapped = DataSourceProxyFactory.wrap(underlying, new QueryInterceptor());

    assertThat(wrapped).isInstanceOf(NonClosingDataSource.class);
    assertThat(wrapped).isNotInstanceOf(Closeable.class);
    assertThat(wrapped).isNotInstanceOf(AutoCloseable.class);
  }

  @Test
  void wrap_preservesUnwrapToProxyDataSource() throws SQLException {
    CountingCloseableDataSource underlying = new CountingCloseableDataSource();

    DataSource wrapped = DataSourceProxyFactory.wrap(underlying, new QueryInterceptor());

    assertThat(wrapped.isWrapperFor(ProxyDataSource.class)).isTrue();
    assertThat(wrapped.unwrap(ProxyDataSource.class)).isInstanceOf(ProxyDataSource.class);
  }

  @Test
  void wrap_doesNotCascadeCloseToUnderlying() {
    CountingCloseableDataSource underlying = new CountingCloseableDataSource();

    DataSource wrapped = DataSourceProxyFactory.wrap(underlying, new QueryInterceptor());

    // The whole point: there is no close() to invoke on the returned instance, so a Spring
    // shutdown that walks AutoCloseable beans cannot reach the underlying. Verify symbolically
    // by asserting the underlying counter stays at zero even after we explicitly traverse the
    // chain looking for a close().
    if (wrapped instanceof AutoCloseable autoCloseable) {
      try {
        autoCloseable.close();
      } catch (Exception ignored) {
        // would have cascaded — but we asserted above this branch is unreachable
      }
    }
    assertThat(underlying.closeCount.get()).isZero();
  }

  /**
   * Minimal {@link DataSource} that implements {@link AutoCloseable} (matching {@code
   * HikariDataSource}) and counts how many times {@code close()} has been called. We only need
   * enough of the contract to flow through datasource-proxy's builder; we never actually open
   * connections in these tests.
   */
  private static final class CountingCloseableDataSource implements DataSource, AutoCloseable {
    final AtomicInteger closeCount = new AtomicInteger();

    @Override
    public void close() {
      closeCount.incrementAndGet();
    }

    @Override
    public Connection getConnection() {
      throw new UnsupportedOperationException("not exercised");
    }

    @Override
    public Connection getConnection(String username, String password) {
      throw new UnsupportedOperationException("not exercised");
    }

    @Override
    public PrintWriter getLogWriter() {
      return null;
    }

    @Override
    public void setLogWriter(PrintWriter out) {}

    @Override
    public void setLoginTimeout(int seconds) {}

    @Override
    public int getLoginTimeout() {
      return 0;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
      throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) {
      if (iface.isInstance(this)) {
        return iface.cast(this);
      }
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
      return iface.isInstance(this);
    }
  }
}
