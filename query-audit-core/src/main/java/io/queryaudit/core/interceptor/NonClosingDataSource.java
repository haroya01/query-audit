package io.queryaudit.core.interceptor;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;
import javax.sql.DataSource;

/**
 * Delegating {@link DataSource} that intentionally does <em>not</em> implement {@link
 * java.io.Closeable} or {@link AutoCloseable}.
 *
 * <p>Spring infers a destroy method on a bean when the bean's runtime type exposes {@code close()}
 * via {@code Closeable}/{@code AutoCloseable}. When the auto-configured {@code BeanPostProcessor}
 * returns a raw {@code ProxyDataSource} (which implements {@code Closeable}), Spring registers it
 * as the lifecycle target and, on context shutdown, calls {@code close()} on the proxy — which
 * cascades to {@code close()} on the underlying {@code HikariDataSource}. Under Spring Boot 4.x
 * with the test-context cache evicting contexts, this cascade has been observed to close the pool
 * of a still-cached sibling context (see GitHub issue #153 for the full analysis).
 *
 * <p>Wrapping the proxy once more in this non-{@code Closeable} decorator removes the destroy
 * inference: Spring leaves the underlying {@code HikariDataSource} as the lifecycle target — which
 * already has {@code @Bean(destroyMethod = "close")} declared by {@code
 * DataSourceAutoConfiguration} — and the cascade goes away. Query interception still works because
 * every {@link #getConnection()} call goes through the wrapped {@code ProxyDataSource}.
 *
 * <p>JDBC unwrap is preserved so callers that need direct access to the underlying
 * {@code ProxyDataSource} or {@code HikariDataSource} can still {@code unwrap()} them.
 *
 * @author haroya
 * @since 0.3.2
 */
public final class NonClosingDataSource implements DataSource {

  private final DataSource delegate;

  public NonClosingDataSource(DataSource delegate) {
    this.delegate = delegate;
  }

  /** Returns the wrapped data source (typically a {@code ProxyDataSource}). */
  public DataSource getDelegate() {
    return delegate;
  }

  @Override
  public Connection getConnection() throws SQLException {
    return delegate.getConnection();
  }

  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    return delegate.getConnection(username, password);
  }

  @Override
  public PrintWriter getLogWriter() throws SQLException {
    return delegate.getLogWriter();
  }

  @Override
  public void setLogWriter(PrintWriter out) throws SQLException {
    delegate.setLogWriter(out);
  }

  @Override
  public void setLoginTimeout(int seconds) throws SQLException {
    delegate.setLoginTimeout(seconds);
  }

  @Override
  public int getLoginTimeout() throws SQLException {
    return delegate.getLoginTimeout();
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return delegate.getParentLogger();
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isInstance(this)) {
      return iface.cast(this);
    }
    if (iface.isInstance(delegate)) {
      return iface.cast(delegate);
    }
    return delegate.unwrap(iface);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isInstance(this) || iface.isInstance(delegate) || delegate.isWrapperFor(iface);
  }
}
