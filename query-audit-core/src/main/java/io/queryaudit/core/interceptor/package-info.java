/**
 * JDBC capture internals.
 *
 * <p>Only {@link io.queryaudit.core.interceptor.QueryCaptureSession} is supported; users call it to
 * attribute background work to the audited test. The remaining public types here, including {@code
 * QueryInterceptor}, {@code DataSourceProxyFactory}, {@code LazyLoadTracker}, and {@code
 * QueryCaptureSnapshot}, are host internals whose signatures may change in any release.
 *
 * @see io.queryaudit.core.interceptor.QueryCaptureSession
 */
package io.queryaudit.core.interceptor;
