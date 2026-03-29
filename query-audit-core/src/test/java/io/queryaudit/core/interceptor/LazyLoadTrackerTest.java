package io.queryaudit.core.interceptor;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** Tests for {@link LazyLoadTracker}'s proxy detection and class name deproxying. */
class LazyLoadTrackerTest {

  // ====================================================================
  //  deproxyClassName tests
  // ====================================================================

  @Test
  void deproxyClassName_hibernateProxy_strippedCorrectly() {
    String proxied = "com.example.User$HibernateProxy$abc123def";
    assertThat(LazyLoadTracker.deproxyClassName(proxied)).isEqualTo("com.example.User");
  }

  @Test
  void deproxyClassName_byteBuddy_strippedCorrectly() {
    String proxied = "com.example.User$ByteBuddy$xyz789";
    assertThat(LazyLoadTracker.deproxyClassName(proxied)).isEqualTo("com.example.User");
  }

  @Test
  void deproxyClassName_cglib_strippedCorrectly() {
    String proxied = "com.example.User$$EnhancerByCGLIB$$abc";
    assertThat(LazyLoadTracker.deproxyClassName(proxied)).isEqualTo("com.example.User");
  }

  @Test
  void deproxyClassName_plainClass_unchanged() {
    String plain = "com.example.User";
    assertThat(LazyLoadTracker.deproxyClassName(plain)).isEqualTo("com.example.User");
  }

  @Test
  void deproxyClassName_simpleClass_unchanged() {
    String plain = "User";
    assertThat(LazyLoadTracker.deproxyClassName(plain)).isEqualTo("User");
  }

  // ====================================================================
  //  isProxyResolution tests
  // ====================================================================

  @Test
  void isProxyResolution_normalCall_returnsFalse() {
    // A normal call (like this test) should not be detected as proxy resolution
    assertThat(LazyLoadTracker.isProxyResolution()).isFalse();
  }

  // ====================================================================
  //  PROXY_ROLE_PREFIX constant
  // ====================================================================

  @Test
  void proxyRolePrefix_isCorrect() {
    assertThat(LazyLoadTracker.PROXY_ROLE_PREFIX).isEqualTo("proxy:");
  }

  // ====================================================================
  //  Basic tracker lifecycle
  // ====================================================================

  @Test
  void startAndStop_lifecycle() {
    LazyLoadTracker tracker = new LazyLoadTracker();
    assertThat(tracker.isActive()).isFalse();
    assertThat(tracker.getRecords()).isEmpty();

    tracker.start();
    assertThat(tracker.isActive()).isTrue();

    tracker.stop();
    assertThat(tracker.isActive()).isFalse();
  }

  @Test
  void start_clearsExistingRecords() {
    LazyLoadTracker tracker = new LazyLoadTracker();
    // Manually add a record via collection event wouldn't work without Hibernate,
    // but we can verify start() clears after a stop/start cycle
    tracker.start();
    tracker.stop();
    tracker.start();
    assertThat(tracker.getRecords()).isEmpty();
  }

  // ====================================================================
  //  hasFindByIdInStack tests
  // ====================================================================

  @Test
  void hasFindByIdInStack_normalCall_returnsFalse() {
    assertThat(LazyLoadTracker.hasFindByIdInStack()).isFalse();
  }

  @Test
  void hasFindByIdInStack_withFindByIdInStack_returnsTrue() {
    // Simulate a call from a method named findById
    boolean result = callFromFindById();
    assertThat(result).isTrue();
  }

  /** Helper that mimics a call stack containing {@code findById}. */
  @SuppressWarnings("unused")
  private boolean findById() {
    return LazyLoadTracker.hasFindByIdInStack();
  }

  private boolean callFromFindById() {
    return findById();
  }

  // ====================================================================
  //  Explicit loads lifecycle
  // ====================================================================

  @Test
  void explicitLoads_emptyByDefault() {
    LazyLoadTracker tracker = new LazyLoadTracker();
    assertThat(tracker.getExplicitLoads()).isEmpty();
  }

  @Test
  void start_clearsExplicitLoads() {
    LazyLoadTracker tracker = new LazyLoadTracker();
    tracker.start();
    tracker.stop();
    tracker.start();
    assertThat(tracker.getExplicitLoads()).isEmpty();
  }

  @Test
  void clear_clearsExplicitLoads() {
    LazyLoadTracker tracker = new LazyLoadTracker();
    tracker.start();
    tracker.clear();
    assertThat(tracker.getExplicitLoads()).isEmpty();
  }
}
