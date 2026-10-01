package io.queryaudit.core.interceptor;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** Tests for {@link LazyLoadTracker}'s proxy detection and class name deproxying. */
class LazyLoadTrackerTest {

  // ====================================================================
  //  deproxyClassName tests
  // ====================================================================

  @Test
  void stopsRecordingAtTheEventLimitAndCountsWhatItDropped() {
    LazyLoadTracker tracker = new LazyLoadTracker(true, 3);
    tracker.start();
    for (int id = 0; id < 4; id++) {
      tracker.recordProxyResolved("com.example.User", id);
    }
    tracker.recordExplicitLoad("com.example.Team", 1, "Service.load:1");

    assertThat(tracker.getRecords()).hasSize(3);
    assertThat(tracker.getExplicitLoads()).isEmpty();
    assertThat(tracker.getDroppedEventCount()).isEqualTo(2);

    tracker.start();
    assertThat(tracker.getDroppedEventCount()).isZero();
  }

  @Test
  void appendingEventsAllocatesLinearly() {
    java.lang.management.ThreadMXBean threads =
        java.lang.management.ManagementFactory.getThreadMXBean();
    org.junit.jupiter.api.Assumptions.assumeTrue(
        threads instanceof com.sun.management.ThreadMXBean bean
            && bean.isThreadAllocatedMemorySupported());
    com.sun.management.ThreadMXBean bean = (com.sun.management.ThreadMXBean) threads;
    bean.setThreadAllocatedMemoryEnabled(true);

    long small = allocatedWhileRecording(bean, 10_000);
    long large = allocatedWhileRecording(bean, 20_000);

    assertThat((double) large / small).isLessThan(3.0);
  }

  private static long allocatedWhileRecording(com.sun.management.ThreadMXBean bean, int events) {
    LazyLoadTracker tracker = new LazyLoadTracker(true, events);
    tracker.start();
    long thread = Thread.currentThread().getId();
    long before = bean.getThreadAllocatedBytes(thread);
    for (int id = 0; id < events; id++) {
      tracker.recordProxyResolved("com.example.User", id);
    }
    long allocated = bean.getThreadAllocatedBytes(thread) - before;
    assertThat(tracker.getRecords()).hasSize(events);
    return allocated;
  }

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
