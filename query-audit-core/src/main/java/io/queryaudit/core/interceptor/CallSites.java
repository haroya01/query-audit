package io.queryaudit.core.interceptor;

import java.util.List;

/**
 * Recognizes generated proxy classes in captured stack frames and selects the frames that name
 * application code.
 *
 * <p>Frames use the captured {@code className.methodName:line} form; the {@code
 * className.methodName(File.java:line)} form is accepted too.
 *
 * @author haroya
 * @since 0.8.0
 */
public final class CallSites {

  private static final String HIBERNATE_PROXY = "$HibernateProxy";

  private CallSites() {}

  /**
   * Returns whether the class is a runtime-generated subclass: a Hibernate entity proxy, with or
   * without the random suffix some Hibernate versions append, or a ByteBuddy or CGLIB class.
   */
  public static boolean isGeneratedSubclass(String className) {
    return className.contains(HIBERNATE_PROXY + "$")
        || className.endsWith(HIBERNATE_PROXY)
        || className.contains("$ByteBuddy$")
        || className.contains("$$SpringCGLIB$$")
        || className.contains("$$EnhancerBySpringCGLIB$$")
        || className.contains("$$EnhancerByCGLIB$$");
  }

  /**
   * Returns whether the class is a JDK dynamic proxy. A proxy for a non-public interface, such as a
   * package-private Spring Data repository, is defined in that interface's package.
   */
  public static boolean isJdkProxy(String className) {
    if (className.startsWith("jdk.proxy") || className.startsWith("com.sun.proxy.")) {
      return true;
    }
    String simpleName = className.substring(className.lastIndexOf('.') + 1);
    return simpleName.length() > "$Proxy".length()
        && simpleName.startsWith("$Proxy")
        && simpleName.substring("$Proxy".length()).chars().allMatch(Character::isDigit);
  }

  /**
   * Returns up to {@code limit} application frames, skipping JDK, Gradle worker, and generated
   * proxy frames. Falls back to the first frame when no other frame remains.
   */
  public static List<String> applicationFrames(String stackTrace, int limit) {
    if (stackTrace == null) {
      return List.of();
    }
    List<String> frames =
        stackTrace.lines().map(String::strip).filter(line -> !line.isEmpty()).toList();
    if (frames.isEmpty()) {
      return List.of();
    }
    List<String> application =
        frames.stream().filter(CallSites::isApplicationFrame).limit(limit).toList();
    return application.isEmpty() ? List.of(frames.get(0)) : application;
  }

  /** Returns the first application frame, or {@code null} for an empty stack. */
  public static String callSite(String stackTrace) {
    List<String> frames = applicationFrames(stackTrace, 1);
    return frames.isEmpty() ? null : frames.get(0);
  }

  static boolean isApplicationFrame(String frame) {
    if (frame.startsWith("java.")
        || frame.startsWith("jdk.")
        || frame.startsWith("sun.")
        || frame.startsWith("worker.org.gradle.")) {
      return false;
    }
    String className = className(frame);
    return !isGeneratedSubclass(className) && !isJdkProxy(className);
  }

  private static String className(String frame) {
    String location = frame;
    int parenthesis = location.indexOf('(');
    if (parenthesis >= 0) {
      location = location.substring(0, parenthesis);
    }
    int colon = location.lastIndexOf(':');
    if (colon >= 0) {
      location = location.substring(0, colon);
    }
    int dot = location.lastIndexOf('.');
    return dot < 0 ? location : location.substring(0, dot);
  }
}
