package io.queryaudit.junit5;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.commons.support.AnnotationSupport;

/**
 * Finds audit annotations the way JUnit registers the extension: directly present or composed, on
 * the test class, its superclasses and interfaces, then its enclosing classes. The nearest
 * declaration wins.
 */
final class AuditAnnotations {
  private AuditAnnotations() {}

  static <A extends Annotation> A onMethod(Method method, Class<A> type) {
    return AnnotationSupport.findAnnotation(method, type).orElse(null);
  }

  static <A extends Annotation> A onClass(Class<?> testClass, Class<A> type) {
    for (Class<?> scope = testClass; scope != null; scope = scope.getEnclosingClass()) {
      for (Class<?> declaring = scope;
          declaring != null && declaring != Object.class;
          declaring = declaring.getSuperclass()) {
        A found = AnnotationSupport.findAnnotation(declaring, type).orElse(null);
        if (found != null) return found;
      }
    }
    return null;
  }

  static boolean registersExtension(Class<?> testClass) {
    for (Class<?> scope = testClass; scope != null; scope = scope.getEnclosingClass()) {
      for (ExtendWith extendWith :
          AnnotationSupport.findRepeatableAnnotations(scope, ExtendWith.class)) {
        for (Class<?> registered : extendWith.value()) {
          if (registered == QueryAuditExtension.class) return true;
        }
      }
    }
    return false;
  }
}
