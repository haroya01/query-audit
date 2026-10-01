package io.queryaudit.spring;

import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

final class AwaitExecutorsDeclared implements Condition {
  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    return Binder.get(context.getEnvironment())
        .bind("query-audit.await-executors", Bindable.listOf(String.class))
        .map(executors -> !executors.isEmpty())
        .orElse(false);
  }
}
