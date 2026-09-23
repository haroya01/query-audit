package io.queryaudit.core.parser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class SqlParserRoutingTest {
  @Test
  void nullNeverCallsEitherBackendAndRetainsMutableEmptyListContract() {
    List<String> first =
        SqlParserRouting.list(
            null,
            ignored -> {
              throw new AssertionError();
            },
            ignored -> {
              throw new AssertionError();
            });
    first.add("caller-owned");
    assertThat(
            SqlParserRouting.<String>list(
                null, ignored -> List.of("bad"), ignored -> List.of("bad")))
        .isEmpty();
    assertThat(
            SqlParserRouting.text(
                null,
                ignored -> {
                  throw new AssertionError();
                },
                ignored -> {
                  throw new AssertionError();
                }))
        .isNull();
    assertThat(EnhancedSqlParser.removeSubqueries(null)).isNull();
  }

  @Test
  void exactLengthLimitUsesAstAndOnlyLargerSqlSkipsIt() {
    AtomicInteger astCalls = new AtomicInteger();
    AtomicInteger fallbackCalls = new AtomicInteger();
    SqlParserRouting.Extraction<String> ast =
        sql -> {
          astCalls.incrementAndGet();
          return "ast";
        };
    Function<String, String> fallback =
        sql -> {
          fallbackCalls.incrementAndGet();
          return "fallback";
        };
    assertThat(SqlParserRouting.text("x".repeat(10_000), ast, fallback)).isEqualTo("ast");
    assertThat(SqlParserRouting.text("x".repeat(10_001), ast, fallback)).isEqualTo("fallback");
    assertThat(astCalls).hasValue(1);
    assertThat(fallbackCalls).hasValue(1);
  }

  @Test
  void successfulEmptyOrNullExtractionDoesNotFallBack() {
    assertThat(
            SqlParserRouting.list(
                "SQL",
                ignored -> List.of(),
                ignored -> {
                  throw new AssertionError();
                }))
        .isEmpty();
    assertThat(
            SqlParserRouting.text(
                "SQL",
                ignored -> null,
                ignored -> {
                  throw new AssertionError();
                }))
        .isNull();
  }

  static List<Throwable> recoverableFailures() {
    return List.of(
        new Exception("unsupported SQL"),
        new IllegalArgumentException("AST shape"),
        new StackOverflowError("deep expression"),
        SqlParserRouting.unsupportedStatement());
  }

  @ParameterizedTest
  @MethodSource("recoverableFailures")
  void preservesHistoricalRecoverableSqlAndTraversalFailureBoundary(Throwable failure) {
    AtomicInteger calls = new AtomicInteger();
    assertThat(
            SqlParserRouting.text(
                "SQL",
                ignored -> {
                  if (failure instanceof Exception exception) throw exception;
                  throw (Error) failure;
                },
                sql -> {
                  calls.incrementAndGet();
                  return "fallback:" + sql;
                }))
        .isEqualTo("fallback:SQL");
    assertThat(calls).hasValue(1);
  }

  static List<Error> fatalFailures() {
    return List.of(
        new NoClassDefFoundError("missing parser"),
        new NoSuchMethodError("incompatible parser"),
        new ExceptionInInitializerError("broken runtime"),
        new AssertionError("internal invariant"),
        new OutOfMemoryError("VM failure"));
  }

  @ParameterizedTest
  @MethodSource("fatalFailures")
  void brokenRuntimeAndVmFailuresAreNotDisguisedAsRegexSuccess(Error failure) {
    assertThatThrownBy(
            () ->
                SqlParserRouting.text(
                    "SQL",
                    ignored -> {
                      throw failure;
                    },
                    ignored -> {
                      throw new AssertionError("fallback must not execute");
                    }))
        .isSameAs(failure);
  }

  @Test
  void fallbackFailureIsNotRetriedOrSuppressed() {
    IllegalStateException failure = new IllegalStateException("fallback failed");
    assertThatThrownBy(
            () ->
                SqlParserRouting.text(
                    "SQL",
                    ignored -> {
                      throw new Exception("unsupported");
                    },
                    ignored -> {
                      throw failure;
                    }))
        .isSameAs(failure);
  }

  @Test
  void unsupportedStatementShapesUseTheSameFallbackAsParseFailures() {
    String update = "UPDATE users SET flag=1 WHERE id=2";
    assertThat(EnhancedSqlParser.extractWhereColumns(update))
        .isEqualTo(SqlParser.extractWhereColumns(update));
    String malformed = "SELECT id FROM users WHERE id = 1 @unsupported";
    assertThat(EnhancedSqlParser.extractWhereColumns(malformed))
        .isEqualTo(SqlParser.extractWhereColumns(malformed));
    // Rejected SQL may be cached, but that cannot change the public fallback result.
    assertThat(EnhancedSqlParser.extractWhereColumns(malformed))
        .isEqualTo(SqlParser.extractWhereColumns(malformed));
  }

  @Test
  void nestedSelectFastPathRetainsTheExactOriginalString() {
    String sql = new String("  select lower(name)  from users -- unchanged\n");
    assertThat(EnhancedSqlParser.removeSubqueries(sql)).isSameAs(sql);
    assertThatThrownBy(() -> SqlParser.removeSubqueries(null))
        .isInstanceOf(NullPointerException.class);
  }
}
