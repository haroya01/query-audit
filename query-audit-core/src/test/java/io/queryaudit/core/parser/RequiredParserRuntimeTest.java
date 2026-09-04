package io.queryaudit.core.parser;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.junit.jupiter.api.Test;

class RequiredParserRuntimeTest {

  @Test
  void missingParserFailsInsteadOfSelectingRegex() throws Exception {
    URL core = EnhancedSqlParser.class.getProtectionDomain().getCodeSource().getLocation();
    try (URLClassLoader loader =
        new URLClassLoader(new URL[] {core}, ClassLoader.getPlatformClassLoader())) {
      assertThatThrownBy(() -> Class.forName(EnhancedSqlParser.class.getName(), true, loader))
          .isInstanceOf(NoClassDefFoundError.class)
          .hasMessageContaining("net/sf/jsqlparser/parser/CCJSqlParserUtil");
    }
  }

  @Test
  void missingVersionMetadataFailsInsteadOfInventingAnIdentity() throws Exception {
    URL core = EnhancedSqlParser.class.getProtectionDomain().getCodeSource().getLocation();
    URL parser = CCJSqlParserUtil.class.getProtectionDomain().getCodeSource().getLocation();
    try (URLClassLoader loader =
        new URLClassLoader(new URL[] {core, parser}, ClassLoader.getPlatformClassLoader()) {
          @Override
          public InputStream getResourceAsStream(String name) {
            if (name.equals("META-INF/maven/com.github.jsqlparser/jsqlparser/pom.properties")) {
              return null;
            }
            return super.getResourceAsStream(name);
          }
        }) {
      assertThatThrownBy(() -> Class.forName(EnhancedSqlParser.class.getName(), true, loader))
          .isInstanceOf(ExceptionInInitializerError.class)
          .hasCauseInstanceOf(IllegalStateException.class)
          .hasRootCauseMessage("JSqlParser version metadata is missing");
    }
  }
}
