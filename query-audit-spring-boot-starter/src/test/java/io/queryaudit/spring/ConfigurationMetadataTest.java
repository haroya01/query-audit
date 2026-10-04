package io.queryaudit.spring;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import org.junit.jupiter.api.Test;

public class ConfigurationMetadataTest {
  @Test
  void allQueryAuditPropertiesHaveDescriptions() throws IOException {
    InputStream configurationMetadata =
        getClass().getResourceAsStream("/META-INF/spring-configuration-metadata.json");

    JsonNode configurationMetadataProperties =
        new ObjectMapper().readTree(configurationMetadata).path("properties");

    for (JsonNode jsonNode : configurationMetadataProperties) {
      if (jsonNode.path("name").textValue().startsWith("query-audit."))
        assertThat(jsonNode.path("description").textValue())
            .as("Description for property '%s'", jsonNode.path("name").asText())
            .isNotBlank();
    }
  }
}
