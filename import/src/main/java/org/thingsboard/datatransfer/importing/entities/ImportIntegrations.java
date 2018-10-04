package org.thingsboard.datatransfer.importing.entities;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.importing.LoadContext;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.converter.Converter;
import org.thingsboard.server.common.data.converter.ConverterType;
import org.thingsboard.server.common.data.id.ConverterId;
import org.thingsboard.server.common.data.id.IntegrationId;
import org.thingsboard.server.common.data.integration.Integration;
import org.thingsboard.server.common.data.integration.IntegrationType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Slf4j
public class ImportIntegrations {

    private final ObjectMapper mapper;
    private final RestClient tbRestClient;
    private final String basePath;
    private final boolean emptyDb;


    public ImportIntegrations(RestClient tbRestClient, ObjectMapper mapper, String basePath, boolean emptyDb) {
        this.tbRestClient = tbRestClient;
        this.mapper = mapper;
        this.basePath = basePath;
        this.emptyDb = emptyDb;
    }

    public void saveIntegrations(LoadContext loadContext) {
        JsonNode jsonNode = null;
        try {
            jsonNode = mapper.readTree(new String(Files.readAllBytes(Paths.get(basePath + "Integrations.json"))));
        } catch (IOException e) {
            log.warn("Could not read integrations file");
        }
        if (jsonNode != null) {
            for (JsonNode node : jsonNode) {
                if (!emptyDb) {
                    Optional<JsonNode> integrationOptional = tbRestClient.getIntegrationByRoutingKey(node.get("routingKey").asText());
                    integrationOptional.ifPresent(integration -> tbRestClient.deleteIntegration(IntegrationId.fromString(integration.get("id").get("id").asText())));
                }
                Integration integration = new Integration();
                integration.setName(node.get("name").asText());
                integration.setRoutingKey(node.get("routingKey").asText());
                integration.setDefaultConverterId(loadContext.getConverterIdMap().get(node.get("defaultConverterId").get("id").asText()));
                integration.setType(IntegrationType.valueOf(node.get("type").asText()));
                if (!node.get("configuration").isNull()) {
                    JsonNode configurationNode = node.get("configuration");
                    integration.setConfiguration(configurationNode);
                }
                if(!node.get("downlinkConverterId").isNull()){
                    integration.setDownlinkConverterId(loadContext.getConverterIdMap().get(node.get("downlinkConverterId").get("id").asText()));
                }
                Integration savedIntagration = tbRestClient.createIntegration(integration);
                loadContext.getIntegrationIdMap().put(node.get("id").get("id").asText(), savedIntagration.getId());
            }
        }
    }


}
