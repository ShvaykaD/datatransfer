package org.thingsboard.datatransfer.importing.entities;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.importing.LoadContext;
import org.thingsboard.server.common.data.id.IntegrationId;
import org.thingsboard.server.common.data.integration.Integration;
import org.thingsboard.server.common.data.integration.IntegrationType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;

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
        JsonNode integrationsNode = null;
        try {
            integrationsNode = mapper.readTree(new String(Files.readAllBytes(Paths.get(basePath + "Integrations.json"))));
        } catch (IOException e) {
            log.warn("Could not read integrations file");
        }
        if (integrationsNode != null) {
            for (JsonNode integrationNode : integrationsNode) {

                IntegrationId integrationId;
                if (emptyDb) {
                    integrationId = createIntegration(loadContext, integrationNode).getId();
                } else {
                    Optional<JsonNode> integrationOptional = tbRestClient.getIntegrationByRoutingKey(integrationNode.get("routingKey").asText());
                    integrationId = integrationOptional.map(jsonNode -> IntegrationId.fromString(jsonNode.get("id").get("id").asText()))
                            .orElseGet(() -> createIntegration(loadContext, integrationNode).getId());
                }
                loadContext.getIntegrationIdMap().put(integrationNode.get("id").get("id").asText(), integrationId);
            }
        }
    }

    private Integration createIntegration(LoadContext loadContext, JsonNode node) {
        Integration integration = new Integration();
        integration.setName(node.get("name").asText());
        integration.setRoutingKey(node.get("routingKey").asText());
        integration.setDefaultConverterId(loadContext.getConverterIdMap().get(node.get("defaultConverterId").get("id").asText()));
        integration.setType(IntegrationType.valueOf(node.get("type").asText()));
        if (!node.get("configuration").isNull()) {
            integration.setConfiguration(node.get("configuration"));
        }
        if (!node.get("downlinkConverterId").isNull()) {
            integration.setDownlinkConverterId(loadContext.getConverterIdMap().get(node.get("downlinkConverterId").get("id").asText()));
        }
        return tbRestClient.createIntegration(integration);
    }


}
