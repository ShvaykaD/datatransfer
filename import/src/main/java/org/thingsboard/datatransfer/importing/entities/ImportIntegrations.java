package org.thingsboard.datatransfer.importing.entities;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.importing.LoadContext;
import org.thingsboard.server.common.data.id.IntegrationId;
import org.thingsboard.server.common.data.integration.Integration;
import org.thingsboard.server.common.data.integration.IntegrationType;

import java.util.Optional;

@Slf4j
public class ImportIntegrations extends ImportEntity {

    private final RestClient tbRestClient;
    private final boolean emptyDb;


    public ImportIntegrations(RestClient tbRestClient, ObjectMapper mapper, String basePath, boolean emptyDb) {
        super(mapper, basePath);
        this.tbRestClient = tbRestClient;
        this.emptyDb = emptyDb;
    }

    public void saveIntegrations(LoadContext loadContext) {
        JsonNode integrationsNode = readFileContentToNode("Integrations.json");
        if (integrationsNode != null) {
            for (JsonNode integrationNode : integrationsNode) {
                loadContext.getIntegrationIdMap().put(integrationNode.get("id").get("id").asText(),
                        getIntegrationId(loadContext, integrationNode));
            }
        }
    }

    private IntegrationId getIntegrationId(LoadContext loadContext, JsonNode integrationNode) {
        IntegrationId integrationId;
        if (emptyDb) {
            integrationId = createIntegration(loadContext, integrationNode).getId();
        } else {
            Optional<JsonNode> integrationOptional = tbRestClient.getIntegrationByRoutingKey(integrationNode.get("routingKey").asText());
            integrationId = integrationOptional.map(jsonNode -> IntegrationId.fromString(jsonNode.get("id").get("id").asText()))
                    .orElseGet(() -> createIntegration(loadContext, integrationNode).getId());
        }
        return integrationId;
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
