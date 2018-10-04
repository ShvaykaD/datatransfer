package org.thingsboard.datatransfer.exporting.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.exporting.SaveContext;
import org.thingsboard.server.common.data.id.ConverterId;
import org.thingsboard.server.common.data.id.IntegrationId;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Slf4j
public class ExportIntegrations extends ExportEntity {

    public ExportIntegrations(RestClient tbRestClient, ObjectMapper mapper, String basePath) {
        super(tbRestClient, mapper, basePath, true);
    }

    public void getIntegrations(SaveContext saveContext, int limit) {
        Optional<JsonNode> integrationsOptional = tbRestClient.findIntegrations(limit);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(basePath + "Integrations.json")))) {
            if (integrationsOptional.isPresent()) {
                ArrayNode integrationArray = (ArrayNode) integrationsOptional.get().get("data");
                String strFromType = "INTEGRATION";
                for (JsonNode integrationNode : integrationArray) {
                    String strIntegrationId = integrationNode.get("id").get("id").asText();
                    String strConverterId = integrationNode.get("defaultConverterId").get("id").asText();
                    ArrayNode integrationIdsNode;
                    if (saveContext.getRelatedIntegrationsToConverterNode().has(strConverterId)) {
                        integrationIdsNode = (ArrayNode) saveContext.getRelatedIntegrationsToConverterNode().get(strConverterId);
                        integrationIdsNode.add(strIntegrationId);
                    } else {
                        integrationIdsNode = mapper.createArrayNode();
                        integrationIdsNode.add(strIntegrationId);
                        saveContext.getRelatedIntegrationsToConverterNode().set(strConverterId, integrationIdsNode);
                    }
                    addRelationToNode(saveContext.getRelationsArray(), strIntegrationId, strFromType);

                    StringBuilder telemetryKeys = getTelemetryKeys(strFromType, strIntegrationId);

                    if (telemetryKeys != null && telemetryKeys.length() != 0) {
                        Optional<JsonNode> telemetryNodeOptional = tbRestClient.getTelemetry(strFromType, strIntegrationId,
                                telemetryKeys.toString(), limit, 0L, System.currentTimeMillis());
                        telemetryNodeOptional.ifPresent(jsonNode ->
                                saveContext.getTelemetryArray().add(createNode(strFromType, strIntegrationId, jsonNode, "telemetry")));
                    }
                    ObjectNode attributesNode = getAttributes(strFromType, strIntegrationId);
                    if (attributesNode != null) {
                        saveContext.getAttributesArray().add(attributesNode);
                    }
                }
                writer.write(mapper.writeValueAsString(integrationArray));
            }
        } catch (IOException e) {
            log.warn("Could not export devices to file.");
        }
    }

}
