package org.thingsboard.datatransfer.exporting.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.exporting.Export;
import org.thingsboard.datatransfer.exporting.SaveContext;

import java.util.Optional;
import java.util.UUID;

@Slf4j
public class ExportEntity {

    final RestClient tbRestClient;
    final ObjectMapper mapper;
    final String basePath;

    public ExportEntity(RestClient tbRestClient, ObjectMapper mapper, String basePath) {
        this.tbRestClient = tbRestClient;
        this.mapper = mapper;
        this.basePath = basePath;
    }

    JsonNode getRelationsFromEntity(String strEntityId, String strFromType) {
        Optional<JsonNode> relationOptional = tbRestClient.getRelationByFrom(strEntityId, strFromType);
        if (relationOptional.isPresent()) {
            JsonNode node = relationOptional.get();
            if (node.isArray() && node.size() != 0) {
                return node;
            }
        }
        return null;
    }

    String getTelemetryKeys(String strFromType, String strEntityId) {
        Optional<JsonNode> telemetryKeysOptional = tbRestClient.getTelemetryKeys(strFromType, strEntityId);
        if (telemetryKeysOptional.isPresent()) {
            JsonNode telemetryKeysNode = telemetryKeysOptional.get();

            StringBuilder keys = new StringBuilder();
            int i = 1;
            for (JsonNode node : telemetryKeysNode) {
                keys.append(node.asText());
                if (telemetryKeysNode.has(i)) {
                    keys.append(",");
                }
                i++;
            }
            return keys.toString();
        }
        return null;
    }

    ObjectNode getAttributes(String strFromType, String strEntityId) {
        Optional<JsonNode> attributesOptional = tbRestClient.getAttributes(strFromType, strEntityId);
        if (attributesOptional.isPresent()) {
            JsonNode jsonNode = attributesOptional.get();
            ObjectNode savedNode = createNode(strFromType, strEntityId, jsonNode, "attributes");
            Optional<JsonNode> attributesKeysByScopeOptional = tbRestClient.getAttributesKeysByScope(strFromType, strEntityId, "SERVER_SCOPE");
            attributesKeysByScopeOptional.ifPresent(node -> savedNode.set("attributeKeys", node));
            return savedNode;
        }
        return null;
    }

    private ObjectNode createNode(String strFromType, String strEntityId, JsonNode node, String dataType) {
        ObjectNode resultNode = mapper.createObjectNode();
        resultNode.put("entityType", strFromType);
        resultNode.put("entityId", strEntityId);
        resultNode.set(dataType, node);
        return resultNode;
    }

    void processEntityNodes(SaveContext saveContext, int limit, JsonNode entityArray, String strFromType) {
        for (JsonNode node : entityArray) {
            String strEntityId = node.get("id").get("id").asText();

            JsonNode relationsFromEntityNode = getRelationsFromEntity(strEntityId, strFromType);
            if (relationsFromEntityNode != null) {
                saveContext.getRelationsArray().add(relationsFromEntityNode);
            }

            //getTelemetry(saveContext, limit, strFromType, strEntityId);
            test(saveContext, strFromType, strEntityId);


            ObjectNode attributesNode = getAttributes(strFromType, strEntityId);
            if (attributesNode != null) {
                saveContext.getAttributesArray().add(attributesNode);
            }
        }
    }

    void getTelemetry(SaveContext saveContext, int limit, String strFromType, String strEntityId) {
        String telemetryKeys = getTelemetryKeys(strFromType, strEntityId);
        if (telemetryKeys != null && telemetryKeys.length() != 0) {
            Optional<JsonNode> telemetryNodeOptional = tbRestClient.getTelemetry(strFromType, strEntityId,
                    telemetryKeys, limit, 0L, System.currentTimeMillis());
            telemetryNodeOptional.ifPresent(jsonNode ->
                    saveContext.getTelemetryArray().add(createNode(strFromType, strEntityId, jsonNode, "telemetry")));
        }
    }

    void test(SaveContext saveContext, String strFromType, String strEntityId) {
        String telemetryKeys = getTelemetryKeys(strFromType, strEntityId);
        if (telemetryKeys != null && telemetryKeys.length() != 0) {






            long startTs = 1420070400000L;
            long endTs = startTs + 2678400000L; // 1 month


            do {

                Optional<JsonNode> telemetryNodeOptional = tbRestClient.getTelemetry(strFromType, strEntityId,
                        telemetryKeys, 2147483647, startTs, endTs);
                if (telemetryNodeOptional.isPresent()) {
                    JsonNode jsonNode = telemetryNodeOptional.get();
                    if (jsonNode.size() > 0) {
                        saveContext.getTelemetryArray().add(createNode(strFromType, strEntityId, jsonNode, "telemetry"));
                    }
                }

                if (saveContext.getTelemetryArray().size() == 50) {
                    Export.writeToFile(UUID.randomUUID() + "_Telemetry.json", saveContext.getTelemetryArray());
                    saveContext.setTelemetryArray(mapper.createArrayNode());
                }


                startTs = endTs;
                endTs = startTs + 2678400000L;

            } while (endTs - System.currentTimeMillis() < 2678400000L);


            if (saveContext.getTelemetryArray().size() > 0) {
                Export.writeToFile(UUID.randomUUID() + "_Telemetry.json", saveContext.getTelemetryArray());
            }

        }
    }

}
