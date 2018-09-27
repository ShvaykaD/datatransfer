package org.thingsboard.datatransfer.exporting.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.exporting.SaveContext;

import java.util.Optional;

public class ExportEntityGroups extends ExportEntity {

    public ExportEntityGroups(RestClient tbRestClient, ObjectMapper mapper, String basePath) {
        super(tbRestClient, mapper, basePath, true);
    }

    public void getEntityGroups(SaveContext saveContext, int limit) {
        for (EntityTypes entityType : EntityTypes.values()) {
            String strFromType = "ENTITY_GROUP";
            Optional<JsonNode> entityGroupsOptional = tbRestClient.getTenantEntityGroups(entityType.name());
            entityGroupsOptional.ifPresent(entityGroupsNode -> {
                for (JsonNode entityGroupNode : entityGroupsNode) {
                    saveContext.getEntityGroups().add(entityGroupNode);
                    String strEntityId = entityGroupNode.get("id").get("id").asText();
                    if (!entityGroupNode.get("name").asText().equals("All")) {
                        Optional<JsonNode> entitiesOptional = tbRestClient.getTenantEntities(strEntityId, limit);
                        entitiesOptional.ifPresent(jsonNode ->
                                saveContext.getEntitiesInGroups().add(createEntityNode(strEntityId, (ObjectNode) jsonNode)));
                    }

                    StringBuilder telemetryKeys = getTelemetryKeys(strFromType, strEntityId);
                    if (telemetryKeys != null && telemetryKeys.length() != 0) {
                        Optional<JsonNode> telemetryNodeOptional = tbRestClient.getTelemetry(strFromType, strEntityId,
                                telemetryKeys.toString(), limit, 0L, System.currentTimeMillis());
                        telemetryNodeOptional.ifPresent(telemetryNode -> saveContext.getTelemetryArray().add(
                                createNode(strFromType, strEntityId, telemetryNode, "telemetry")));
                    }

                    ObjectNode attributesNode = getAttributes(strFromType, strEntityId);
                    if (attributesNode != null) {
                        saveContext.getAttributesArray().add(attributesNode);
                    }
                }
            });
        }
    }

    private ObjectNode createEntityNode(String strEntityId, ObjectNode objectNode) {
        ObjectNode savedEntitiesNode = mapper.createObjectNode();
        savedEntitiesNode.put("entityGroupId", strEntityId);
        savedEntitiesNode.setAll(objectNode);
        return savedEntitiesNode;
    }
}
