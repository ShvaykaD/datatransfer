package org.thingsboard.datatransfer.exporting.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.exporting.SaveContext;
import org.thingsboard.server.common.data.EntityType;

import java.util.Optional;

public class ExportEntityGroups extends ExportEntity {

    public ExportEntityGroups(RestClient tbRestClient, ObjectMapper mapper, String basePath) {
        super(tbRestClient, mapper, basePath, false);
    }

    public void getEntityGroups(SaveContext saveContext, int limit) {

        for (EntityTypes entityType : EntityTypes.values()) {
            String strFromType = "ENTITY_GROUP";
            Optional<JsonNode> entityGroupsOptional = tbRestClient.getTenantEntityGroups(entityType.name());
            entityGroupsOptional.ifPresent(jsonNode -> {
                for (JsonNode node : jsonNode) {
                    saveContext.getEntityGroups().add(node);
                    String strEntityId = node.get("id").get("id").asText();
                    if (!node.get("name").asText().equals("All")) {
                        Optional<JsonNode> entitiesOptional = tbRestClient.getTenantEntities(strEntityId, limit);
                        if (entitiesOptional.isPresent()) {
                            ObjectNode savedEntitiesNode = mapper.createObjectNode();
                            savedEntitiesNode.put("entityGroupId", strEntityId);
                            savedEntitiesNode.setAll((ObjectNode) entitiesOptional.get());
                            saveContext.getEntitiesInGroups().add(savedEntitiesNode);
                        }
                    }


                    StringBuilder telemetryKeys = getTelemetryKeys(strFromType, strEntityId);

                    if (telemetryKeys != null && telemetryKeys.length() != 0) {
                        Optional<JsonNode> telemetryNodeOptional = tbRestClient.getTelemetry(strFromType, strEntityId,
                                telemetryKeys.toString(), limit, 0L, System.currentTimeMillis());
                        if (telemetryNodeOptional.isPresent()) {
                            JsonNode node1 = telemetryNodeOptional.get();
                            saveContext.getTelemetryArray().add(createNode(strFromType, strEntityId, node1, "telemetry"));

                        }
                    }
                    ObjectNode attributesNode = getAttributes(strFromType, strEntityId);
                    if (attributesNode != null) {
                        saveContext.getAttributesArray().add(attributesNode);
                    }





                }
            });
        }


    }

}
