package org.thingsboard.datatransfer.exporting.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.exporting.SaveContext;

import java.util.Optional;

public class ExportEntityGroups extends ExportEntity {

    public ExportEntityGroups(RestClient tbRestClient, ObjectMapper mapper, String basePath) {
        super(tbRestClient, mapper, basePath);
    }

    public void getEntityGroups(SaveContext saveContext, int limit) {
        String strFromType = "ENTITY_GROUP";
        for (EntityTypes entityType : EntityTypes.values()) {
            Optional<JsonNode> entityGroupsOptional = tbRestClient.getTenantEntityGroups(entityType.name());
            entityGroupsOptional.ifPresent(entityGroupsNode -> {
                for (JsonNode entityGroupNode : entityGroupsNode) {
                    if (entityGroupNode.get("name").asText().equals("All")) {
                        continue;
                    }
                    saveContext.getEntityGroups().add(entityGroupNode);

                    String strEntityId = entityGroupNode.get("id").get("id").asText();
                    Optional<JsonNode> entitiesOptional = tbRestClient.getTenantEntities(strEntityId, limit);
                    entitiesOptional.ifPresent(jsonNode ->
                            saveContext.getEntitiesInGroups().add(createEntityNode(strEntityId, (ObjectNode) jsonNode)));

                    //getTelemetry(saveContext, limit, strFromType, strEntityId);
                    test(saveContext, strFromType, strEntityId);

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
