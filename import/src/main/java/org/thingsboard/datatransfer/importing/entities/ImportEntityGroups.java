package org.thingsboard.datatransfer.importing.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.importing.LoadContext;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.group.EntityGroup;
import org.thingsboard.server.common.data.id.EntityGroupId;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Slf4j
public class ImportEntityGroups {

    private final ObjectMapper mapper;
    private final RestClient tbRestClient;
    private final String basePath;
    private final boolean emptyDb;

    public ImportEntityGroups(RestClient tbRestClient, ObjectMapper mapper, String basePath, boolean emptyDb) {
        this.tbRestClient = tbRestClient;
        this.mapper = mapper;
        this.basePath = basePath;
        this.emptyDb = emptyDb;
    }

    public void saveTenantEntityGroups(LoadContext loadContext) {
        Map<String, EntityGroupId> entityGroupNames = new HashMap<>();
        if (!emptyDb) {
            for (EntityTypes entityType : EntityTypes.values()) {
                Optional<JsonNode> entityGroupsOptional = tbRestClient.getTenantEntityGroups(entityType.name());
                if (entityGroupsOptional.isPresent()) {
                    for (JsonNode node : entityGroupsOptional.get()) {
                        String entityGroupName = node.get("name").asText();
                        if (!entityGroupName.equals("All")) {
                            entityGroupNames.put(entityGroupName, new EntityGroupId(UUID.fromString(node.get("id").get("id").asText())));
                        }
                    }
                }
            }
        }

        JsonNode jsonNode = null;
        try {
            jsonNode = mapper.readTree(new String(Files.readAllBytes(Paths.get(basePath + "EntityGroups.json"))));
        } catch (IOException e) {
            log.warn("Could not read entity groups file");
        }

        if (jsonNode != null) {
            for (JsonNode node : jsonNode) {
                String entityGroupName = node.get("name").asText();

                EntityGroupId entityGroupId;
                if (emptyDb || !entityGroupNames.containsKey(entityGroupName)) {
                    entityGroupId = createEntityGroup(node, entityGroupName).getId();
                } else {
                    entityGroupId = entityGroupNames.get(entityGroupName);
                }
                loadContext.getEntityGroupIdMap().put(node.get("id").get("id").asText(), entityGroupId);
            }
        }
    }

    private EntityGroup createEntityGroup(JsonNode node, String entityGroupName) {
        EntityGroup entityGroup = new EntityGroup();
        entityGroup.setName(entityGroupName);
        entityGroup.setType(EntityType.valueOf(node.get("type").asText()));
        entityGroup.setConfiguration(node.get("configuration"));
        return tbRestClient.createEntityGroup(entityGroup);
    }

    public void addTenantEntitiesToGroups(LoadContext loadContext) {
        JsonNode jsonNode = null;
        try {
            jsonNode = mapper.readTree(new String(Files.readAllBytes(Paths.get(basePath + "EntitiesInGroups.json"))));
        } catch (IOException e) {
            log.warn("Could not read entity groups file");
        }

        if (jsonNode != null) {
            for (JsonNode groupEntityNode : jsonNode) {
                List<String> strEntityIds = new ArrayList<>();
                for (JsonNode entityNode : groupEntityNode.get("data")) {
                    switch (entityNode.get("id").get("entityType").asText()) {
                        case "CUSTOMER":
                            strEntityIds.add(loadContext.getCustomerIdMap().get(entityNode.get("id").get("id").asText()).toString());
                            break;
                        case "DEVICE":
                            strEntityIds.add(loadContext.getDeviceIdMap().get(entityNode.get("id").get("id").asText()).toString());
                            break;
                        case "ASSET":
                            strEntityIds.add(loadContext.getAssetIdMap().get(entityNode.get("id").get("id").asText()).toString());
                            break;
                    }
                }
                tbRestClient.addEntitiesToEntityGroup(loadContext.getEntityGroupIdMap().get(groupEntityNode.get("entityGroupId").asText()).toString(),
                        strEntityIds);
            }
        }
    }

}
