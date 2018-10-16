package org.thingsboard.datatransfer.importing.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.importing.LoadContext;
import org.thingsboard.server.common.data.EntityView;
import org.thingsboard.server.common.data.id.EntityViewId;
import org.thingsboard.server.common.data.objects.TelemetryEntityView;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.thingsboard.datatransfer.importing.Import.NULL_UUID;

public class ImportEntityViews extends ImportEntity {


    private final RestClient tbRestClient;
    private final boolean emptyDb;

    public ImportEntityViews(RestClient tbRestClient, ObjectMapper mapper, String basePath, boolean emptyDb) {
        super(mapper, basePath);
        this.tbRestClient = tbRestClient;
        this.emptyDb = emptyDb;
    }

    public void saveTenantEntityViews(LoadContext loadContext, int limit) throws IOException {
        Map<String, EntityViewId> entityViewMap = new HashMap<>();
        if (!emptyDb) {
            Optional<JsonNode> entityViewsOptional = tbRestClient.findTenantEntityViews(limit);
            if (entityViewsOptional.isPresent()) {
                for (JsonNode node : entityViewsOptional.get().get("data")) {
                    entityViewMap.put(node.get("name").asText(), EntityViewId.fromString(node.get("id").get("id").asText()));
                }
            }
        }

        JsonNode entityViewsNode = readFileContentToNode("EntityViews.json");
        if (entityViewsNode != null) {
            for (JsonNode entityViewNode : entityViewsNode) {
                loadContext.getEntityViewIdMap().put(entityViewNode.get("id").get("id").asText(), getEntityViewId(entityViewMap, entityViewNode, loadContext));
            }
        }
    }

    private EntityViewId getEntityViewId(Map<String, EntityViewId> entityViewMap, JsonNode node, LoadContext loadContext) throws IOException {
        String entityViewName = node.get("name").asText();
        EntityViewId entityViewId;
        if (emptyDb || !entityViewMap.containsKey(entityViewName)) {
            entityViewId = createEntityView(node, loadContext).getId();
        } else {
            entityViewId = entityViewMap.get(entityViewName);
        }
        return entityViewId;
    }

    private EntityView createEntityView(JsonNode node, LoadContext loadContext) throws IOException {
        EntityView entityView = new EntityView();
        entityView.setName(node.get("name").asText());
        entityView.setType(node.get("type").asText());
        entityView.setKeys(mapper.readValue(mapper.writeValueAsString(node.get("keys")), TelemetryEntityView.class));
        entityView.setStartTimeMs(node.get("startTimeMs").asLong());
        entityView.setStartTimeMs(node.get("endTimeMs").asLong());
        if (node.get("entityId").get("entityType").asText().equals("DEVICE")) {
            entityView.setEntityId(loadContext.getDeviceIdMap().get(node.get("entityId").get("id").asText()));
        } else {
            entityView.setEntityId(loadContext.getAssetIdMap().get(node.get("entityId").get("id").asText()));
        }
        assignEntityViewToCustomer(loadContext, node, entityView);
        return tbRestClient.createEntityView(entityView);
    }


    private void assignEntityViewToCustomer(LoadContext loadContext, JsonNode node, EntityView savedEntityView) {
        String strCustomerId = node.get("customerId").get("id").asText();
        if (!strCustomerId.equals(NULL_UUID)) {
            if (loadContext.getCustomerIdMap().containsKey(strCustomerId)) {
                tbRestClient.assignEntityView(loadContext.getCustomerIdMap().get(strCustomerId), savedEntityView.getId());
            } else {
                tbRestClient.assignEntityViewToPublicCustomer(savedEntityView.getId());
            }
        }
    }


}


