package org.thingsboard.datatransfer.importing.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.importing.LoadContext;
import org.thingsboard.server.common.data.Dashboard;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;

/**
 * Created by mshvayka on 13.09.18.
 */
@Slf4j
public class ImportDashboards {

    private final ObjectMapper mapper;
    private final RestClient tbRestClient;
    private final String basePath;

    public ImportDashboards(RestClient tbRestClient, ObjectMapper mapper, String basePath) {
        this.tbRestClient = tbRestClient;
        this.mapper = mapper;
        this.basePath = basePath;
    }

    public void saveTenantDashboards(LoadContext loadContext) {
        JsonNode jsonNode = null;
        try {
            jsonNode = mapper.readTree(new String(Files.readAllBytes(Paths.get(basePath + "Dashboards.json"))));
        } catch (IOException e) {
            log.warn("Could not read dashboards file");
        }
        if (jsonNode != null) {
            for (JsonNode node : jsonNode) {
                Dashboard dashboard = createDashboard(node, loadContext);
                loadContext.getDashboardIdMap().put(node.get("id").get("id").asText(), dashboard.getId());
                assignDashboardToCustomers(loadContext, node, dashboard);
            }
        }
    }

    private void assignDashboardToCustomers(LoadContext loadContext, JsonNode node, Dashboard savedDashboard) {
        if (!node.get("assignedCustomers").isNull()) {
            ArrayNode customersArray = (ArrayNode) node.get("assignedCustomers");
            for (JsonNode customerNode : customersArray) {
                if (customerNode.get("public").asBoolean()) {
                    tbRestClient.assignDashboardToPublicCustomer(savedDashboard.getId());
                } else {
                    tbRestClient.assignDashboard(loadContext.getCustomerIdMap().get(customerNode.get("customerId").get("id").asText()), savedDashboard.getId());
                }
            }
        }
    }

    private Dashboard createDashboard(JsonNode node, LoadContext loadContext) {
        Dashboard dashboard = new Dashboard();
        dashboard.setTitle(node.get("title").asText());
        if (!node.get("configuration").isNull()) {
            JsonNode configurationNode = node.get("configuration");
            for (Iterator<JsonNode> iter = configurationNode.get("entityAliases").elements(); iter.hasNext(); ) {
                JsonNode aliasNode = iter.next();
                changeAliasConfiguration(loadContext, aliasNode);
            }
            dashboard.setConfiguration(configurationNode);
        }
        return tbRestClient.createDashboard(dashboard);
    }

    private void changeAliasConfiguration(LoadContext loadContext, JsonNode aliasNode) {
        ObjectNode node = (ObjectNode) aliasNode.get("filter");
        ObjectNode objectNode;
        String entityType;
        ArrayNode arrayNode;
        switch (node.get("type").asText()) {
            case "entityName":
                break;
            case "entityGroupName":
                break;
            case "singleEntity":
                objectNode = (ObjectNode) node.get("singleEntity");
                entityType = objectNode.get("entityType").asText();
                switchEntityType(loadContext, objectNode, entityType);
                break;
            case "entityList":
                entityType = node.get("entityType").asText();
                switch (entityType) {
                    case "DEVICE":
                        arrayNode = mapper.createArrayNode();
                        for (JsonNode entityId : node.get("entityList")) {
                            arrayNode.add(loadContext.getDeviceIdMap().get(entityId.asText()).toString());
                        }
                        node.set("entityList", arrayNode);
                        break;
                    case "ASSET":
                        arrayNode = mapper.createArrayNode();
                        for (JsonNode entityId : node.get("entityList")) {
                            arrayNode.add(loadContext.getAssetIdMap().get(entityId.asText()).toString());
                        }
                        node.set("entityList", arrayNode);
                        break;
                    case "CUSTOMER":
                        arrayNode = mapper.createArrayNode();
                        for (JsonNode entityId : node.get("entityList")) {
                            arrayNode.add(loadContext.getCustomerIdMap().get(entityId.asText()).toString());
                        }
                        node.set("entityList", arrayNode);
                        break;
                }
                break;
            case "entityGroupList":
                arrayNode = mapper.createArrayNode();
                for (JsonNode entityId : node.get("entityGroupList")) {
                    arrayNode.add(loadContext.getEntityGroupIdMap().get(entityId.asText()).toString());
                }
                node.set("entityList", arrayNode);
                break;
            case "stateEntity":
                objectNode = (ObjectNode) node.get("defaultStateEntity");
                entityType = objectNode.get("entityType").asText();
                switchEntityType(loadContext, objectNode, entityType);
                break;
            case "relationsQuery":
                if (!node.get("defaultStateEntity").isNull()) {
                    objectNode = (ObjectNode) node.get("defaultStateEntity");
                } else {
                    objectNode = (ObjectNode) node.get("rootEntity");
                }
                entityType = objectNode.get("entityType").asText();
                switchEntityType(loadContext, objectNode, entityType);
                break;
            default:
                log.warn("Such alias type is not supported!");
        }
    }

    private void switchEntityType(LoadContext loadContext, ObjectNode objectNode, String entityType) {
        switch (entityType) {
            case "DEVICE":
                objectNode.put("id", loadContext.getDeviceIdMap().get(objectNode.get("id").asText()).toString());
                break;
            case "ASSET":
                objectNode.put("id", loadContext.getAssetIdMap().get(objectNode.get("id").asText()).toString());
                break;
            case "CUSTOMER":
                objectNode.put("id", loadContext.getCustomerIdMap().get(objectNode.get("id").asText()).toString());
                break;
        }
    }
}
