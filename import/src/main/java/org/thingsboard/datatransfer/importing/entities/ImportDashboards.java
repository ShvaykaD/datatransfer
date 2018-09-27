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
        ArrayNode arrayNode;
        ObjectNode filterNode = (ObjectNode) aliasNode.get("filter");
        String aliasType = filterNode.get("type").asText();
        switch (aliasType) {
            case "entityGroup":
                filterNode.put(aliasType, loadContext.getEntityGroupIdMap().get(filterNode.get(aliasType).asText()).toString());
                break;
            case "singleEntity":
                changeEntityId(loadContext, (ObjectNode) filterNode.get(aliasType));
                break;
            case "entityList":
                arrayNode = mapper.createArrayNode();
                for (JsonNode entityId : filterNode.get(aliasType)) {
                    String strEntityId = getNewEntityId(filterNode.get("entityType").asText(), loadContext, entityId);
                    if (strEntityId != null) {
                        arrayNode.add(strEntityId);
                    }
                }
                filterNode.set(aliasType, arrayNode);
                break;
            case "entityGroupList":
                arrayNode = mapper.createArrayNode();
                for (JsonNode entityId : filterNode.get(aliasType)) {
                    arrayNode.add(loadContext.getEntityGroupIdMap().get(entityId.asText()).toString());
                }
                filterNode.set(aliasType, arrayNode);
                break;
            case "stateEntity":
                changeEntityId(loadContext, (ObjectNode) filterNode.get("defaultStateEntity"));
                break;
            case "relationsQuery":
            case "assetSearchQuery":
            case "deviceSearchQuery":
                ObjectNode objectNode;
                if (!filterNode.get("defaultStateEntity").isNull()) {
                    objectNode = (ObjectNode) filterNode.get("defaultStateEntity");
                } else {
                    objectNode = (ObjectNode) filterNode.get("rootEntity");
                }
                changeEntityId(loadContext, objectNode);
                break;
            default:
                log.warn("Such alias type [{}] is not supported!", aliasType);
        }
    }

    private String getNewEntityId(String entityType, LoadContext loadContext, JsonNode entityId) {
        switch (entityType) {
            case "DEVICE":
                return loadContext.getDeviceIdMap().get(entityId.asText()).toString();
            case "ASSET":
                return loadContext.getAssetIdMap().get(entityId.asText()).toString();
            case "CUSTOMER":
                return loadContext.getCustomerIdMap().get(entityId.asText()).toString();
            default:
                log.warn("Such entity type [{}] is not supported!", entityType);
                return null;
        }
    }

    private void changeEntityId(LoadContext loadContext, ObjectNode objectNode) {
        String id = objectNode.get("id").asText();
        String entityType = objectNode.get("entityType").asText();
        switch (entityType) {
            case "DEVICE":
                objectNode.put("id", loadContext.getDeviceIdMap().get(id).toString());
                break;
            case "ASSET":
                objectNode.put("id", loadContext.getAssetIdMap().get(id).toString());
                break;
            case "CUSTOMER":
                objectNode.put("id", loadContext.getCustomerIdMap().get(id).toString());
                break;
            default:
                log.warn("Such entity type [{}] is not supported!", entityType);
        }
    }
}
