package org.thingsboard.datatransfer.importing.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.importing.LoadContext;
import org.thingsboard.server.common.data.Dashboard;

import java.util.Iterator;

/**
 * Created by mshvayka on 13.09.18.
 */
@Slf4j
public class ImportDashboards extends ImportEntity {

    private final RestClient tbRestClient;

    public ImportDashboards(RestClient tbRestClient, ObjectMapper mapper, String basePath) {
        super(mapper, basePath);
        this.tbRestClient = tbRestClient;
    }

    public void saveTenantDashboards(LoadContext loadContext) {
        JsonNode dashboardsNode = readFileContentToNode("Dashboards.json");
        if (dashboardsNode != null) {
            for (JsonNode dashboardNode : dashboardsNode) {
                log.info("Trying create dashboard...{}", dashboardNode.get("title").asText());
                Dashboard dashboard = createDashboard(dashboardNode, loadContext);
                log.info("created dashboard");
                loadContext.getDashboardIdMap().put(dashboardNode.get("id").get("id").asText(), dashboard.getId());
                assignDashboardToCustomers(loadContext, dashboardNode, dashboard);
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
                    tbRestClient.assignDashboard(loadContext.getCustomerIdMap().get(customerNode.get("customerId").get("id").asText()),
                            savedDashboard.getId());
                }
            }
        }
    }

    private Dashboard createDashboard(JsonNode node, LoadContext loadContext) {
        Dashboard dashboard = new Dashboard();
        dashboard.setTitle(node.get("title").asText());
        if (!node.get("configuration").isNull()) {
            JsonNode configurationNode = node.get("configuration");
            if (configurationNode.has("entityAliases")) {
                for (Iterator<JsonNode> iter = configurationNode.get("entityAliases").elements(); iter.hasNext(); ) {
                    JsonNode aliasNode = iter.next();
                    changeAliasConfiguration(loadContext, aliasNode);
                }
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
                if (!filterNode.get("defaultStateEntity").isNull()) {
                    changeEntityId(loadContext, (ObjectNode) filterNode.get("defaultStateEntity"));
                }
                break;
            case "relationsQuery":
            case "assetSearchQuery":
            case "deviceSearchQuery":
            case "entityViewSearchQuery":
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
            case "DASHBOARD":
                return loadContext.getDashboardIdMap().get(entityId.asText()).toString();
            case "CONVERTER":
                return loadContext.getConverterIdMap().get(entityId.asText()).toString();
            case "INTEGRATION":
                return loadContext.getIntegrationIdMap().get(entityId.asText()).toString();
            case "SCHEDULER_EVENT":
                return loadContext.getSchedulerEventIdMap().get(entityId.asText()).toString();
            case "ENTITY_VIEW":
                return loadContext.getEntityViewIdMap().get(entityId.asText()).toString();
            case "BLOB_ENTITY":
                return loadContext.getBlobEntityIdMap().get(entityId.asText()).toString();
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
                if (loadContext.getDeviceIdMap().containsKey(id)) {
                    objectNode.put("id", loadContext.getDeviceIdMap().get(id).toString());
                }
                break;
            case "ASSET":
                if (loadContext.getAssetIdMap().containsKey(id)) {
                    objectNode.put("id", loadContext.getAssetIdMap().get(id).toString());
                }
                break;
            case "CUSTOMER":
                if (loadContext.getCustomerIdMap().containsKey(id)) {
                    objectNode.put("id", loadContext.getCustomerIdMap().get(id).toString());
                }
                break;
            case "DASHBOARD":
                if (loadContext.getDashboardIdMap().containsKey(id)) {
                    objectNode.put("id", loadContext.getDashboardIdMap().get(id).toString());
                }
                break;
            case "CONVERTER":
                if (loadContext.getConverterIdMap().containsKey(id)) {
                    objectNode.put("id", loadContext.getConverterIdMap().get(id).toString());
                }
                break;
            case "INTEGRATION":
                if (loadContext.getIntegrationIdMap().containsKey(id)) {
                    objectNode.put("id", loadContext.getIntegrationIdMap().get(id).toString());
                }
                break;
            case "SCHEDULER_EVENT":
                if (loadContext.getSchedulerEventIdMap().containsKey(id)) {
                    objectNode.put("id", loadContext.getSchedulerEventIdMap().get(id).toString());
                }
                break;
            case "ENTITY_VIEW":
                if (loadContext.getEntityViewIdMap().containsKey(id)) {
                    objectNode.put("id", loadContext.getEntityViewIdMap().get(id).toString());
                }
                break;
            case "BLOB_ENTITY":
                if (loadContext.getBlobEntityIdMap().containsKey(id)) {
                    objectNode.put("id", loadContext.getBlobEntityIdMap().get(id).toString());
                }
                break;
            default:
                log.warn("Such entity type [{}] is not supported!", entityType);
        }
    }
}
