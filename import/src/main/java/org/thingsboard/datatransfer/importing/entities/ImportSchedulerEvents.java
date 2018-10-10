package org.thingsboard.datatransfer.importing.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.importing.LoadContext;
import org.thingsboard.server.common.data.scheduler.SchedulerEvent;

import static org.thingsboard.datatransfer.importing.Import.NULL_UUID;

@Slf4j
public class ImportSchedulerEvents extends ImportEntity {

    private final RestClient tbRestClient;
    private final String tenantUserId;

    public ImportSchedulerEvents(RestClient tbRestClient, ObjectMapper mapper, String basePath, String tenantUserId) {
        super(mapper, basePath);
        this.tbRestClient = tbRestClient;
        this.tenantUserId = tenantUserId;
    }

    public void saveSchedulerEvents(LoadContext loadContext) {
        JsonNode schedulersArray = readFileContentToNode("SchedulerEvents.json");
        if (schedulersArray != null) {
            for (JsonNode schedulerNode : schedulersArray) {
                SchedulerEvent schedulerEvent = createSchedulerEvent(loadContext, schedulerNode);
                loadContext.getSchedulerEventIdMap().put(schedulerNode.get("id").get("id").asText(), schedulerEvent.getId());
            }
        }
    }

    // TODO: 10.10.18 case TENANT; case BLOB_ENTITY
    private SchedulerEvent createSchedulerEvent(LoadContext loadContext, JsonNode node) {
        SchedulerEvent savedSchedulerEvent;
        SchedulerEvent schedulerEvent = new SchedulerEvent();
        schedulerEvent.setName(node.get("name").asText());
        schedulerEvent.setType(node.get("type").asText());
        assignSchedulerEventToCustomer(loadContext, node, schedulerEvent);
        schedulerEvent.setSchedule(node.get("schedule"));


        String eventType = node.get("type").asText();
        ObjectNode configurationNode = (ObjectNode) node.get("configuration");
        if (eventType.equals("generateReport")) {
            ObjectNode reportNode = (ObjectNode) configurationNode.get("msgBody").get("reportConfig");
            if (reportNode.get("useCurrentUserCredentials").asBoolean()) {
                reportNode.put("userId", tenantUserId);
            } else if (loadContext.getUserIdMap().containsKey(reportNode.get("userId").asText())) {
                reportNode.put("userId", String.valueOf(loadContext.getUserIdMap().get(reportNode.get("userId").asText())));
            }
            if (loadContext.getDashboardIdMap().containsKey(reportNode.get("dashboardId").asText())) {
                reportNode.put("dashboardId", String.valueOf(loadContext.getDashboardIdMap().get(reportNode.get("dashboardId").asText())));
            }
            configurationNode.set("reportConfig", reportNode);
        } else {
            String entityType = configurationNode.get("originatorId").get("entityType").asText();
            switch (entityType) {
                case "ENTITY_GROUP":
                    if (loadContext.getEntityGroupIdMap().containsKey(configurationNode.get("originatorId").get("id").asText())) {
                        configurationNode.put("id", String.valueOf(loadContext.getEntityGroupIdMap().get(configurationNode.get("originatorId").get("id").asText())));
                    }
                    break;
                case "DEVICE":
                    if (loadContext.getDeviceIdMap().containsKey(configurationNode.get("originatorId").get("id").asText())) {
                        configurationNode.put("id", String.valueOf(loadContext.getDeviceIdMap().get(configurationNode.get("originatorId").get("id").asText())));
                    }
                    break;
                case "ASSET":
                    if (loadContext.getAssetIdMap().containsKey(configurationNode.get("originatorId").get("id").asText())) {
                        configurationNode.put("id", String.valueOf(loadContext.getAssetIdMap().get(configurationNode.get("originatorId").get("id").asText())));
                    }
                    break;
                case "CUSTOMER":
                    if (loadContext.getCustomerIdMap().containsKey(configurationNode.get("originatorId").get("id").asText())) {
                        configurationNode.put("id", String.valueOf(loadContext.getCustomerIdMap().get(configurationNode.get("originatorId").get("id").asText())));
                    }
                case "DASHBOARD":
                    if (loadContext.getDashboardIdMap().containsKey(configurationNode.get("originatorId").get("id").asText())) {
                        configurationNode.put("id", String.valueOf(loadContext.getDashboardIdMap().get(configurationNode.get("originatorId").get("id").asText())));
                    }
                    break;
                case "CONVERTER":
                    if (loadContext.getConverterIdMap().containsKey(configurationNode.get("originatorId").get("id").asText())) {
                        configurationNode.put("id", String.valueOf(loadContext.getConverterIdMap().get(configurationNode.get("originatorId").get("id").asText())));
                    }
                    break;
                case "INTEGRATION":
                    if (loadContext.getIntegrationIdMap().containsKey(configurationNode.get("originatorId").get("id").asText())) {
                        configurationNode.put("id", String.valueOf(loadContext.getIntegrationIdMap().get(configurationNode.get("originatorId").get("id").asText())));
                    }
                    break;
                case "SCHEDULER_EVENT":
                    if (loadContext.getSchedulerEventIdMap().containsKey(configurationNode.get("originatorId").get("id").asText())) {
                        configurationNode.put("id", String.valueOf(loadContext.getSchedulerEventIdMap().get(configurationNode.get("originatorId").get("id").asText())));
                    }
                    break;
                default:
                    log.warn("Entity type is not supported: {}", entityType);
            }
        }

        schedulerEvent.setConfiguration(configurationNode);
        savedSchedulerEvent = tbRestClient.createSchedulerEvent(schedulerEvent);
        return savedSchedulerEvent;
    }

    private void assignSchedulerEventToCustomer(LoadContext loadContext, JsonNode node, SchedulerEvent savedSchedulerEvent) {
        String strCustomerId = node.get("customerId").get("id").asText();
        if (!strCustomerId.equals(NULL_UUID) && loadContext.getCustomerIdMap().containsKey(strCustomerId)) {
            savedSchedulerEvent.setCustomerId(loadContext.getCustomerIdMap().get(strCustomerId));
        }
    }


}