package org.thingsboard.datatransfer.importing.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.importing.LoadContext;
import org.thingsboard.server.common.data.rule.NodeConnectionInfo;
import org.thingsboard.server.common.data.rule.RuleChain;
import org.thingsboard.server.common.data.rule.RuleChainConnectionInfo;
import org.thingsboard.server.common.data.rule.RuleChainMetaData;
import org.thingsboard.server.common.data.rule.RuleNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class ImportRuleChains extends ImportEntity {

    private static final String TB_CHECK_RELATION_NODE = "org.thingsboard.rule.engine.filter.TbCheckRelationNode";
    private static final String TB_MSG_GENERATOR_NODE = "org.thingsboard.rule.engine.debug.TbMsgGeneratorNode";
    private static final String TB_INTEGRATION_DOWNLINK_NODE = "org.thingsboard.rule.engine.integration.TbIntegrationDownlinkNode";
    private static final String TB_GENERATE_REPORT_NODE = "org.thingsboard.rule.engine.report.TbGenerateReportNode";
    private static final String TB_ALARMS_COUNT_NODE = "org.thingsboard.rule.engine.analytics.latest.alarm.TbAlarmsCountNode";
    private static final String TB_SIMPLE_AGG_MSG_NODE = "org.thingsboard.rule.engine.analytics.incoming.TbSimpleAggMsgNode";
    private static final String TB_AGG_LATEST_TELEMETRY_NODE = "org.thingsboard.rule.engine.analytics.latest.telemetry.TbAggLatestTelemetryNode";

    private final RestClient tbRestClient;
    private final String tenantUserId;

    public ImportRuleChains(RestClient tbRestClient, ObjectMapper mapper, String basePath, String tenantUserId) {
        super(mapper, basePath);
        this.tbRestClient = tbRestClient;
        this.tenantUserId = tenantUserId;
    }

    public void saveRuleChains(LoadContext loadContext) throws IOException {
        JsonNode ruleChainsNode = readFileContentToNode("RuleChains.json");
        if (ruleChainsNode != null) {
            for (JsonNode ruleChainNode : ruleChainsNode) {
                RuleChain savedRuleChain = createRuleChain(ruleChainNode);
                if (ruleChainNode.get("root").asBoolean()) {
                    tbRestClient.setRootRuleChain(savedRuleChain.getId());
                }
                loadContext.getRuleChainIdMap().put(ruleChainNode.get("id").get("id").asText(), savedRuleChain.getId());
            }

            for (JsonNode ruleChainNode : ruleChainsNode) {
                tbRestClient.saveRuleChainMetaData(createRuleChainMetaData(loadContext, ruleChainNode));
            }
        }
    }

    private RuleChain createRuleChain(JsonNode ruleChainNode) {
        RuleChain ruleChain = new RuleChain();
        ruleChain.setRoot(false);
        ruleChain.setDebugMode(ruleChainNode.get("debugMode").asBoolean());
        ruleChain.setName(ruleChainNode.get("name").asText());
        return tbRestClient.createRuleChain(ruleChain);
    }

    private RuleChainMetaData createRuleChainMetaData(LoadContext loadContext, JsonNode ruleChainNode) throws IOException {
        RuleChainMetaData oldRuleChainMetaData = mapper.readValue(ruleChainNode.get("metadata").asText(), RuleChainMetaData.class);

        RuleChainMetaData newRuleChainMetaData = new RuleChainMetaData();
        newRuleChainMetaData.setRuleChainId(loadContext.getRuleChainIdMap().get(oldRuleChainMetaData.getRuleChainId().toString()));
        newRuleChainMetaData.setFirstNodeIndex(oldRuleChainMetaData.getFirstNodeIndex());
        newRuleChainMetaData.setNodes(createRuleNodesListForRuleChain(loadContext, oldRuleChainMetaData));
        createRuleNodeConnections(newRuleChainMetaData, oldRuleChainMetaData);
        createRuleChainConnections(loadContext, newRuleChainMetaData, oldRuleChainMetaData);
        return newRuleChainMetaData;
    }

    private List<RuleNode> createRuleNodesListForRuleChain(LoadContext loadContext, RuleChainMetaData oldRuleChainMetaData) {
        List<RuleNode> newRuleNodes = new ArrayList<>();
        for (RuleNode oldRuleNode : oldRuleChainMetaData.getNodes()) {
            newRuleNodes.add(createRuleNode(loadContext, oldRuleChainMetaData, oldRuleNode));
        }
        return newRuleNodes;
    }

    private RuleNode createRuleNode(LoadContext loadContext, RuleChainMetaData oldRuleChainMetaData, RuleNode oldRuleNode) {
        RuleNode newRuleNode = new RuleNode();
        newRuleNode.setRuleChainId(loadContext.getRuleChainIdMap().get(oldRuleChainMetaData.getRuleChainId().toString()));
        newRuleNode.setConfiguration(createRuleNodeConfiguration(loadContext, oldRuleNode));
        newRuleNode.setDebugMode(oldRuleNode.isDebugMode());
        newRuleNode.setName(oldRuleNode.getName());
        newRuleNode.setType(oldRuleNode.getType());
        newRuleNode.setAdditionalInfo(oldRuleNode.getAdditionalInfo());
        return newRuleNode;
    }

    private ObjectNode createRuleNodeConfiguration(LoadContext loadContext, RuleNode oldRuleNode) {
        ObjectNode ruleNodeConfiguration = (ObjectNode) oldRuleNode.getConfiguration();
        String ruleNodeType = ruleNodeConfiguration.get("type").asText();
        switch (ruleNodeType) {
            case TB_CHECK_RELATION_NODE:
                changeRuleNodeConfiguration(loadContext, ruleNodeConfiguration, "entityType", "entityId");
                break;
            case TB_MSG_GENERATOR_NODE:
                changeRuleNodeConfiguration(loadContext, ruleNodeConfiguration, "originatorType", "originatorId");
                break;
            case TB_INTEGRATION_DOWNLINK_NODE:
                ruleNodeConfiguration.put("integrationId", loadContext.getIntegrationIdMap()
                        .get(ruleNodeConfiguration.get("integrationId").asText()).toString());
                break;
            case TB_GENERATE_REPORT_NODE:
                ObjectNode reportConfigNode = (ObjectNode) ruleNodeConfiguration.get("reportConfig");
                if (!reportConfigNode.isNull()) {
                    String userIdStr = reportConfigNode.get("userId").asText();
                    if (loadContext.getUserIdMap().containsKey(userIdStr)) {
                        reportConfigNode.put("userId", loadContext.getUserIdMap().get(userIdStr).toString());
                    } else {
                        reportConfigNode.put("userId", tenantUserId);
                    }
                    reportConfigNode.put("dashboardId", loadContext.getDashboardIdMap()
                            .get(reportConfigNode.get("dashboardId").asText()).toString());
                }
                break;
            case TB_ALARMS_COUNT_NODE:
            case TB_SIMPLE_AGG_MSG_NODE:
            case TB_AGG_LATEST_TELEMETRY_NODE:
                changeConfigurationForAnalyticsRuleNodes(loadContext, ruleNodeConfiguration);
                break;
            default:
                log.warn("Such rule node type [{}] should not be changed", ruleNodeType);
        }
        return ruleNodeConfiguration;
    }

    private void changeConfigurationForAnalyticsRuleNodes(LoadContext loadContext, ObjectNode ruleNodeConfiguration) {
        JsonNode parentEntitiesQueryNode = ruleNodeConfiguration.get("parentEntitiesQuery");
        switch (parentEntitiesQueryNode.get("type").asText()) {
            case "relationsQuery":
                ObjectNode rootEntityIdNode = (ObjectNode) parentEntitiesQueryNode.get("rootEntityId");
                changeRuleNodeConfiguration(loadContext, rootEntityIdNode, "entityType", "id");
                break;
            case "group":
                ObjectNode entityGroupIdNode = (ObjectNode) parentEntitiesQueryNode.get("entityGroupId");
                entityGroupIdNode.put("id", loadContext.getEntityGroupIdMap()
                        .get(entityGroupIdNode.get("id").asText()).toString());
                break;
            case "single":
                ObjectNode entityIdNode = (ObjectNode) parentEntitiesQueryNode.get("entityId");
                changeRuleNodeConfiguration(loadContext, entityIdNode, "entityType", "id");
                break;
        }
    }

    private void changeRuleNodeConfiguration(LoadContext loadContext, ObjectNode ruleNodeConfiguration, String typeField, String idField) {
        String entityType = ruleNodeConfiguration.get(typeField).asText();
        String entityId = ruleNodeConfiguration.get(idField).asText();
        switch (entityType) {
            case "DEVICE":
                ruleNodeConfiguration.put(idField, loadContext.getDeviceIdMap().get(entityId).toString());
                break;
            case "ASSET":
                ruleNodeConfiguration.put(idField, loadContext.getAssetIdMap().get(entityId).toString());
                break;
            /*case "ENTITY_VIEW":

                break;*/
            /*case "TENANT":

                break;*/
            case "CUSTOMER":
                ruleNodeConfiguration.put(idField, loadContext.getCustomerIdMap().get(entityId).toString());
                break;
            case "DASHBOARD":
                ruleNodeConfiguration.put(idField, loadContext.getDashboardIdMap().get(entityId).toString());
                break;
            case "CONVERTER":
                ruleNodeConfiguration.put(idField, loadContext.getConverterIdMap().get(entityId).toString());
                break;
            case "INTEGRATION":
                ruleNodeConfiguration.put(idField, loadContext.getIntegrationIdMap().get(entityId).toString());
                break;
            case "SCHEDULER_EVENT":
                ruleNodeConfiguration.put(idField, loadContext.getSchedulerEventIdMap().get(entityId).toString());
                break;
            /*case "BLOB_ENTITY":

                break;*/
            default:
                log.warn("Such entity type: [{}] is not supported for rule node type: {}", entityType, ruleNodeConfiguration.get("type").asText());
        }
    }

    private void createRuleNodeConnections(RuleChainMetaData newRuleChainMetaData, RuleChainMetaData oldRuleChainMetaData) {
        List<NodeConnectionInfo> connections = oldRuleChainMetaData.getConnections();
        if (connections != null) {
            for (NodeConnectionInfo info : connections) {
                newRuleChainMetaData.addConnectionInfo(info.getFromIndex(), info.getToIndex(), info.getType());
            }
        }
    }

    private void createRuleChainConnections(LoadContext loadContext, RuleChainMetaData newRuleChainMetaData, RuleChainMetaData oldRuleChainMetaData) {
        List<RuleChainConnectionInfo> ruleChainConnections = oldRuleChainMetaData.getRuleChainConnections();
        if (ruleChainConnections != null) {
            for (RuleChainConnectionInfo info : ruleChainConnections) {
                newRuleChainMetaData.addRuleChainConnectionInfo(info.getFromIndex(), loadContext.getRuleChainIdMap()
                        .get(info.getTargetRuleChainId().toString()), info.getType(), info.getAdditionalInfo());
            }
        }
    }
}
