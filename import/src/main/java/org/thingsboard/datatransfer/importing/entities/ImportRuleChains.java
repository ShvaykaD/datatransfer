package org.thingsboard.datatransfer.importing.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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

    private final RestClient tbRestClient;

    public ImportRuleChains(RestClient tbRestClient, ObjectMapper mapper, String basePath) {
        super(mapper, basePath);
        this.tbRestClient = tbRestClient;
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
                RuleChainMetaData newRuleChainMetaData = new RuleChainMetaData();
                RuleChainMetaData oldRuleChainMetaData = mapper.readValue(ruleChainNode.get("metadata").asText(), RuleChainMetaData.class);

                newRuleChainMetaData.setRuleChainId(loadContext.getRuleChainIdMap().get(oldRuleChainMetaData.getRuleChainId().toString()));
                newRuleChainMetaData.setFirstNodeIndex(oldRuleChainMetaData.getFirstNodeIndex());

                List<RuleNode> oldRuleNodes = oldRuleChainMetaData.getNodes();
                List<RuleNode> newRuleNodes = new ArrayList<>();
                for (RuleNode oldRuleNode : oldRuleNodes) {
                    RuleNode newRuleNode = new RuleNode();
                    newRuleNode.setRuleChainId(loadContext.getRuleChainIdMap().get(oldRuleChainMetaData.getRuleChainId().toString()));

                    //TODO: change configuration for different nodes
                    newRuleNode.setConfiguration(oldRuleNode.getConfiguration());


                    newRuleNode.setDebugMode(oldRuleNode.isDebugMode());
                    newRuleNode.setName(oldRuleNode.getName());
                    newRuleNode.setType(oldRuleNode.getType());
                    newRuleNode.setAdditionalInfo(oldRuleNode.getAdditionalInfo());

                    newRuleNodes.add(newRuleNode);
                }
                newRuleChainMetaData.setNodes(newRuleNodes);

                List<NodeConnectionInfo> connections = oldRuleChainMetaData.getConnections();
                if (connections != null) {
                    for (NodeConnectionInfo info : connections) {
                        newRuleChainMetaData.addConnectionInfo(info.getFromIndex(), info.getToIndex(), info.getType());
                    }
                }

                List<RuleChainConnectionInfo> ruleChainConnections = oldRuleChainMetaData.getRuleChainConnections();
                if (ruleChainConnections != null) {
                    for (RuleChainConnectionInfo info : ruleChainConnections) {
                        newRuleChainMetaData.addRuleChainConnectionInfo(info.getFromIndex(), loadContext.getRuleChainIdMap()
                                .get(info.getTargetRuleChainId().toString()), info.getType(), info.getAdditionalInfo());
                    }
                }

                tbRestClient.saveRuleChainMetaData(newRuleChainMetaData);
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
}
