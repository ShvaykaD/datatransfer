package org.thingsboard.datatransfer.importing.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.importing.LoadContext;
import org.thingsboard.server.common.data.rule.RuleChain;
import org.thingsboard.server.common.data.rule.RuleChainMetaData;

import java.io.IOException;

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
                if (ruleChainNode.get("root").asBoolean()) {
                    continue;
                }


                RuleChain savedRuleChain = createRuleChain(ruleChainNode);
                loadContext.getRuleChainIdMap().put(ruleChainNode.get("id").get("id").asText(), savedRuleChain.getId());


                RuleChainMetaData ruleChainMetaData = mapper.readValue(ruleChainNode.get("metadata").asText(), RuleChainMetaData.class);

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
