package org.thingsboard.datatransfer.exporting.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.exporting.Export;
import org.thingsboard.server.common.data.id.RuleChainId;

import java.util.Optional;
import java.util.UUID;

@Slf4j
public class ExportRuleChains extends ExportEntity {

    public ExportRuleChains(RestClient tbRestClient, ObjectMapper mapper, String basePath) {
        super(tbRestClient, mapper, basePath);
    }

    public void getRuleChains(int limit) {
        Optional<JsonNode> ruleChainsOptional = tbRestClient.findRuleChains(limit);

        if (ruleChainsOptional.isPresent()) {
            JsonNode ruleChainsNode = ruleChainsOptional.get().get("data");

            for (JsonNode ruleChainNode : ruleChainsNode) {
                ObjectNode ruleChainObject = (ObjectNode) ruleChainNode;

                Optional<JsonNode> ruleChainMetaDataOptional = tbRestClient.getRuleChainMetaData(
                        new RuleChainId(UUID.fromString(ruleChainNode.get("id").get("id").asText())));

                ruleChainMetaDataOptional.ifPresent(jsonNode -> ruleChainObject.set("metadata", jsonNode));
            }
            Export.writeToFile("RuleChains.json", ruleChainsNode);
        }
    }

}
