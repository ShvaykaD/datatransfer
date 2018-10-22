package org.thingsboard.datatransfer.importing.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.datatransfer.importing.Client;
import org.thingsboard.datatransfer.importing.LoadContext;
import org.thingsboard.server.common.data.id.EntityId;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import static org.thingsboard.datatransfer.importing.Import.EXECUTOR_SERVICE;
import static org.thingsboard.datatransfer.importing.Import.TB_BASE_URL;
import static org.thingsboard.datatransfer.importing.Import.TB_TOKEN;
import static org.thingsboard.datatransfer.importing.Import.THRESHOLD;

@Slf4j
public class ImportAttributes extends ImportEntity {

    private final Client httpClient;

    public ImportAttributes(ObjectMapper mapper, String basePath, Client httpClient) {
        super(mapper, basePath);
        this.httpClient = httpClient;
    }

    public void saveAttributes(LoadContext loadContext) {
        JsonNode jsonNode = readFileContentToNode("Attributes.json");
        if (jsonNode != null) {
            List<Future> resultList = new ArrayList<>();
            for (JsonNode node : jsonNode) {
                resultList.add(EXECUTOR_SERVICE.submit(() -> retryUntilDone(() -> {
                    for (JsonNode attributeNode : node.get("attributes")) {
                        ObjectNode savingNode = createSavingNode(attributeNode);
                        String entityType = node.get("entityType").asText();
                        EntityId entityId = getEntityId(loadContext, node.get("entityId").asText(), entityType);
                        if (entityId != null) {
                            if (checkIfPresent(node.get("attributeKeys"), attributeNode.get("key").asText())) {
                                log.info("Pushing server attributes to {} [{}]", entityType, entityId);
                                httpClient.sendData(TB_BASE_URL + "/api/plugins/telemetry/" + entityType + "/" +
                                        entityId.toString() + "/attributes/SERVER_SCOPE", savingNode, TB_TOKEN);
                            } else {
                                log.info("Pushing shared attributes to {} [{}]", entityType, entityId);
                                httpClient.sendData(TB_BASE_URL + "/api/plugins/telemetry/" + entityType + "/" +
                                        entityId.toString() + "/attributes/SHARED_SCOPE", savingNode, TB_TOKEN);
                            }
                        }
                    }
                    return true;
                })));
                if (resultList.size() > THRESHOLD) {
                    waitForPack(resultList);
                }

            }
            waitForPack(resultList);
        }
    }

    private boolean checkIfPresent(JsonNode arrayNode, String key) {
        for (JsonNode node : arrayNode) {
            if (node.asText().equals(key)) {
                return true;
            }
        }
        return false;
    }

    private ObjectNode createSavingNode(JsonNode object) {
        ObjectNode savingNode = mapper.createObjectNode();
        savingNode.set(object.get("key").asText(), object.get("value"));
        return savingNode;
    }

}
