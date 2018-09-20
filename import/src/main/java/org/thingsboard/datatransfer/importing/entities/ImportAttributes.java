package org.thingsboard.datatransfer.importing.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.importing.Client;
import org.thingsboard.datatransfer.importing.LoadContext;
import org.thingsboard.server.common.data.id.EntityId;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import static org.thingsboard.datatransfer.importing.Import.*;

@Slf4j
public class ImportAttributes extends ImportEntity {

    private final ObjectMapper mapper;
    private final String basePath;
    private final Client httpClient;

    public ImportAttributes(ObjectMapper mapper, String basePath, Client httpClient) {
        this.mapper = mapper;
        this.basePath = basePath;
        this.httpClient = httpClient;
    }

    public void saveAttributes(LoadContext loadContext) {
        JsonNode jsonNode = null;
        try {
            jsonNode = mapper.readTree(new String(Files.readAllBytes(Paths.get(basePath + "Attributes.json"))));
        } catch (IOException e) {
            log.warn("Could not read telemetry file");
        }

        if (jsonNode != null) {
            List<Future> resultList = new ArrayList<>();
            for (JsonNode node : jsonNode) {
                resultList.add(EXECUTOR_SERVICE.submit(() -> retryUntilDone(() -> {
                    JsonNode attributesNode = node.get("attributes");
                    for (JsonNode attributeNode : attributesNode) {
                        ObjectNode savingNode = createSavingNode(attributeNode);
                        String entityType = node.get("entityType").asText();
                        EntityId entityId = getEntityId(loadContext, node, entityType);
                        if (entityId != null) {
                            if (checkIfPresent(node.get("attributeKeys"), attributeNode.get("key").asText())) {
                                log.info("Pushing server scope attributes to {} [{}]", entityType, entityId);
                                httpClient.sendData(TB_BASE_URL + "/api/plugins/telemetry/" + entityType + "/" +
                                        entityId.toString() + "/attributes/SERVER_SCOPE", savingNode, TB_TOKEN);
                            } else {
                                log.info("Pushing shared scope attributes to {} [{}]", entityType, entityId);
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

    private EntityId getEntityId(LoadContext loadContext, JsonNode node, String entityType) {
        EntityId entityId = null;
        switch (entityType) {
            case "DEVICE":
                entityId = loadContext.getDeviceIdMap().get(node.get("entityId").asText());
                break;
            case "ASSET":
                entityId = loadContext.getAssetIdMap().get(node.get("entityId").asText());
                break;
            case "CUSTOMER":
                entityId = loadContext.getCustomerIdMap().get(node.get("entityId").asText());
                break;
            default:
                log.warn("Entity type is not supported: {}", entityType);
        }
        return entityId;
    }

    private ObjectNode createSavingNode(JsonNode object) {
        ObjectNode savingNode = mapper.createObjectNode();
        savingNode.set(object.get("key").asText(), object.get("value"));
        return savingNode;
    }

}
