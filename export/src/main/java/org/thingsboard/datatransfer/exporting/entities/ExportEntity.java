package org.thingsboard.datatransfer.exporting.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.exporting.Client;
import org.thingsboard.datatransfer.exporting.Export;
import org.thingsboard.datatransfer.exporting.SaveContext;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.thingsboard.datatransfer.exporting.Export.*;

@Slf4j
public class ExportEntity {

    final RestClient tbRestClient;
    final ObjectMapper mapper;
    final String basePath;
    final Client httpClient;

    public ExportEntity(RestClient tbRestClient, ObjectMapper mapper, String basePath, Client httpClient) {
        this.tbRestClient = tbRestClient;
        this.mapper = mapper;
        this.basePath = basePath;
        this.httpClient = httpClient;
    }

    JsonNode getRelationsFromEntity(String strEntityId, String strFromType) {
        Optional<JsonNode> relationOptional = tbRestClient.getRelationByFrom(strEntityId, strFromType);
        if (relationOptional.isPresent()) {
            JsonNode node = relationOptional.get();
            if (node.isArray() && node.size() != 0) {
                return node;
            }
        }
        return null;
    }

    private JsonNode getTelemetryKeys(String strFromType, String strEntityId) {
        Optional<JsonNode> telemetryKeysOptional = tbRestClient.getTelemetryKeys(strFromType, strEntityId);
        return telemetryKeysOptional.orElse(null);
    }

    ObjectNode getAttributes(String strFromType, String strEntityId) {
        Optional<JsonNode> attributesOptional = tbRestClient.getAttributes(strFromType, strEntityId);
        if (attributesOptional.isPresent()) {
            JsonNode jsonNode = attributesOptional.get();
            ObjectNode savedNode = createNode(strFromType, strEntityId, jsonNode, "attributes");
            Optional<JsonNode> attributesKeysByScopeOptional = tbRestClient.getAttributesKeysByScope(strFromType, strEntityId, "SERVER_SCOPE");
            attributesKeysByScopeOptional.ifPresent(node -> savedNode.set("attributeKeys", node));
            return savedNode;
        }
        return null;
    }

    private ObjectNode createNode(String strFromType, String strEntityId, JsonNode node, String dataType) {
        ObjectNode resultNode = mapper.createObjectNode();
        resultNode.put("entityType", strFromType);
        resultNode.put("entityId", strEntityId);
        resultNode.set(dataType, node);
        return resultNode;
    }

    void processEntityNodes(SaveContext saveContext, int limit, JsonNode entityArray, String strFromType) {
        for (JsonNode node : entityArray) {
            String strEntityId = node.get("id").get("id").asText();

            JsonNode relationsFromEntityNode = getRelationsFromEntity(strEntityId, strFromType);
            if (relationsFromEntityNode != null) {
                saveContext.getRelationsArray().add(relationsFromEntityNode);
            }

            gavnoCodeAsync(saveContext, strFromType, strEntityId);

            ObjectNode attributesNode = getAttributes(strFromType, strEntityId);
            if (attributesNode != null) {
                saveContext.getAttributesArray().add(attributesNode);
            }
        }
    }

    void gavnoCode(SaveContext saveContext, String strFromType, String strEntityId) {
        JsonNode telemetryKeys = getTelemetryKeys(strFromType, strEntityId);
        if (telemetryKeys != null && telemetryKeys.size() > 0) {
            for (JsonNode node : telemetryKeys) {
                long startTs = 0L;
                long endTs = 1577836800000L;
                String key = node.asText();

                int returnedSize = 0;
                do {
                    log.info("Getting telemetry for entity: [{}] key: [{}]", strEntityId, key);
                    Optional<JsonNode> telemetryNodeOptional = tbRestClient.getTelemetry(strFromType, strEntityId, key, 10000, startTs, endTs, "ASC");
                    if (telemetryNodeOptional.isPresent()) {
                        JsonNode jsonNode = telemetryNodeOptional.get();
                        if (jsonNode.size() > 0) {
                            ArrayNode arrayNode = (ArrayNode) jsonNode.get(key);
                            returnedSize = arrayNode.size();
                            saveContext.getTelemetryArray().add(createNode(strFromType, strEntityId, jsonNode, "telemetry"));

                            if (saveContext.getTelemetryArray().size() == 30) {
                                Export.writeToFile(UUID.randomUUID() + "_Telemetry.json", saveContext.getTelemetryArray());
                                saveContext.setTelemetryArray(mapper.createArrayNode());
                            }

                            startTs = arrayNode.get(returnedSize - 1).get("ts").asLong();
                        }
                    }
                } while (returnedSize == 10000);
            }
            if (saveContext.getTelemetryArray().size() > 0) {
                Export.writeToFile(UUID.randomUUID() + "_Telemetry.json", saveContext.getTelemetryArray());
            }
        }
    }

    void gavnoCodeAsync(SaveContext saveContext, String strFromType, String strEntityId) {
        JsonNode telemetryKeys = getTelemetryKeys(strFromType, strEntityId);
        if (telemetryKeys != null && telemetryKeys.size() > 0) {
            List<Future> resultList = new ArrayList<>();
            for (JsonNode node : telemetryKeys) {
                resultList.add(EXECUTOR_SERVICE.submit(() -> retryUntilDone(() -> {
                    long startTs = 0L;
                    long endTs = 1577836800000L;
                    int limit = 10000;
                    String key = node.asText();

                    int returnedSize = 0;
                    do {
                        log.info("Getting telemetry for entity: [{}] key: [{}]", strEntityId, key);

                        JsonNode jsonNode = httpClient.getData(TB_BASE_URL + "/api/plugins/telemetry/" +
                                strFromType + "/" + strEntityId + "/values/timeseries?limit=" + limit + "&orderBy=ASC&keys=" +
                                key + "&startTs=" + startTs + "&endTs=" + endTs, TB_TOKEN);
                        if (jsonNode.size() > 0) {
                            ArrayNode arrayNode = (ArrayNode) jsonNode.get(key);
                            returnedSize = arrayNode.size();
                            saveContext.getTelemetryArray().add(createNode(strFromType, strEntityId, jsonNode, "telemetry"));

                            if (saveContext.getTelemetryArray().size() == 50) {
                                Export.writeToFile(UUID.randomUUID() + "_Telemetry.json", saveContext.getTelemetryArray());
                                saveContext.setTelemetryArray(mapper.createArrayNode());
                            }

                            startTs = arrayNode.get(returnedSize - 1).get("ts").asLong();
                        }

                    } while (returnedSize == limit);

                    if (saveContext.getTelemetryArray().size() > 0) {
                        Export.writeToFile(UUID.randomUUID() + "_Telemetry.json", saveContext.getTelemetryArray());
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

    private void waitForPack(List<Future> resultList) {
        try {
            for (Future future : resultList) {
                future.get();
            }
        } catch (Exception e) {
            log.error("Failed to complete task", e);
        }
        resultList.clear();
    }

    private void retryUntilDone(Callable task) {
        int tries = 0;
        while (true) {
            if (tries > 5) {
                return;
            }
            try {
                task.call();
                return;
            } catch (Throwable th) {
                log.warn("Task failed, repeat in 3 seconds", th);
                try {
                    TimeUnit.SECONDS.sleep(3);
                } catch (InterruptedException e) {
                    throw new IllegalStateException("Thread interrupted", e);
                }
            }
            tries++;
        }
    }

}
