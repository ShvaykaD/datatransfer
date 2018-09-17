package org.thingsboard.datatransfer.importing.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.datatransfer.importing.Client;
import org.thingsboard.datatransfer.importing.LoadContext;
import org.thingsboard.server.common.data.id.EntityId;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.thingsboard.datatransfer.importing.Import.EXECUTOR_SERVICE;
import static org.thingsboard.datatransfer.importing.Import.TB_BASE_URL;
import static org.thingsboard.datatransfer.importing.Import.TB_TOKEN;
import static org.thingsboard.datatransfer.importing.Import.THRESHOLD;

@Slf4j
public class ImportTelemetry {

    private final ObjectMapper mapper;
    private final String basePath;
    private final Client httpClient;

    public ImportTelemetry(ObjectMapper mapper, String basePath, Client httpClient) {
        this.mapper = mapper;
        this.basePath = basePath;
        this.httpClient = httpClient;
    }

    public void saveTelemetry(LoadContext loadContext) {
        JsonNode jsonNode = null;
        try {
            jsonNode = mapper.readTree(new String(Files.readAllBytes(Paths.get(basePath + "Telemetry.json"))));
        } catch (IOException e) {
            log.warn("Could not read telemetry file");
        }

        if (jsonNode != null) {
            List<Future> resultList = new ArrayList<>();
            for (JsonNode node : jsonNode) {
                resultList.add(EXECUTOR_SERVICE.submit(() -> retryUntilDone(() -> {
                    JsonNode telemetryNode = node.get("telemetry");
                    for (Iterator<String> iterator = telemetryNode.fieldNames(); iterator.hasNext(); ) {
                        String field = iterator.next();
                        JsonNode fieldArray = telemetryNode.get(field);
                        for (JsonNode object : fieldArray) {
                            ObjectNode savingNode = createSavingNode(field, object);
                            String entityType = node.get("entityType").asText();
                            EntityId entityId = getEntityId(loadContext, node, entityType);
                            log.info("Pushing telemetry to {} [{}]", entityType, entityId);
                            httpClient.sendData(TB_BASE_URL + "/api/plugins/telemetry/" + entityType + "/" +
                                    entityId.toString() + "/timeseries/data", savingNode, TB_TOKEN);
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

    private EntityId getEntityId(LoadContext loadContext, JsonNode node,
                                 String entityType) {
        EntityId entityId;
        if (entityType.equals("DEVICE")) {
            entityId = loadContext.getDeviceIdMap().get(node.get("entityId").asText());
        } else {
            entityId = loadContext.getAssetIdMap().get(node.get("entityId").asText());
        }
        return entityId;
    }

    private ObjectNode createSavingNode(String field, JsonNode object) {
        ObjectNode savingNode = mapper.createObjectNode();
        savingNode.set("ts", object.get("ts"));
        savingNode.set("values", mapper.createObjectNode().set(field, object.get("value")));
        return savingNode;
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
                log.error("Task failed, repeat in 3 seconds", th);
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
