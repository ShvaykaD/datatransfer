package org.thingsboard.datatransfer.importing.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.datatransfer.importing.LoadContext;
import org.thingsboard.server.common.data.id.EntityId;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Slf4j
class ImportEntity {

    protected final ObjectMapper mapper;
    protected final String basePath;

    ImportEntity(ObjectMapper mapper, String basePath) {
        this.mapper = mapper;
        this.basePath = basePath;
    }

    JsonNode readFileContentToNode(String fileName) {
        JsonNode jsonNode = null;
        try {
            jsonNode = mapper.readTree(new String(Files.readAllBytes(Paths.get(basePath + fileName))));
        } catch (IOException e) {
            log.warn("Could not read [{}] file", fileName);
        }
        return jsonNode;
    }

    JsonElement readFileContentToGson(String fileName) {
        JsonParser parser = new JsonParser();
        JsonElement jsonElement = null;
        try {
            jsonElement = parser.parse(new String(Files.readAllBytes(Paths.get(basePath + fileName))));
        } catch (IOException e) {
            log.warn("Could not read [{}] file", fileName);
        }
        return jsonElement;
    }

    void waitForPack(List<Future> resultList) {
        try {
            for (Future future : resultList) {
                future.get();
            }
        } catch (Exception e) {
            log.error("Failed to complete task", e);
        }
        resultList.clear();
    }

    void retryUntilDone(Callable task) {
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

    EntityId getEntityId(LoadContext loadContext, String strEntityId, String entityType) {
        EntityId entityId = null;
        switch (entityType) {
            case "DEVICE":
                entityId = loadContext.getDeviceIdMap().get(strEntityId);
                break;
            case "ASSET":
                entityId = loadContext.getAssetIdMap().get(strEntityId);
                break;
            case "CUSTOMER":
                entityId = loadContext.getCustomerIdMap().get(strEntityId);
                break;
            case "ENTITY_GROUP":
                entityId = loadContext.getEntityGroupIdMap().get(strEntityId);
                break;
            case "CONVERTER":
                entityId = loadContext.getConverterIdMap().get(strEntityId);
                break;
            case "INTEGRATION":
                entityId = loadContext.getIntegrationIdMap().get(strEntityId);
                break;
            default:
                log.warn("Entity type is not supported: {}", entityType);
        }
        return entityId;
    }

}
