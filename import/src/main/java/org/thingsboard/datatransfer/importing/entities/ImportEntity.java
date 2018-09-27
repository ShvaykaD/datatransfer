package org.thingsboard.datatransfer.importing.entities;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.datatransfer.importing.LoadContext;
import org.thingsboard.server.common.data.id.EntityId;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Slf4j
abstract class ImportEntity {

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

    EntityId getEntityId(LoadContext loadContext, JsonNode node, String entityType) {
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
            case "ENTITY_GROUP":
                entityId = loadContext.getEntityGroupIdMap().get(node.get("entityId").asText());
                break;
            case "CONVERTER":
                entityId = loadContext.getConverterIdMap().get(node.get("entityId").asText());
                break;
            default:
                log.warn("Entity type is not supported: {}", entityType);
        }
        return entityId;
    }

}
