package org.thingsboard.datatransfer.importing.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.datatransfer.importing.Client;
import org.thingsboard.datatransfer.importing.LoadContext;
import org.thingsboard.server.common.data.id.EntityId;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Future;

import static org.thingsboard.datatransfer.importing.Import.EXECUTOR_SERVICE;
import static org.thingsboard.datatransfer.importing.Import.TB_BASE_URL;
import static org.thingsboard.datatransfer.importing.Import.TB_TOKEN;
import static org.thingsboard.datatransfer.importing.Import.THRESHOLD;

@Slf4j
public class ImportTelemetry extends ImportEntity {

    private final Client httpClient;

    public ImportTelemetry(ObjectMapper mapper, String basePath, Client httpClient) {
        super(mapper, basePath);
        this.httpClient = httpClient;
    }

    public void saveTelemetry(LoadContext loadContext) {
        File dir = new File(basePath);
        for (File file : Objects.requireNonNull(dir.listFiles())) {
            if (file.getName().endsWith("_Telemetry.json")) {
                JsonNode jsonNode = readFileContentToNode(file.getName());

                if (jsonNode != null) {
                    List<Future> resultList = new ArrayList<>();
                    for (JsonNode node : jsonNode) {
                        resultList.add(EXECUTOR_SERVICE.submit(() -> retryUntilDone(() -> {
                            JsonNode telemetryNode = node.get("telemetry");
                            String entityType = node.get("entityType").asText();
                            EntityId entityId = getEntityId(loadContext, node, entityType);
                            for (Iterator<String> iterator = telemetryNode.fieldNames(); iterator.hasNext(); ) {
                                String field = iterator.next();
                                JsonNode fieldArray = telemetryNode.get(field);
                                ArrayNode savingArray = mapper.createArrayNode();

                                for (JsonNode object : fieldArray) {
                                    if (savingArray.size() == 1000) {
                                        log.info("Pushing telemetry to {} [{}]", entityType, entityId);
                                        httpClient.sendData(TB_BASE_URL + "/api/plugins/telemetry/" + entityType + "/" +
                                                entityId.toString() + "/timeseries/data?key=" + field, savingArray, TB_TOKEN);
                                        savingArray.removeAll();
                                    }
                                    savingArray.add(object);
                                }
                                if (savingArray.size() > 0){
                                    log.info("Pushing telemetry to {} [{}]", entityType, entityId);
                                    httpClient.sendData(TB_BASE_URL + "/api/plugins/telemetry/" + entityType + "/" +
                                            entityId.toString() + "/timeseries/data?key=" + field, savingArray, TB_TOKEN);
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
        }
    }
}
