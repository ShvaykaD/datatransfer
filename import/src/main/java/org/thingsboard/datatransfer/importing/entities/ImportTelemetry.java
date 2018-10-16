package org.thingsboard.datatransfer.importing.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.datatransfer.importing.Client;
import org.thingsboard.datatransfer.importing.LoadContext;
import org.thingsboard.server.common.data.id.EntityId;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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
        for (File file : dir.listFiles()) {
            if (file.getName().endsWith("_Telemetry.json")) {
                JsonNode jsonNode = readFileContentToNode(file.getName());

                if (jsonNode != null) {
                    List<Future> resultList = new ArrayList<>();
                    for (JsonNode node : jsonNode) {
                        JsonNode telemetryNode = node.get("telemetry");
                        for (Iterator<String> iterator = telemetryNode.fieldNames(); iterator.hasNext(); ) {
                            String field = iterator.next();
                            JsonNode fieldArray = telemetryNode.get(field);
                            for (JsonNode object : fieldArray) {
                                resultList.add(EXECUTOR_SERVICE.submit(() -> retryUntilDone(() -> {
                                    ObjectNode savingNode = createSavingNode(field, object);
                                    String entityType = node.get("entityType").asText();
                                    EntityId entityId = getEntityId(loadContext, node, entityType);
                                    log.info("Pushing telemetry to {} [{}]", entityType, entityId);
                                    httpClient.sendData(TB_BASE_URL + "/api/plugins/telemetry/" + entityType + "/" +
                                            entityId.toString() + "/timeseries/data", savingNode, TB_TOKEN);
                                    return true;
                                })));
                            }
                        }

                        if (resultList.size() > THRESHOLD) {
                            waitForPack(resultList);
                        }
                    }
                    waitForPack(resultList);
                }
            }
        }
    }


    private ObjectNode createSavingNode(String field, JsonNode object) {
        ObjectNode savingNode = mapper.createObjectNode();
        savingNode.set("ts", object.get("ts"));
        savingNode.set("values", mapper.createObjectNode().set(field, object.get("value")));
        return savingNode;
    }
}
