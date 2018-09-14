package org.thingsboard.datatransfer.importing.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.server.common.data.id.AssetId;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Map;

@Slf4j
public class ImportTelemetry {

    private final ObjectMapper mapper;
    private final RestClient tbRestClient;
    private final String basePath;

    public ImportTelemetry(RestClient tbRestClient, ObjectMapper mapper, String basePath) {
        this.tbRestClient = tbRestClient;
        this.mapper = mapper;
        this.basePath = basePath;
    }

    public void saveTelemetry(Map<String, AssetId> assetsIdMap) {
        try {
            String content = new String(Files.readAllBytes(Paths.get(basePath + "Telemetry.json")));
            JsonNode jsonNode = mapper.readTree(content);
            if (jsonNode.isArray()) {
                for (JsonNode node : jsonNode) {
                    JsonNode telemetryNode = node.get("telemetry");
                    for (Iterator<String> iterator = telemetryNode.fieldNames(); iterator.hasNext(); ) {
                        String field = iterator.next();
                        JsonNode fieldArray = telemetryNode.get(field);
                        for (JsonNode object : fieldArray) {
                            ObjectNode savingNode = mapper.createObjectNode();
                            savingNode.set("ts", object.get("ts"));
                            savingNode.set("values", mapper.createObjectNode().set(field, object.get("value")));
                            tbRestClient.saveTelemetry(node.get("entityType").asText(), assetsIdMap.get(node.get("entityId").asText()).toString(), savingNode);
                        }
                    }
                }
            }
        } catch (IOException e) {
            log.warn("");
        }
    }

}
