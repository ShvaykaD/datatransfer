package org.thingsboard.datatransfer.importing.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.server.common.data.id.AssetId;
import org.thingsboard.server.common.data.id.CustomerId;
import org.thingsboard.server.common.data.id.DeviceId;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
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
                    int code = tbRestClient.saveTelemetry(node.get("entityType").asText(), assetsIdMap.get(node.get("entityId").asText()).toString(), node.get("temp").get(0));
                    if (code == 200) {
                        System.out.println("YES!");
                    } else {
                        System.out.println("NO!");
                    }
                }
            }
        } catch (IOException e) {
            log.warn("");
        }
    }

}
