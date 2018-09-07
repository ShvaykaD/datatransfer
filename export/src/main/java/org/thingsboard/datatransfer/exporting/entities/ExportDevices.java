package org.thingsboard.datatransfer.exporting.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.security.DeviceCredentials;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

@Slf4j
public class ExportDevices {

    private final ObjectMapper mapper;
    private final RestClient tbRestClient;
    private final String basePath;

    public ExportDevices(RestClient tbRestClient, ObjectMapper mapper, String basePath) {
        this.tbRestClient = tbRestClient;
        this.mapper = mapper;
        this.basePath = basePath;
    }

    public void getTenantDevices() {
        Optional<JsonNode> devicesOptional = tbRestClient.findTenantDevices(1000);
        BufferedWriter writer;
        try {
            writer = new BufferedWriter(new FileWriter(new File(basePath + "Devices.json")));
            if (devicesOptional.isPresent()) {
                ArrayNode deviceArray = (ArrayNode) devicesOptional.get().get("data");
                for (JsonNode deviceNode : deviceArray) {
                    String strDeviceId = deviceNode.get("id").get("id").asText();
                    DeviceCredentials deviceCredentials = tbRestClient.getCredentials(new DeviceId(UUID.fromString(strDeviceId)));
                    ObjectNode deviceNodeWithCredentials = (ObjectNode) deviceNode;
                    deviceNodeWithCredentials.put("credentialsType", deviceCredentials.getCredentialsType().name());
                    deviceNodeWithCredentials.put("credentialsId", deviceCredentials.getCredentialsId());
                    deviceNodeWithCredentials.put("credentialsValue", deviceCredentials.getCredentialsValue());
                }
                writer.write(mapper.writeValueAsString(deviceArray));
            }
            writer.close();
        } catch (IOException e) {
            log.warn("");
        }
    }

}
