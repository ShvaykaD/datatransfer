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
public class ExportDevices extends ExportEntity {

    public ExportDevices(RestClient tbRestClient, ObjectMapper mapper, String basePath) {
        super(tbRestClient, mapper, basePath);
    }

    public void getTenantDevices(ArrayNode relationsArray, ArrayNode telemetryArray, int limit) {
        Optional<JsonNode> devicesOptional = tbRestClient.findTenantDevices(limit);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(basePath + "Devices.json")))) {
            if (devicesOptional.isPresent()) {
                ArrayNode deviceArray = (ArrayNode) devicesOptional.get().get("data");
                String strFromType = "DEVICE";
                for (JsonNode deviceNode : deviceArray) {
                    String strDeviceId = deviceNode.get("id").get("id").asText();
                    addRelationToNode(relationsArray, strDeviceId, strFromType);
                    addDeviceCredentialsToDeviceNode((ObjectNode) deviceNode, strDeviceId);

                    StringBuilder keys = getTelemetryKeys(strFromType, strDeviceId);

                    if (keys != null && keys.length() != 0) {
                        Optional<JsonNode> telemetryNodeOptional = tbRestClient.getTelemetry(strFromType, strDeviceId,
                                keys.toString(), limit, 0L, System.currentTimeMillis());
                        telemetryNodeOptional.ifPresent(jsonNode ->
                                telemetryArray.add(createTelemetryNode(strFromType, strDeviceId, jsonNode)));
                    }
                }
                writer.write(mapper.writeValueAsString(deviceArray));
            }
        } catch (IOException e) {
            log.warn("Could not export devices to file.");
        }
    }

    private void addDeviceCredentialsToDeviceNode(ObjectNode deviceNode, String strDeviceId) {
        DeviceCredentials deviceCredentials = tbRestClient.getCredentials(new DeviceId(UUID.fromString(strDeviceId)));
        deviceNode.put("credentialsType", deviceCredentials.getCredentialsType().name());
        deviceNode.put("credentialsId", deviceCredentials.getCredentialsId());
        deviceNode.put("credentialsValue", deviceCredentials.getCredentialsValue());
    }
}
