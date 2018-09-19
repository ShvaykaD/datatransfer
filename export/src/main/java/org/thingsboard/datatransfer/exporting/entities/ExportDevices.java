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

    public void getTenantDevices(ArrayNode relationsArray, ArrayNode telemetryArray, ArrayNode attributesArray, int limit) {
        Optional<JsonNode> devicesOptional = tbRestClient.findTenantDevices(limit);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(basePath + "Devices.json")))) {
            if (devicesOptional.isPresent()) {
                ArrayNode deviceArray = (ArrayNode) devicesOptional.get().get("data");
                String strFromType = "DEVICE";
                for (JsonNode deviceNode : deviceArray) {
                    String strDeviceId = deviceNode.get("id").get("id").asText();
                    addRelationToNode(relationsArray, strDeviceId, strFromType);
                    addDeviceCredentialsToDeviceNode((ObjectNode) deviceNode, strDeviceId);

                    StringBuilder telemetryKeys = getTelemetryKeys(strFromType, strDeviceId);

                    if (telemetryKeys != null && telemetryKeys.length() != 0) {
                        Optional<JsonNode> telemetryNodeOptional = tbRestClient.getTelemetry(strFromType, strDeviceId,
                                telemetryKeys.toString(), limit, 0L, System.currentTimeMillis());
                        telemetryNodeOptional.ifPresent(jsonNode ->
                                telemetryArray.add(createNode(strFromType, strDeviceId, jsonNode, "telemetry")));
                    }
                    Optional<JsonNode> attributesOptional = tbRestClient.getAttributes(strFromType, strDeviceId);
                    attributesOptional.ifPresent(jsonNode ->
                            attributesArray.add(createNode(strFromType, strDeviceId, jsonNode, "attributes")));

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
