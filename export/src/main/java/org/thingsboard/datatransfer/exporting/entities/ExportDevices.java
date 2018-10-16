package org.thingsboard.datatransfer.exporting.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.exporting.Export;
import org.thingsboard.datatransfer.exporting.SaveContext;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.security.DeviceCredentials;

import java.util.Optional;
import java.util.UUID;

@Slf4j
public class ExportDevices extends ExportEntity {

    public ExportDevices(RestClient tbRestClient, ObjectMapper mapper, String basePath) {
        super(tbRestClient, mapper, basePath);
    }

    public void getTenantDevices(SaveContext saveContext, int limit) {
        Optional<JsonNode> devicesOptional = tbRestClient.findTenantDevices(limit);

        if (devicesOptional.isPresent()) {
            JsonNode devicesArray = devicesOptional.get().get("data");
            String strFromType = "DEVICE";
            for (JsonNode deviceNode : devicesArray) {
                String strDeviceId = deviceNode.get("id").get("id").asText();

                JsonNode relationsFromEntityNode = getRelationsFromEntity(strDeviceId, strFromType);
                if (relationsFromEntityNode != null) {
                    saveContext.getRelationsArray().add(relationsFromEntityNode);
                }

                addDeviceCredentialsToDeviceNode((ObjectNode) deviceNode, strDeviceId);

                //getTelemetry(saveContext, limit, strFromType, strDeviceId);
                test(saveContext, strFromType, strDeviceId);

                ObjectNode attributesNode = getAttributes(strFromType, strDeviceId);
                if (attributesNode != null) {
                    saveContext.getAttributesArray().add(attributesNode);
                }
            }
            Export.writeToFile("Devices.json", devicesArray);
        }
    }

    private void addDeviceCredentialsToDeviceNode(ObjectNode deviceNode, String strDeviceId) {
        DeviceCredentials deviceCredentials = tbRestClient.getDeviceCredentials(new DeviceId(UUID.fromString(strDeviceId)));
        deviceNode.put("credentialsType", deviceCredentials.getCredentialsType().name());
        deviceNode.put("credentialsId", deviceCredentials.getCredentialsId());
        deviceNode.put("credentialsValue", deviceCredentials.getCredentialsValue());
    }

}
