package org.thingsboard.datatransfer.importing.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.importing.LoadContext;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.security.DeviceCredentials;
import org.thingsboard.server.common.data.security.DeviceCredentialsType;

import java.util.Optional;

import static org.thingsboard.datatransfer.importing.Import.NULL_UUID;

@Slf4j
public class ImportDevices extends ImportEntity {

    private final RestClient tbRestClient;
    private final boolean emptyDb;

    public ImportDevices(RestClient tbRestClient, ObjectMapper mapper, String basePath, boolean emptyDb) {
        super(mapper, basePath);
        this.tbRestClient = tbRestClient;
        this.emptyDb = emptyDb;
    }

    public void saveTenantDevices(LoadContext loadContext) {
        JsonNode devicesNode = readFileContentToNode("Devices.json");
        if (devicesNode != null) {
            for (JsonNode deviceNode : devicesNode) {
                Device device = findOrCreateDevice(deviceNode);
                loadContext.getDeviceIdMap().put(deviceNode.get("id").get("id").asText(), device.getId());
                assignDeviceToCustomer(loadContext, deviceNode, device);
                createCredentialsForDevice(deviceNode, device);
            }
        }
    }

    private Device findOrCreateDevice(JsonNode node) {
        Device device;
        if (emptyDb) {
            device = createDevice(node);
        } else {
            Optional<Device> deviceOptional = tbRestClient.findDevice(node.get("name").asText());
            device = deviceOptional.orElseGet(() -> createDevice(node));
        }
        return device;
    }

    private void createCredentialsForDevice(JsonNode node, Device device) {
        DeviceCredentials deviceCredentialsOptional = tbRestClient.getDeviceCredentials(device.getId());
        if (node.get("credentialsType").asText().equals(DeviceCredentialsType.ACCESS_TOKEN.name())) {
            deviceCredentialsOptional.setCredentialsType(DeviceCredentialsType.ACCESS_TOKEN);
        } else {
            deviceCredentialsOptional.setCredentialsType(DeviceCredentialsType.X509_CERTIFICATE);
        }
        deviceCredentialsOptional.setCredentialsId(node.get("credentialsId").asText());
        deviceCredentialsOptional.setCredentialsValue(node.get("credentialsValue").asText());
        tbRestClient.saveDeviceCredentials(deviceCredentialsOptional);
    }

    private void assignDeviceToCustomer(LoadContext loadContext, JsonNode node, Device savedDevice) {
        String strCustomerId = node.get("customerId").get("id").asText();
        if (!strCustomerId.equals(NULL_UUID)) {
            if (loadContext.getCustomerIdMap().containsKey(strCustomerId)) {
                tbRestClient.assignDevice(loadContext.getCustomerIdMap().get(strCustomerId), savedDevice.getId());
            } else {
                tbRestClient.assignDeviceToPublicCustomer(savedDevice.getId());
            }
        }
    }

    private Device createDevice(JsonNode node) {
        Device savedDevice;
        if (node.get("additionalInfo").has("gateway")) {
            Device device = new Device();
            device.setName(node.get("name").asText());
            device.setType(node.get("type").asText());
            device.setAdditionalInfo(mapper.createObjectNode().putObject("additionalInfo").put("gateway", true));
            savedDevice = tbRestClient.createDevice(device);
        } else {
            savedDevice = tbRestClient.createDevice(node.get("name").asText(), node.get("type").asText());
        }
        return savedDevice;
    }

}
