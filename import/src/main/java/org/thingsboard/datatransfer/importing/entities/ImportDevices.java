package org.thingsboard.datatransfer.importing.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.importing.LoadContext;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.id.CustomerId;
import org.thingsboard.server.common.data.security.DeviceCredentials;
import org.thingsboard.server.common.data.security.DeviceCredentialsType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;

import static org.thingsboard.datatransfer.importing.Import.NULL_UUID;

@Slf4j
public class ImportDevices {

    private final ObjectMapper mapper;
    private final RestClient tbRestClient;
    private final String basePath;
    private final boolean emptyDb;

    public ImportDevices(RestClient tbRestClient, ObjectMapper mapper, String basePath, boolean emptyDb) {
        this.tbRestClient = tbRestClient;
        this.mapper = mapper;
        this.basePath = basePath;
        this.emptyDb = emptyDb;
    }

    public void saveTenantDevices(LoadContext loadContext) {
        JsonNode jsonNode = null;
        try {
            jsonNode = mapper.readTree(new String(Files.readAllBytes(Paths.get(basePath + "Devices.json"))));
        } catch (IOException e) {
            log.warn("Could not read devices file");
        }
        if (jsonNode != null) {
            for (JsonNode node : jsonNode) {
                if (!emptyDb) {
                    Optional<Device> deviceOptional = tbRestClient.findDevice(node.get("name").asText());
                    deviceOptional.ifPresent(device -> tbRestClient.deleteDevice(device.getId()));
                }
                Device device = createDevice(node);
                loadContext.getDeviceIdMap().put(node.get("id").get("id").asText(), device.getId());

                assignDeviceToCustomer(loadContext, node, device);
                createCredentialsForDevice(node, device);
            }
        }
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
