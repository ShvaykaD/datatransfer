package org.thingsboard.datatransfer.importing.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.id.CustomerId;
import org.thingsboard.server.common.data.security.DeviceCredentials;
import org.thingsboard.server.common.data.security.DeviceCredentialsType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

@Slf4j
public class ImportDevices {

    public static final String NULL_UUID = "13814000-1dd2-11b2-8080-808080808080";

    private final ObjectMapper mapper;
    private final RestClient tbRestClient;
    private final String basePath;

    public ImportDevices(RestClient tbRestClient, ObjectMapper mapper, String basePath) {
        this.tbRestClient = tbRestClient;
        this.mapper = mapper;
        this.basePath = basePath;
    }

    public void saveTenantDevices(Map<String, CustomerId> customerIdMap) {
        try {
            String content = new String(Files.readAllBytes(Paths.get(basePath + "Devices.json")));
            JsonNode jsonNode = mapper.readTree(content);
            if (jsonNode.isArray()) {
                for (JsonNode node : jsonNode) {
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

                    String strCustomerId = node.get("customerId").get("id").asText();
                    if (!strCustomerId.equals(NULL_UUID)) {
                        if (customerIdMap.containsKey(strCustomerId)) {
                            tbRestClient.assignDevice(customerIdMap.get(strCustomerId), savedDevice.getId());
                        } else {
                            tbRestClient.assignDeviceToPublicCustomer(savedDevice.getId());
                        }

                    }
                    DeviceCredentials deviceCredentialsOptional = tbRestClient.getCredentials(savedDevice.getId());
                    if (node.get("credentialsType").asText().equals(DeviceCredentialsType.ACCESS_TOKEN.name())) {
                        deviceCredentialsOptional.setCredentialsType(DeviceCredentialsType.ACCESS_TOKEN);
                    } else {
                        deviceCredentialsOptional.setCredentialsType(DeviceCredentialsType.X509_CERTIFICATE);
                    }
                    deviceCredentialsOptional.setCredentialsId(node.get("credentialsId").asText());
                    deviceCredentialsOptional.setCredentialsValue(node.get("credentialsValue").asText());
                    tbRestClient.saveDeviceCredentials(deviceCredentialsOptional);
                }
            }
        } catch (IOException e) {
            log.warn("");
        }
    }

}
