package entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.server.common.data.Customer;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.id.CustomerId;
import org.thingsboard.server.common.data.security.DeviceCredentials;
import org.thingsboard.server.common.data.security.DeviceCredentialsType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class ImportCustomers {

    private final ObjectMapper mapper;
    private final RestClient tbRestClient;
    private final String basePath;

    public ImportCustomers(RestClient tbRestClient, ObjectMapper mapper, String basePath) {
        this.tbRestClient = tbRestClient;
        this.mapper = mapper;
        this.basePath = basePath;
    }

    public void saveTenantCustomers(Map<String, CustomerId> customerIdMap) {
        try {
            String content = new String(Files.readAllBytes(Paths.get(basePath + "Customers.json")));
            JsonNode jsonNode = mapper.readTree(content);
            if (jsonNode.isArray()) {
                for (JsonNode node : jsonNode) {
                    if (!node.get("additionalInfo").has("isPublic")) {
                        Customer customer = tbRestClient.createCustomer(node.get("title").asText());
                        customerIdMap.put(node.get("id").get("id").asText(), customer.getId());
                    }
                }
            }
        } catch (IOException e) {
            log.warn("");
        }
    }

}
