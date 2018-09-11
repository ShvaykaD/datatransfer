package org.thingsboard.datatransfer.importing.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.server.common.data.Customer;
import org.thingsboard.server.common.data.id.CustomerId;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class ImportCustomers {

    private final ObjectMapper mapper;
    private final RestClient tbRestClient;
    private final String basePath;
    private final boolean emptyDb;

    public ImportCustomers(RestClient tbRestClient, ObjectMapper mapper, String basePath, boolean emptyDb) {
        this.tbRestClient = tbRestClient;
        this.mapper = mapper;
        this.basePath = basePath;
        this.emptyDb = emptyDb;
    }

    public void saveTenantCustomers(Map<String, CustomerId> customerIdMap) {
        try {
            String content = new String(Files.readAllBytes(Paths.get(basePath + "Customers.json")));
            JsonNode jsonNode = mapper.readTree(content);
            if (jsonNode.isArray()) {
                for (JsonNode node : jsonNode) {
                    if (!node.get("additionalInfo").has("isPublic")) {
                        if (!emptyDb) {
                            Optional<Customer> customerOptional = tbRestClient.findCustomer(node.get("title").asText());
                            customerOptional.ifPresent(customer -> tbRestClient.deleteCustomer(customer.getId()));
                        }
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
