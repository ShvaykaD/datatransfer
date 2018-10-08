package org.thingsboard.datatransfer.importing.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.importing.LoadContext;
import org.thingsboard.server.common.data.Customer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;

@Slf4j
public class ImportCustomers {

    private final RestClient tbRestClient;
    private final ObjectMapper mapper;
    private final String basePath;
    private final boolean emptyDb;

    public ImportCustomers(RestClient tbRestClient, ObjectMapper mapper, String basePath, boolean emptyDb) {
        this.tbRestClient = tbRestClient;
        this.mapper = mapper;
        this.basePath = basePath;
        this.emptyDb = emptyDb;
    }

    public void saveTenantCustomers(LoadContext loadContext) {
        JsonNode customersNode = null;
        try {
            customersNode = mapper.readTree(new String(Files.readAllBytes(Paths.get(basePath + "Customers.json"))));
        } catch (IOException e) {
            log.warn("Could not read customers file");
        }
        if (customersNode != null) {
            for (JsonNode customerNode : customersNode) {
                if (!customerNode.get("additionalInfo").has("isPublic")) {
                    String customerTitle = customerNode.get("title").asText();

                    Customer customer;
                    if (emptyDb) {
                        customer = tbRestClient.createCustomer(customerTitle);
                    } else {
                        Optional<Customer> customerOptional = tbRestClient.findCustomer(customerTitle);
                        customer = customerOptional.orElseGet(() -> tbRestClient.createCustomer(customerTitle));
                    }
                    loadContext.getCustomerIdMap().put(customerNode.get("id").get("id").asText(), customer.getId());
                }
            }
        }
    }

}
