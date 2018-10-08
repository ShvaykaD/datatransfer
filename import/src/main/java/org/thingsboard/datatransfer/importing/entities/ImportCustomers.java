package org.thingsboard.datatransfer.importing.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.importing.LoadContext;
import org.thingsboard.server.common.data.Customer;

import java.util.Optional;

@Slf4j
public class ImportCustomers extends ImportEntity {

    private final RestClient tbRestClient;
    private final boolean emptyDb;

    public ImportCustomers(RestClient tbRestClient, ObjectMapper mapper, String basePath, boolean emptyDb) {
        super(mapper, basePath);
        this.tbRestClient = tbRestClient;
        this.emptyDb = emptyDb;
    }

    public void saveTenantCustomers(LoadContext loadContext) {
        JsonNode customersNode = readFileContentToNode("Customers.json");
        if (customersNode != null) {
            for (JsonNode customerNode : customersNode) {
                if (!customerNode.get("additionalInfo").has("isPublic")) {
                    Customer customer = findOrCreateCustomer(customerNode.get("title").asText());
                    loadContext.getCustomerIdMap().put(customerNode.get("id").get("id").asText(), customer.getId());
                }
            }
        }
    }

    private Customer findOrCreateCustomer(String customerTitle) {
        Customer customer;
        if (emptyDb) {
            customer = tbRestClient.createCustomer(customerTitle);
        } else {
            Optional<Customer> customerOptional = tbRestClient.findCustomer(customerTitle);
            customer = customerOptional.orElseGet(() -> tbRestClient.createCustomer(customerTitle));
        }
        return customer;
    }

}
