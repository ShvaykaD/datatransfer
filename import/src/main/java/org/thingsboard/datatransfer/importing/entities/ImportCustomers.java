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
                    Customer customer = findOrCreateCustomer(customerNode);
                    loadContext.getCustomerIdMap().put(customerNode.get("id").get("id").asText(), customer.getId());
                }
            }
        }
    }

    private Customer findOrCreateCustomer(JsonNode node) {
        Customer customer;
        if (emptyDb) {
            customer = createCustomer(node);
        } else {
            Optional<Customer> customerOptional = tbRestClient.findCustomer(node.get("title").asText());
            customer = customerOptional.orElseGet(() -> createCustomer(node));
        }
        return customer;
    }


    private Customer createCustomer(JsonNode node) {
        Customer savedCustomer;
        Customer customer = new Customer();
        customer.setTitle(node.get("title").asText());
        if (node.hasNonNull("additionalInfo")) {
            customer.setAdditionalInfo(node.get("additionalInfo"));
        }
        if (node.hasNonNull("country")) {
            customer.setCountry(node.get("country").asText());
        }
        if (node.hasNonNull("state")) {
            customer.setState(node.get("state").asText());
        }
        if (node.hasNonNull("city")) {
            customer.setCity(node.get("city").asText());
        }
        if (node.hasNonNull("address")) {
            customer.setAddress(node.get("address").asText());
        }
        if (node.hasNonNull("address2")) {
            customer.setAddress2(node.get("address2").asText());
        }
        if (node.hasNonNull("zip")) {
            customer.setAddress2(node.get("zip").asText());
        }
        if (node.hasNonNull("phone")) {
            customer.setAddress2(node.get("phone").asText());
        }
        if (node.hasNonNull("email")) {
            customer.setAddress2(node.get("email").asText());
        }
        savedCustomer = tbRestClient.createCustomer(customer);
        return savedCustomer;
    }
}

