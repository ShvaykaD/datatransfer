package org.thingsboard.datatransfer.exporting.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.exporting.Client;
import org.thingsboard.datatransfer.exporting.Export;
import org.thingsboard.datatransfer.exporting.SaveContext;

import java.util.Optional;

@Slf4j
public class ExportCustomers extends ExportEntity {

    public ExportCustomers(RestClient tbRestClient, ObjectMapper mapper, String basePath, Client httpClient) {
        super(tbRestClient, mapper, basePath, httpClient);
    }

    public void getTenantCustomers(SaveContext saveContext, int limit) {
        Optional<JsonNode> customersOptional = tbRestClient.findTenantCustomers(limit);

        if (customersOptional.isPresent()) {
            JsonNode customerArray = customersOptional.get().get("data");
            processEntityNodes(saveContext, limit, customerArray, "CUSTOMER");
            Export.writeToFile("Customers.json", customerArray);
        }
    }

}
