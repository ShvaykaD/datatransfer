package org.thingsboard.datatransfer.exporting.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Optional;

@Slf4j
public class ExportCustomers {

    private final ObjectMapper mapper;
    private final RestClient tbRestClient;
    private final String basePath;

    public ExportCustomers(RestClient tbRestClient, ObjectMapper mapper, String basePath) {
        this.tbRestClient = tbRestClient;
        this.mapper = mapper;
        this.basePath = basePath;
    }

    public void getTenantCustomers() {
        Optional<JsonNode> customersOptional = tbRestClient.findTenantCustomers(1000);

        BufferedWriter writer;
        try {
            writer = new BufferedWriter(new FileWriter(new File(basePath + "Customers.json")));
            if (customersOptional.isPresent()) {
                ArrayNode customerArray = (ArrayNode) customersOptional.get().get("data");
                writer.write(mapper.writeValueAsString(customerArray));
            }
            writer.close();
        } catch (IOException e) {
            log.warn("");
        }
    }

}
