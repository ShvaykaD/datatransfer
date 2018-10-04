package org.thingsboard.datatransfer.exporting.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.exporting.SaveContext;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Optional;

@Slf4j
public class ExportCustomers extends ExportEntity {

    public ExportCustomers(RestClient tbRestClient, ObjectMapper mapper, String basePath, boolean isPe) {
        super(tbRestClient, mapper, basePath, isPe);
    }

    public void getTenantCustomers(SaveContext saveContext, int limit) {
        Optional<JsonNode> customersOptional = tbRestClient.findTenantCustomers(limit);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(basePath + "Customers.json")))) {
            if (customersOptional.isPresent()) {
                ArrayNode customerArray = (ArrayNode) customersOptional.get().get("data");
                String strFromType = "CUSTOMER";
                for (JsonNode customerNode : customerArray) {
                    String strCustomerId = customerNode.get("id").get("id").asText();

                    addRelationToNode(saveContext.getRelationsArray(), strCustomerId, strFromType);
                    ObjectNode attributesNode = getAttributes(strFromType, strCustomerId);
                    if (attributesNode != null) {
                        saveContext.getAttributesArray().add(attributesNode);
                    }
                    StringBuilder telemetryKeys = getTelemetryKeys(strFromType, strCustomerId);

                    if (telemetryKeys != null && telemetryKeys.length() != 0) {
                        Optional<JsonNode> telemetryNodeOptional = tbRestClient.getTelemetry(strFromType, strCustomerId,
                                telemetryKeys.toString(), limit, 0L, System.currentTimeMillis());
                        telemetryNodeOptional.ifPresent(jsonNode ->
                                saveContext.getTelemetryArray().add(createNode(strFromType, strCustomerId, jsonNode, "telemetry")));
                    }
                }
                writer.write(mapper.writeValueAsString(customerArray));
            }
        } catch (IOException e) {
            log.warn("Could not export customers to file.");
        }
    }

}
