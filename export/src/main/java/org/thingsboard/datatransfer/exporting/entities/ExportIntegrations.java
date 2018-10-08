package org.thingsboard.datatransfer.exporting.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.exporting.SaveContext;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Optional;

@Slf4j
public class ExportIntegrations extends ExportEntity {

    public ExportIntegrations(RestClient tbRestClient, ObjectMapper mapper, String basePath) {
        super(tbRestClient, mapper, basePath);
    }

    public void getIntegrations(SaveContext saveContext, int limit) {
        Optional<JsonNode> integrationsOptional = tbRestClient.findIntegrations(limit);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(basePath + "Integrations.json")))) {
            if (integrationsOptional.isPresent()) {
                JsonNode integrationsArray = integrationsOptional.get().get("data");
                String strFromType = "INTEGRATION";

                processEntityNodes(saveContext, limit, integrationsArray, strFromType);

                writer.write(mapper.writeValueAsString(integrationsArray));
            }
        } catch (IOException e) {
            log.warn("Could not export devices to file.");
        }
    }

}
