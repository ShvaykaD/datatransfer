package org.thingsboard.datatransfer.exporting.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.exporting.Export;
import org.thingsboard.datatransfer.exporting.SaveContext;

import java.util.Optional;

@Slf4j
public class ExportIntegrations extends ExportEntity {

    public ExportIntegrations(RestClient tbRestClient, ObjectMapper mapper, String basePath) {
        super(tbRestClient, mapper, basePath);
    }

    public void getIntegrations(SaveContext saveContext, int limit) {
        Optional<JsonNode> integrationsOptional = tbRestClient.findIntegrations(limit);

        if (integrationsOptional.isPresent()) {
            JsonNode integrationsArray = integrationsOptional.get().get("data");
            processEntityNodes(saveContext, limit, integrationsArray, "INTEGRATION");
            Export.writeToFile("Integrations.json", integrationsArray);
        }
    }
}
