package org.thingsboard.datatransfer.exporting.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.exporting.Client;
import org.thingsboard.datatransfer.exporting.Export;
import org.thingsboard.datatransfer.exporting.SaveContext;

import java.util.Optional;

@Slf4j
public class ExportEntityViews extends ExportEntity {


    public ExportEntityViews(RestClient tbRestClient, ObjectMapper mapper, String basePath, Client httpClient) {
        super(tbRestClient, mapper, basePath, httpClient);
    }

    public void getTenantEntityViews(SaveContext saveContext, int limit) {
        Optional<JsonNode> entityViewsOptional = tbRestClient.findTenantEntityViews(limit);

        if (entityViewsOptional.isPresent()) {
            JsonNode entityViewsArray = entityViewsOptional.get().get("data");
            processEntityNodes(saveContext, limit, entityViewsArray, "ENTITY_VIEW");
            Export.writeToFile("EntityViews.json", entityViewsArray);
        }
    }

}
