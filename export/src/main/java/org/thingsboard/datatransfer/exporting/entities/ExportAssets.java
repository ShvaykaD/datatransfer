package org.thingsboard.datatransfer.exporting.entities;

/**
 * Created by mshvayka on 11.09.18.
 */

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.exporting.Client;
import org.thingsboard.datatransfer.exporting.Export;
import org.thingsboard.datatransfer.exporting.SaveContext;

import java.util.Optional;

@Slf4j
public class ExportAssets extends ExportEntity {

    public ExportAssets(RestClient tbRestClient, ObjectMapper mapper, String basePath, Client httpClient) {
        super(tbRestClient, mapper, basePath, httpClient);
    }

    public void getTenantAssets(SaveContext saveContext, int limit) {
        Optional<JsonNode> assetsOptional = tbRestClient.findTenantAssets(limit);

        if (assetsOptional.isPresent()) {
            JsonNode assetsArray = assetsOptional.get().get("data");
            processEntityNodes(saveContext, limit, assetsArray, "ASSET");
            Export.writeToFile("Assets.json", assetsArray);
        }
    }

}
