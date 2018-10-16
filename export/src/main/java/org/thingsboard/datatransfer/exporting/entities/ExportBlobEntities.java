package org.thingsboard.datatransfer.exporting.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.exporting.Export;

import java.util.Optional;

@Slf4j
public class ExportBlobEntities extends ExportEntity {

    public ExportBlobEntities(RestClient tbRestClient, ObjectMapper mapper, String basePath) {
        super(tbRestClient, mapper, basePath);
    }

    public void getTenantBlobEntities(int limit) {
        Optional<JsonNode> blobEntitiesOptional = tbRestClient.findBlobEntities(limit);
        blobEntitiesOptional.ifPresent(jsonNode -> Export.writeToFile("BlobEntities.json", jsonNode.get("data")));
    }
}
