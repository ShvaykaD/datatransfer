package org.thingsboard.datatransfer.exporting.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.exporting.Client;
import org.thingsboard.datatransfer.exporting.Export;
import org.thingsboard.server.common.data.blob.BlobEntity;
import org.thingsboard.server.common.data.id.BlobEntityId;

import java.util.Optional;

@Slf4j
public class ExportBlobEntities extends ExportEntity {

    private final ArrayNode blobEntitiesArray;

    public ExportBlobEntities(RestClient tbRestClient, ObjectMapper mapper, String basePath, Client httpClient) {
        super(tbRestClient, mapper, basePath, httpClient);
        blobEntitiesArray = mapper.createArrayNode();
    }

    public void getTenantBlobEntities(int limit) {
        Optional<JsonNode> blobEntitiesOptional = tbRestClient.findBlobEntities(limit);

        if (blobEntitiesOptional.isPresent()) {
            for (JsonNode node : blobEntitiesOptional.get().get("data")) {
                ObjectNode blobEntityDataNode = (ObjectNode) node;

                Optional<BlobEntity> blobEntityOptional = tbRestClient.getBlobEntityById(BlobEntityId.fromString(blobEntityDataNode.get("id").get("id").asText()));
                if (blobEntityOptional.isPresent()) {
                    blobEntityDataNode.put("data", blobEntityOptional.get().getData().array());
                    blobEntitiesArray.add((blobEntityDataNode));
                }
            }
        }
        Export.writeToFile("BlobEntities.json", blobEntitiesArray);
    }
}
