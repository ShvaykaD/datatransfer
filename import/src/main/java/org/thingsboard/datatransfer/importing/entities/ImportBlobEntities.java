package org.thingsboard.datatransfer.importing.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.importing.LoadContext;
import org.thingsboard.server.common.data.blob.BlobEntity;
import org.thingsboard.server.common.data.id.CustomerId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

import static org.thingsboard.datatransfer.importing.Import.NULL_UUID;

@Slf4j
public class ImportBlobEntities extends ImportEntity {

    private final RestClient tbRestClient;

    public ImportBlobEntities(RestClient tbRestClient, ObjectMapper mapper, String basePath) {
        super(mapper, basePath);
        this.tbRestClient = tbRestClient;
    }

    public void saveTenantBlobEntities(LoadContext loadContext) {
        JsonNode blobEntitiesNode = readFileContentToNode("BlobEntities.json");
        if (blobEntitiesNode != null) {
            for (JsonNode blobEntityNode : blobEntitiesNode) {
                BlobEntity savedBlobEntity = createBlobEntity(loadContext, blobEntityNode);
                loadContext.getBlobEntityIdMap().put(blobEntityNode.get("id").get("id").asText(), savedBlobEntity.getId());
            }
        }
    }

    private BlobEntity createBlobEntity(LoadContext loadContext, JsonNode blobEntityNode) {
        BlobEntity blobEntity = new BlobEntity();
        try {
            blobEntity.setData(ByteBuffer.wrap(blobEntityNode.get("data").binaryValue()));
        } catch (IOException e) {
            log.warn("Could not get data as binary...");
        }
        blobEntity.setContentType(blobEntityNode.get("contentType").asText());
        blobEntity.setName(blobEntityNode.get("name").asText());
        blobEntity.setType("report");

        String strCustomerId = blobEntityNode.get("customerId").get("id").asText();
        if (strCustomerId.equals(NULL_UUID)) {
            blobEntity.setCustomerId(new CustomerId(UUID.fromString(NULL_UUID)));
        } else {
            blobEntity.setCustomerId(loadContext.getCustomerIdMap().get(strCustomerId));
        }
        return tbRestClient.createBlobEntity(blobEntity);
    }
}
