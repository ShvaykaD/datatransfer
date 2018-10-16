package org.thingsboard.datatransfer.importing.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.importing.LoadContext;
import org.thingsboard.server.common.data.asset.Asset;
import org.thingsboard.server.common.data.blob.BlobEntity;

import java.nio.ByteBuffer;

@Slf4j
public class ImportBlobEntities extends ImportEntity {

    private final RestClient tbRestClient;
    private final boolean emptyDb;

    ImportBlobEntities(RestClient tbRestClient, ObjectMapper mapper, String basePath, boolean emptyDb) {
        super(mapper, basePath);
        this.tbRestClient = tbRestClient;
        this.emptyDb = emptyDb;
    }

    public void saveTenantBlobEntities(LoadContext loadContext) {
        JsonNode blobEntitiesNode = readFileContentToNode("BlobEntities.json");
        if (blobEntitiesNode != null) {
            for (JsonNode blobEntityNode : blobEntitiesNode) {

                BlobEntity blobEntity = new BlobEntity();
                /*blobEntity.setData(ByteBuffer.wrap(blobEntityNode.getData()));
                blobEntity.setContentType(reportData.getContentType());
                blobEntity.setName(reportData.getName());
                blobEntity.setType("report");


                blobEntity.setCustomerId(user.getCustomerId());*/

                BlobEntity savedBlobEntity = tbRestClient.createBlobEntity(blobEntity);

                loadContext.getBlobEntityIdMap().put(blobEntityNode.get("id").get("id").asText(), savedBlobEntity.getId());
            }
        }
    }
}
