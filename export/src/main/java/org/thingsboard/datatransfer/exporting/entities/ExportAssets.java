package org.thingsboard.datatransfer.exporting.entities;

/**
 * Created by mshvayka on 11.09.18.
 */

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
public class ExportAssets {

    private final ObjectMapper mapper;
    private final RestClient tbRestClient;
    private final String basePath;

    public ExportAssets(RestClient tbRestClient, ObjectMapper mapper, String basePath) {
        this.tbRestClient = tbRestClient;
        this.mapper = mapper;
        this.basePath = basePath;
    }

    public void getTenantAssets(ArrayNode relationsArray) {
        Optional<JsonNode> assetsOptional = tbRestClient.findTenantAssets(1000);

        BufferedWriter writer;
        try {
            writer = new BufferedWriter(new FileWriter(new File(basePath + "Assets.json")));
            if (assetsOptional.isPresent()) {
                ArrayNode assetsArray = (ArrayNode) assetsOptional.get().get("data");

                String strFromType = "ASSET";
                for (JsonNode assetNode : assetsArray) {
                    String strAssetId = assetNode.get("id").get("id").asText();

                    Optional<JsonNode> relationOptional = tbRestClient.getRelationByFrom(strAssetId, strFromType);
                    if (relationOptional.isPresent()) {
                        JsonNode node = relationOptional.get();
                        if (node.isArray() && node.size() != 0) {
                            relationsArray.add(node);
                        }
                    }
                }


                writer.write(mapper.writeValueAsString(assetsArray));
            }
            writer.close();
        } catch (IOException e) {
            log.warn("");
        }

    }

}
