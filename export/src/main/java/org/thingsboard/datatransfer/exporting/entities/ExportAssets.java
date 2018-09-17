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
public class ExportAssets extends ExportEntity {

    public ExportAssets(RestClient tbRestClient, ObjectMapper mapper, String basePath) {
        super(tbRestClient, mapper, basePath);
    }

    public void getTenantAssets(ArrayNode relationsArray, ArrayNode telemetryArray, int limit) {
        Optional<JsonNode> assetsOptional = tbRestClient.findTenantAssets(limit);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(basePath + "Assets.json")))) {
            if (assetsOptional.isPresent()) {
                ArrayNode assetsArray = (ArrayNode) assetsOptional.get().get("data");

                String strFromType = "ASSET";
                for (JsonNode assetNode : assetsArray) {
                    String strAssetId = assetNode.get("id").get("id").asText();
                    addRelationToNode(relationsArray, strAssetId, strFromType);

                    StringBuilder keys = getTelemetryKeys(strFromType, strAssetId);

                    if (keys != null && keys.length() != 0) {
                        Optional<JsonNode> telemetryNodeOptional = tbRestClient.getTelemetry(strFromType, strAssetId,
                                keys.toString(), limit, 0L, System.currentTimeMillis());
                        telemetryNodeOptional.ifPresent(jsonNode ->
                                telemetryArray.add(createTelemetryNode(strFromType, strAssetId, jsonNode)));
                    }
                }
                writer.write(mapper.writeValueAsString(assetsArray));
            }
        } catch (IOException e) {
            log.warn("Could not export assets to file.");
        }
    }

}
