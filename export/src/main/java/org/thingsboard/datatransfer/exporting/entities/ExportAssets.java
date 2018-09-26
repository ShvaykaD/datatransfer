package org.thingsboard.datatransfer.exporting.entities;

/**
 * Created by mshvayka on 11.09.18.
 */

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.exporting.SaveContext;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Optional;

@Slf4j
public class ExportAssets extends ExportEntity {

    public ExportAssets(RestClient tbRestClient, ObjectMapper mapper, String basePath, boolean isPe) {
        super(tbRestClient, mapper, basePath, isPe);
    }

    public void getTenantAssets(SaveContext saveContext, int limit) {
        Optional<JsonNode> assetsOptional = tbRestClient.findTenantAssets(limit);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(basePath + "Assets.json")))) {
            if (assetsOptional.isPresent()) {
                ArrayNode assetsArray = (ArrayNode) assetsOptional.get().get("data");
                String strFromType = "ASSET";
                for (JsonNode assetNode : assetsArray) {
                    String strAssetId = assetNode.get("id").get("id").asText();
                    addRelationToNode(saveContext.getRelationsArray(), strAssetId, strFromType);

                    StringBuilder telemetryKeys = getTelemetryKeys(strFromType, strAssetId);

                    if (telemetryKeys != null && telemetryKeys.length() != 0) {
                        Optional<JsonNode> telemetryNodeOptional = tbRestClient.getTelemetry(strFromType, strAssetId,
                                telemetryKeys.toString(), limit, 0L, System.currentTimeMillis());
                        telemetryNodeOptional.ifPresent(jsonNode ->
                                saveContext.getTelemetryArray().add(createNode(strFromType, strAssetId, jsonNode, "telemetry")));
                    }
                    ObjectNode attributesNode = getAttributes(strFromType, strAssetId);
                    if (attributesNode != null) {
                        saveContext.getAttributesArray().add(attributesNode);
                    }
                }
                writer.write(mapper.writeValueAsString(assetsArray));
            }
        } catch (IOException e) {
            log.warn("Could not export assets to file.");
        }
    }

}
