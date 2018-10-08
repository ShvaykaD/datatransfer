package org.thingsboard.datatransfer.exporting.entities;

/**
 * Created by mshvayka on 11.09.18.
 */

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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

    public ExportAssets(RestClient tbRestClient, ObjectMapper mapper, String basePath) {
        super(tbRestClient, mapper, basePath);
    }

    public void getTenantAssets(SaveContext saveContext, int limit) {
        Optional<JsonNode> assetsOptional = tbRestClient.findTenantAssets(limit);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(basePath + "Assets.json")))) {
            if (assetsOptional.isPresent()) {
                JsonNode assetsArray = assetsOptional.get().get("data");
                String strFromType = "ASSET";

                processEntityNodes(saveContext, limit, assetsArray, strFromType);

                writer.write(mapper.writeValueAsString(assetsArray));
            }
        } catch (IOException e) {
            log.warn("Could not export assets to file.");
        }
    }

}
