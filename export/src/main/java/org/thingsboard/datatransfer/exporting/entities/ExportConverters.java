package org.thingsboard.datatransfer.exporting.entities;

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

/**
 * Created by mshvayka on 13.09.18.
 */
@Slf4j
public class ExportConverters extends ExportEntity {

    public ExportConverters(RestClient tbRestClient, ObjectMapper mapper, String basePath) {
        super(tbRestClient, mapper, basePath);
    }

    public void getConverters(SaveContext saveContext, int limit) {
        Optional<JsonNode> convertersOptional = tbRestClient.findConverters(limit);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(basePath + "Converters.json")))) {
            if (convertersOptional.isPresent()) {
                JsonNode convertersArray = convertersOptional.get().get("data");
                String strFromType = "CONVERTER";

                processEntityNodes(saveContext, limit, convertersArray, strFromType);

                writer.write(mapper.writeValueAsString(convertersArray));
            }
        } catch (IOException e) {
            log.warn("Could not export dashboards to file.");
        }
    }
}
