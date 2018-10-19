package org.thingsboard.datatransfer.exporting.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.exporting.Client;
import org.thingsboard.datatransfer.exporting.Export;
import org.thingsboard.datatransfer.exporting.SaveContext;

import java.util.Optional;

/**
 * Created by mshvayka on 13.09.18.
 */
@Slf4j
public class ExportConverters extends ExportEntity {

    public ExportConverters(RestClient tbRestClient, ObjectMapper mapper, String basePath, Client httpClient) {
        super(tbRestClient, mapper, basePath, httpClient);
    }

    public void getConverters(SaveContext saveContext, int limit) {
        Optional<JsonNode> convertersOptional = tbRestClient.findConverters(limit);

        if (convertersOptional.isPresent()) {
            JsonNode convertersArray = convertersOptional.get().get("data");
            processEntityNodes(saveContext, limit, convertersArray, "CONVERTER");
            Export.writeToFile("Converters.json", convertersArray);
        }
    }
}
