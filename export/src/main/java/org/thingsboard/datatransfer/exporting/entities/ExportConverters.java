package org.thingsboard.datatransfer.exporting.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.exporting.SaveContext;
import org.thingsboard.server.common.data.id.ConverterId;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

/**
 * Created by mshvayka on 13.09.18.
 */
@Slf4j
public class ExportConverters extends ExportEntity {

    private final ArrayNode converterArrayNode;

    public ExportConverters(RestClient tbRestClient, ObjectMapper mapper, String basePath) {
        super(tbRestClient, mapper, basePath, false);
        converterArrayNode = mapper.createArrayNode();
    }

    public void getConverters(SaveContext saveContext, int limit) {
        Optional<JsonNode> convertersOptional = tbRestClient.findConverters(limit);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(basePath + "Converters.json")))) {
            if (convertersOptional.isPresent()) {
                ArrayNode convertersArray = (ArrayNode) convertersOptional.get().get("data");
                String strFromType = "CONVERTER";
                for (JsonNode node : convertersArray) {
                    String strConverterId = node.get("id").get("id").asText();
                    addRelationToNode(saveContext.getRelationsArray(), strConverterId, strFromType);

                    ObjectNode converterNode = (ObjectNode) node;
                    if (saveContext.getRelatedIntegrationsToConverterNode().has(strConverterId)) {
                        converterNode.put("integrationId", saveContext.getRelatedIntegrationsToConverterNode().get(strConverterId));
                    }
                    converterArrayNode.add(converterNode);

                    StringBuilder telemetryKeys = getTelemetryKeys(strFromType, strConverterId);

                    if (telemetryKeys != null && telemetryKeys.length() != 0) {
                        Optional<JsonNode> telemetryNodeOptional = tbRestClient.getTelemetry(strFromType, strConverterId,
                                telemetryKeys.toString(), limit, 0L, System.currentTimeMillis());
                        telemetryNodeOptional.ifPresent(jsonNode ->
                                saveContext.getTelemetryArray().add(createNode(strFromType, strConverterId, jsonNode, "telemetry")));
                    }
                    ObjectNode attributesNode = getAttributes(strFromType, strConverterId);
                    if (attributesNode != null) {
                        saveContext.getAttributesArray().add(attributesNode);
                    }

                }
                writer.write(mapper.writeValueAsString(converterArrayNode));
            }
        } catch (IOException e) {
            log.warn("Could not export dashboards to file.");
        }
    }
}
