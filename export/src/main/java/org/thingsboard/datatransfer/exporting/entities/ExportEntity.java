package org.thingsboard.datatransfer.exporting.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;

import java.util.Optional;

@Slf4j
public class ExportEntity {

    final RestClient tbRestClient;
    final ObjectMapper mapper;
    final String basePath;

    public ExportEntity(RestClient tbRestClient, ObjectMapper mapper, String basePath) {
        this.tbRestClient = tbRestClient;
        this.mapper = mapper;
        this.basePath = basePath;
    }

    void addRelationToNode(ArrayNode relationsArray, String strEntityId, String strFromType) {
        Optional<JsonNode> relationOptional = tbRestClient.getRelationByFrom(strEntityId, strFromType);
        if (relationOptional.isPresent()) {
            JsonNode node = relationOptional.get();
            if (node.isArray() && node.size() != 0) {
                relationsArray.add(node);
            }
        }
    }

    StringBuilder getTelemetryKeys(String strFromType, String strEntityId) {
        Optional<JsonNode> telemetryKeysOptional = tbRestClient.getTelemetryKeys(strFromType, strEntityId);
        if (telemetryKeysOptional.isPresent()) {
            JsonNode telemetryKeysNode = telemetryKeysOptional.get();

            StringBuilder keys = new StringBuilder();
            int i = 1;
            for (JsonNode node : telemetryKeysNode) {
                keys.append(node.asText());
                if (telemetryKeysNode.has(i)) {
                    keys.append(",");
                }
                i++;
            }
            return keys;
        }
        return null;
    }

    ObjectNode createTelemetryNode(String strFromType, String strEntityId, JsonNode node) {
        ObjectNode telemetryNode = mapper.createObjectNode();
        telemetryNode.put("entityType", strFromType);
        telemetryNode.put("entityId", strEntityId);
        telemetryNode.set("telemetry", node);
        return telemetryNode;
    }
}
