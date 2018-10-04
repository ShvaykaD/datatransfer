package org.thingsboard.datatransfer.exporting;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.Data;
import org.thingsboard.server.common.data.id.ConverterId;
import org.thingsboard.server.common.data.id.IntegrationId;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class SaveContext {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final ArrayNode relationsArray = MAPPER.createArrayNode();
    private final ArrayNode telemetryArray = MAPPER.createArrayNode();
    private final ArrayNode attributesArray = MAPPER.createArrayNode();
    private final ArrayNode entityGroups = MAPPER.createArrayNode();
    private final ArrayNode entitiesInGroups = MAPPER.createArrayNode();

    private final ObjectNode relatedIntegrationsToConverterNode = MAPPER.createObjectNode();
}
