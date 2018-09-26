package org.thingsboard.datatransfer.exporting;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import lombok.Data;

@Data
public class SaveContext {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final ArrayNode relationsArray = MAPPER.createArrayNode();
    private final ArrayNode telemetryArray = MAPPER.createArrayNode();
    private final ArrayNode attributesArray = MAPPER.createArrayNode();
    private final ArrayNode entityGroups = MAPPER.createArrayNode();
    private final ArrayNode entitiesInGroups = MAPPER.createArrayNode();

}
