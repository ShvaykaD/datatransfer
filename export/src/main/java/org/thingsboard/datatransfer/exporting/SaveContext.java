package org.thingsboard.datatransfer.exporting;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import lombok.Data;

@Data
public class SaveContext {

    private final ArrayNode relationsArray;
    private ArrayNode telemetryArray;
    private final ArrayNode attributesArray;
    private final ArrayNode entityGroups;
    private final ArrayNode entitiesInGroups;

    public SaveContext(ObjectMapper mapper) {
        this.relationsArray = mapper.createArrayNode();
        this.telemetryArray = mapper.createArrayNode();
        this.attributesArray = mapper.createArrayNode();
        this.entityGroups = mapper.createArrayNode();
        this.entitiesInGroups = mapper.createArrayNode();
    }
}
