package org.thingsboard.datatransfer.exporting.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.exporting.Export;
import org.thingsboard.datatransfer.exporting.SaveContext;
import org.thingsboard.server.common.data.id.SchedulerEventId;

import java.util.Optional;

@Slf4j
public class ExportSchedulerEvents extends ExportEntity {

    private ArrayNode schedulerEventsArrayNode;

    public ExportSchedulerEvents(RestClient tbRestClient, ObjectMapper mapper, String basePath) {
        super(tbRestClient, mapper, basePath);
        schedulerEventsArrayNode = mapper.createArrayNode();
    }

    public void getSchedulerEvents(SaveContext saveContext) {
        Optional<JsonNode> schedulerEventsOptional = tbRestClient.findSchedulerEvent();
        if (schedulerEventsOptional.isPresent()) {
            JsonNode schedulerEventsNode = schedulerEventsOptional.get();
            for (JsonNode node : schedulerEventsNode) {
                String strSchedulerEventId = node.get("id").get("id").asText();

                JsonNode relationsFromEntityNode = getRelationsFromEntity(strSchedulerEventId, "SCHEDULER_EVENT");
                if (relationsFromEntityNode != null) {
                    saveContext.getRelationsArray().add(relationsFromEntityNode);
                }

                Optional<JsonNode> schedulerEventOptional = tbRestClient.getSchedulerEventById(SchedulerEventId.fromString(strSchedulerEventId));
                if (schedulerEventOptional.isPresent()) {
                    JsonNode savedSchedulerEvent = schedulerEventOptional.get();
                    schedulerEventsArrayNode.add(savedSchedulerEvent);
                }
            }
            Export.writeToFile("SchedulerEvents.json", schedulerEventsArrayNode);
        }
    }
}
