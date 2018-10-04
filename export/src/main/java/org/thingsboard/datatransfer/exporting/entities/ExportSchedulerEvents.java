package org.thingsboard.datatransfer.exporting.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.exporting.SaveContext;
import org.thingsboard.server.common.data.id.SchedulerEventId;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Optional;

@Slf4j
public class ExportSchedulerEvents extends ExportEntity {

    private ArrayNode schedulerEventsArrayNode;

    public ExportSchedulerEvents(RestClient tbRestClient, ObjectMapper mapper, String basePath) {
        super(tbRestClient, mapper, basePath, true);
        schedulerEventsArrayNode = mapper.createArrayNode();
    }

    public void getSchedulerEvents(SaveContext saveContext) {
        Optional<JsonNode> schedulerEventsOptional = tbRestClient.findSchedulerEvent();

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(basePath + "SchedulerEvents.json")))) {
            if (schedulerEventsOptional.isPresent()) {
                ArrayNode schedulerEventsNode = (ArrayNode) schedulerEventsOptional.get();
                String strFromType = "SCHEDULER_EVENT";
                for (JsonNode node : schedulerEventsNode) {
                    String strSchedulerEventId = node.get("id").get("id").asText();
                    addRelationToNode(saveContext.getRelationsArray(), strSchedulerEventId, strFromType);
                    Optional<JsonNode> schedulerEventOptional = tbRestClient.getSchedulerEventById(SchedulerEventId.fromString(strSchedulerEventId));
                    if (schedulerEventOptional.isPresent()) {
                        JsonNode savedSchedulerEvent = schedulerEventOptional.get();
                        schedulerEventsArrayNode.add(savedSchedulerEvent);
                    }
                }
                writer.write(mapper.writeValueAsString(schedulerEventsArrayNode));
            }
        } catch (IOException e) {
            log.warn("Could not export dashboards to file.");
        }
    }
}
