package org.thingsboard.datatransfer.importing.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.importing.LoadContext;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.id.CustomerId;
import org.thingsboard.server.common.data.scheduler.SchedulerEvent;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;

import static org.thingsboard.datatransfer.importing.Import.NULL_UUID;

@Slf4j
public class ImportSchedulerEvents {

    private final ObjectMapper mapper;
    private final RestClient tbRestClient;
    private final String basePath;


    public ImportSchedulerEvents(RestClient tbRestClient, ObjectMapper mapper, String basePath) {
        this.tbRestClient = tbRestClient;
        this.mapper = mapper;
        this.basePath = basePath;
    }

    public void saveSchedulerEvents(LoadContext loadContext) {
        JsonNode jsonNode = null;
        try {
            jsonNode = mapper.readTree(new String(Files.readAllBytes(Paths.get(basePath + "SchedulerEvents.json"))));
        } catch (IOException e) {
            log.warn("Could not read scheduler events file");
        }
        if (jsonNode != null) {
            for (JsonNode node : jsonNode) {
                SchedulerEvent schedulerEvent = createSchedulerEvent(loadContext, node);
                loadContext.getSchedulerEventIdMap().put(node.get("id").get("id").asText(), schedulerEvent.getId());
            }
        }
    }


    private SchedulerEvent createSchedulerEvent(LoadContext loadContext, JsonNode node) {
        SchedulerEvent savedSchedulerEvent;
        SchedulerEvent schedulerEvent = new SchedulerEvent();
        schedulerEvent.setName(node.get("name").asText());
        schedulerEvent.setType(node.get("type").asText());
        if (!(node.get("configuration")).isNull()) {
            schedulerEvent.setConfiguration(node.get("configuration"));
        }
        /*switch (node.get("type").asText()) {
            case "generateReport":
                break;
            case "sendRpcRequest":
                break;
            case "updateAttributes":
                break;
             default:
                 log.warn("Scheduler event type is not supported: {}", node.get("type").asText());
        }
*/
        schedulerEvent.setSchedule(node.get("schedule"));
        assignSchedulerEventToCustomer(loadContext, node, schedulerEvent);
        savedSchedulerEvent = tbRestClient.createSchedulerEvent(schedulerEvent);
        return savedSchedulerEvent;

    }

    private void assignSchedulerEventToCustomer(LoadContext loadContext, JsonNode node, SchedulerEvent savedSchedulerEvent) {
        String strCustomerId = node.get("customerId").get("id").asText();
        if (!strCustomerId.equals(NULL_UUID) && loadContext.getCustomerIdMap().containsKey(strCustomerId)) {
            savedSchedulerEvent.setCustomerId(loadContext.getCustomerIdMap().get(strCustomerId));
        }
    }


}