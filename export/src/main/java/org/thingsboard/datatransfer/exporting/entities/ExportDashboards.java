package org.thingsboard.datatransfer.exporting.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.server.common.data.Dashboard;
import org.thingsboard.server.common.data.id.DashboardId;

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
public class ExportDashboards {

    private ArrayNode dashboardNode;

    private final ObjectMapper mapper;
    private final RestClient tbRestClient;
    private final String basePath;

    public ExportDashboards(RestClient tbRestClient, ObjectMapper mapper, String basePath) {
        this.tbRestClient = tbRestClient;
        this.mapper = mapper;
        this.basePath = basePath;
        this.dashboardNode = mapper.createArrayNode();
    }

    public void getTenantDashboards(ArrayNode relationsArray) {
        Optional<JsonNode> dashboardsOptional = tbRestClient.findTenantDashboards(1000);
        BufferedWriter writer;
        try {
            writer = new BufferedWriter(new FileWriter(new File(basePath + "Dashboards.json")));
            if (dashboardsOptional.isPresent()) {
                ArrayNode dashboardArray = (ArrayNode) dashboardsOptional.get().get("data");
                for (JsonNode node : dashboardArray) {
                    String strDashboardId = node.get("id").get("id").asText();

                    Optional<JsonNode> dashboardOptional =  tbRestClient.getDashboardById(new DashboardId(UUID.fromString(strDashboardId)));
                    dashboardOptional.ifPresent(jsonNode -> dashboardNode.add(jsonNode));

                    String strFromType = "DASHBOARD";
                    Optional<JsonNode> relationOptional = tbRestClient.getRelationByFrom(strDashboardId, strFromType);
                    if (relationOptional.isPresent()) {
                        JsonNode jsonNode = relationOptional.get();
                        if (jsonNode.isArray() && jsonNode.size() != 0) {
                            relationsArray.add(jsonNode);
                        }
                    }
                }
                writer.write(mapper.writeValueAsString(dashboardNode));
            }
            writer.close();
        } catch (IOException e) {
            log.warn("");
        }

    }
}
