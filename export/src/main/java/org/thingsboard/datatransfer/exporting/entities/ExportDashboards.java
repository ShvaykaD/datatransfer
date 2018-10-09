package org.thingsboard.datatransfer.exporting.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.exporting.Export;
import org.thingsboard.datatransfer.exporting.SaveContext;
import org.thingsboard.server.common.data.id.DashboardId;

import java.util.Optional;

/**
 * Created by mshvayka on 13.09.18.
 */
@Slf4j
public class ExportDashboards extends ExportEntity {

    private final ArrayNode dashboardNode;

    public ExportDashboards(RestClient tbRestClient, ObjectMapper mapper, String basePath) {
        super(tbRestClient, mapper, basePath);
        dashboardNode = mapper.createArrayNode();
    }

    public void getTenantDashboards(SaveContext saveContext, int limit) {
        Optional<JsonNode> dashboardsOptional = tbRestClient.findTenantDashboards(limit);

        if (dashboardsOptional.isPresent()) {
            JsonNode dashboardArray = dashboardsOptional.get().get("data");
            for (JsonNode node : dashboardArray) {
                String strDashboardId = node.get("id").get("id").asText();

                JsonNode relationsFromEntityNode = getRelationsFromEntity(strDashboardId, "DASHBOARD");
                if (relationsFromEntityNode != null) {
                    saveContext.getRelationsArray().add(relationsFromEntityNode);
                }

                Optional<JsonNode> dashboardOptional = tbRestClient.getDashboardById(DashboardId.fromString(strDashboardId));
                dashboardOptional.ifPresent(dashboardNode::add);
            }
            Export.writeToFile("Dashboards.json", dashboardNode);
        }

    }
}
