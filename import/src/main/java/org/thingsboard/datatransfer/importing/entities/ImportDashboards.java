package org.thingsboard.datatransfer.importing.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.server.common.data.Dashboard;
import org.thingsboard.server.common.data.id.CustomerId;
import org.thingsboard.server.common.data.id.DashboardId;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

/**
 * Created by mshvayka on 13.09.18.
 */
@Slf4j
public class ImportDashboards {

    private final ObjectMapper mapper;
    private final RestClient tbRestClient;
    private final String basePath;

    public ImportDashboards(RestClient tbRestClient, ObjectMapper mapper, String basePath) {
        this.tbRestClient = tbRestClient;
        this.mapper = mapper;
        this.basePath = basePath;
    }


    public void saveTenantDashboards(Map<String, CustomerId> customersIdMap, Map<String, DashboardId> dashboardIdMap) {
        try {
            String content = new String(Files.readAllBytes(Paths.get(basePath + "Dashboards.json")));
            JsonNode jsonNode = mapper.readTree(content);
            if (jsonNode.isArray()) {
                for (JsonNode node : jsonNode) {
                    Dashboard dashboard = new Dashboard();
                    dashboard.setTitle(node.get("title").asText());
                    if (node.get("configuration") != null) {
                        dashboard.setConfiguration(node);
                    }
                    Dashboard savedDashboard = tbRestClient.createDashboard(dashboard);
                    dashboardIdMap.put(node.get("id").get("id").asText(), savedDashboard.getId());
                    if (node.get("assignedCustomers") != null) {
                        ArrayNode customersArray = (ArrayNode) node.get("assignedCustomers");
                        for (JsonNode customerNode : customersArray) {
                            if (customerNode.get("public").asBoolean()) {
                                tbRestClient.assignDashboardToPublicCustomer(savedDashboard.getId());
                            }else{
                                tbRestClient.assignDashboard(customersIdMap.get(customerNode.get("customerId").get("id").asText()), savedDashboard.getId());
                            }
                        }
                    }

                }
            }
        } catch (IOException e) {
            log.warn("");
        }

    }
}
