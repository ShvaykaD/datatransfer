package org.thingsboard.datatransfer.importing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.importing.entities.*;
import org.thingsboard.server.common.data.id.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class Import {

    private static final Map<String, CustomerId> CUSTOMERS_MAP = new HashMap<>();
    private static final Map<String, AssetId> ASSETS_MAP = new HashMap<>();
    private static final Map<String, DeviceId> DEVICES_MAP = new HashMap<>();
    private static final Map<String, DashboardId> DASHBOARD_MAP = new HashMap<>();
    public static final String NULL_UUID = "13814000-1dd2-11b2-8080-808080808080";

    public static String BASE_PATH;
    public static String TB_BASE_URL;
    public static String TB_TOKEN;
    public static boolean EMPTY_DB;

    public static int THRESHOLD;
    public static ExecutorService EXECUTOR_SERVICE;

    public static void main(String[] args) {
        ObjectMapper mapper = new ObjectMapper();
        Properties properties = new Properties();
        String filename;
        if (args.length > 0) {
            filename = args[0];
        } else {
            filename = "config.properties";
        }
        try (InputStream input = new FileInputStream(filename)) {
            properties.load(input);

            THRESHOLD = Integer.parseInt(properties.getProperty("threshold"));
            EXECUTOR_SERVICE = Executors.newFixedThreadPool(Integer.parseInt(properties.getProperty("threadsCount")));

            BASE_PATH = properties.getProperty("basePath");
            TB_BASE_URL = properties.getProperty("tbBaseURL");
            RestClient tbRestClient = new RestClient(TB_BASE_URL);
            tbRestClient.login(properties.getProperty("tbLogin"), properties.getProperty("tbPassword"));
            TB_TOKEN = tbRestClient.getToken();
            EMPTY_DB = Boolean.parseBoolean(properties.getProperty("emptyDb"));

            log.info("Start importing...");

            ImportCustomers customers = new ImportCustomers(tbRestClient, mapper, BASE_PATH, EMPTY_DB);
            customers.saveTenantCustomers(CUSTOMERS_MAP);

            ImportDevices devices = new ImportDevices(tbRestClient, mapper, BASE_PATH, EMPTY_DB);
            devices.saveTenantDevices(CUSTOMERS_MAP, DEVICES_MAP);

            ImportAssets assets = new ImportAssets(tbRestClient, mapper, BASE_PATH, EMPTY_DB);
            assets.saveTenantAssets(CUSTOMERS_MAP, ASSETS_MAP);

            ImportDashboards dashboards = new ImportDashboards(tbRestClient, mapper, BASE_PATH);
            dashboards.saveTenantDashboards(CUSTOMERS_MAP, DASHBOARD_MAP);

            ImportTelemetry telemetry = new ImportTelemetry(tbRestClient, mapper, BASE_PATH);
            telemetry.saveTelemetry(ASSETS_MAP);


            try {
                String content = new String(Files.readAllBytes(Paths.get(BASE_PATH + "Relations.json")));
                JsonNode jsonNode = mapper.readTree(content);
                if (jsonNode.isArray()) {
                    for (JsonNode nodeArray : jsonNode) {
                        for (JsonNode node : nodeArray) {
                            String fromType = node.get("from").get("entityType").asText();
                            String fromId = node.get("from").get("id").asText();
                            String toType = node.get("to").get("entityType").asText();
                            String toId = node.get("to").get("id").asText();
                            String relationType = node.get("type").asText();
                            EntityId entityId;
                            switch (fromType) {
                                case "CUSTOMER":
                                    CustomerId customerId = CUSTOMERS_MAP.get(fromId);
                                    entityId = getToEntityId(toType, toId);
                                    tbRestClient.makeRelation(relationType, customerId, entityId);
                                    break;
                                case "ASSET":
                                    AssetId assetId = ASSETS_MAP.get(fromId);
                                    entityId = getToEntityId(toType, toId);
                                    tbRestClient.makeRelation(relationType, assetId, entityId);
                                    break;
                                case "DEVICE":
                                    DeviceId deviceId = DEVICES_MAP.get(fromId);
                                    entityId = getToEntityId(toType, toId);
                                    tbRestClient.makeRelation(relationType, deviceId, entityId);
                                    break;
                                case "DASHBOARD":
                                    DashboardId dashboardId = DASHBOARD_MAP.get(fromId);
                                    entityId = getToEntityId(toType, toId);
                                    tbRestClient.makeRelation(relationType, dashboardId, entityId);
                                    break;
                                default:
                                    log.warn("ERROR!!!");
                            }
                        }
                    }
                }
            } catch (IOException e) {
                log.warn("");
            }


            log.info("Ended importing successfully!");
            EXECUTOR_SERVICE.shutdown();
        } catch (Exception e) {
            log.error("Could not read properties file {}", filename, e);
        }
    }

    private static EntityId getToEntityId(String toType, String toId) {
        EntityId entityId;
        switch (toType) {
            case "CUSTOMER":
                entityId = CUSTOMERS_MAP.get(toId);
                break;
            case "ASSET":
                entityId = ASSETS_MAP.get(toId);
                break;
            case "DEVICE":
                entityId = DEVICES_MAP.get(toId);
                break;
            case "DASHBOARD":
                entityId = DASHBOARD_MAP.get(toId);
                break;
            default:
                throw new RuntimeException();
        }
        return entityId;
    }

}
