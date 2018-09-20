package org.thingsboard.datatransfer.importing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.importing.entities.*;
import org.thingsboard.server.common.data.id.AssetId;
import org.thingsboard.server.common.data.id.CustomerId;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.EntityId;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class Import {

    public static final String NULL_UUID = "13814000-1dd2-11b2-8080-808080808080";
    private static final LoadContext LOAD_CONTEXT = new LoadContext();

    public static String TB_BASE_URL;
    public static String TB_TOKEN;
    private static String BASE_PATH;

    public static int THRESHOLD;
    public static ExecutorService EXECUTOR_SERVICE;

    public static void main(String[] args) {
        ObjectMapper mapper = new ObjectMapper();
        Client httpClient = new Client();
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

            TB_BASE_URL = properties.getProperty("tbBaseURL");
            RestClient tbRestClient = new RestClient(TB_BASE_URL);
            tbRestClient.login(properties.getProperty("tbLogin"), properties.getProperty("tbPassword"));
            TB_TOKEN = tbRestClient.getToken();

            boolean emptyDb = Boolean.parseBoolean(properties.getProperty("emptyDb"));
            BASE_PATH = properties.getProperty("basePath");

            log.info("Start importing...");

            ImportCustomers customers = new ImportCustomers(tbRestClient, mapper, BASE_PATH, emptyDb);
            customers.saveTenantCustomers(LOAD_CONTEXT);

            ImportDevices devices = new ImportDevices(tbRestClient, mapper, BASE_PATH, emptyDb);
            devices.saveTenantDevices(LOAD_CONTEXT);

            ImportAssets assets = new ImportAssets(tbRestClient, mapper, BASE_PATH, emptyDb);
            assets.saveTenantAssets(LOAD_CONTEXT);

            ImportTelemetry telemetry = new ImportTelemetry(mapper, BASE_PATH, httpClient);
            telemetry.saveTelemetry(LOAD_CONTEXT);

            ImportAttributes attributes = new ImportAttributes(mapper, BASE_PATH, httpClient);
            attributes.saveAttributes(LOAD_CONTEXT);

            importRelations(mapper, tbRestClient);
            log.info("Ended importing successfully!");
            EXECUTOR_SERVICE.shutdown();
        } catch (Exception e) {
            log.error("Could not read properties file {}", filename, e);
        }
    }

    private static void importRelations(ObjectMapper mapper, RestClient tbRestClient) {
        JsonNode relations = null;
        try {
            relations = mapper.readTree(new String(Files.readAllBytes(Paths.get(BASE_PATH + "Relations.json"))));
        } catch (IOException e) {
            log.warn("Could not read relations file");
        }
        if (relations != null) {
            for (JsonNode nodeArray : relations) {
                for (JsonNode node : nodeArray) {
                    String fromType = node.get("from").get("entityType").asText();
                    String fromId = node.get("from").get("id").asText();
                    String toType = node.get("to").get("entityType").asText();
                    String toId = node.get("to").get("id").asText();
                    String relationType = node.get("type").asText();
                    EntityId entityId;
                    switch (fromType) {
                        case "CUSTOMER":
                            CustomerId customerId = LOAD_CONTEXT.getCustomerIdMap().get(fromId);
                            entityId = getToEntityId(toType, toId);
                            if (entityId != null) {
                                tbRestClient.makeRelation(relationType, customerId, entityId);
                            }
                            break;
                        case "ASSET":
                            AssetId assetId = LOAD_CONTEXT.getAssetIdMap().get(fromId);
                            entityId = getToEntityId(toType, toId);
                            if (entityId != null) {
                                tbRestClient.makeRelation(relationType, assetId, entityId);
                            }
                            break;
                        case "DEVICE":
                            DeviceId deviceId = LOAD_CONTEXT.getDeviceIdMap().get(fromId);
                            entityId = getToEntityId(toType, toId);
                            if (entityId != null) {
                                tbRestClient.makeRelation(relationType, deviceId, entityId);
                            }
                            break;
                        default:
                            log.warn("Entity type is not supported: {}", fromType);
                    }
                }
            }
        }
    }

    private static EntityId getToEntityId(String toType, String toId) {
        EntityId toEntityId = null;
        switch (toType) {
            case "CUSTOMER":
                toEntityId = LOAD_CONTEXT.getCustomerIdMap().get(toId);
                break;
            case "ASSET":
                toEntityId = LOAD_CONTEXT.getAssetIdMap().get(toId);
                break;
            case "DEVICE":
                toEntityId = LOAD_CONTEXT.getDeviceIdMap().get(toId);
                break;
            default:
                log.warn("Entity type is not supported: {}", toType);
        }
        return toEntityId;
    }

}
