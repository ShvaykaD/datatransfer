package org.thingsboard.datatransfer.importing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.importing.entities.*;
import org.thingsboard.server.common.data.EntityView;
import org.thingsboard.server.common.data.id.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;
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

            String strTenantUserId = null;
            Optional<JsonNode> tenantUser = tbRestClient.getCurruntTenantUser();
            if (tenantUser.isPresent()) {
                strTenantUserId = tenantUser.get().get("id").get("id").asText();
            }

            boolean emptyDb = Boolean.parseBoolean(properties.getProperty("emptyDb"));
            boolean isPe = Boolean.parseBoolean(properties.getProperty("isPe"));
            int limit = Integer.parseInt(properties.getProperty("limit"));
            BASE_PATH = properties.getProperty("basePath");

            log.info("Start importing...");

            ImportCustomers customers = new ImportCustomers(tbRestClient, mapper, BASE_PATH, emptyDb);
            customers.saveTenantCustomers(LOAD_CONTEXT);

            log.info("Start importing...devices");


            ImportDevices devices = new ImportDevices(tbRestClient, mapper, BASE_PATH, emptyDb);
            devices.saveTenantDevices(LOAD_CONTEXT);

            log.info("Start importing...assets");

            ImportAssets assets = new ImportAssets(tbRestClient, mapper, BASE_PATH, emptyDb);
            assets.saveTenantAssets(LOAD_CONTEXT);

            log.info("Start importing...pe-entities");

            if (isPe) {
                ImportEntityGroups entityGroups = new ImportEntityGroups(tbRestClient, mapper, BASE_PATH, false);
                entityGroups.saveTenantEntityGroups(LOAD_CONTEXT);
                entityGroups.addTenantEntitiesToGroups(LOAD_CONTEXT);

                ImportEntityViews entityViews = new ImportEntityViews(tbRestClient, mapper, BASE_PATH, false);
                entityViews.saveTenantEntityViews(LOAD_CONTEXT, limit);

                ImportConverters converters = new ImportConverters(tbRestClient, mapper, BASE_PATH, false);
                converters.saveConverters(LOAD_CONTEXT, limit);

                ImportIntegrations integrations = new ImportIntegrations(tbRestClient, mapper, BASE_PATH, false);
                integrations.saveIntegrations(LOAD_CONTEXT);

                ImportSchedulerEvents schedulerEvents = new ImportSchedulerEvents(tbRestClient, mapper, BASE_PATH, strTenantUserId);
                schedulerEvents.saveSchedulerEvents(LOAD_CONTEXT);

            }

            log.info("Start importing...dashboards");

            ImportDashboards dashboards = new ImportDashboards(tbRestClient, mapper, BASE_PATH);
            dashboards.saveTenantDashboards(LOAD_CONTEXT);

            ImportUsers users = new ImportUsers(tbRestClient, mapper, BASE_PATH, emptyDb);
            users.saveCustomersUsers(LOAD_CONTEXT, limit);

            //TODO: finish
            ImportRuleChains ruleChains = new ImportRuleChains(tbRestClient, mapper, BASE_PATH);
            ruleChains.saveRuleChains(LOAD_CONTEXT);

            log.info("Start importing...blobEntities");

            ImportBlobEntities blobEntities = new ImportBlobEntities(tbRestClient, mapper, BASE_PATH);
            blobEntities.saveTenantBlobEntities(LOAD_CONTEXT);


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

    //TODO: case TENANT
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
                        case "DASHBOARD":
                            DashboardId dashboardId = LOAD_CONTEXT.getDashboardIdMap().get(fromId);
                            entityId = getToEntityId(toType, toId);
                            if (entityId != null) {
                                tbRestClient.makeRelation(relationType, dashboardId, entityId);
                            }
                            break;
                        case "CONVERTER":
                            ConverterId converterId = LOAD_CONTEXT.getConverterIdMap().get(fromId);
                            entityId = getToEntityId(toType, toId);
                            if (entityId != null) {
                                tbRestClient.makeRelation(relationType, converterId, entityId);
                            }
                            break;
                        case "INTEGRATION":
                            IntegrationId integrationId = LOAD_CONTEXT.getIntegrationIdMap().get(fromId);
                            entityId = getToEntityId(toType, toId);
                            if (entityId != null) {
                                tbRestClient.makeRelation(relationType, integrationId, entityId);
                            }
                            break;
                        case "SCHEDULER_EVENT":
                            SchedulerEventId schedulerEventId = LOAD_CONTEXT.getSchedulerEventIdMap().get(fromId);
                            entityId = getToEntityId(toType, toId);
                            if (entityId != null) {
                                tbRestClient.makeRelation(relationType, schedulerEventId, entityId);
                            }
                            break;
                        case "ENTITY_VIEW":
                            EntityViewId entityViewId = LOAD_CONTEXT.getEntityViewIdMap().get(fromId);
                            entityId = getToEntityId(toType, toId);
                            if (entityId != null) {
                                tbRestClient.makeRelation(relationType, entityViewId, entityId);
                            }
                            break;
                        case "BLOB_ENTITY":
                            BlobEntityId blobEntityId = LOAD_CONTEXT.getBlobEntityIdMap().get(fromId);
                            entityId = getToEntityId(toType, toId);
                            if (entityId != null) {
                                tbRestClient.makeRelation(relationType, blobEntityId, entityId);
                            }
                            break;
                        default:
                            log.warn("Entity type is not supported: {}", fromType);
                    }
                }
            }
        }
    }

    //TODO: case TENANT
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
            case "DASHBOARD":
                toEntityId = LOAD_CONTEXT.getDashboardIdMap().get(toId);
                break;
            case "CONVERTER":
                toEntityId = LOAD_CONTEXT.getConverterIdMap().get(toId);
                break;
            case "INTEGRATION":
                toEntityId = LOAD_CONTEXT.getIntegrationIdMap().get(toId);
                break;
            case "SCHEDULER_EVENT":
                toEntityId = LOAD_CONTEXT.getSchedulerEventIdMap().get(toId);
                break;
            case "ENTITY_VIEW":
                toEntityId = LOAD_CONTEXT.getEntityViewIdMap().get(toId);
                break;
            case "BLOB_ENTITY":
                toEntityId = LOAD_CONTEXT.getBlobEntityIdMap().get(toId);
                break;
            default:
                log.warn("Entity type is not supported: {}", toType);
        }
        return toEntityId;
    }

}
