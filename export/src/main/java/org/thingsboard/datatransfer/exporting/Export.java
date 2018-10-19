package org.thingsboard.datatransfer.exporting;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.exporting.entities.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class Export {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final SaveContext SAVE_CONTEXT = new SaveContext(MAPPER);

    private static String BASE_PATH;
    public static String TB_TOKEN;
    public static String TB_BASE_URL;

    public static int THRESHOLD;
    public static ExecutorService EXECUTOR_SERVICE;

    public static void main(String[] args) {
        Properties properties = new Properties();
        Client httpClient = new Client(MAPPER);
        String filename;
        if (args.length > 0) {
            filename = args[0];
        } else {
            filename = "config.properties";
        }
        try (InputStream input = new FileInputStream(filename)) {
            properties.load(input);

            BASE_PATH = properties.getProperty("basePath");
            int limit = Integer.parseInt(properties.getProperty("limit"));
            boolean isPe = Boolean.parseBoolean(properties.getProperty("isPe"));

            THRESHOLD = Integer.parseInt(properties.getProperty("threshold"));
            EXECUTOR_SERVICE = Executors.newFixedThreadPool(Integer.parseInt(properties.getProperty("threadsCount")));

            TB_BASE_URL = properties.getProperty("tbBaseURL");
            RestClient tbRestClient = new RestClient(TB_BASE_URL);
            tbRestClient.login(properties.getProperty("tbLogin"), properties.getProperty("tbPassword"));
            TB_TOKEN = tbRestClient.getToken();

            log.info("Start exporting...");
            ExportCustomers customers = new ExportCustomers(tbRestClient, MAPPER, BASE_PATH, httpClient);
            customers.getTenantCustomers(SAVE_CONTEXT, limit);

            ExportUsers users = new ExportUsers(tbRestClient, MAPPER, BASE_PATH, httpClient);
            users.getAllCustomerUsers(limit);

            ExportDevices devices = new ExportDevices(tbRestClient, MAPPER, BASE_PATH, httpClient);
            devices.getTenantDevices(SAVE_CONTEXT, limit);

            ExportAssets assets = new ExportAssets(tbRestClient, MAPPER, BASE_PATH, httpClient);
            assets.getTenantAssets(SAVE_CONTEXT, limit);

            if (isPe) {
                ExportEntityGroups entityGroups = new ExportEntityGroups(tbRestClient, MAPPER, BASE_PATH, httpClient);
                entityGroups.getEntityGroups(SAVE_CONTEXT, limit);

                ExportIntegrations integrations = new ExportIntegrations(tbRestClient, MAPPER, BASE_PATH, httpClient);
                integrations.getIntegrations(SAVE_CONTEXT, limit);

                ExportConverters converters = new ExportConverters(tbRestClient, MAPPER, BASE_PATH, httpClient);
                converters.getConverters(SAVE_CONTEXT, limit);

                ExportSchedulerEvents schedulerEvents = new ExportSchedulerEvents(tbRestClient, MAPPER, BASE_PATH, httpClient);
                schedulerEvents.getSchedulerEvents(SAVE_CONTEXT);

                ExportBlobEntities blobEntities = new ExportBlobEntities(tbRestClient, MAPPER, BASE_PATH, httpClient);
                blobEntities.getTenantBlobEntities(limit);

                ExportEntityViews entityViews = new ExportEntityViews(tbRestClient, MAPPER, BASE_PATH, httpClient);
                entityViews.getTenantEntityViews(SAVE_CONTEXT, limit);

            }

            ExportDashboards dashboards = new ExportDashboards(tbRestClient, MAPPER, BASE_PATH, httpClient);
            dashboards.getTenantDashboards(SAVE_CONTEXT, limit);

            ExportRuleChains ruleChains = new ExportRuleChains(tbRestClient, MAPPER, BASE_PATH, httpClient);
            ruleChains.getRuleChains(limit);

            writeToFile("Relations.json", SAVE_CONTEXT.getRelationsArray());
            writeToFile("Attributes.json", SAVE_CONTEXT.getAttributesArray());
            writeToFile("EntityGroups.json", SAVE_CONTEXT.getEntityGroups());
            writeToFile("EntitiesInGroups.json", SAVE_CONTEXT.getEntitiesInGroups());

            log.info("Ended exporting successfully!");
            EXECUTOR_SERVICE.shutdown();
        } catch (Exception e) {
            log.error("Could not read properties file {}", filename, e);
        }
    }

    public static void writeToFile(String fileName, JsonNode node) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(BASE_PATH + fileName)))) {
            writer.write(MAPPER.writeValueAsString(node));
        } catch (IOException e) {
            log.warn("Could not write data {} to file: {}", node, fileName);
        }
    }

}
