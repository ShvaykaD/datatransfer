package org.thingsboard.datatransfer.exporting;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.exporting.entities.ExportAssets;
import org.thingsboard.datatransfer.exporting.entities.ExportConverters;
import org.thingsboard.datatransfer.exporting.entities.ExportCustomers;
import org.thingsboard.datatransfer.exporting.entities.ExportDashboards;
import org.thingsboard.datatransfer.exporting.entities.ExportDevices;
import org.thingsboard.datatransfer.exporting.entities.ExportEntityGroups;
import org.thingsboard.datatransfer.exporting.entities.ExportIntegrations;
import org.thingsboard.datatransfer.exporting.entities.ExportRuleChains;
import org.thingsboard.datatransfer.exporting.entities.ExportSchedulerEvents;
import org.thingsboard.datatransfer.exporting.entities.ExportUsers;

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
    private static String TB_TOKEN;

    private static int THRESHOLD;
    private static ExecutorService EXECUTOR_SERVICE;

    public static void main(String[] args) {
        Properties properties = new Properties();
        Client httpClient = new Client();
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

            RestClient tbRestClient = new RestClient(properties.getProperty("tbBaseURL"));
            tbRestClient.login(properties.getProperty("tbLogin"), properties.getProperty("tbPassword"));
            TB_TOKEN = tbRestClient.getToken();

            log.info("Start exporting...");
            ExportCustomers customers = new ExportCustomers(tbRestClient, MAPPER, BASE_PATH);
            customers.getTenantCustomers(SAVE_CONTEXT, limit);

            ExportUsers users = new ExportUsers(tbRestClient, MAPPER, BASE_PATH);
            users.getAllCustomerUsers(limit);

            ExportDevices devices = new ExportDevices(tbRestClient, MAPPER, BASE_PATH);
            devices.getTenantDevices(SAVE_CONTEXT, limit);

            ExportAssets assets = new ExportAssets(tbRestClient, MAPPER, BASE_PATH);
            assets.getTenantAssets(SAVE_CONTEXT, limit);

            if (isPe) {
                ExportEntityGroups entityGroups = new ExportEntityGroups(tbRestClient, MAPPER, BASE_PATH);
                entityGroups.getEntityGroups(SAVE_CONTEXT, limit);

                ExportIntegrations integrations = new ExportIntegrations(tbRestClient, MAPPER, BASE_PATH);
                integrations.getIntegrations(SAVE_CONTEXT, limit);

                ExportConverters converters = new ExportConverters(tbRestClient, MAPPER, BASE_PATH);
                converters.getConverters(SAVE_CONTEXT, limit);

                ExportSchedulerEvents schedulerEvents = new ExportSchedulerEvents(tbRestClient, MAPPER, BASE_PATH);
                schedulerEvents.getSchedulerEvents(SAVE_CONTEXT);
            }

            ExportDashboards dashboards = new ExportDashboards(tbRestClient, MAPPER, BASE_PATH);
            dashboards.getTenantDashboards(SAVE_CONTEXT, limit);

            ExportRuleChains ruleChains = new ExportRuleChains(tbRestClient, MAPPER, BASE_PATH);
            ruleChains.getRuleChains(limit);

            writeToFile("Relations.json", SAVE_CONTEXT.getRelationsArray());
            writeToFile("Telemetry.json", SAVE_CONTEXT.getTelemetryArray());
            writeToFile("Attributes.json", SAVE_CONTEXT.getAttributesArray());
            writeToFile("EntityGroups.json", SAVE_CONTEXT.getEntityGroups());
            writeToFile("EntitiesInGroups.json", SAVE_CONTEXT.getEntitiesInGroups());

            log.info("Ended exporting successfully!");
        } catch (Exception e) {
            log.error("Could not read properties file {}", filename, e);
        }
    }

    public static void writeToFile(String fileName, JsonNode node) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(BASE_PATH + fileName)))) {
            writer.write(MAPPER.writeValueAsString(node));
        } catch (IOException e) {
            log.warn("Could not write data to file: {}", node);
        }
    }

}
