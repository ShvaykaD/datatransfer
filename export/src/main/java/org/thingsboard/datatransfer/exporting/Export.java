package org.thingsboard.datatransfer.exporting;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
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
    private static final SaveContext SAVE_CONTEXT = new SaveContext();

    public static String BASE_PATH;
    public static String TB_BASE_URL;
    public static String TB_TOKEN;
    public static int LIMIT;

    public static int THRESHOLD;
    public static ExecutorService EXECUTOR_SERVICE;

    public static void main(String[] args) {
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
            LIMIT = Integer.parseInt(properties.getProperty("limit"));
            boolean isPe = Boolean.parseBoolean(properties.getProperty("isPe"));

            TB_BASE_URL = properties.getProperty("tbBaseURL");
            RestClient tbRestClient = new RestClient(TB_BASE_URL);
            tbRestClient.login(properties.getProperty("tbLogin"), properties.getProperty("tbPassword"));
            TB_TOKEN = tbRestClient.getToken();

            log.info("Start exporting...");
            new ExportEntity(tbRestClient, MAPPER, BASE_PATH, isPe);

            ExportCustomers customers = new ExportCustomers(tbRestClient, MAPPER, BASE_PATH, isPe);
            customers.getTenantCustomers(SAVE_CONTEXT, LIMIT);

            ExportDevices devices = new ExportDevices(tbRestClient, MAPPER, BASE_PATH, isPe);
            devices.getTenantDevices(SAVE_CONTEXT, LIMIT);

            ExportAssets assets = new ExportAssets(tbRestClient, MAPPER, BASE_PATH, isPe);
            assets.getTenantAssets(SAVE_CONTEXT, LIMIT);

            if (isPe) {
                ExportEntityGroups entityGroups = new ExportEntityGroups(tbRestClient, MAPPER, BASE_PATH);
                entityGroups.getEntityGroups(SAVE_CONTEXT, LIMIT);

                ExportConverters converters = new ExportConverters(tbRestClient, MAPPER, BASE_PATH);
                converters.getConverters(SAVE_CONTEXT, LIMIT);
            }

            ExportDashboards dashboards = new ExportDashboards(tbRestClient, MAPPER, BASE_PATH);
            dashboards.getTenantDashboards(SAVE_CONTEXT, LIMIT);

            writeToFile("Relations.json", SAVE_CONTEXT.getRelationsArray());
            writeToFile("Telemetry.json", SAVE_CONTEXT.getTelemetryArray());
            writeToFile("Attributes.json", SAVE_CONTEXT.getAttributesArray());
            writeToFile("EntityGroups.json", SAVE_CONTEXT.getEntityGroups());
            writeToFile("EntitiesInGroups.json", SAVE_CONTEXT.getEntitiesInGroups());

            log.info("Ended exporting successfully!");
            EXECUTOR_SERVICE.shutdown();
        } catch (Exception e) {
            log.error("Could not read properties file {}", filename, e);
        }
    }

    private static void writeToFile(String fileName, JsonNode node) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(BASE_PATH + fileName)))) {
            writer.write(MAPPER.writeValueAsString(node));
        } catch (IOException e) {
            log.warn("Could not write data to file: {}", node);
        }
    }

}
