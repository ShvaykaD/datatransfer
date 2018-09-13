package org.thingsboard.datatransfer.exporting;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.exporting.entities.ExportAssets;
import org.thingsboard.datatransfer.exporting.entities.ExportCustomers;
import org.thingsboard.datatransfer.exporting.entities.ExportDashboards;
import org.thingsboard.datatransfer.exporting.entities.ExportDevices;
import org.thingsboard.server.common.data.relation.EntityRelation;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class Export {

    public static String BASE_PATH;
    public static String TB_BASE_URL;
    public static String TB_TOKEN;

    public static int THRESHOLD;
    public static ExecutorService EXECUTOR_SERVICE;

    public static void main(String[] args) {
        ObjectMapper mapper = new ObjectMapper();
        ArrayNode relationsArray = mapper.createArrayNode();
        ArrayNode telemetryArray = mapper.createArrayNode();
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

            log.info("Start exporting...");

            ExportCustomers customers = new ExportCustomers(tbRestClient, mapper, BASE_PATH);
            customers.getTenantCustomers(relationsArray);

            ExportDevices devices = new ExportDevices(tbRestClient, mapper, BASE_PATH);
            devices.getTenantDevices(relationsArray);

            ExportAssets assets = new ExportAssets(tbRestClient, mapper, BASE_PATH);
            assets.getTenantAssets(relationsArray, telemetryArray);

            ExportDashboards dashboards = new ExportDashboards(tbRestClient, mapper , BASE_PATH);
            dashboards.getTenantDashboards(relationsArray);


            try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(BASE_PATH + "Relations.json")))) {
                writer.write(mapper.writeValueAsString(relationsArray));
                writer.close();
            } catch (IOException e) {
                log.warn("");
            }

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(BASE_PATH + "Telemetry.json")))) {
                writer.write(mapper.writeValueAsString(telemetryArray));
                writer.close();
            } catch (IOException e) {
                log.warn("");
            }

            log.info("Ended exporting successfully!");
            EXECUTOR_SERVICE.shutdown();
        } catch (Exception e) {
            log.error("Could not read properties file {}", filename, e);
        }
    }

}
