import com.fasterxml.jackson.databind.ObjectMapper;
import entities.ImportCustomers;
import entities.ImportDevices;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.server.common.data.id.CustomerId;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class Import {

    public static final Map<String, CustomerId> CUSTOMERS_MAP = new HashMap<>();

    public static String BASE_PATH;
    public static String TB_BASE_URL;
    public static String TB_TOKEN;

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

            log.info("Start exporting...");

            ImportCustomers customers = new ImportCustomers(tbRestClient, mapper, BASE_PATH);
            customers.saveTenantCustomers(CUSTOMERS_MAP);

            ImportDevices devices = new ImportDevices(tbRestClient, mapper, BASE_PATH);
            devices.saveTenantDevices(CUSTOMERS_MAP);

            log.info("Ended importing successfully!");
            EXECUTOR_SERVICE.shutdown();
        } catch (Exception e) {
            log.error("Could not read properties file {}", filename, e);
        }
    }

}
