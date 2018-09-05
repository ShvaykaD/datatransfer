package entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

@Slf4j
public class ImportDevices {

    private final ObjectMapper mapper;
    private final RestClient tbRestClient;
    private final String basePath;

    public ImportDevices(RestClient tbRestClient, ObjectMapper mapper, String basePath) {
        this.tbRestClient = tbRestClient;
        this.mapper = mapper;
        this.basePath = basePath;
    }

    public void saveTenantDevices() {

        try {
            String content = new String(Files.readAllBytes(Paths.get(basePath + "Devices.json")));
            JsonNode jsonNode = mapper.readTree(content);
            if (jsonNode.isArray()) {
                for (JsonNode node : jsonNode) {
                    tbRestClient.createDevice(node.get("name").asText(), node.get("type").asText());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
