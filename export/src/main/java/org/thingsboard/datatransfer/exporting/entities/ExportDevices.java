package org.thingsboard.datatransfer.exporting.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Optional;

@Slf4j
public class ExportDevices {

    private final ObjectMapper mapper;
    private final RestClient tbRestClient;
    private final String basePath;

    public ExportDevices(RestClient tbRestClient, ObjectMapper mapper, String basePath) {
        this.tbRestClient = tbRestClient;
        this.mapper = mapper;
        this.basePath = basePath;
    }

    public void getTenantDevices() {
        Optional<JsonNode> optional = tbRestClient.findTenantDevices(10);

        BufferedWriter writer;
        try {
            writer = new BufferedWriter(new FileWriter(new File(basePath + "Devices.json")));
            if (optional.isPresent()) {
                writer.write(mapper.writeValueAsString(optional.get().get("data")));
            }
            writer.close();
        } catch (IOException e) {
            log.warn("");
        }

    }

}
