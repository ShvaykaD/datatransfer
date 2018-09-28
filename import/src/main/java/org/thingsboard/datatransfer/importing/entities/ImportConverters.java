package org.thingsboard.datatransfer.importing.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.importing.LoadContext;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.converter.Converter;
import org.thingsboard.server.common.data.converter.ConverterType;
import org.thingsboard.server.common.data.id.ConverterId;
import org.thingsboard.server.common.data.id.IntegrationId;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

@Slf4j
public class ImportConverters {

    private final ObjectMapper mapper;
    private final RestClient tbRestClient;
    private final String basePath;
    private final boolean emptyDb;

    public ImportConverters(RestClient tbRestClient, ObjectMapper mapper, String basePath, boolean emptyDb) {
        this.tbRestClient = tbRestClient;
        this.mapper = mapper;
        this.basePath = basePath;
        this.emptyDb = emptyDb;
    }

    public void saveConverters(LoadContext loadContext) {
        Map<String, ConverterId> converterNames = new HashMap<>();
        if (!emptyDb) {
            Optional<JsonNode> convertersOptional = tbRestClient.findConverters(10000);
            if (convertersOptional.isPresent()) {
                ArrayNode convertersArray = (ArrayNode) convertersOptional.get().get("data");
                for (JsonNode node : convertersArray) {
                    converterNames.put(node.get("name").asText(), new ConverterId(UUID.fromString(node.get("id").get("id").asText())));
                }
            }
        }
        JsonNode jsonNode = null;
        try {
            jsonNode = mapper.readTree(new String(Files.readAllBytes(Paths.get(basePath + "Converters.json"))));
        } catch (IOException e) {
            log.warn("Could not read converters file");
        }
        if (jsonNode != null) {
            for (JsonNode node : jsonNode) {
                String converterName = node.get("name").asText();
                if (!emptyDb && converterNames.containsKey(converterName)) {
                    if (node.has("integrationId")) {
                        for (JsonNode integrationId : node.get("integrationId")) {
                            tbRestClient.deleteIntegration(IntegrationId.fromString(integrationId.asText()));
                        }
                    }
                    tbRestClient.deleteConverter(converterNames.get(converterName));
                }
                Converter converter = new Converter();
                converter.setName(node.get("name").asText());
                converter.setType(ConverterType.valueOf(node.get("type").asText()));
                if (!node.get("configuration").isNull()) {
                    JsonNode configurationNode = node.get("configuration");
                    converter.setConfiguration(configurationNode);
                }
                Converter savedConverter = tbRestClient.createConverter(converter);
                loadContext.getConverterIdMap().put(node.get("id").get("id").asText(), savedConverter.getId());
            }
        }
    }

}
