package org.thingsboard.datatransfer.importing.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.importing.LoadContext;
import org.thingsboard.server.common.data.converter.Converter;
import org.thingsboard.server.common.data.converter.ConverterType;
import org.thingsboard.server.common.data.id.ConverterId;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class ImportConverters extends ImportEntity {

    private final RestClient tbRestClient;
    private final boolean emptyDb;

    public ImportConverters(RestClient tbRestClient, ObjectMapper mapper, String basePath, boolean emptyDb) {
        super(mapper, basePath);
        this.tbRestClient = tbRestClient;
        this.emptyDb = emptyDb;
    }

    public void saveConverters(LoadContext loadContext, int limit) {
        Map<String, ConverterId> convertersMap = new HashMap<>();
        if (!emptyDb) {
            Optional<JsonNode> convertersOptional = tbRestClient.findConverters(limit);
            if (convertersOptional.isPresent()) {
                for (JsonNode node : convertersOptional.get().get("data")) {
                    convertersMap.put(node.get("name").asText(), ConverterId.fromString(node.get("id").get("id").asText()));
                }
            }
        }
        JsonNode convertersNode = readFileContentToNode("Converters.json");
        if (convertersNode != null) {
            for (JsonNode converterNode : convertersNode) {
                loadContext.getConverterIdMap().put(converterNode.get("id").get("id").asText(),
                        getConverterId(convertersMap, converterNode));
            }
        }
    }

    private ConverterId getConverterId(Map<String, ConverterId> convertersMap, JsonNode node) {
        String converterName = node.get("name").asText();
        ConverterId converterId;
        if (emptyDb || !convertersMap.containsKey(converterName)) {
            converterId = createConverter(node).getId();
        } else {
            converterId = convertersMap.get(converterName);
        }
        return converterId;
    }

    private Converter createConverter(JsonNode node) {
        Converter converter = new Converter();
        converter.setName(node.get("name").asText());
        converter.setType(ConverterType.valueOf(node.get("type").asText()));
        if (!node.get("configuration").isNull()) {
            converter.setConfiguration(node.get("configuration"));
        }
        return tbRestClient.createConverter(converter);
    }

}
