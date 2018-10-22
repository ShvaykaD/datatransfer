package org.thingsboard.datatransfer.importing.entities;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.math.NumberUtils;
import org.thingsboard.datatransfer.importing.Client;
import org.thingsboard.datatransfer.importing.LoadContext;
import org.thingsboard.server.common.data.id.EntityId;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Future;

import static org.thingsboard.datatransfer.importing.Import.*;

@Slf4j
public class ImportTelemetry extends ImportEntity {

    private static final String CAN_T_PARSE_VALUE = "Can't parse value: ";

    private final Client httpClient;

    public ImportTelemetry(ObjectMapper mapper, String basePath, Client httpClient) {
        super(mapper, basePath);
        this.httpClient = httpClient;
    }

    public void saveTelemetry(LoadContext loadContext) {
        File dir = new File(basePath);
        for (File file : Objects.requireNonNull(dir.listFiles())) {
            if (file.getName().endsWith("_Telemetry.json")) {
                JsonElement jsonElement = readFileContentToGson(file.getName());

                if (jsonElement != null) {
                    List<Future> resultList = new ArrayList<>();
                    if (jsonElement.isJsonArray()) {
                        for (JsonElement element : jsonElement.getAsJsonArray()) {
                            resultList.add(EXECUTOR_SERVICE.submit(() -> retryUntilDone(() -> {
                                JsonObject jsonObject = element.getAsJsonObject();
                                String entityType = jsonObject.get("entityType").getAsString();
                                EntityId entityId = getEntityId(loadContext, jsonObject.get("entityId").getAsString(), entityType);

                                JsonObject telemetryObject = jsonObject.get("telemetry").getAsJsonObject();
                                for (String field : telemetryObject.keySet()) {
                                    JsonArray fieldValuesArray = telemetryObject.get(field).getAsJsonArray();
                                    JsonArray parsedArray = new JsonArray();
                                    for (JsonElement fieldValueElement : fieldValuesArray) {
                                        parsedArray.add(parseObject(fieldValueElement.getAsJsonObject()));
                                    }
                                    JsonArray savingArray = new JsonArray();
                                    for (JsonElement parsedObject : parsedArray) {
                                        if (savingArray.size() == 1000) {
                                            log.info("Pushing telemetry to {} [{}]", entityType, entityId);
                                            httpClient.sendGsonData(TB_BASE_URL + "/api/plugins/telemetry/" + entityType + "/" +
                                                    entityId.toString() + "/timeseries/data?key=" + field, savingArray, TB_TOKEN);
                                            savingArray = new JsonArray();
                                        }
                                        savingArray.add(parsedObject);
                                    }
                                    if (savingArray.size() > 0) {
                                        log.info("Pushing telemetry to {} [{}]", entityType, entityId);
                                        httpClient.sendGsonData(TB_BASE_URL + "/api/plugins/telemetry/" + entityType + "/" +
                                                entityId.toString() + "/timeseries/data?key=" + field, savingArray, TB_TOKEN);
                                    }
                                }
                                return true;
                            })));
                            if (resultList.size() > THRESHOLD) {
                                waitForPack(resultList);
                            }
                        }
                    }
                    waitForPack(resultList);
                }
            }
        }
    }


    private JsonElement parseObject(JsonObject object) {
        JsonElement parsedElement = null;
        if (object.has("ts") && object.has("value")) {
            parsedElement = parseTelemetryWithTs(object);
        }
        return parsedElement;
    }

    private JsonElement parseTelemetryWithTs(JsonObject object) {
        JsonObject resultObject = new JsonObject();
        resultObject.addProperty("ts", object.get("ts").getAsLong());

        String strValue = object.get("value").getAsString();
        if (strValue.equals("true") || strValue.equals("false")) {
            resultObject.addProperty("value", Boolean.parseBoolean(strValue));
        } else if (NumberUtils.isCreatable(strValue)) {
            if (strValue.contains(".")) {
                resultObject.addProperty("value", Double.parseDouble(strValue));
            } else {
                resultObject.addProperty("value", Long.parseLong(strValue));
            }
        } else {
            resultObject.addProperty("value", strValue);
        }
        return resultObject;
    }

}
