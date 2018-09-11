package org.thingsboard.datatransfer.importing.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.server.common.data.asset.Asset;
import org.thingsboard.server.common.data.id.CustomerId;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;

/**
 * Created by mshvayka on 11.09.18.
 */
@Slf4j
public class ImportAssets {

    public static final String NULL_UUID = "13814000-1dd2-11b2-8080-808080808080";

    private final ObjectMapper mapper;
    private final RestClient tbRestClient;
    private final String basePath;
    private final boolean emptyDb;

    public ImportAssets(RestClient tbRestClient, ObjectMapper mapper, String basePath, boolean emptyDb) {
        this.tbRestClient = tbRestClient;
        this.mapper = mapper;
        this.basePath = basePath;
        this.emptyDb = emptyDb;
    }

    public void saveTenantAssets(Map<String, CustomerId> customerIdMap) {
        try {
            String content = new String(Files.readAllBytes(Paths.get(basePath + "Assets.json")));
            JsonNode jsonNode = mapper.readTree(content);
            if (jsonNode.isArray()) {
                for (JsonNode node : jsonNode) {
                    if (!emptyDb) {
                        Optional<Asset> assetOptional = tbRestClient.findAsset(node.get("name").asText());
                        assetOptional.ifPresent(asset -> tbRestClient.deleteAsset(asset.getId()));
                    }
                    Asset savedAsset = tbRestClient.createAsset(node.get("name").asText(), node.get("type").asText());
                    String strCustomerId = node.get("customerId").get("id").asText();
                    if (!strCustomerId.equals(NULL_UUID)) {
                        if (customerIdMap.containsKey(strCustomerId)) {
                            tbRestClient.assignAsset(customerIdMap.get(strCustomerId), savedAsset.getId());
                        } else {
                            tbRestClient.assignAssetToPublicCustomer(savedAsset.getId());
                        }
                    }
                }
            }
        } catch (IOException e) {
            log.warn("");
        }
    }
}
