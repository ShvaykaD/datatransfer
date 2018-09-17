package org.thingsboard.datatransfer.importing.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.importing.LoadContext;
import org.thingsboard.server.common.data.asset.Asset;
import org.thingsboard.server.common.data.id.CustomerId;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;

import static org.thingsboard.datatransfer.importing.Import.NULL_UUID;

/**
 * Created by mshvayka on 11.09.18.
 */
@Slf4j
public class ImportAssets {

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

    public void saveTenantAssets(LoadContext loadContext) {
        JsonNode jsonNode = null;
        try {
            jsonNode = mapper.readTree(new String(Files.readAllBytes(Paths.get(basePath + "Assets.json"))));
        } catch (IOException e) {
            log.warn("Could not read assets file");
        }
        if (jsonNode != null) {
            for (JsonNode node : jsonNode) {
                String assetName = node.get("name").asText();
                if (!emptyDb) {
                    Optional<Asset> assetOptional = tbRestClient.findAsset(assetName);
                    assetOptional.ifPresent(asset -> tbRestClient.deleteAsset(asset.getId()));
                }
                Asset asset = tbRestClient.createAsset(assetName, node.get("type").asText());
                loadContext.getAssetIdMap().put(node.get("id").get("id").asText(), asset.getId());
                assignAssetToCustomer(loadContext.getCustomerIdMap(), node, asset);
            }
        }
    }

    private void assignAssetToCustomer(Map<String, CustomerId> customerIdMap, JsonNode node, Asset asset) {
        String strCustomerId = node.get("customerId").get("id").asText();
        if (!strCustomerId.equals(NULL_UUID)) {
            if (customerIdMap.containsKey(strCustomerId)) {
                tbRestClient.assignAsset(customerIdMap.get(strCustomerId), asset.getId());
            } else {
                tbRestClient.assignAssetToPublicCustomer(asset.getId());
            }
        }
    }
}
