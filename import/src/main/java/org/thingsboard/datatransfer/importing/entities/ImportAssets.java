package org.thingsboard.datatransfer.importing.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.importing.LoadContext;
import org.thingsboard.server.common.data.asset.Asset;
import org.thingsboard.server.common.data.id.CustomerId;

import java.util.Map;
import java.util.Optional;

import static org.thingsboard.datatransfer.importing.Import.NULL_UUID;

/**
 * Created by mshvayka on 11.09.18.
 */
@Slf4j
public class ImportAssets extends ImportEntity {

    private final RestClient tbRestClient;
    private final boolean emptyDb;

    public ImportAssets(RestClient tbRestClient, ObjectMapper mapper, String basePath, boolean emptyDb) {
        super(mapper, basePath);
        this.tbRestClient = tbRestClient;
        this.emptyDb = emptyDb;
    }

    public void saveTenantAssets(LoadContext loadContext) {
        JsonNode assetsNode = readFileContentToNode("Assets.json");
        if (assetsNode != null) {
            for (JsonNode assetNode : assetsNode) {
                Asset asset = findOrCreateAsset(assetNode, assetNode.get("name").asText());
                loadContext.getAssetIdMap().put(assetNode.get("id").get("id").asText(), asset.getId());
                assignAssetToCustomer(loadContext.getCustomerIdMap(), assetNode, asset);
            }
        }
    }

    private Asset findOrCreateAsset(JsonNode node, String assetName) {
        Asset asset;
        if (emptyDb) {
            asset = tbRestClient.createAsset(assetName, node.get("type").asText());
        } else {
            Optional<Asset> assetOptional = tbRestClient.findAsset(assetName);
            asset = assetOptional.orElseGet(() -> tbRestClient.createAsset(assetName, node.get("type").asText()));
        }
        return asset;
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
