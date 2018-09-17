package org.thingsboard.datatransfer.importing;

import lombok.Data;
import org.thingsboard.server.common.data.id.AssetId;
import org.thingsboard.server.common.data.id.CustomerId;
import org.thingsboard.server.common.data.id.DeviceId;

import java.util.HashMap;
import java.util.Map;

@Data
public class LoadContext {

    private final Map<String, CustomerId> customerIdMap = new HashMap<>();
    private final Map<String, AssetId> assetIdMap = new HashMap<>();
    private final Map<String, DeviceId> deviceIdMap = new HashMap<>();

}
