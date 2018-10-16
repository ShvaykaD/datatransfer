package org.thingsboard.datatransfer.importing;

import lombok.Data;

import org.thingsboard.server.common.data.id.*;


import java.util.HashMap;
import java.util.Map;

@Data
public class LoadContext {

    private final Map<String, CustomerId> customerIdMap = new HashMap<>();
    private final Map<String, AssetId> assetIdMap = new HashMap<>();
    private final Map<String, DeviceId> deviceIdMap = new HashMap<>();
    private final Map<String, DashboardId> dashboardIdMap = new HashMap<>();
    private final Map<String, EntityGroupId> entityGroupIdMap = new HashMap<>();
    private final Map<String, ConverterId> converterIdMap = new HashMap<>();
    private final Map<String, IntegrationId> integrationIdMap = new HashMap<>();
    private final Map<String, SchedulerEventId> schedulerEventIdMap = new HashMap<>();
    private final Map<String, UserId> userIdMap = new HashMap<>();
    private final Map<String, RuleChainId> ruleChainIdMap = new HashMap<>();
    private final Map<String, BlobEntityId> blobEntityIdMap = new HashMap<>();
    private final Map<String, EntityViewId> entityViewIdMap = new HashMap<>();

}
