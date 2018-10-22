package org.thingsboard.datatransfer.importing.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.importing.LoadContext;
import org.thingsboard.server.common.data.User;
import org.thingsboard.server.common.data.id.UserId;
import org.thingsboard.server.common.data.security.Authority;
import org.thingsboard.server.common.data.security.UserCredentials;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Slf4j
public class ImportUsers extends ImportEntity {

    private final RestClient tbRestClient;
    private final boolean emptyDb;

    public ImportUsers(RestClient tbRestClient, ObjectMapper mapper, String basePath, boolean emptyDb) {
        super(mapper, basePath);
        this.tbRestClient = tbRestClient;
        this.emptyDb = emptyDb;
    }

    public void saveCustomersUsers(LoadContext loadContext, int limit) {
        Map<String, UserId> userEmailsMap = new HashMap<>();
        if (!emptyDb) {
            Optional<JsonNode> usersOptional = tbRestClient.findAllCustomerUsers(limit);
            if (usersOptional.isPresent()) {
                for (JsonNode node : usersOptional.get().get("data")) {
                    userEmailsMap.put(node.get("email").asText(), new UserId(UUID.fromString(node.get("id").get("id").asText())));
                }
            }
        }
        JsonNode usersNode = readFileContentToNode("Users.json");
        if (usersNode != null) {
            for (JsonNode userNode : usersNode) {
                UserId userId = getUserId(loadContext, userEmailsMap, userNode);
                loadContext.getUserIdMap().put(userNode.get("id").get("id").asText(), userId);
                saveUserCredentials(userNode, userId);
            }
        }
    }

    private UserId getUserId(LoadContext loadContext, Map<String, UserId> userEmailsMap, JsonNode userNode) {
        String email = userNode.get("email").asText();
        UserId userId;
        if (emptyDb || !userEmailsMap.containsKey(email)) {
            userId = createUser(userNode, loadContext).getId();
        } else {
            userId = userEmailsMap.get(email);
        }
        return userId;
    }

    private void saveUserCredentials(JsonNode node, UserId userId) {
        UserCredentials credentials = tbRestClient.getUserCredentials(userId);
        credentials.setPassword(node.get("password").asText());
        credentials.setEnabled(true);
        credentials.setActivateToken(null);
        tbRestClient.saveUserCredentials(credentials);
    }

    private User createUser(JsonNode node, LoadContext loadContext) {
        User user = new User();
        user.setAuthority(Authority.parse(node.get("authority").asText()));
        user.setEmail(node.get("email").asText());
        user.setFirstName(node.get("firstName").asText());
        user.setLastName(node.get("lastName").asText());
        user.setCustomerId(loadContext.getCustomerIdMap().get(node.get("customerId").get("id").asText()));
        if (!node.get("additionalInfo").isNull()) {
            ObjectNode additionalInfoNode = (ObjectNode) node.get("additionalInfo");
            additionalInfoNode.put("defaultDashboardId", String.valueOf(loadContext.getDashboardIdMap().get(node.get("additionalInfo").get("defaultDashboardId").asText())));
            user.setAdditionalInfo(additionalInfoNode);
        }
        return tbRestClient.createUser(user);
    }

}
