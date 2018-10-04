package org.thingsboard.datatransfer.importing.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.importing.LoadContext;
import org.thingsboard.server.common.data.User;
import org.thingsboard.server.common.data.id.*;
import org.thingsboard.server.common.data.security.Authority;
import org.thingsboard.server.common.data.security.UserCredentials;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Slf4j
public class ImportUsers {

    private final ObjectMapper mapper;
    private final RestClient tbRestClient;
    private final String basePath;
    private final boolean emptyDb;

    public ImportUsers(RestClient tbRestClient, ObjectMapper mapper, String basePath, boolean emptyDb) {
        this.tbRestClient = tbRestClient;
        this.mapper = mapper;
        this.basePath = basePath;
        this.emptyDb = emptyDb;
    }


    public void saveCustomersUsers(LoadContext loadContext, int limit) {
        Map<String, UserId> userEmailsMap = new HashMap<>();
        if (!emptyDb) {
            Optional<JsonNode> usersOptional = tbRestClient.findAllCustomerUsers(limit);
            if (usersOptional.isPresent()) {
                ArrayNode usersArray = (ArrayNode) usersOptional.get().get("data");
                for (JsonNode node : usersArray) {
                    userEmailsMap.put(node.get("email").asText(), new UserId(UUID.fromString(node.get("id").get("id").asText())));

                }
            }
        }
        JsonNode jsonNode = null;
        try {
            jsonNode = mapper.readTree(new String(Files.readAllBytes(Paths.get(basePath + "Users.json"))));
        } catch (IOException e) {
            log.warn("Could not read users file");
        }
        if (jsonNode != null) {
            for (JsonNode node : jsonNode) {
                if (!emptyDb && userEmailsMap.containsKey(node.get("email").asText())) {
                    tbRestClient.deleteUser(UserId.fromString(node.get("id").get("id").asText()));
                }
                User user = createUser(node, loadContext);
                loadContext.getUserIdMap().put(node.get("id").get("id").asText(), user.getId());
                UserCredentials credentials = tbRestClient.getUserCredentials(user.getId());
                credentials.setPassword(node.get("password").asText());
                credentials.setEnabled(true);
                credentials.setActivateToken(null);
                tbRestClient.saveUserCredentials(credentials);
            }
        }
    }

    private User createUser(JsonNode node, LoadContext loadContext) {
        User user = new User();
        user.setAuthority(Authority.parse(node.get("authority").asText()));
        user.setEmail(node.get("email").asText());
        user.setCustomerId(loadContext.getCustomerIdMap().get(node.get("customerId").get("id").asText()));
        if (!node.get("additionalInfo").isNull()) {
            ObjectNode additionalInfoNode = (ObjectNode) node.get("additionalInfo");
            additionalInfoNode.put("defaultDashboardId", String.valueOf(loadContext.getDashboardIdMap().get(node.get("additionalInfo").get("defaultDashboardId").asText())));
            user.setAdditionalInfo(additionalInfoNode);
        }
        return tbRestClient.createUser(user);
    }

}
