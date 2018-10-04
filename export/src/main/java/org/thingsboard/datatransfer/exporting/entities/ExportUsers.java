package org.thingsboard.datatransfer.exporting.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.exporting.SaveContext;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.UserId;
import org.thingsboard.server.common.data.security.DeviceCredentials;
import org.thingsboard.server.common.data.security.UserCredentials;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

@Slf4j
public class ExportUsers extends ExportEntity {


    public ExportUsers(RestClient tbRestClient, ObjectMapper mapper, String basePath, boolean isPe) {
        super(tbRestClient, mapper, basePath, isPe);
    }

    public void getAllCustomerUsers(SaveContext saveContext, int limit) {
        Optional<JsonNode> usersOptional = tbRestClient.findAllCustomerUsers(limit);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(basePath + "Users.json")))) {
            if (usersOptional.isPresent()) {
                ArrayNode userArray = (ArrayNode) usersOptional.get().get("data");
                for (JsonNode userNode : userArray) {
                    String strUserId = userNode.get("id").get("id").asText();
                    addUserCredentialsToUserNode((ObjectNode) userNode, strUserId);
                }
                writer.write(mapper.writeValueAsString(userArray));
            }
        } catch (IOException e) {
            log.warn("Could not export customers to file.");
        }
    }

    private void addUserCredentialsToUserNode(ObjectNode userNode, String strUserId) {
        UserCredentials userCredentials = tbRestClient.getUserCredentials(new UserId(UUID.fromString(strUserId)));
        userNode.put("password", userCredentials.getPassword());
    }

}
