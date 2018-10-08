package org.thingsboard.datatransfer.exporting.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.datatransfer.exporting.SaveContext;
import org.thingsboard.server.common.data.id.UserId;
import org.thingsboard.server.common.data.security.UserCredentials;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Optional;

@Slf4j
public class ExportUsers extends ExportEntity {


    public ExportUsers(RestClient tbRestClient, ObjectMapper mapper, String basePath) {
        super(tbRestClient, mapper, basePath);
    }

    public void getAllCustomerUsers(int limit) {
        Optional<JsonNode> usersOptional = tbRestClient.findAllCustomerUsers(limit);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(basePath + "Users.json")))) {
            if (usersOptional.isPresent()) {
                JsonNode usersArray = usersOptional.get().get("data");
                for (JsonNode userNode : usersArray) {
                    addUserCredentialsToUserNode((ObjectNode) userNode, userNode.get("id").get("id").asText());
                }
                writer.write(mapper.writeValueAsString(usersArray));
            }
        } catch (IOException e) {
            log.warn("Could not export customers to file.");
        }
    }

    private void addUserCredentialsToUserNode(ObjectNode userNode, String strUserId) {
        UserCredentials userCredentials = tbRestClient.getUserCredentials(UserId.fromString(strUserId));
        userNode.put("password", userCredentials.getPassword());
    }

}
