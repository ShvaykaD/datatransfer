package org.thingsboard.datatransfer.importing;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.gson.JsonElement;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.apache.http.entity.ContentType;

import java.io.IOException;

@Slf4j
public class Client {

    private HttpClient client;

    Client() {
        MultiThreadedHttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();
        HttpConnectionManagerParams params = new HttpConnectionManagerParams();
        params.setDefaultMaxConnectionsPerHost(1000);
        connectionManager.setParams(params);
        this.client = new HttpClient(connectionManager);
    }

    public int sendData(String uri, JsonNode node, String token) throws IOException {
        PostMethod post = new PostMethod(uri);
        post.setRequestEntity(new StringRequestEntity(node.toString(), ContentType.APPLICATION_JSON.getMimeType(), "UTF-8"));
        post.setRequestHeader("Accept", "application/json");
        post.setRequestHeader("Content-type", "application/json");
        post.setRequestHeader("X-Authorization", "Bearer " + token);
        try {
            return client.executeMethod(post);
        } finally {
            post.releaseConnection();
        }
    }

    public int sendGsonData(String uri, JsonElement node, String token) throws IOException {
        PostMethod post = new PostMethod(uri);
        post.setRequestEntity(new StringRequestEntity(node.toString(), ContentType.APPLICATION_JSON.getMimeType(), "UTF-8"));
        post.setRequestHeader("Accept", "application/json");
        post.setRequestHeader("Content-type", "application/json");
        post.setRequestHeader("X-Authorization", "Bearer " + token);
        try {
            return client.executeMethod(post);
        } finally {
            post.releaseConnection();
        }
    }

}
