package org.thingsboard.datatransfer.exporting;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;

import java.io.IOException;

@Slf4j
public
class Client {

    private HttpClient client;
    private final ObjectMapper mapper;

    public Client(ObjectMapper mapper) {
        MultiThreadedHttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();
        HttpConnectionManagerParams params = new HttpConnectionManagerParams();
        params.setDefaultMaxConnectionsPerHost(1000);
        connectionManager.setParams(params);
        this.client = new HttpClient(connectionManager);
        this.mapper = mapper;
    }

    public JsonNode getData(String uri, String token) throws IOException {
        GetMethod getMethod = new GetMethod(uri);
        getMethod.setRequestHeader("Accept", "application/json");
        getMethod.setRequestHeader("X-Authorization", "Bearer " + token);
        try {
            client.executeMethod(getMethod);
            return mapper.readTree(getMethod.getResponseBodyAsStream());
        } finally {
            getMethod.releaseConnection();
        }
    }

}
