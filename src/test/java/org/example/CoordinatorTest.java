package org.example;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.example.dto.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.UnsupportedEncodingException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class CoordinatorTest {
    private Coordinator coordinator;
    private Thread serverThread;
    private int testPort = 8081;

    private String url = "http://localhost:" + testPort;

    private ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    public void setUp() {
        serverThread = new Thread(() -> {
            coordinator = new Coordinator();
            try {
                coordinator.run(testPort);
            } catch (Exception e) {
                System.out.println("Test Server failed to start");
                e.printStackTrace();
            }
        });
        serverThread.start(); // Start server in a separate thread
    }

    @AfterEach
    public void tearDown() {
        coordinator.stop();
        coordinator.deleteCsvFiles();
    }

    // helper function to send a POST request
    private HttpResponseData sendPostRequest(String endPoint, String jsonInputString) {
        HttpPost request = new HttpPost(url + endPoint);
        request.setHeader("Content-Type", "application/json");
        try {
            request.setEntity(new StringEntity(jsonInputString));
        } catch (UnsupportedEncodingException e) {
            System.out.println("Error in sendPostRequest!!!!!");
            e.printStackTrace();
            return null;
        }
        try (CloseableHttpClient client = HttpClients.createDefault();
            CloseableHttpResponse response = client.execute(request)
        ) {
            String responseBody = EntityUtils.toString(response.getEntity());
            int statusCode = response.getStatusLine().getStatusCode();
            return new HttpResponseData(statusCode, responseBody);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Error in sendPostRequest!!!!!");
        return null;
    }

    @Test
    void myTest() throws Exception {
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpGet request = new HttpGet("http://localhost:" + testPort + "/status");
            String responseBody = EntityUtils.toString(client.execute(request).getEntity());
            assertEquals("ok", responseBody);
        }
    }

    // 1. test CRUD operations for SQL
    @Test
    void testCRUDSQL() throws Exception {
        // CREATE
        CreateRequestDto createRequestDto = new CreateRequestDto();
        createRequestDto.setStatement("CREATE TABLE students (id INT PRIMARY KEY, name VARCHAR(255), age INT)");
        createRequestDto.setDatabaseType("SQL");
        createRequestDto.setReplicaCount(3);
        createRequestDto.setPartitionType("none");
        createRequestDto.setNumPartitions(1);
        String createRequestJson = objectMapper.writeValueAsString(createRequestDto);
        HttpResponseData res = sendPostRequest("/create", createRequestJson);
        if (res == null) {
            throw new Exception("Error in create request");
        }
        JsonNode rootNode = objectMapper.readTree(res.getResponseBody());
        JsonNode messageNode = rootNode.get("message");
        assertEquals(200, res.getStatusCode());
        assertNotNull(messageNode);
        assertEquals("ok", messageNode.asText());

        // INSERT
        InsertRequestDto insertRequestDto = new InsertRequestDto();
        insertRequestDto.setStatement("INSERT INTO students (id, name, age) VALUES (1, 'Alice', 20)");
        insertRequestDto.setDatabaseType("SQL");
        String insertRequestJson = objectMapper.writeValueAsString(insertRequestDto);
        res = sendPostRequest("/insert", insertRequestJson);
        if (res == null) {
            throw new Exception("Error in insert request");
        }
        assertEquals(200, res.getStatusCode());

        // After insert, SELECT should get the expected result
        SelectRequestDto selectRequestDto = new SelectRequestDto();
        selectRequestDto.setStatement("SELECT * FROM students");
        selectRequestDto.setDatabaseType("SQL");
        String selectRequestJson = objectMapper.writeValueAsString(selectRequestDto);
        res = sendPostRequest("/select", selectRequestJson);
        if (res == null) {
            throw new Exception("Error in select request");
        }
        assertEquals(200, res.getStatusCode());
        assertEquals("1,'Alice',20,\n", res.getResponseBody());

        // UPDATE
        UpdateRequestDto updateRequestDto = new UpdateRequestDto();
        updateRequestDto.setStatement("UPDATE students SET age = 21 WHERE id = 1");
        updateRequestDto.setDatabaseType("SQL");
        String updateRequestJson = objectMapper.writeValueAsString(updateRequestDto);
        res = sendPostRequest("/update", updateRequestJson);
        if (res == null) {
            throw new Exception("Error in update request");
        }
        assertEquals(200, res.getStatusCode());

        // After update, SELECT should get the updated result
        res = sendPostRequest("/select", selectRequestJson);
        if (res == null) {
            throw new Exception("Error in select request");
        }
        assertEquals(200, res.getStatusCode());
        assertEquals("1,'Alice',21,\n", res.getResponseBody());

        // DELETE
        DeleteRequestDto deleteRequestDto = new DeleteRequestDto();
        deleteRequestDto.setStatement("DELETE FROM students WHERE id = 1");
        deleteRequestDto.setDatabaseType("SQL");
        String deleteRequestJson = objectMapper.writeValueAsString(deleteRequestDto);
        res = sendPostRequest("/delete", deleteRequestJson);
        if (res == null) {
            throw new Exception("Error in delete request");
        }
        assertEquals(200, res.getStatusCode());

        // After delete, SELECT should get empty result
        res = sendPostRequest("/select", selectRequestJson);
        if (res == null) {
            throw new Exception("Error in select request");
        }
        assertEquals(200, res.getStatusCode());
        assertEquals("", res.getResponseBody());
    }
    // 2. Test CRUD operations for NoSQL
    @Test
    void testCRUDNoSQL() throws Exception {
        // CREATE
        CreateRequestDto createRequestDto = new CreateRequestDto();
        createRequestDto.setStatement("CREATE TABLE students");
        createRequestDto.setDatabaseType("NoSQL");
        createRequestDto.setReplicaCount(3);
        createRequestDto.setPartitionType("none");
        createRequestDto.setNumPartitions(1);
        String createRequestJson = objectMapper.writeValueAsString(createRequestDto);
        HttpResponseData res = sendPostRequest("/create", createRequestJson);
        if (res == null) {
            throw new Exception("Error in create request");
        }
        JsonNode rootNode = objectMapper.readTree(res.getResponseBody());
        JsonNode messageNode = rootNode.get("message");
        assertEquals(200, res.getStatusCode());
        assertNotNull(messageNode);
        assertEquals("ok", messageNode.asText());

        // INSERT
        InsertRequestDto insertRequestDto = new InsertRequestDto();
        insertRequestDto.setStatement("INSERT students id 1 name 'Alice' age 20");
        insertRequestDto.setDatabaseType("NoSQL");
        String insertRequestJson = objectMapper.writeValueAsString(insertRequestDto);
        res = sendPostRequest("/insert", insertRequestJson);
        if (res == null) {
            throw new Exception("Error in insert request");
        }
        assertEquals(200, res.getStatusCode());

        // After insert, SELECT should get the expected result
        SelectRequestDto selectRequestDto = new SelectRequestDto();
        selectRequestDto.setStatement("SELECT students");
        selectRequestDto.setDatabaseType("NoSQL");
        String selectRequestJson = objectMapper.writeValueAsString(selectRequestDto);
        res = sendPostRequest("/select", selectRequestJson);
        if (res == null) {
            throw new Exception("Error in select request");
        }
        assertEquals(200, res.getStatusCode());
        assertEquals("id,1,name,'Alice',age,20,\n", res.getResponseBody());

        // UPDATE
        UpdateRequestDto updateRequestDto = new UpdateRequestDto();
        updateRequestDto.setStatement("UPDATE students age 21 WHERE id 1");
        updateRequestDto.setDatabaseType("NoSQL");
        String updateRequestJson = objectMapper.writeValueAsString(updateRequestDto);
        res = sendPostRequest("/update", updateRequestJson);
        if (res == null) {
            throw new Exception("Error in update request");
        }

        // After update, SELECT should get the updated result
        res = sendPostRequest("/select", selectRequestJson);
        if (res == null) {
            throw new Exception("Error in select request");
        }
        assertEquals(200, res.getStatusCode());
        assertEquals("id,1,name,'Alice',age,21,\n", res.getResponseBody());

        // DELETE
        DeleteRequestDto deleteRequestDto = new DeleteRequestDto();
        deleteRequestDto.setStatement("DELETE students WHERE id 1");
        deleteRequestDto.setDatabaseType("NoSQL");
        String deleteRequestJson = objectMapper.writeValueAsString(deleteRequestDto);
        res = sendPostRequest("/delete", deleteRequestJson);
        if (res == null) {
            throw new Exception("Error in delete request");
        }
        assertEquals(200, res.getStatusCode());

        // After delete, SELECT should get empty result
        res = sendPostRequest("/select", selectRequestJson);
        if (res == null) {
            throw new Exception("Error in select request");
        }
        assertEquals(200, res.getStatusCode());
        assertEquals("", res.getResponseBody());
    }

    // 3. Test all replicas are in sync for SQL

    // 4. Test all replicas are in sync for NoSQL

    // 5. If replica is down, the system can read but cannot update, insert, or delete for SQL

    // 6. If replica is down, the system can read but cannot update, insert, or delete for NoSQL

    // 7. Test horizontal partitioning for SQL

    // 8. Test horizontal partitioning for NoSQL

    // 9. Test vertical partitioning for SQL

    // 10. Test vertical partitioning for NoSQL

    // 11. Test CRUD operations for SQL with horizontal partitioning

    // 12. Test CRUD operations for NoSQL with horizontal partitioning

    // 13. Test CRUD operations for SQL with vertical partitioning

    // 14. Test CRUD operations for NoSQL with vertical partitioning

    // 15. Test caching
}