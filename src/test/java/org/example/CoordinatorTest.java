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
import java.sql.Time;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

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
    @Test
    void testReplicaSyncSQL() throws Exception {
        // CREATE then INSERT
        CreateRequestDto createRequestDto = new CreateRequestDto();
        createRequestDto.setStatement("CREATE TABLE students (id INT PRIMARY KEY, name VARCHAR(255), age INT)");
        createRequestDto.setDatabaseType("SQL");
        createRequestDto.setReplicaCount(3);
        createRequestDto.setPartitionType("none");
        createRequestDto.setNumPartitions(1);
        // no need to test this, previous tests already cover this
        String createRequestJson = objectMapper.writeValueAsString(createRequestDto);
        sendPostRequest("/create", createRequestJson);
        // after CREATE, will create .csv files, check .csv file names are correct
        // table-DBType-partitionId-replicaId
        // students-SQL-0-0.csv
        // students-SQL-0-1.csv
        // students-SQL-0-2.csv
        List<String> csvFiles = coordinator.listCsvFiles();
        assertEquals(3, csvFiles.size());
        assertTrue(csvFiles.contains("students-SQL-0-0.csv"));
        assertTrue(csvFiles.contains("students-SQL-0-1.csv"));
        assertTrue(csvFiles.contains("students-SQL-0-2.csv"));

        InsertRequestDto insertRequestDto = new InsertRequestDto();
        insertRequestDto.setStatement("INSERT INTO students (id, name, age) VALUES (1, 'Alice', 20)");
        insertRequestDto.setDatabaseType("SQL");
        String insertRequestJson = objectMapper.writeValueAsString(insertRequestDto);
        sendPostRequest("/insert", insertRequestJson);
        // check all replicas have the same data by reading csv files
        String read0 = coordinator.readFromCsv("students-SQL-0-0.csv");
        String read1 = coordinator.readFromCsv("students-SQL-0-1.csv");
        String read2 = coordinator.readFromCsv("students-SQL-0-2.csv");
        assertEquals(read0, read1);
        assertEquals(read1, read2);
        assertEquals(read0, read2);

        // UPDATE should also be in sync
        UpdateRequestDto updateRequestDto = new UpdateRequestDto();
        updateRequestDto.setStatement("UPDATE students SET age = 21 WHERE id = 1");
        updateRequestDto.setDatabaseType("SQL");
        String updateRequestJson = objectMapper.writeValueAsString(updateRequestDto);
        sendPostRequest("/update", updateRequestJson);
        // check all replicas have the same data by reading csv files
        read0 = coordinator.readFromCsv("students-SQL-0-0.csv");
        read1 = coordinator.readFromCsv("students-SQL-0-1.csv");
        read2 = coordinator.readFromCsv("students-SQL-0-2.csv");
        assertEquals(read0, read1);
        assertEquals(read1, read2);
        assertEquals(read0, read2);

        // DELETE should also be in sync
        DeleteRequestDto deleteRequestDto = new DeleteRequestDto();
        deleteRequestDto.setStatement("DELETE FROM students WHERE id = 1");
        deleteRequestDto.setDatabaseType("SQL");
        String deleteRequestJson = objectMapper.writeValueAsString(deleteRequestDto);
        sendPostRequest("/delete", deleteRequestJson);
        // check all replicas have the same data by reading csv files
        read0 = coordinator.readFromCsv("students-SQL-0-0.csv");
        read1 = coordinator.readFromCsv("students-SQL-0-1.csv");
        read2 = coordinator.readFromCsv("students-SQL-0-2.csv");
        assertEquals(read0, read1);
        assertEquals(read1, read2);
        assertEquals(read0, read2);

    }

    // 4. Test all replicas are in sync for NoSQL
    @Test
    void testReplicaSyncNoSQL() throws Exception {
        // CREATE then INSERT
        CreateRequestDto createRequestDto = new CreateRequestDto();
        createRequestDto.setStatement("CREATE TABLE students");
        createRequestDto.setDatabaseType("NoSQL");
        createRequestDto.setReplicaCount(3);
        createRequestDto.setPartitionType("none");
        createRequestDto.setNumPartitions(1);
        // no need to test this, previous tests already cover this
        String createRequestJson = objectMapper.writeValueAsString(createRequestDto);
        sendPostRequest("/create", createRequestJson);
        // after CREATE, will create .csv files, check .csv file names are correct
        // table-DBType-partitionId-replicaId
        // students-NoSQL-0-0.csv
        // students-NoSQL-0-1.csv
        // students-NoSQL-0-2.csv
        List<String> csvFiles = coordinator.listCsvFiles();
        assertEquals(3, csvFiles.size());
        assertTrue(csvFiles.contains("students-NoSQL-0-0.csv"));
        assertTrue(csvFiles.contains("students-NoSQL-0-1.csv"));
        assertTrue(csvFiles.contains("students-NoSQL-0-2.csv"));

        InsertRequestDto insertRequestDto = new InsertRequestDto();
        insertRequestDto.setStatement("INSERT students id 1 name 'Alice' age 20");
        insertRequestDto.setDatabaseType("NoSQL");
        String insertRequestJson = objectMapper.writeValueAsString(insertRequestDto);
        sendPostRequest("/insert", insertRequestJson);
        // check all replicas have the same data by reading csv files
        String read0 = coordinator.readFromCsv("students-NoSQL-0-0.csv");
        String read1 = coordinator.readFromCsv("students-NoSQL-0-1.csv");
        String read2 = coordinator.readFromCsv("students-NoSQL-0-2.csv");
        assertEquals(read0, read1);
        assertEquals(read1, read2);
        assertEquals(read0, read2);

        // UPDATE should also be in sync
        UpdateRequestDto updateRequestDto = new UpdateRequestDto();
        updateRequestDto.setStatement("UPDATE students age 21 WHERE id 1");
        updateRequestDto.setDatabaseType("NoSQL");
        String updateRequestJson = objectMapper.writeValueAsString(updateRequestDto);
        sendPostRequest("/update", updateRequestJson);
        // check all replicas have the same data by reading csv files
        read0 = coordinator.readFromCsv("students-NoSQL-0-0.csv");
        read1 = coordinator.readFromCsv("students-NoSQL-0-1.csv");
        read2 = coordinator.readFromCsv("students-NoSQL-0-2.csv");
        assertEquals(read0, read1);
        assertEquals(read1, read2);
        assertEquals(read0, read2);

        // DELETE should also be in sync
        DeleteRequestDto deleteRequestDto = new DeleteRequestDto();
        deleteRequestDto.setStatement("DELETE students WHERE id 1");
        deleteRequestDto.setDatabaseType("NoSQL");
        String deleteRequestJson = objectMapper.writeValueAsString(deleteRequestDto);
        sendPostRequest("/delete", deleteRequestJson);
        read0 = coordinator.readFromCsv("students-NoSQL-0-0.csv");
        read1 = coordinator.readFromCsv("students-NoSQL-0-1.csv");
        read2 = coordinator.readFromCsv("students-NoSQL-0-2.csv");
        assertEquals(read0, read1);
        assertEquals(read1, read2);
        assertEquals(read0, read2);
    }

    // 5. If replica is down, the system can read but cannot update, insert, or delete for SQL
    @Test
    void testReplicaDownBecomesReadOnlySQL() throws Exception {
        // CREATE
        CreateRequestDto createRequestDto = new CreateRequestDto();
        createRequestDto.setStatement("CREATE TABLE students (id INT PRIMARY KEY, name VARCHAR(255), age INT)");
        createRequestDto.setDatabaseType("SQL");
        createRequestDto.setReplicaCount(3);
        createRequestDto.setPartitionType("none");
        createRequestDto.setNumPartitions(1);
        String createRequestJson = objectMapper.writeValueAsString(createRequestDto);
        sendPostRequest("/create", createRequestJson);

        // INSERT
        InsertRequestDto insertRequestDto = new InsertRequestDto();
        insertRequestDto.setStatement("INSERT INTO students (id, name, age) VALUES (1, 'Alice', 20)");
        insertRequestDto.setDatabaseType("SQL");
        String insertRequestJson = objectMapper.writeValueAsString(insertRequestDto);
        sendPostRequest("/insert", insertRequestJson);

        // students-SQL -> DatabaseNodeClient -> {tableName: list of replicas}
        Map<String, DatabaseNodeClient> dbs = coordinator.getDatabases();
        DatabaseNodeClient dbClient = dbs.get("students-SQL");

        // shut down one replica
        dbClient.stopReplica(0, 0);
        // sleep for 3 seconds to allow heartbeat to detect replica is down
        Thread.sleep(3000);
        // should not be able to write
        UpdateRequestDto updateRequestDto = new UpdateRequestDto();
        updateRequestDto.setStatement("UPDATE students SET age = 21 WHERE id = 1");
        updateRequestDto.setDatabaseType("SQL");
        String updateRequestJson = objectMapper.writeValueAsString(updateRequestDto);
        HttpResponseData res = sendPostRequest("/update", updateRequestJson);
        if (res == null) {
            throw new Exception("Error in update request");
        }
        assertEquals(400, res.getStatusCode());
        JsonNode rootNode = objectMapper.readTree(res.getResponseBody());
        JsonNode messageNode = rootNode.get("message");
        assertNotNull(messageNode);
        assertEquals("database in read-only mode due to failure", messageNode.asText());

        // should not be able to insert
        insertRequestDto = new InsertRequestDto();
        insertRequestDto.setStatement("INSERT INTO students (id, name, age) VALUES (2, 'Bob', 21)");
        insertRequestDto.setDatabaseType("SQL");
        insertRequestJson = objectMapper.writeValueAsString(insertRequestDto);
        res = sendPostRequest("/insert", insertRequestJson);
        if (res == null) {
            throw new Exception("Error in insert request");
        }
        assertEquals(400, res.getStatusCode());
        rootNode = objectMapper.readTree(res.getResponseBody());
        messageNode = rootNode.get("message");
        assertNotNull(messageNode);
        assertEquals("database in read-only mode due to failure", messageNode.asText());

        // should not be able to delete
        DeleteRequestDto deleteRequestDto = new DeleteRequestDto();
        deleteRequestDto.setStatement("DELETE FROM students WHERE id = 1");
        deleteRequestDto.setDatabaseType("SQL");
        String deleteRequestJson = objectMapper.writeValueAsString(deleteRequestDto);
        res = sendPostRequest("/delete", deleteRequestJson);
        if (res == null) {
            throw new Exception("Error in delete request");
        }
        assertEquals(400, res.getStatusCode());
        rootNode = objectMapper.readTree(res.getResponseBody());
        messageNode = rootNode.get("message");
        assertNotNull(messageNode);
        assertEquals("database in read-only mode due to failure", messageNode.asText());

        // should be able to read
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
    }

    // 6. If replica is down, the system can read but cannot update, insert, or delete for NoSQL
    @Test
    void testReplicaDownBecomeReadOnlyNoSQL() throws Exception {
        // CREATE
        CreateRequestDto createRequestDto = new CreateRequestDto();
        createRequestDto.setStatement("CREATE TABLE students");
        createRequestDto.setDatabaseType("NoSQL");
        createRequestDto.setReplicaCount(3);
        createRequestDto.setPartitionType("none");
        createRequestDto.setNumPartitions(1);
        String createRequestJson = objectMapper.writeValueAsString(createRequestDto);
        sendPostRequest("/create", createRequestJson);

        // INSERT
        InsertRequestDto insertRequestDto = new InsertRequestDto();
        insertRequestDto.setStatement("INSERT students id 1 name 'Alice' age 20");
        insertRequestDto.setDatabaseType("NoSQL");
        String insertRequestJson = objectMapper.writeValueAsString(insertRequestDto);
        sendPostRequest("/insert", insertRequestJson);

        // students-NoSQL -> DatabaseNodeClient -> {tableName: list of replicas}
        Map<String, DatabaseNodeClient> dbs = coordinator.getDatabases();
        DatabaseNodeClient dbClient = dbs.get("students-NoSQL");

        // shut down one replica
        dbClient.stopReplica(0, 0);
        // sleep for 3 seconds to allow heartbeat to detect replica is down
        Thread.sleep(3000);

        // should not be able to update
        UpdateRequestDto updateRequestDto = new UpdateRequestDto();
        updateRequestDto.setStatement("UPDATE students age 21 WHERE id 1");
        updateRequestDto.setDatabaseType("NoSQL");
        String updateRequestJson = objectMapper.writeValueAsString(updateRequestDto);
        HttpResponseData res = sendPostRequest("/update", updateRequestJson);
        if (res == null) {
            throw new Exception("Error in update request");
        }
        assertEquals(400, res.getStatusCode());
        JsonNode rootNode = objectMapper.readTree(res.getResponseBody());
        JsonNode messageNode = rootNode.get("message");
        assertNotNull(messageNode);
        assertEquals("database in read-only mode due to failure", messageNode.asText());

        // should not be able to insert
        insertRequestDto = new InsertRequestDto();
        insertRequestDto.setStatement("INSERT students id 2 name 'Bob' age 21");
        insertRequestDto.setDatabaseType("NoSQL");
        insertRequestJson = objectMapper.writeValueAsString(insertRequestDto);
        res = sendPostRequest("/insert", insertRequestJson);
        if (res == null) {
            throw new Exception("Error in insert request");
        }
        assertEquals(400, res.getStatusCode());
        rootNode = objectMapper.readTree(res.getResponseBody());
        messageNode = rootNode.get("message");
        assertNotNull(messageNode);
        assertEquals("database in read-only mode due to failure", messageNode.asText());

        // should not be able to delete
        DeleteRequestDto deleteRequestDto = new DeleteRequestDto();
        deleteRequestDto.setStatement("DELETE students WHERE id 1");
        deleteRequestDto.setDatabaseType("NoSQL");
        String deleteRequestJson = objectMapper.writeValueAsString(deleteRequestDto);
        res = sendPostRequest("/delete", deleteRequestJson);
        if (res == null) {
            throw new Exception("Error in delete request");
        }
        assertEquals(400, res.getStatusCode());
        rootNode = objectMapper.readTree(res.getResponseBody());
        messageNode = rootNode.get("message");
        assertNotNull(messageNode);
        assertEquals("database in read-only mode due to failure", messageNode.asText());

        // should be able to read
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
    }

    // 7. Test horizontal partitioning for SQL
    @Test
    void testHorizontalPartitionSQL() throws Exception {
        // CREATE replica = 2, partition = 2
        CreateRequestDto createRequestDto = new CreateRequestDto();
        createRequestDto.setStatement("CREATE TABLE students (id INT PRIMARY KEY, name VARCHAR(255), age INT)");
        createRequestDto.setDatabaseType("SQL");
        createRequestDto.setReplicaCount(2);
        createRequestDto.setPartitionType("horizontal");
        createRequestDto.setNumPartitions(2);
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

        // should have 4 csv files with names: students-SQL-0-0.csv, students-SQL-0-1.csv, students-SQL-1-0.csv, students-SQL-1-1.csv
        List<String> csvFiles = coordinator.listCsvFiles();
        assertEquals(4, csvFiles.size());
        assertTrue(csvFiles.contains("students-SQL-0-0.csv"));
        assertTrue(csvFiles.contains("students-SQL-0-1.csv"));
        assertTrue(csvFiles.contains("students-SQL-1-0.csv"));
        assertTrue(csvFiles.contains("students-SQL-1-1.csv"));

        // INSERT with id = 1, 2, 3. 1 in partition 1, 2 in partition 0, 3 in partition 1
        InsertRequestDto insertRequestDto = new InsertRequestDto();
        insertRequestDto.setStatement("INSERT INTO students (id, name, age) VALUES (1, 'Alice', 20)");
        insertRequestDto.setDatabaseType("SQL");
        String insertRequestJson = objectMapper.writeValueAsString(insertRequestDto);
        sendPostRequest("/insert", insertRequestJson);

        insertRequestDto.setStatement("INSERT INTO students (id, name, age) VALUES (2, 'Bob', 21)");
        insertRequestJson = objectMapper.writeValueAsString(insertRequestDto);
        sendPostRequest("/insert", insertRequestJson);

        insertRequestDto.setStatement("INSERT INTO students (id, name, age) VALUES (3, 'Charlie', 22)");
        insertRequestJson = objectMapper.writeValueAsString(insertRequestDto);
        sendPostRequest("/insert", insertRequestJson);

        // check the csv files have the correct data
        String readPartition0 = coordinator.readFromCsv("students-SQL-0-0.csv");
        assertEquals("id,name,age\n2,'Bob',21,", readPartition0);
        String readPartition1 = coordinator.readFromCsv("students-SQL-1-0.csv");
        assertEquals("id,name,age\n1,'Alice',20,\n3,'Charlie',22,", readPartition1);

        // check SELECT works
        SelectRequestDto selectRequestDto = new SelectRequestDto();
        selectRequestDto.setStatement("SELECT * FROM students");
        selectRequestDto.setDatabaseType("SQL");
        String selectRequestJson = objectMapper.writeValueAsString(selectRequestDto);
        res = sendPostRequest("/select", selectRequestJson);
        if (res == null) {
            throw new Exception("Error in select request");
        }
        assertEquals(200, res.getStatusCode());
        assertEquals("2,'Bob',21,\n1,'Alice',20,\n3,'Charlie',22,\n", res.getResponseBody());

        // check UPDATE works
        UpdateRequestDto updateRequestDto = new UpdateRequestDto();
        updateRequestDto.setStatement("UPDATE students SET age = 32 WHERE id = 2");
        updateRequestDto.setDatabaseType("SQL");
        String updateRequestJson = objectMapper.writeValueAsString(updateRequestDto);
        res = sendPostRequest("/update", updateRequestJson);
        if (res == null) {
            throw new Exception("Error in update request");
        }
        assertEquals(200, res.getStatusCode());

        // select to check if update worked
        res = sendPostRequest("/select", selectRequestJson);
        if (res == null) {
            throw new Exception("Error in select request");
        }
        assertEquals(200, res.getStatusCode());
        assertEquals("2,'Bob',32,\n1,'Alice',20,\n3,'Charlie',22,\n", res.getResponseBody());

        // check DELETE works
        DeleteRequestDto deleteRequestDto = new DeleteRequestDto();
        deleteRequestDto.setStatement("DELETE FROM students WHERE id = 1");
        deleteRequestDto.setDatabaseType("SQL");
        String deleteRequestJson = objectMapper.writeValueAsString(deleteRequestDto);
        res = sendPostRequest("/delete", deleteRequestJson);
        if (res == null) {
            throw new Exception("Error in delete request");
        }
        assertEquals(200, res.getStatusCode());

        // select to check if delete worked
        res = sendPostRequest("/select", selectRequestJson);
        if (res == null) {
            throw new Exception("Error in select request");
        }
        assertEquals(200, res.getStatusCode());
        assertEquals("2,'Bob',32,\n3,'Charlie',22,\n", res.getResponseBody());
    }

    // 8. Test horizontal partitioning for NoSQL
    @Test
    void testHorizontalPartitionNoSQL() throws Exception {
        // CREATE replica = 2, partition = 2
        CreateRequestDto createRequestDto = new CreateRequestDto();
        createRequestDto.setStatement("CREATE TABLE students");
        createRequestDto.setDatabaseType("NoSQL");
        createRequestDto.setReplicaCount(2);
        createRequestDto.setPartitionType("horizontal");
        createRequestDto.setNumPartitions(2);
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

        // should have 4 csv files with names: students-NoSQL-0-0.csv, students-NoSQL-0-1.csv, students-NoSQL-1-0.csv, students-NoSQL-1-1.csv
        List<String> csvFiles = coordinator.listCsvFiles();
        assertEquals(4, csvFiles.size());
        assertTrue(csvFiles.contains("students-NoSQL-0-0.csv"));
        assertTrue(csvFiles.contains("students-NoSQL-0-1.csv"));
        assertTrue(csvFiles.contains("students-NoSQL-1-0.csv"));
        assertTrue(csvFiles.contains("students-NoSQL-1-1.csv"));

        // INSERT with id = 1, 2, 3. 1 in partition 1, 2 in partition 0, 3 in partition 1
        InsertRequestDto insertRequestDto = new InsertRequestDto();
        insertRequestDto.setStatement("INSERT students id 1 name 'Alice' age 20");
        insertRequestDto.setDatabaseType("NoSQL");
        String insertRequestJson = objectMapper.writeValueAsString(insertRequestDto);
        sendPostRequest("/insert", insertRequestJson);

        insertRequestDto.setStatement("INSERT students id 2 name 'Bob' age 21");
        insertRequestJson = objectMapper.writeValueAsString(insertRequestDto);
        sendPostRequest("/insert", insertRequestJson);

        insertRequestDto.setStatement("INSERT students id 3 name 'Charlie' age 22");
        insertRequestJson = objectMapper.writeValueAsString(insertRequestDto);
        sendPostRequest("/insert", insertRequestJson);

        // check the csv files
        String readPartition0 = coordinator.readFromCsv("students-NoSQL-0-0.csv");
        assertEquals("id,2,name,'Bob',age,21,\n", readPartition0);
        String readPartition1 = coordinator.readFromCsv("students-NoSQL-1-0.csv");
        assertEquals("id,1,name,'Alice',age,20,\nid,3,name,'Charlie',age,22,\n", readPartition1);

        // check SELECT works
        SelectRequestDto selectRequestDto = new SelectRequestDto();
        selectRequestDto.setStatement("SELECT students");
        selectRequestDto.setDatabaseType("NoSQL");
        String selectRequestJson = objectMapper.writeValueAsString(selectRequestDto);
        res = sendPostRequest("/select", selectRequestJson);
        if (res == null) {
            throw new Exception("Error in select request");
        }
        assertEquals(200, res.getStatusCode());
        assertEquals("id,2,name,'Bob',age,21,\nid,1,name,'Alice',age,20,\nid,3,name,'Charlie',age,22,\n", res.getResponseBody());

        // check UPDATE works
        UpdateRequestDto updateRequestDto = new UpdateRequestDto();
        updateRequestDto.setStatement("UPDATE students age 32 WHERE id 2");
        updateRequestDto.setDatabaseType("NoSQL");
        String updateRequestJson = objectMapper.writeValueAsString(updateRequestDto);
        res = sendPostRequest("/update", updateRequestJson);
        if (res == null) {
            throw new Exception("Error in update request");
        }
        assertEquals(200, res.getStatusCode());

        // select to check if update worked
        res = sendPostRequest("/select", selectRequestJson);
        if (res == null) {
            throw new Exception("Error in select request");
        }
        assertEquals(200, res.getStatusCode());
        assertEquals("id,2,name,'Bob',age,32,\nid,1,name,'Alice',age,20,\nid,3,name,'Charlie',age,22,\n", res.getResponseBody());

        // check DELETE works
        DeleteRequestDto deleteRequestDto = new DeleteRequestDto();
        deleteRequestDto.setStatement("DELETE students WHERE id 1");
        deleteRequestDto.setDatabaseType("NoSQL");
        String deleteRequestJson = objectMapper.writeValueAsString(deleteRequestDto);
        res = sendPostRequest("/delete", deleteRequestJson);
        if (res == null) {
            throw new Exception("Error in delete request");
        }
        assertEquals(200, res.getStatusCode());

        // select to check if delete worked
        res = sendPostRequest("/select", selectRequestJson);
        if (res == null) {
            throw new Exception("Error in select request");
        }
        assertEquals(200, res.getStatusCode());
        assertEquals("id,2,name,'Bob',age,32,\nid,3,name,'Charlie',age,22,\n", res.getResponseBody());

    }

    // 9. Test vertical partitioning for SQL

    // 10. Test vertical partitioning for NoSQL

    // 11. Test caching
}