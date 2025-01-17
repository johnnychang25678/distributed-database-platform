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
import org.junit.jupiter.api.*;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class CoordinatorTest {
    private Coordinator coordinator;
    private Thread serverThread;
    private int testPort = 8081;

    private String url = "http://localhost:" + testPort;

    private ObjectMapper objectMapper = new ObjectMapper();
    private static TestResultsSummary results = new TestResultsSummary();

    /**
     * Sets up the testing environment before each test case. It starts the server on a separate thread.
     */
    @BeforeEach
    public void setUp() {
        serverThread = new Thread(() -> {
            coordinator = new Coordinator();
            try {
                coordinator.run(testPort);
                coordinator.deleteCsvFiles(); // remove before tests just in case
            } catch (Exception e) {
                System.out.println("Test Server failed to start");
                e.printStackTrace();
            }
        });
        serverThread.start(); // Start server in a separate thread
    }

    /**
     * Cleans up the testing environment after each test case. It stops the server and cleans up generated CSV files.
     */
    @AfterEach
    public void tearDown() {
        coordinator.stop();
        coordinator.deleteCsvFiles();
    }

    /**
     * Sends a POST request to a specified endpoint with JSON input.
     *
     * @param endPoint The API endpoint to which the request is sent.
     * @param jsonInputString JSON string to be sent in the request body.
     * @return HttpResponseData containing the status code and response body.
     */
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
    /**
     * TEST1: Tests CRUD operations for an SQL database. Validates creation, insertion, selection,
     * update, and deletion of database entries.
     */
    @Test
    void testCRUDSQL() throws Exception {
        System.out.println("1. Testing CRUD operations for SQL");
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
        results.setTestResult("Test_CRUD_SQL", true, 12.5);
    }
    /**
     * TEST2: Tests CRUD operations for a NoSQL database. This includes validating the creation, insertion,
     * selection, update, and deletion operations.
     */
    @Test
    void testCRUDNoSQL() throws Exception {
        System.out.println("2. Testing CRUD operations for NoSQL");
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
        results.setTestResult("Test_CRUD_NoSQL", true, 12.5);
    }

    /**
     * TEST3: Tests synchronization of all replicas in an SQL environment to ensure data consistency across replicas.
     */
    @Test
    void testReplicaSyncSQL() throws Exception {
        System.out.println("3. Testing all replicas are in sync for SQL");
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
        results.setTestResult("Test_Replica_Sync_SQL", true, 15);
    }

    /**
     * TEST4: Tests synchronization of all replicas in a NoSQL environment to ensure consistency across replicas.
     */
    @Test
    void testReplicaSyncNoSQL() throws Exception {
        System.out.println("4. Testing all replicas are in sync for NoSQL");
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
        results.setTestResult("Test_Replica_Sync_NoSQL", true, 15);
    }

    /**
     * TEST5: Tests behavior when a replica is down in an SQL environment. Verifies that the system can still read but
     * cannot write (update, insert, delete).
     */
    @Test
    void testReplicaDownBecomesReadOnlySQL() throws Exception {
        System.out.println("5. Testing Replica Down Becomes Read Only for SQL");
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

        // bring replica back up
        dbClient.startReplica(0, 0);
        // sleep for 3 seconds to allow heartbeat to detect replica is up
        Thread.sleep(3000);
        // should be able to write
        res = sendPostRequest("/update", updateRequestJson);
        if (res == null) {
            throw new Exception("Error in update request");
        }
        assertEquals(200, res.getStatusCode());

        // should be able to insert
        res = sendPostRequest("/insert", insertRequestJson);
        if (res == null) {
            throw new Exception("Error in insert request");
        }
        assertEquals(200, res.getStatusCode());

        // read to check if update and insert worked
        res = sendPostRequest("/select", selectRequestJson);
        if (res == null) {
            throw new Exception("Error in select request");
        }
        assertEquals(200, res.getStatusCode());
        assertEquals("1,'Alice',21,\n2,'Bob',21,\n", res.getResponseBody());
        results.setTestResult("Test_Replica_Down_SQL", true, 25);
    }

    /**
     * TEST6: Tests behavior when a replica is down in a NoSQL environment. Ensures that the system can read but cannot
     * write (update, insert, delete).
     */
    @Test
    void testReplicaDownBecomeReadOnlyNoSQL() throws Exception {
        System.out.println("6. Testing Replica Down Become Read Only for NoSQL");
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

        // bring replica back up
        dbClient.startReplica(0, 0);
        // sleep for 3 seconds to allow heartbeat to detect replica is up
        Thread.sleep(3000);
        // should be able to write
        res = sendPostRequest("/update", updateRequestJson);
        if (res == null) {
            throw new Exception("Error in update request");
        }
        assertEquals(200, res.getStatusCode());

        // should be able to insert
        res = sendPostRequest("/insert", insertRequestJson);
        if (res == null) {
            throw new Exception("Error in insert request");
        }
        assertEquals(200, res.getStatusCode());

        // read to check if update and insert worked
        res = sendPostRequest("/select", selectRequestJson);
        if (res == null) {
            throw new Exception("Error in select request");
        }
        assertEquals(200, res.getStatusCode());
        assertEquals("id,1,name,'Alice',age,21,\nid,2,name,'Bob',age,21,\n", res.getResponseBody());
        results.setTestResult("Test_Replica_Down_NoSQL", true, 25);
    }

    /**
     * TEST7: Tests horizontal partitioning strategy for an SQL database to validate partition-specific data handling.
     */
    @Test
    void testHorizontalPartitionSQL() throws Exception {
        System.out.println("7. Testing horizontal partitioning for SQL");
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
        results.setTestResult("Test_Horizontal_Partition_SQL", true, 15);
    }

    /**
     * TEST8: Tests horizontal partitioning strategy for a NoSQL database to validate partition-specific data handling.
     */
    @Test
    void testHorizontalPartitionNoSQL() throws Exception {
        System.out.println("8. Testing horizontal partitioning for NoSQL");
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
        assertEquals("id,2,name,'Bob',age,32,\nid,3,name,'Charlie',age,22\n", res.getResponseBody());
        results.setTestResult("Test_Horizontal_Partition_NoSQL", true, 15);
    }

    /**
     * TEST9: Tests vertical partitioning strategy for an SQL database to ensure data is properly distributed across partitions.
     */
    @Test
    void testVerticalPartitionSQL() throws Exception {
        System.out.println("9. Testing vertical partitioning for SQL");
        // CREATE replica = 2, partition = 2, [[id, name], [age]]
        CreateRequestDto createRequestDto = new CreateRequestDto();
        createRequestDto.setStatement("CREATE TABLE students (id INT PRIMARY KEY, name VARCHAR(255), age INT)");
        createRequestDto.setDatabaseType("SQL");
        createRequestDto.setReplicaCount(2);
        createRequestDto.setPartitionType("vertical");
        createRequestDto.setVerticalPartitionColumns(Arrays.asList(Arrays.asList("id", "name"), Arrays.asList("age")));
        createRequestDto.setNumPartitions(2); // need to match the size of verticalPartitionColumns
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

        // make sure correct columns in each csv file. partion 0 has id, name, partition 1 has age
        String readPartition0 = coordinator.readFromCsv("students-SQL-0-0.csv");
        assertEquals("id,name", readPartition0);
        String readPartition1 = coordinator.readFromCsv("students-SQL-1-0.csv");
        assertEquals("age", readPartition1);

        // INSERT a data with id, name, age, should be split into 2 csv files
        InsertRequestDto insertRequestDto = new InsertRequestDto();
        insertRequestDto.setStatement("INSERT INTO students (id, name, age) VALUES (1, 'Alice', 20)");
        insertRequestDto.setDatabaseType("SQL");
        String insertRequestJson = objectMapper.writeValueAsString(insertRequestDto);
        sendPostRequest("/insert", insertRequestJson);

        // check the csv files
        readPartition0 = coordinator.readFromCsv("students-SQL-0-0.csv");
        assertEquals("id,name\n1,'Alice',", readPartition0);
        readPartition1 = coordinator.readFromCsv("students-SQL-1-0.csv");
        assertEquals("age\n20,", readPartition1);

        // SELECT should work
        SelectRequestDto selectRequestDto = new SelectRequestDto();
        selectRequestDto.setStatement("SELECT * FROM students");
        selectRequestDto.setDatabaseType("SQL");
        String selectRequestJson = objectMapper.writeValueAsString(selectRequestDto);
        res = sendPostRequest("/select", selectRequestJson);
        if (res == null) {
            throw new Exception("Error in select request");
        }
        assertEquals(200, res.getStatusCode());
        assertEquals("1,'Alice',20,", res.getResponseBody());

        // UPDATE should work
        // limitation: can only update the columns in the same partition as the WHERE clause
        UpdateRequestDto updateRequestDto = new UpdateRequestDto();
        updateRequestDto.setStatement("UPDATE students SET name = 'Bob' WHERE id = 1");
        updateRequestDto.setDatabaseType("SQL");
        String updateRequestJson = objectMapper.writeValueAsString(updateRequestDto);
        res = sendPostRequest("/update", updateRequestJson);
        if (res == null) {
            throw new Exception("Error in update request");
        }
        assertEquals(200, res.getStatusCode());

        // check if update worked
        res = sendPostRequest("/select", selectRequestJson);
        if (res == null) {
            throw new Exception("Error in select request");
        }
        assertEquals(200, res.getStatusCode());
        assertEquals("1,'Bob',20,", res.getResponseBody());

        // DELETE should work
        DeleteRequestDto deleteRequestDto = new DeleteRequestDto();
        deleteRequestDto.setStatement("DELETE FROM students WHERE id = 1");
        deleteRequestDto.setDatabaseType("SQL");
        String deleteRequestJson = objectMapper.writeValueAsString(deleteRequestDto);
        res = sendPostRequest("/delete", deleteRequestJson);
        if (res == null) {
            throw new Exception("Error in delete request");
        }
        assertEquals(200, res.getStatusCode());

        // check if delete worked
        res = sendPostRequest("/select", selectRequestJson);
        if (res == null) {
            throw new Exception("Error in select request");
        }
        assertEquals(200, res.getStatusCode());
        assertEquals("", res.getResponseBody());
        results.setTestResult("Test_Vertical_Partition_SQL", true, 25);
    }

    /**
     * TEST10: Tests the effectiveness of a caching mechanism by verifying cache integrity after CRUD operations.
     */
    @Test
    void testCache() throws Exception {
        System.out.println("10. Testing caching mechanism");
        // CREATE
        CreateRequestDto createRequestDto = new CreateRequestDto();
        createRequestDto.setStatement("CREATE TABLE students (id INT PRIMARY KEY, name VARCHAR(255), age INT)");
        createRequestDto.setDatabaseType("SQL");
        createRequestDto.setReplicaCount(2);
        createRequestDto.setPartitionType("none");
        createRequestDto.setNumPartitions(1);
        String createRequestJson = objectMapper.writeValueAsString(createRequestDto);
        sendPostRequest("/create", createRequestJson);

        // INSERT some data
        InsertRequestDto insertRequestDto = new InsertRequestDto();
        insertRequestDto.setStatement("INSERT INTO students (id, name, age) VALUES (1, 'Alice', 20)");
        insertRequestDto.setDatabaseType("SQL");
        String insertRequestJson = objectMapper.writeValueAsString(insertRequestDto);
        sendPostRequest("/insert", insertRequestJson);

        // SELECT to cache the data
        SelectRequestDto selectRequestDto = new SelectRequestDto();
        selectRequestDto.setStatement("SELECT * FROM students");
        selectRequestDto.setDatabaseType("SQL");
        String selectRequestJson = objectMapper.writeValueAsString(selectRequestDto);
        sendPostRequest("/select", selectRequestJson);

        // check if the data is cached by reading the cache map directly
        ConcurrentHashMap<String, String> cache = coordinator.getCache();
        assertEquals(1, cache.size());
        assertTrue(cache.containsKey("students-SQL"));
        assertEquals("1,'Alice',20,\n", cache.get("students-SQL"));

        // INSERT new data
        insertRequestDto.setStatement("INSERT INTO students (id, name, age) VALUES (2, 'Bob', 21)");
        insertRequestJson = objectMapper.writeValueAsString(insertRequestDto);
        sendPostRequest("/insert", insertRequestJson);

        // cache should be invalidated after INSERT
        assertEquals(0, cache.size());

        // SELECT to cache the new data
        sendPostRequest("/select", selectRequestJson);

        // check if the new data is cached
        assertEquals(1, cache.size());
        assertTrue(cache.containsKey("students-SQL"));
        assertEquals("1,'Alice',20,\n2,'Bob',21,\n", cache.get("students-SQL"));

        // UPDATE should invalidate the cache
        UpdateRequestDto updateRequestDto = new UpdateRequestDto();
        updateRequestDto.setStatement("UPDATE students SET age = 22 WHERE id = 1");
        updateRequestDto.setDatabaseType("SQL");
        String updateRequestJson = objectMapper.writeValueAsString(updateRequestDto);
        sendPostRequest("/update", updateRequestJson);

        // cache should be invalidated after UPDATE
        assertEquals(0, cache.size());

        // SELECT to cache the updated data
        sendPostRequest("/select", selectRequestJson);

        // check if the updated data is cached
        assertEquals(1, cache.size());
        assertTrue(cache.containsKey("students-SQL"));
        assertEquals("1,'Alice',22,\n2,'Bob',21,\n", cache.get("students-SQL"));

        // DELETE should invalidate the cache
        DeleteRequestDto deleteRequestDto = new DeleteRequestDto();
        deleteRequestDto.setStatement("DELETE FROM students WHERE id = 1");
        deleteRequestDto.setDatabaseType("SQL");
        String deleteRequestJson = objectMapper.writeValueAsString(deleteRequestDto);
        sendPostRequest("/delete", deleteRequestJson);

        // cache should be invalidated after DELETE
        assertEquals(0, cache.size());

        // SELECT to cache the updated data
        sendPostRequest("/select", selectRequestJson);

        // check if the updated data is cached
        assertEquals(1, cache.size());
        assertTrue(cache.containsKey("students-SQL"));
        assertEquals("2,'Bob',21,\n", cache.get("students-SQL"));
        results.setTestResult("Test_Caching", true, 5);
    }

    /**
     * TEST11: Tests the system's ability to handle concurrent requests, ensuring data consistency and performance under load.
     */
    @Test
    void testConcurrentRequests() throws Exception {
        System.out.println("11. Testing concurrency mechanism");
        // CREATE
        CreateRequestDto createRequestDto = new CreateRequestDto();
        createRequestDto.setStatement("CREATE TABLE students (id INT PRIMARY KEY, name VARCHAR(255), age INT)");
        createRequestDto.setDatabaseType("SQL");
        createRequestDto.setReplicaCount(2);
        createRequestDto.setPartitionType("none");
        createRequestDto.setNumPartitions(1);
        String createRequestJson = objectMapper.writeValueAsString(createRequestDto);
        sendPostRequest("/create", createRequestJson);

        // use a loop to start 10 threads to insert concurrently
        ExecutorService executor = Executors.newFixedThreadPool(10);
        for (int i = 0; i < 10; i++) {
            InsertRequestDto insertRequestDto = new InsertRequestDto();
            int age = 20 + i;
            insertRequestDto.setStatement("INSERT INTO students (id, name, age) VALUES (" + i + ", 'Alice', " + age + ")");
            insertRequestDto.setDatabaseType("SQL");
            String insertRequestJson = objectMapper.writeValueAsString(insertRequestDto);
            executor.execute(new InsertTask(insertRequestJson));
        }
        executor.shutdown();

        // wait for all threads to finish
        Thread.sleep(5000);

        // check if all 10 inserts are successful by SELECT
        SelectRequestDto selectRequestDto = new SelectRequestDto();
        selectRequestDto.setStatement("SELECT * FROM students");
        selectRequestDto.setDatabaseType("SQL");
        String selectRequestJson = objectMapper.writeValueAsString(selectRequestDto);
        HttpResponseData res = sendPostRequest("/select", selectRequestJson);
        if (res == null) {
            throw new Exception("Error in select request");
        }
        assertEquals(200, res.getStatusCode());
        String responseBody = res.getResponseBody();
        for (int i = 0; i < 10; i++) {
            assertTrue(responseBody.contains(i + ",'Alice'," + (20 + i)));
        }
        results.setTestResult("Test_Concurrency", true, 15);
    }
    class InsertTask implements Runnable {
        private String insertRequestJson;

        public InsertTask(String insertRequestJson) {
            this.insertRequestJson = insertRequestJson;
        }

        @Override
        public void run() {
            sendPostRequest("/insert", insertRequestJson);
        }
    }

    public static class TestResultsSummary {
        private Map<String, Boolean> testResults = new HashMap<>();
        private Map<String, Double> testPoints = new HashMap<>();

        // Example method to set test results
        public void setTestResult(String testName, boolean passed, double points) {
            testResults.put(testName, passed);
            testPoints.put(testName, passed ? points : 0); // Award points only if the test passed
        }

        // Print summary report
        public void printSummary() {
            int passedTests = 0;
            int totalTests = testResults.size();
            Double totalPoints = 0.0;

            System.out.println("\n********** Test Summary Report ***********");
            for (Map.Entry<String, Boolean> entry : testResults.entrySet()) {
                String testName = entry.getKey();
                String result = testResults.get(testName) ? "PASS" : "FAIL";
                if (entry.getValue()) {
                    passedTests++;
                }
                Double points = testPoints.get(testName);
                totalPoints += points;
                System.out.println(testName + ": " + result + " - (Points: " + points + ")");
            }

            System.out.println("Passed: " + passedTests + " / " + totalTests);
            System.out.println("Score: " + totalPoints + " / " + 180.0 + " ( 100% ) ");
            System.out.println("******************************************");
        }
    }
    /**
     * This method prints a final summary of all test results once all test cases have completed.
     */
    @AfterAll
    public static void finalResults() {
        results.printSummary();
    }
}