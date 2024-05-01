package org.example;


import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CoordinatorTest {
    private Coordinator coordinator;
    private Thread serverThread;
    private int testPort = 8081;

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
        // remove all the .csv files

    }

    @Test
    void myTest() throws Exception {
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpGet request = new HttpGet("http://localhost:" + testPort + "/status");
            String responseBody = EntityUtils.toString(client.execute(request).getEntity());
            assertEquals("ok", responseBody);
        }
    }

    // test for POST /create endpoint
    @Test
    void testCreate() throws Exception {
    }
}