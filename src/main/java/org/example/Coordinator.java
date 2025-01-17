package org.example;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.*;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.update.UpdateSet;
import org.example.config.HorizontalPartitionConfig;
import org.example.config.PartitionConfig;
import org.example.config.VerticalPartitionConfig;
import org.example.dto.*;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.insert.Insert;
import org.example.exception.CannotWriteException;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.rmi.NoSuchObjectException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Coordinates the creation, selection, and deletion of tables.
 */
public class Coordinator {
    /**
     * Main method to start the program.
     *
     * @param args Command line arguments. If provided, the server will listen on the specified port.
     * @throws IOException If there is an error reading from or writing to the network.
     */
    public static void main(String[] args) throws IOException {
        int serverPort = 8080;
        if (args.length > 0) {
            try {
                serverPort = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid port number provided, using default port " + serverPort);
            }
        }
        Coordinator coordinator = new Coordinator();
        coordinator.run(serverPort);
    }

    private ObjectMapper mapper = new ObjectMapper();
    // tablename-"SQL | NoSQL" -> DatabaseNodeClient
    private Map<String, DatabaseNodeClient> databases = new ConcurrentHashMap<>();
    // for test
    public Map<String, DatabaseNodeClient> getDatabases() {
        return databases;
    }
    private HttpServer server;
    private Registry registry;

    private ConcurrentHashMap<String, String> cache = new ConcurrentHashMap<>();
    // for test
    public ConcurrentHashMap<String, String> getCache() {
        return cache;
    }
    /**
     * Coordinates the creation, insertion, selection, update, and deletion of tables.
     * @param port the server to listen on
     * @throws IOException if an error occurs while creating the server
     */
    public void run(int port) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);

        server.createContext("/create", new CreateHandler());
        server.createContext("/insert", new InsertHandler());
        server.createContext("/select", new SelectHandler());
        server.createContext("/update", new UpdateHandler());
        server.createContext("/delete", new DeleteHandler());

        server.setExecutor(Executors.newCachedThreadPool()); // to avoid creating and destroying thread every request
        server.start();
        registry = LocateRegistry.createRegistry(1099); // for RMI

    }

    /**
     * Stops the server and unregisters the RMI registry.
     */
    // for test
    public void stop() {
        server.stop(1);
        try {
            UnicastRemoteObject.unexportObject(registry, true);
        } catch (NoSuchObjectException e) {
            e.printStackTrace();
        }
    }


    /**
     * Handler that accepts /create POST request, the request body is a JSON object with key "statement" and value "CREATE TABLE ..."
     * The handler will parse the sql statement with jSqlParser, and if it's valid,
     * it will create a DatabaseNode() object and add it to the list of nodes
     */
    private class CreateHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("POST".equals(exchange.getRequestMethod())){
                try {
                    CreateRequestDto createRequestDto = mapper.readValue(exchange.getRequestBody(), CreateRequestDto.class);
                    createRequestDto.validate();
                    String databaseType = createRequestDto.getDatabaseType();
                    String statementString = createRequestDto.getStatement();
                    int replicaCount = createRequestDto.getReplicaCount();
                    String partitionType = createRequestDto.getPartitionType();
                    int numPartitions = createRequestDto.getNumPartitions();
                    List<List<String>> verticalPartitionColumns = createRequestDto.getVerticalPartitionColumns();
                    PartitionConfig partitionConfig = partitionType.equals("horizontal") ?
                            new HorizontalPartitionConfig(numPartitions) : partitionType.equals("vertical") ?
                            new VerticalPartitionConfig(numPartitions, verticalPartitionColumns) : null;
                    if (databaseType.equals("SQL")) {
                        // get the statement from the request body
                        Statement statement = CCJSqlParserUtil.parse(statementString);
                        if (statement instanceof CreateTable) {
                            CreateTable create = (CreateTable) statement;
                            String tableName = create.getTable().getName();
                            List<String> columnNames = create.getColumnDefinitions().stream().map(ColumnDefinition::getColumnName).toList();
                            DatabaseNodeClient node = new DatabaseNodeClient(
                                    tableName,
                                    columnNames,
                                    replicaCount,
                                    partitionConfig
                            );
                            String key = tableName + "-SQL";
                            if (databases.containsKey(key)) {
                                handleBadRequest(exchange, "table already exists");
                                return;
                            }
                            databases.put(key, node);
                        } else {
                            handleBadRequest(exchange, "invalid create statement");
                            return;
                        }
                    } else if (databaseType.equals("NoSQL")) {
                        // handle NoSQL
                        // CREATE TABLE users
                        // schema is not needed
                        if (statementString.trim().startsWith("CREATE TABLE")) {
                            String[] split = statementString.split(" ");
                            if (split.length != 3) {
                                handleBadRequest(exchange, "invalid create statement");
                                return;
                            }
                            String tableName = split[2];
                            String key = tableName + "-NoSQL";
                            if (databases.containsKey(key)) {
                                handleBadRequest(exchange, "table already exists");
                                return;
                            }
                            databases.put(key, new DatabaseNodeClient(tableName, null, replicaCount, partitionConfig));
                        } else {
                            handleBadRequest(exchange);
                            return;
                        }
                    } else {
                        handleBadRequest(exchange);
                        return;
                    }

                } catch (DatabindException | JSQLParserException | IllegalArgumentException e) {
                    e.printStackTrace();
                    handleBadRequest(exchange);
                    return;
                } catch (Exception e) {
                    e.printStackTrace();
                }
                handleOkResponse(exchange);
            }
        }
    }

    /**
     * Insert Handler
     */
    private class InsertHandler implements HttpHandler {
        /**
         * Handles HTTP POST requests for inserting data into a table.
         * @param exchange the HTTP exchange
         * @throws IOException if an I/O error occurs
         */
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("POST".equals(exchange.getRequestMethod())) {
                try {
                    InsertRequestDto insertRequestDto =
                            mapper.readValue(exchange.getRequestBody(), InsertRequestDto.class);
                    String databaseType = insertRequestDto.getDatabaseType();
                    if (databaseType.equals("SQL")) {
                        // get the statement from the request body
                        String statementString = insertRequestDto.getStatement();
                        Statement statement = CCJSqlParserUtil.parse(statementString);
                        // check if the statement is an insert statement
                        if (statement instanceof Insert) {
                            Insert insert = (Insert) statement;
                            String tableName = insert.getTable().getName();
                            String key = tableName + "-SQL";
                            // check if the table exists
                            if (!databases.containsKey(key)) {
                                handleBadRequest(exchange, "table not exist");
                                return;
                            }
                            // check if insert.getColumns() is a subset of databases.get(tableName).getColumns()
                            List<String> insertCols = insert.getColumns().stream().map(Column::getColumnName).toList();
                            List<String> tableCols = databases.get(key).getColumns();
                            if (!tableCols.containsAll(insertCols)) {
                                handleBadRequest(exchange, "invalid columns");
                                return;
                            }
                            List<String> values = insert.getValues().getExpressions().stream().map(Expression::toString).toList();
                            databases.get(key).insertSQL(insertCols, values);
                            cache.remove(key);
                        } else {
                            handleBadRequest(exchange, "invalid insert statement");
                            return;
                        }
                    } else if (databaseType.equals("NoSQL")){
                        // handle NoSQL
                        // INSERT key1 value1 key2 value2
                        String statementString = insertRequestDto.getStatement();
                        List<String> statementList = Arrays.asList(statementString.split(" "));
                        String insert = statementList.get(0);
                        if (!insert.equals("INSERT")) {
                            handleBadRequest(exchange, "invalid insert statement");
                            return;
                        }
                        if (statementList.size() < 4) {
                            // at least on key-value pair
                            handleBadRequest(exchange, "invalid insert statement");
                            return;
                        }
                        String tableName = statementList.get(1);
                        String key = tableName + "-NoSQL";
                        if (!databases.containsKey(key)) {
                            handleBadRequest(exchange, "table not exist");
                            return;
                        }
                        List<String> kvPairs = statementList.subList(2, statementList.size());
                        databases.get(key).insertNoSQL(kvPairs);
                        cache.remove(key);
                    } else {
                        handleBadRequest(exchange);
                        return;
                    }
                } catch(DatabindException | JSQLParserException e) {
                    e.printStackTrace();
                    handleBadRequest(exchange);
                    return;
                }
                catch (CannotWriteException e) {
                    handleBadRequest(exchange, "database in read-only mode due to failure");
                    return;
                } catch (Exception e) {
                    e.printStackTrace();
                }
                handleOkResponse(exchange);
            }
        }
    }

    /**
     * Select Handler
     */
    private class SelectHandler implements HttpHandler {
        /**
         * Handles HTTP POST requests for SELECT operations.
         *
         * @param exchange the HTTP exchange
         * @throws IOException if an I/O error occurs
         */
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("POST".equals(exchange.getRequestMethod())) {
                try {
                    SelectRequestDto selectRequestDto = mapper.readValue(exchange.getRequestBody(), SelectRequestDto.class);
                    String databaseType = selectRequestDto.getDatabaseType();
                    if (databaseType.equals("SQL")) {
                        // get the statement from the request body
                        String statementString = selectRequestDto.getStatement();
                        Statement statement = CCJSqlParserUtil.parse(statementString);
                        // check if the statement is a select statement
                        if (statement instanceof PlainSelect) {
                            PlainSelect select = (PlainSelect) statement;
                            Table table = (Table) select.getFromItem();
                            String tableName = table.getName();
                            String key = tableName + "-SQL";
                            // check if the table exists
                            if (!databases.containsKey(key)) {
                                handleBadRequest(exchange, "table not exist");
                                return;
                            }
                            // will just support select * for now
                            String result = databases.get(key).selectSQL();
                            cache.put(key, result);
                            handleResponse(exchange, 200, result);
                        } else {
                            handleBadRequest(exchange, "invalid select statement");
                        }
                    } else if (databaseType.equals("NoSQL")) {
                        // handle NoSQL
                        // SELECT Users (just support select all for now)
                        String statementString = selectRequestDto.getStatement();
                        String[] split = statementString.split(" ");
                        if (split.length != 2) {
                            handleBadRequest(exchange, "invalid select statement");
                            return;
                        }
                        String tableName = split[1];
                        String key = tableName + "-NoSQL";
                        if (!databases.containsKey(key)) {
                            handleBadRequest(exchange, "table not exist");
                            return;
                        }
                        // will just support select * for now
                        String result = databases.get(key).selectNoSQL();
                        handleResponse(exchange, 200, result);
                        cache.put(key, result);
                    } else {
                        handleBadRequest(exchange);
                    }
                } catch (DatabindException | JSQLParserException e) {
                    e.printStackTrace();
                    handleBadRequest(exchange);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Update Handler
     */
    private class UpdateHandler implements HttpHandler {
        /**
         * Handles HTTP POST requests for UPDATE operations.
         *
         * @param exchange the HTTP exchange
         * @throws IOException if an I/O error occurs
         */
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("POST".equals(exchange.getRequestMethod())) {
                try {
                    UpdateRequestDto updateRequestDto =
                            mapper.readValue(exchange.getRequestBody(), UpdateRequestDto.class);
                    String databaseType = updateRequestDto.getDatabaseType();
                    if (databaseType.equals("SQL")) {
                        // get the statement from the request body
                        String statementString = updateRequestDto.getStatement();
                        Statement statement = CCJSqlParserUtil.parse(statementString);
                        // check if the statement is an update statement
                        if (statement instanceof Update) {
                            Update update = (Update) statement;
                            Table table = update.getTable();
                            String tableName = table.getName();
                            String key = tableName + "-SQL";
                            if (!databases.containsKey(key)) {
                                handleBadRequest(exchange, "table not exist");
                                return;
                            }
                            List<UpdateSet> updateSets = update.getUpdateSets();
                            List<String> cols = new ArrayList<>();
                            List<String> values = new ArrayList<>();
                            for (UpdateSet updateSet : updateSets) {
                                cols.add(updateSet.getColumns().get(0).toString());
                                values.add(updateSet.getValues().get(0).toString());
                            }
                            // just support simple where now
                            Expression where = update.getWhere();
                            databases.get(key).updateSQL(cols, values, where.toString());
                            cache.remove(key);
                        } else {
                            handleBadRequest(exchange, "invalid update statement");
                            return;
                        }
                    } else if (databaseType.equals("NoSQL")) {
                        // handle NoSQL
                        // UPDATE Users key1 value1 key2 value2 WHERE key3 value3
                        String statementString = updateRequestDto.getStatement();
                        List<String> statementList = Arrays.asList(statementString.split(" "));
                        String update = statementList.get(0);
                        if (!update.equals("UPDATE")) {
                            handleBadRequest(exchange, "invalid update statement");
                            return;
                        }
                        if (statementList.size() < 4) {
                            // at least on key-value pair
                            handleBadRequest(exchange, "invalid update statement");
                            return;
                        }
                        String tableName = statementList.get(1);
                        String key = tableName + "-NoSQL";
                        if (!databases.containsKey(key)) {
                            handleBadRequest(exchange, "table not exist");
                            return;
                        }
                        // find WHERE
                        int whereIndex = statementList.indexOf("WHERE");
                        if (whereIndex == -1) {
                            handleBadRequest(exchange, "invalid update statement");
                            return;
                        }
                        List<String> kvPairs = statementList.subList(2, whereIndex);
                        List<String> where = statementList.subList(whereIndex + 1, statementList.size()); // only support simple where for now
                        databases.get(key).updateNoSQL(kvPairs, where);
                        cache.remove(key);
                    } else {
                        handleBadRequest(exchange);
                        return;
                    }

                } catch (DatabindException | JSQLParserException e) {
                    e.printStackTrace();
                    handleBadRequest(exchange);
                    return;
                } catch (CannotWriteException e) {
                    handleBadRequest(exchange, "database in read-only mode due to failure");
                    return;
                } catch (Exception e) {
                    e.printStackTrace();
                }
                handleOkResponse(exchange);
            }
        }
    }

    /**
     * Delete Handler
     */
    private class DeleteHandler implements HttpHandler {
        /**
         * Handles HTTP POST requests for DELETE operations.
         *
         * @param exchange the HTTP exchange
         * @throws IOException if an I/O error occurs
         */
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("POST".equals(exchange.getRequestMethod())) {
                try {
                    DeleteRequestDto deleteRequestDto =
                            mapper.readValue(exchange.getRequestBody(), DeleteRequestDto.class);
                    String databaseType = deleteRequestDto.getDatabaseType();
                    if (databaseType.equals("SQL")) {
                        // get the statement from the request body
                        String statementString = deleteRequestDto.getStatement();
                        Statement statement = CCJSqlParserUtil.parse(statementString);
                        // check if the statement is an update statement
                        if (statement instanceof Delete) {
                            Delete delete = (Delete) statement;
                            Table table = delete.getTable();
                            String tableName = table.getName();
                            String key = tableName + "-SQL";
                            if (!databases.containsKey(key)) {
                                handleBadRequest(exchange, "table not exist");
                                return;
                            }
                            // just support simple where now
                            Expression where = delete.getWhere();
                            databases.get(key).deleteSQL(where.toString());
                            cache.remove(key);
                        } else {
                            handleBadRequest(exchange);
                            return;
                        }
                    } else if (databaseType.equals("NoSQL")){
                        // handle NoSQL
                        // DELETE Users WHERE key value
                        String statementString = deleteRequestDto.getStatement();
                        List<String> statementList = Arrays.asList(statementString.split(" "));
                        String delete = statementList.get(0);
                        if (!delete.equals("DELETE")) {
                            handleBadRequest(exchange, "invalid delete statement");
                            return;
                        }
                        if (statementList.size() < 5) {
                            // at least on key-value pair
                            handleBadRequest(exchange, "invalid delete statement");
                            return;
                        }
                        String tableName = statementList.get(1);
                        String key = tableName + "-NoSQL";
                        if (!databases.containsKey(key)) {
                            handleBadRequest(exchange, "table not exist");
                            return;
                        }
                        // find WHERE
                        int whereIndex = statementList.indexOf("WHERE");
                        if (whereIndex == -1) {
                            handleBadRequest(exchange, "invalid delete statement");
                            return;
                        }
                        List<String> where = statementList.subList(whereIndex + 1, statementList.size());
                        databases.get(key).deleteNoSQL(where);
                        cache.remove(key);
                    } else {
                        handleBadRequest(exchange);
                        return;
                    }

                } catch (DatabindException | JSQLParserException e) {
                    e.printStackTrace();
                    handleBadRequest(exchange);
                    return;
                } catch (CannotWriteException e) {
                    handleBadRequest(exchange, "database in read-only mode due to failure");
                    return;
                } catch (Exception e) {
                    e.printStackTrace();
                }
                handleOkResponse(exchange);
            }
        }
    }

    // ****************** helper functions ********************
    /**
     * Sends a HTTP response with a specific status code and response body.
     *
     * @param exchange The HttpExchange object representing the current HTTP request-response exchange.
     * @param statusCode The HTTP status code to send in the response.
     * @param response The String body of the response to be sent.
     * @throws IOException if an I/O error occurs.
     */
    private void handleResponse(HttpExchange exchange, int statusCode, String response) throws IOException {
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(statusCode, response.getBytes().length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(response.getBytes());
        }
    }

    /**
     * Handles the successful HTTP response by sending a 200 status code with a JSON message "ok".
     *
     * @param exchange The HttpExchange object for the current request-response.
     * @throws IOException if an I/O error occurs during response sending.
     */
    private void handleOkResponse(HttpExchange exchange) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        SuccessResponse res = new SuccessResponse("ok");
        String jsonResponse = mapper.writeValueAsString(res);
        handleResponse(exchange, 200, jsonResponse);
    }
    /**
     * Encapsulates a successful response message.
     */
    private class SuccessResponse{
        public String message;
        public SuccessResponse(String message){
            this.message = message;
        }
    }
    /**
     * Handles the HTTP 400 Bad Request response with a predefined error message.
     *
     * @param exchange The HttpExchange object to send the response.
     * @throws IOException if an I/O error occurs during response sending.
     */
    private void handleBadRequest(HttpExchange exchange) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        BadRequestResponse res = new BadRequestResponse("invalid post body");
        String jsonResponse = mapper.writeValueAsString(res);
        handleResponse(exchange, 400, jsonResponse);
    }
    /**
     * Handles the HTTP 400 Bad Request response with a custom error message.
     *
     * @param exchange The HttpExchange object for the current request-response.
     * @param body The custom message to include in the bad request response.
     * @throws IOException if an I/O error occurs during response sending.
     */
    private void handleBadRequest(HttpExchange exchange, String body) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        BadRequestResponse res = new BadRequestResponse(body);
        String jsonResponse = mapper.writeValueAsString(res);
        handleResponse(exchange, 400, jsonResponse);
    }
    /**
     * Encapsulates a bad request response message.
     */
    private class BadRequestResponse {
        public String message;
        public BadRequestResponse(String message) {
            this.message = message;
        }
    }

    /**
     * Deletes all CSV files in the current directory and its subdirectories.
     */
    public void deleteCsvFiles() {
        Path directory = Paths.get("").toAbsolutePath();
        try {
            Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    if (file.toString().endsWith(".csv")) {
                        Files.delete(file);
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * Lists all CSV files in the current directory.
     *
     * @return A list of CSV file names in the current directory.
     * @throws IOException if an I/O error occurs while accessing the directory.
     */
    public List<String> listCsvFiles() throws IOException {
        Path directoryPath = Paths.get("").toAbsolutePath();
        if (!Files.exists(directoryPath)) {
            return List.of();  // Return an empty list if the directory does not exist
        }

        try (Stream<Path> stream = Files.walk(directoryPath, 1)) {  // Depth 1 to only look in the specified directory
            return stream
                    .filter(path -> path.toString().endsWith(".csv"))
                    .map(Path::getFileName)
                    .map(Path::toString)
                    .collect(Collectors.toList());
        }
    }
    /**
     * Reads and returns the content of a specified CSV file.
     *
     * @param fileName The name of the CSV file to read.
     * @return The content of the CSV file as a String.
     */
    public String readFromCsv(String fileName) {
        try {
            return Files.readString(Paths.get(fileName));
        } catch (IOException e) {
            e.printStackTrace();
            return "";
        }
    }
}
