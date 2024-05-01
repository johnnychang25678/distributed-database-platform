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

public class Coordinator {
    // main method to start the program
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

    public void run(int port) throws IOException {
        // an http server that listens on the specified port
        server = HttpServer.create(new InetSocketAddress(port), 0);
        // create a new context for the server
        server.createContext("/create", new CreateHandler());
        server.createContext("/insert", new InsertHandler());
        server.createContext("/select", new SelectHandler());
        server.createContext("/update", new UpdateHandler());
        server.createContext("/delete", new DeleteHandler());

        // for test
        server.createContext("/status", new StatusHandler());

        server.setExecutor(Executors.newCachedThreadPool()); // to avoid creating and destroying thread every request
        server.start();
        registry = LocateRegistry.createRegistry(1099); // for RMI
        System.out.println("Coordinator server started on port " + port);
    }

    // for test
    public void stop() {
        System.out.println("Stopping Coordinator server...");
        server.stop(1);
        try {
            System.out.println("Stopping RMI registry...");
            UnicastRemoteObject.unexportObject(registry, true);
        } catch (NoSuchObjectException e) {
            System.out.println("Error stopping RMI registry");
            e.printStackTrace();
        }
        System.out.println("Coordinator server stopped!");
    }


    // Handler that accepts /create POST request, the request body is a JSON object with key "statement" and value "CREATE TABLE ..."
    // The handler will parse the sql statement with jSqlParser, and if it's valid,
    // it will create a DatabaseNode() object and add it to the list of nodes
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
                        System.out.println(statementString);
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

    private class InsertHandler implements HttpHandler {
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
                        System.out.println(statementString);
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
                        System.out.println(statementString);
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

    private class SelectHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("POST".equals(exchange.getRequestMethod())) {
                try {
                    SelectRequestDto selectRequestDto = mapper.readValue(exchange.getRequestBody(), SelectRequestDto.class);
                    String databaseType = selectRequestDto.getDatabaseType();
                    if (databaseType.equals("SQL")) {
                        // get the statement from the request body
                        String statementString = selectRequestDto.getStatement();
                        System.out.println(statementString);
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
                        System.out.println(statementString);
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

    private class UpdateHandler implements HttpHandler {
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
                        System.out.println(statementString);
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
                        System.out.println(statementString);
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

    private class DeleteHandler implements HttpHandler {
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
                        System.out.println(statementString);
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
                        System.out.println(statementString);
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

    // just a testing endpoint
    private class StatusHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("GET".equals(exchange.getRequestMethod())) {
                try {
                    handleResponse(exchange, 200, "ok");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    // ****************** helper functions ********************
    private void handleResponse(HttpExchange exchange, int statusCode, String response) throws IOException {
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(statusCode, response.getBytes().length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(response.getBytes());
        }
    }

    private void handleOkResponse(HttpExchange exchange) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        SuccessResponse res = new SuccessResponse("ok");
        String jsonResponse = mapper.writeValueAsString(res);
        handleResponse(exchange, 200, jsonResponse);
    }

    private class SuccessResponse{
        public String message;
        public SuccessResponse(String message){
            this.message = message;
        }
    }
    private void handleBadRequest(HttpExchange exchange) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        BadRequestResponse res = new BadRequestResponse("invalid post body");
        String jsonResponse = mapper.writeValueAsString(res);
        handleResponse(exchange, 400, jsonResponse);
    }
    private void handleBadRequest(HttpExchange exchange, String body) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        BadRequestResponse res = new BadRequestResponse(body);
        String jsonResponse = mapper.writeValueAsString(res);
        handleResponse(exchange, 400, jsonResponse);
    }
    private class BadRequestResponse {
        public String message;
        public BadRequestResponse(String message) {
            this.message = message;
        }
    }

    // ******************* for test *************************
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

    public String readFromCsv(String fileName) {
        try {
            return Files.readString(Paths.get(fileName));
        } catch (IOException e) {
            e.printStackTrace();
            return "";
        }
    }
}
