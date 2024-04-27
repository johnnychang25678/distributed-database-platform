package org.example;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.*;
import net.sf.jsqlparser.expression.Expression;
import org.example.dto.CreateRequestDto;
import org.example.dto.InsertRequestDto;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.insert.Insert;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.Executors;

public class Coordinator {
    // main method to start the program
    public static void main(String[] args) throws IOException {
        // create a new instance of the Coordinator class
        Coordinator coordinator = new Coordinator();
        // call the run method on the coordinator instance
        coordinator.run(8080);
    }

    private ObjectMapper mapper = new ObjectMapper();
    private Map<String, DatabaseNodeClient> databases = new HashMap<>();

    private void run(int port) throws IOException {
        // an http server that listens on the specified port
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        // create a new context for the server
        server.createContext("/create", new CreateHandler());
        server.createContext("/insert", new InsertHandler());
        server.setExecutor(Executors.newCachedThreadPool()); // to avoid creating and destroying thread every request
        server.start();
        System.out.println("Coordinator server started on port " + port);
    }

    // Handler that accepts /create POST request, the request body is a JSON object with key "statement" and value "CREATE TABLE ..."
    // The handler will parse the sql statement with jSqlParser, and if it's valid,
    // it will create a DatabaseNode() object and add it to the list of nodes
    private class CreateHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("POST".equals(exchange.getRequestMethod())){
                try {
                    CreateRequestDto stmtRequestDto = mapper.readValue(exchange.getRequestBody(), CreateRequestDto.class);
                    String databaseType = stmtRequestDto.getDatabaseType();
                    String statementString = stmtRequestDto.getStatement();
                    int replicaCount = stmtRequestDto.getReplicaCount();
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
                                    replicaCount
                            );
                            if (databases.containsKey(tableName)) {
                                handleBadRequest(exchange);
                                return;
                            }
                            databases.put(tableName, node);
                        } else {
                            handleBadRequest(exchange);
                        }
                    } else {
                        // handle NoSQL
                    }

                } catch (DatabindException | JSQLParserException e) {
                    e.printStackTrace();
                    handleBadRequest(exchange);
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
                            // check if the table exists
                            if (!databases.containsKey(tableName)) {
                                handleBadRequest(exchange, "table not exist");
                                return;
                            }
                            // check if insert.getColumns() is a subset of databases.get(tableName).getColumns()
                            List<String> insertCols = insert.getColumns().stream().map(Column::getColumnName).toList();
                            List<String> tableCols = databases.get(tableName).getColumns();
                            if (!tableCols.containsAll(insertCols)) {
                                handleBadRequest(exchange, "invalid columns");
                                return;
                            }
                            List<String> values = insert.getValues().getExpressions().stream().map(Expression::toString).toList();
                            databases.get(tableName).insert(insertCols, values);

                        } else {
                            handleBadRequest(exchange);
                        }
                    } else {
                        // handle NoSQL
                    }

                } catch(DatabindException | JSQLParserException e) {
                    e.printStackTrace();
                    handleBadRequest(exchange);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                handleOkResponse(exchange);
            }
        }
    }

    // ****************** helper functions ********************
    private void handleResponse(HttpExchange exchange, String response) throws IOException {
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, response.getBytes().length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(response.getBytes());
        }
    }

    private void handleOkResponse(HttpExchange exchange) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        SuccessResponse res = new SuccessResponse("ok");
        String jsonResponse = mapper.writeValueAsString(res);
        handleResponse(exchange, jsonResponse);
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
        handleResponse(exchange, jsonResponse);
    }
    private void handleBadRequest(HttpExchange exchange, String body) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        BadRequestResponse res = new BadRequestResponse(body);
        String jsonResponse = mapper.writeValueAsString(res);
        handleResponse(exchange, jsonResponse);
    }
    private class BadRequestResponse {
        public String message;
        public BadRequestResponse(String message) {
            this.message = message;
        }
    }
}
