package org.example;

import java.io.*;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;

// RMI server implementation, provide service
public class DatabaseNodeReplica extends UnicastRemoteObject implements DatabaseNodeInterface{
    private String tableName;
    private List<String> columns;
    private String csvFileName;

    public DatabaseNodeReplica(String tableName, List<String> columns) throws RemoteException {
        super();
        this.tableName = tableName;
        this.csvFileName = tableName + ".csv";
        this.columns = columns;
        // create csv header with column names
        try {
            FileWriter fileWriter = new FileWriter(csvFileName);
            try (BufferedWriter writer = new BufferedWriter(fileWriter)) {
                String header = String.join(",", columns);
                writer.write(header);
            }

        } catch (IOException e) {
            System.out.println("Error creating csv file");
            e.printStackTrace();
        }


    }

    @Override
    public String select() throws RemoteException {
        // return all data as string from csv file
        StringBuilder data = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new FileReader(csvFileName))) {
            reader.readLine(); // skip header
            String line;
            while ((line = reader.readLine()) != null) {
                data.append(line).append("\n");
            }
        } catch (IOException e) {
            System.out.println("Error reading from csv file");
            e.printStackTrace();
        }
        return data.toString();
    }

    @Override
    public void insert(List<String> insertColumns, List<String> values) throws RemoteException {
        // write to csv file
        StringBuilder csvRow = new StringBuilder();
        for (String column : this.columns) {
            if (insertColumns.contains(column)) {
                csvRow.append(values.get(insertColumns.indexOf(column)));
            }
            csvRow.append(",");
        }
        try {
            FileWriter fileWriter = new FileWriter(csvFileName, true);
            try (BufferedWriter writer = new BufferedWriter(fileWriter)) {
                writer.newLine();
                writer.write(csvRow.toString());
            }
        } catch (IOException e) {
            System.out.println("Error writing to csv file");
            e.printStackTrace();
        }
    }

    @Override
    public void update() throws RemoteException {

    }
}
