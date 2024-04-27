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
        try {
            return readAll();
        } catch (IOException e) {
            System.out.println("Error reading from csv file");
            e.printStackTrace();
        }
        return "";
    }

    private String readAll() throws IOException {
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
    public void update(List<String> columns, List<String> values, String[] where) throws RemoteException {
        try {
            boolean updated = false;
            File tempFile = new File("temp-" + csvFileName);
            // read row and update if where condition is met, write to temp file
            try (BufferedReader reader = new BufferedReader(new FileReader(csvFileName));
                 BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile))
            ) {
                String header = reader.readLine();
                // match header columns with where condition [0]
                String[] headerColumns = header.split(",");
                int whereIndex = -1;
                for (int i = 0; i < headerColumns.length; i++) {
                    if (headerColumns[i].equals(where[0])) {
                        whereIndex = i;
                        break;
                    }
                }
                if (whereIndex == -1) {
                    System.out.println("Where column not found");
                    return;
                }
                // write header to temp file
                writer.write(header);
                // read each row
                String line;
                while ((line = reader.readLine()) != null) {
                    // if where condition is met, update columns with values
                    String[] row = line.split(",");
                    if (row[whereIndex].equals(where[1])) {
                        for (int i = 0; i < columns.size(); i++) {
                            for (int j = 0; j < headerColumns.length; j++) {
                                if (headerColumns[j].equals(columns.get(i))) {
                                    row[j] = values.get(i);
                                    updated = true;
                                }
                            }
                        }
                    }
                    writer.newLine();
                    writer.write(String.join(",", row));
                }
            }
            if (!updated) {
                System.out.println("No rows updated");
                // delete temp file
                tempFile.delete();
                return;
            }
            // delete original file and rename temp file
            File originalFile = new File(csvFileName);
            if (originalFile.delete()) {
                if (!tempFile.renameTo(originalFile)) {
                    System.out.println("Error renaming temp file");
                }
            } else {
                System.out.println("Error deleting original file");
            }
        } catch (IOException e) {
            System.out.println("Error updating csv file");
            e.printStackTrace();
        }
    }
}
