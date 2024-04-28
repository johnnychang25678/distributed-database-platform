package org.example;

import java.io.*;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Arrays;
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
            if (columns != null) {
                FileWriter fileWriter = new FileWriter(csvFileName);
                try (BufferedWriter writer = new BufferedWriter(fileWriter)) {
                    String header = String.join(",", columns);
                    writer.write(header);
                }
            }
            // if NoSQL, columns == null, no need to write header (schema-less)
        } catch (IOException e) {
            System.out.println("Error creating csv file");
            e.printStackTrace();
        }


    }

    @Override
    public String selectSQL() throws RemoteException {
        try {
            return readAll(true);
        } catch (IOException e) {
            System.out.println("Error reading from csv file");
            e.printStackTrace();
        }
        return "";
    }

    @Override
    public String selectNoSQL() throws RemoteException {
        try {
            return readAll(false);
        } catch (IOException e) {
            System.out.println("Error reading from csv file");
            e.printStackTrace();
        }
        return "";
    }

    private String readAll(boolean skipHeader) throws IOException {
        // return all data as string from csv file
        StringBuilder data = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new FileReader(csvFileName))) {
            if (skipHeader) {
                reader.readLine(); // skip header
            }
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
    public void insertSQL(List<String> insertColumns, List<String> values) throws RemoteException {
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

    private void updateHelper(List<String> columns, List<String> values, String[] where, boolean isUpdate) {
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
                    tempFile.delete();
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
                        if (isUpdate) {
                            for (int i = 0; i < columns.size(); i++) {
                                for (int j = 0; j < headerColumns.length; j++) {
                                    if (headerColumns[j].equals(columns.get(i))) {
                                        row[j] = values.get(i);
                                        updated = true;
                                    }
                                }
                            }
                        } else {
                            // do not write this line
                            updated = true;
                            continue;
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

    @Override
    public void updateSQL(List<String> columns, List<String> values, String[] where) throws RemoteException {
        updateHelper(columns, values, where, true);
    }

    @Override
    public void deleteSQL(String[] where) throws RemoteException {
        updateHelper(new ArrayList<>(), new ArrayList<>(), where, false);
    }

    @Override
    public void insertNoSQL(List<String> kvPairs) throws RemoteException {
        // [key1, value1, key2, value2, ...]
        StringBuilder csvRow = new StringBuilder();
        for (int i = 0; i < kvPairs.size(); i += 2) {
            csvRow.append(kvPairs.get(i)).append(",").append(kvPairs.get(i + 1)).append(",");
        }
        try {
            FileWriter fileWriter = new FileWriter(csvFileName, true);
            try (BufferedWriter writer = new BufferedWriter(fileWriter)) {
                writer.write(csvRow.toString());
                writer.newLine();
            }
        } catch (IOException e) {
            System.out.println("Error writing to csv file");
            e.printStackTrace();
        }
    }

    @Override
    public void updateNoSQL(List<String> kvPairs, List<String> where) throws RemoteException {
        // update all rows with where condition
        try {
            boolean updated = false;
            File tempFile = new File("temp-" + csvFileName);
            try (BufferedReader reader = new BufferedReader(new FileReader(csvFileName));
                 BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile))
            ) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] row = line.split(","); // key, value, key, value
                    // System.out.println("*************" + Arrays.toString(row) + "*************");
                    for (int i = 0; i < row.length; i += 2) {
                        String key = row[i];
                        if (key.equals(where.get(0)) && row[i + 1].equals(where.get(1))) {
                            // update key value pairs
                            for (int j = 0; j < kvPairs.size(); j += 2) {
                                // should scan all keys and update values
                                for (int k = 0; k < row.length; k += 2) {
                                    if (row[k].equals(kvPairs.get(j))) {
                                        row[k + 1] = kvPairs.get(j + 1);
                                        updated = true;
                                    }
                                }
                            }

                        }
                    }
                    writer.write(String.join(",", row));
                    writer.newLine();
                }
            }
            if (!updated) {
                System.out.println("No rows updated");
                tempFile.delete();
                return;
            }
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

    @Override
    public void deleteNoSQL(List<String> where) throws RemoteException {
        // delete all rows with where condition
        try {
            boolean deleted = false;
            File tempFile = new File("temp-" + csvFileName);
            try (BufferedReader reader = new BufferedReader(new FileReader(csvFileName));
                 BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile))
            ) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] row = line.split(","); // key, value, key, value
                    boolean match = true;
                    for (int i = 0; i < row.length; i += 2) {
                        String key = row[i];
                        if (key.equals(where.get(0)) && row[i + 1].equals(where.get(1))) {
                            match = false;
                            deleted = true;
                            break;
                        }
                    }
                    if (match) {
                        writer.write(String.join(",", row));
                        writer.newLine();
                    }
                }
            }
            if (!deleted) {
                System.out.println("No rows deleted");
                tempFile.delete();
                return;
            }
            File originalFile = new File(csvFileName);
            if (originalFile.delete()) {
                if (!tempFile.renameTo(originalFile)) {
                    System.out.println("Error renaming temp file");
                }
            } else {
                System.out.println("Error deleting original file");
            }
        } catch (IOException e) {
            System.out.println("Error deleting from csv file");
            e.printStackTrace();
        }
    }
}
