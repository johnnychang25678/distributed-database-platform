package org.example;

import java.io.*;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class provides the functionality of a database node replica, handling data storage and retrieval
 * through remote method invocation (RMI). It supports both SQL and NoSQL data operations.
 */
public class DatabaseNodeReplica extends UnicastRemoteObject implements DatabaseNodeInterface{
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private String tableName;
    public String getTableName() {
        return tableName;
    }
    private List<String> columns;
    private String csvFileName;

    private boolean isServerAlive = true;
    public boolean isServerAlive() {
        return isServerAlive;
    }
    public void setServerAlive(boolean serverAlive) {
        isServerAlive = serverAlive;
    }
    /**
     * Constructs a DatabaseNodeReplica with specified table name and columns.
     * It initializes a CSV file to store data for SQL operations.
     *
     * @param tableName the name of the table associated with this replica.
     * @param columns a list of column names used in SQL table, null if this is for NoSQL storage.
     * @throws RemoteException if an error occurs during remote method setup.
     */
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
            } else {
                // if NoSQL, columns == null, no need to write header (schema-less) but still need to create file
                File file = new File(csvFileName);
                file.createNewFile();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * Heartbeat method to check if the server is alive.
     *
     * @return always true, indicating the server is alive.
     * @throws RemoteException if an error occurs during the remote call.
     */
    @Override
    public boolean heartbeatRequest() throws RemoteException {
        return true;
    }
    /**
     * Reads all data from the CSV file for SQL operations.
     *
     * @return a string containing all rows from the CSV file.
     * @throws RemoteException if an error occurs during the remote call.
     */
    @Override
    public String selectSQL() throws RemoteException {
        try {
            return readAll(true);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }
    /**
     * Reads all data from the CSV file for NoSQL operations.
     *
     * @return a string containing all rows from the CSV file.
     * @throws RemoteException if an error occurs during the remote call.
     */
    @Override
    public String selectNoSQL() throws RemoteException {
        try {
            return readAll(false);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }
    /**
     * Reads all data from a CSV file, optionally skipping the header line.
     *
     * @param skipHeader if true, skips the first line of the CSV (header).
     * @return a string containing all rows of the file.
     * @throws IOException if an error occurs reading the file.
     */
    private String readAll(boolean skipHeader) throws IOException {
        // return all data as string from csv file
        rwLock.readLock().lock();
        StringBuilder data = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new FileReader(csvFileName))) {
            if (skipHeader) {
                reader.readLine(); // skip header
            }
            String line;
            while ((line = reader.readLine()) != null && !line.isEmpty()) {
                data.append(line).append("\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            rwLock.readLock().unlock();
        }
        return data.toString();
    }
    /**
     * Inserts data into a CSV file for SQL operations.
     *
     * @param insertColumns columns to insert data into.
     * @param values corresponding values for the columns.
     * @throws RemoteException if an error occurs during the remote call.
     */
    @Override
    public void insertSQL(List<String> insertColumns, List<String> values) throws RemoteException {
        // write to csv file
        rwLock.writeLock().lock();
        StringBuilder csvRow = new StringBuilder();
        try {
            for (String column : this.columns) {
                if (insertColumns.contains(column)) {
                    csvRow.append(values.get(insertColumns.indexOf(column)));
                }
                csvRow.append(",");
            }
            FileWriter fileWriter = new FileWriter(csvFileName, true);
            try (BufferedWriter writer = new BufferedWriter(fileWriter)) {
                writer.newLine();
                writer.write(csvRow.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            rwLock.writeLock().unlock();
        }
    }
    /**
     * Helper method for updating or deleting rows based on a condition.
     *
     * @param columns columns to update.
     * @param values new values for the specified columns.
     * @param where condition array where the first element is the column name and the second is the value to match.
     * @param isUpdate true if updating, false if deleting.
     * @return list of row indices affected.
     */
    private List<Integer> updateSQLHelper(List<String> columns, List<String> values, String[] where, boolean isUpdate) {
        // isUpdate is to differentiate between update and delete
        rwLock.writeLock().lock();
        List<Integer> updatedRows = new ArrayList<>();
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
                    tempFile.delete();
                    return new ArrayList<>();
                }
                // write header to temp file
                writer.write(header);
                // read each row
                String line;
                int count = 0;
                while ((line = reader.readLine()) != null) {
                    // if where condition is met, update columns with values
                    String[] row = line.split(",");
                    if (row[whereIndex].equals(where[1])) {
                        if (isUpdate) {
                            for (int i = 0; i < columns.size(); i++) {
                                for (int j = 0; j < headerColumns.length; j++) {
                                    if (headerColumns[j].equals(columns.get(i))) {
                                        row[j] = values.get(i);
                                        updatedRows.add(count);
                                        updated = true;
                                    }
                                }
                            }
                        } else {
                            // for deletion, just do not write this line, later on this file will replace the original file
                            updatedRows.add(count);
                            updated = true;
                            continue;
                        }
                    }
                    count++;
                    writer.newLine();
                    writer.write(String.join(",", row) + ",");
                }
            }
            if (!updated) {
                // delete temp file
                tempFile.delete();
            }
            // delete original file and rename temp file
            File originalFile = new File(csvFileName);
            if (originalFile.delete()) {
                if (!tempFile.renameTo(originalFile)) {
                    //System.out.println("Error renaming temp file");
                }
            } else {
                //System.out.println("Error deleting original file");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            rwLock.writeLock().unlock();
        }
        return updatedRows;
    }
    /**
     * Updates SQL data based on conditions, supports updating multiple columns.
     *
     * @param columns columns to update.
     * @param values new values for the specified columns.
     * @param where condition array where the first element is the column name and the second is the value to match.
     * @return list of row indices that were updated.
     * @throws RemoteException if an error occurs during the remote call.
     */
    @Override
    public List<Integer> updateSQL(List<String> columns, List<String> values, String[] where) throws RemoteException {
        return updateSQLHelper(columns, values, where, true);
    }
    /**
     * Deletes SQL rows based on a specified condition.
     *
     * @param where condition array where the first element is the column name and the second is the value to match.
     * @return list of row indices that were deleted.
     * @throws RemoteException if an error occurs during the remote call.
     */
    @Override
    public List<Integer> deleteSQL(String[] where) throws RemoteException {
        return updateSQLHelper(new ArrayList<>(), new ArrayList<>(), where, false);
    }
    /**
     * Handles delete operations for SQL data based on row numbers.
     *
     * @param rows list of row indices to delete.
     */
    @Override
    public void deleteByRowSQL(List<Integer> rows) {
        // given row numbers, delete rows (for vertical partitioning deletion)
        rwLock.writeLock().lock();
        boolean deleted = false;
        Collections.sort(rows);
        try {
            File tempFile = new File("temp-" + csvFileName);
            try (BufferedReader reader = new BufferedReader(new FileReader(csvFileName));
                 BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile))
            ) {
                String header = reader.readLine();
                writer.write(header);
                String line;
                int count = 0;
                while ((line = reader.readLine()) != null) {
                    if (rows.contains(count)) {
                        deleted = true;
                        count++;
                        continue;
                    }
                    writer.newLine();
                    writer.write(line);
                    count++;
                }
            }
            if (!deleted) {
                tempFile.delete();
                return;
            }
            File originalFile = new File(csvFileName);
            if (originalFile.delete()) {
                if (!tempFile.renameTo(originalFile)) {
                    //System.out.println("Error renaming temp file");
                }
            } else {
                //System.out.println("Error deleting original file");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            rwLock.writeLock().unlock();
        }
    }
    /**
     * Handles insert operations for NoSQL data by appending key-value pairs to the CSV file.
     *
     * @param kvPairs list of key-value pairs to insert.
     * @throws RemoteException if an error occurs during the remote call.
     */
    @Override
    public void insertNoSQL(List<String> kvPairs) throws RemoteException {
        // [key1, value1, key2, value2, ...]
        rwLock.writeLock().lock();
        StringBuilder csvRow = new StringBuilder();
        try {
            for (int i = 0; i < kvPairs.size(); i += 2) {
                csvRow.append(kvPairs.get(i)).append(",").append(kvPairs.get(i + 1)).append(",");
            }
            FileWriter fileWriter = new FileWriter(csvFileName, true);
            try (BufferedWriter writer = new BufferedWriter(fileWriter)) {
                writer.write(csvRow.toString());
                writer.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            rwLock.writeLock().unlock();
        }
    }
    /**
     * Handles update operations for NoSQL data based on key-value matching conditions.
     *
     * @param kvPairs list of key-value pairs to update.
     * @param where conditions to match for updating.
     * @throws RemoteException if an error occurs during the remote call.
     */
    @Override
    public void updateNoSQL(List<String> kvPairs, List<String> where) throws RemoteException {
        // update all rows with where condition
        rwLock.writeLock().lock();
        try {
            boolean updated = false;
            File tempFile = new File("temp-" + csvFileName);
            try (BufferedReader reader = new BufferedReader(new FileReader(csvFileName));
                 BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile))
            ) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] row = line.split(","); // key, value, key, value
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
                    writer.write(",");
                    writer.newLine();
                }
            }
            if (!updated) {
                tempFile.delete();
                return;
            }
            File originalFile = new File(csvFileName);
            if (originalFile.delete()) {
                if (!tempFile.renameTo(originalFile)) {
                    //System.out.println("Error renaming temp file");
                }
            } else {
                //System.out.println("Error deleting original file");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            rwLock.writeLock().unlock();
        }
    }
    /**
     * Handles delete operations for NoSQL data based on key-value matching conditions.
     *
     * @param where conditions to match for deletion.
     * @throws RemoteException if an error occurs during the remote call.
     */
    @Override
    public void deleteNoSQL(List<String> where) throws RemoteException {
        // delete all rows with where condition
        rwLock.writeLock().lock();
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
                tempFile.delete();
                return;
            }
            File originalFile = new File(csvFileName);
            if (originalFile.delete()) {
                if (!tempFile.renameTo(originalFile)) {
                    //System.out.println("Error renaming temp file");
                }
            } else {
                //System.out.println("Error deleting original file");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            rwLock.writeLock().unlock();
        }
    }
}
