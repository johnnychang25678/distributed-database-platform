package org.example;

import org.example.config.PartitionConfig;
import org.example.config.VerticalPartitionConfig;
import org.example.exception.CannotWriteException;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;
/**
 * Constructs a client for managing a distributed database system with partitioning and replication.
 * The client can handle both SQL and NoSQL databases with horizontal or vertical partitioning.
 */
public class DatabaseNodeClient {
    private String tableName;
    private List<String> columns;
    private int replicaCount;
    private int numPartitions;
    private String partitionType;
    private String dbType;

    // partitionId -> list of replicas
    private Map<Integer, List<DatabaseNodeReplica>> reps = new HashMap<>();
    /**
     * Stops a replica of a database node by unbinding it from the RMI registry.
     *
     * @param partitionId the partition identifier of the replica.
     * @param replicaId the replica identifier within the partition.
     */
    public void stopReplica(int partitionId, int replicaId) {
        try {
            DatabaseNodeReplica replica = reps.get(partitionId).get(replicaId);
            // boolean r = UnicastRemoteObject.unexportObject(replica, true);
            Registry registry = LocateRegistry.getRegistry(1099);
            registry.unbind(replica.getTableName());
        } catch (RemoteException | NotBoundException e) {
            e.printStackTrace();
        }
    }
    /**
     * Starts a replica of a database node by binding it back to the RMI registry.
     *
     * @param partitionId the partition identifier of the replica.
     * @param replicaId the replica identifier within the partition.
     */
    public void startReplica(int partitionId, int replicaId) {
        try {
            DatabaseNodeReplica replica = reps.get(partitionId).get(replicaId);
            Registry registry = LocateRegistry.getRegistry(1099);
            registry.rebind(replica.getTableName(), replica);
            // replica.setServerAlive(true);
        } catch (RemoteException e) {
            e.printStackTrace();
        }
    }
    private Map<String, Integer> columnToPartition = new HashMap<>(); // for vertical partitioning
    /**
     * Constructs a client for managing a distributed database system with partitioning and replication.
     * The client can handle both SQL and NoSQL databases with horizontal or vertical partitioning.
     *
     * @param tableName the name of the table.
     * @param columns the list of column names if it's a SQL type database, null for NoSQL.
     * @param replicaCount the number of replicas per partition.
     * @param partitionConfig the configuration object specifying the partition type and number.
     * @throws RemoteException if there is an issue with remote method invocation during setup.
     */
    public DatabaseNodeClient(String tableName, List<String> columns,
                              int replicaCount, PartitionConfig partitionConfig) throws RemoteException {
        this.tableName = tableName;
        this.columns = columns;
        this.replicaCount = replicaCount;

        this.dbType = columns == null ? "NoSQL" : "SQL";
        if (this.dbType.equals("NoSQL") && partitionConfig != null && partitionConfig.getPartitionType().equals("vertical")) {
            throw new IllegalArgumentException("NoSQL databases cannot have vertical partitioning");
        }

        this.partitionType = partitionConfig == null ? "none" : partitionConfig.getPartitionType();
        this.numPartitions = partitionConfig == null ? 1 : partitionConfig.getNumPartitions(); // if none, numPartitions = 1

        if (partitionType.equals("horizontal") || partitionType.equals("none")) {
            for (int i = 0; i < this.numPartitions; i++) {
                // create replicas
                List<DatabaseNodeReplica> replicas = new ArrayList<>();
                Registry registry = LocateRegistry.getRegistry(1099);
                for (int j = 0; j < replicaCount; j++) {
                    // table-DBType-partitionId-replicaId
                    String uniqueName = tableName + "-" + this.dbType + "-" + i + "-" + j;
                    DatabaseNodeReplica dbReplica = new DatabaseNodeReplica(uniqueName, columns);
                    registry.rebind(uniqueName, dbReplica);
                    replicas.add(dbReplica);
                }
                reps.put(i, replicas);
            }
        } else if (partitionType.equals("vertical")) {
            VerticalPartitionConfig verticalPartitionConfig = (VerticalPartitionConfig) partitionConfig;
            List<List<String>> colGroup = verticalPartitionConfig.getColumns();
            // [[a,b,c], [d,e,f], [g,h,i]]
            for (int i = 0; i < colGroup.size(); i++) {
                // add column to partition map
                for (String col : colGroup.get(i)) {
                    columnToPartition.put(col, i);
                    // [a:0, b:0, c:0, d:1, e:1, f:1, g:2, h:2, i:2]
                }
                // create replicas
                List<DatabaseNodeReplica> replicas = new ArrayList<>();
                Registry registry = LocateRegistry.getRegistry(1099);
                for (int j = 0; j < replicaCount; j++) {
                    String uniqueName = tableName + "-" + this.dbType + "-" + i + "-" + j;
                    // create columns by different groups
                    DatabaseNodeReplica dbReplica = new DatabaseNodeReplica(uniqueName, colGroup.get(i));
                    registry.rebind(uniqueName, dbReplica);
                    replicas.add(dbReplica);
                }
                reps.put(i, replicas);
            }
        }
        this.startHeartbeat();
    }

    public List<String> getColumns() {
        return columns;
    }
    /**
     * Periodically checks the health of all replicas in all partitions and updates their alive status.
     * This method starts a heartbeat thread that continuously monitors the status of each replica.
     */
    public void startHeartbeat() {
        new Thread(() -> {
            while (true) {
                for (List<DatabaseNodeReplica> replicas : reps.values()) {
                    for (DatabaseNodeReplica replica : replicas) {
                        try {
                            DatabaseNodeInterface stub = getReplicaStub(replica.getTableName()); // this can throw
                            if (stub == null) {
                                replica.setServerAlive(false);
                                continue;
                            }
                            if (!stub.heartbeatRequest()){
                                replica.setServerAlive(false);
                            } else {
                                replica.setServerAlive(true);
                            }
                        } catch (RemoteException | NotBoundException e) {
                            replica.setServerAlive(false);
                        }
                    }
                }

                // Sleep for 1 sec before checking again
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }
    /**
     * Retrieves the stub for communication with a database node replica using RMI.
     *
     * @param tableName the name of the table to access the stub.
     * @return the remote interface for the database node.
     * @throws RemoteException if there is an issue with remote method invocation.
     * @throws NotBoundException if the replica is not bound in the RMI registry.
     */
    private DatabaseNodeInterface getReplicaStub(String tableName) throws RemoteException, NotBoundException {
        Registry registry = LocateRegistry.getRegistry(1099);
        return (DatabaseNodeInterface) registry.lookup(tableName);
    }
    /**
     * Inserts data into a SQL database, considering the partitioning and replica details.
     *
     * @param columns the column names for the insert operation.
     * @param values the corresponding values to be inserted.
     * @throws CannotWriteException if the operation cannot be completed due to replica failures.
     */
    public void insertSQL(List<String> columns, List<String> values) throws CannotWriteException {
        if (this.partitionType.equals("horizontal")) {
            // insert by key % numPartitions
            int partitionId = Integer.parseInt(values.get(0)) % this.numPartitions;
            checkAlive(partitionId);
            for (DatabaseNodeReplica replica : reps.get(partitionId)) {
                try {
                    DatabaseNodeInterface stub = getReplicaStub(replica.getTableName());
                    stub.insertSQL(columns, values);
                } catch (RemoteException | NotBoundException e) {
                    e.printStackTrace();
                }
            }
        } else if (this.partitionType.equals("vertical")) {
            Map<String, String> kvMap = new HashMap<>();
            for (int i = 0; i < columns.size(); i++) {
                kvMap.put(columns.get(i), values.get(i));
            }
            List<List<String>> rearrangedColumns = new ArrayList<>();
            for (int i = 0; i < numPartitions; i++) {
                rearrangedColumns.add(new ArrayList<>());
            }
            for (String col : columns) {
                int partitionId = columnToPartition.get(col);
                rearrangedColumns.get(partitionId).add(col);
            }
            // first check if all partitions are alive
            for (int i = 0; i < numPartitions; i++) {
                checkAlive(i);
            }
            for (int i = 0; i < numPartitions; i++) {
                List<String> rearrangedValues = new ArrayList<>();
                for (String col : rearrangedColumns.get(i)) {
                    rearrangedValues.add(kvMap.get(col));
                }
                for (DatabaseNodeReplica replica : reps.get(i)) {
                    try {
                        DatabaseNodeInterface stub = getReplicaStub(replica.getTableName());
                        stub.insertSQL(rearrangedColumns.get(i), rearrangedValues);
                    } catch (RemoteException | NotBoundException e) {
                        e.printStackTrace();
                    }
                }
            }
        } else if (this.partitionType.equals("none")) {
            // insert into all replicas
            int partitionId = 0;
            checkAlive(partitionId);
            for (DatabaseNodeReplica replica : reps.get(partitionId)) {
                try {
                    if (!replica.isServerAlive()) {
                        // one of replicas is down, should not able to insert
                        throw new CannotWriteException("One of the replicas is down");
                    }
                    DatabaseNodeInterface stub = getReplicaStub(replica.getTableName());
                    stub.insertSQL(columns, values);
                } catch (RemoteException | NotBoundException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Inserts key-value pairs into a NoSQL database, respecting the partitioning and replica information.
     *
     * @param kvPairs the key-value pairs to be inserted.
     * @throws CannotWriteException if the operation cannot proceed due to replica failures.
     */
    public void insertNoSQL(List<String> kvPairs) throws CannotWriteException {
        if (this.partitionType.equals("horizontal")) {
            // insert by key % numPartitions
            // example [id, 1, name, "John"]
            int partitionId = Integer.parseInt(kvPairs.get(1)) % this.numPartitions;
            checkAlive(partitionId);
            for (DatabaseNodeReplica replica : reps.get(partitionId)) {
                try {
                    DatabaseNodeInterface stub = getReplicaStub(replica.getTableName());
                    List<String> serializableList = new ArrayList<>(kvPairs);
                    stub.insertNoSQL(serializableList);
                } catch (RemoteException | NotBoundException e) {
                    e.printStackTrace();
                }
            }
        } else if (this.partitionType.equals("none")) {
            // insert into all replicas
            int partitionId = 0;
            checkAlive(partitionId);
            for (DatabaseNodeReplica replica : reps.get(partitionId)) {
                try {
                    DatabaseNodeInterface stub = getReplicaStub(replica.getTableName());
                    List<String> serializableList = new ArrayList<>(kvPairs);
                    stub.insertNoSQL(serializableList);
                } catch (RemoteException | NotBoundException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    /**
     * Retrieves data from a SQL database, handling horizontal or vertical partitioning.
     *
     * @return the concatenated string of results from all replicas and partitions.
     */
    public String selectSQL() {
        if (this.partitionType.equals("horizontal") || this.partitionType.equals("vertical")) {
            // read from all partitions and aggregate
            List<String> resultList = new ArrayList<>();
            try {
                if (this.partitionType.equals("horizontal")) {
                    for (int i = 0; i < numPartitions; i++) {
                        // read from the first replica with isServerAlive = true
                        for (DatabaseNodeReplica replica : reps.get(i)) {
                            if (replica.isServerAlive()) {
                                DatabaseNodeInterface stub = getReplicaStub(replica.getTableName());
                                resultList.add(stub.selectSQL());
                                break;
                            }
                        }
                    }
                } else {
                    // vertical: need to read from all partitions and aggregate the result
                    Map<Integer, List<String>> partitionResults = new HashMap<>();
                    for (int i = 0; i < numPartitions; i++) {
                        for (DatabaseNodeReplica replica : reps.get(i)) {
                            if (replica.isServerAlive()) {
                                DatabaseNodeInterface stub = getReplicaStub(replica.getTableName());
                                String res = stub.selectSQL();
                                // split by \n to array list
                                List<String> resList = new ArrayList<>(Arrays.asList(res.split("\n")));
                                if (!partitionResults.containsKey(i)) {
                                    partitionResults.put(i, resList);
                                }
                                break;
                            }
                        }
                    }
                    int rowCount = partitionResults.get(0).size();
                    for (int i = 0; i < rowCount; i++) {
                        List<String> row = new ArrayList<>();
                        for (int j = 0; j < numPartitions; j++) {
                            row.add(partitionResults.get(j).get(i));
                        }
                        resultList.add(String.join("",row));
                    }
                }
            } catch (RemoteException | NotBoundException e) {
                e.printStackTrace();
            }
            // convert the result array to a string with newlines
            return String.join("", resultList);
        } else if (this.partitionType.equals("none")) {
            // read from first replica
            try {
                // only one partition
                for (DatabaseNodeReplica replica : reps.get(0)) {
                    if (replica.isServerAlive()) {
                        DatabaseNodeInterface stub = getReplicaStub(replica.getTableName());
                        return stub.selectSQL();
                    }
                }
            } catch (RemoteException | NotBoundException e) {
                e.printStackTrace();
            }
        }
        return "";
    }
    /**
     * Retrieves data from a NoSQL database, considering all active replicas across partitions.
     *
     * @return the concatenated string of results from all replicas.
     */
    public String selectNoSQL() {
        if (this.partitionType.equals("horizontal")){
            // read from all partitions and aggregate
            List<String> resultList = new ArrayList<>();
            try {
                for (int i = 0; i < numPartitions; i++) {
                    // read from the first replica with isServerAlive = true
                    for (DatabaseNodeReplica replica : reps.get(i)) {
                        if (replica.isServerAlive()) {
                            DatabaseNodeInterface stub = getReplicaStub(replica.getTableName());
                            resultList.add(stub.selectNoSQL());
                            break;
                        }
                    }
                }
            } catch (RemoteException | NotBoundException e) {
                e.printStackTrace();
            }
            // convert the result array to a string with newlines
            return String.join("", resultList);
        } else if (this.partitionType.equals("none")) {
            // read from first replica
            try {
                for (DatabaseNodeReplica replica : reps.get(0)) {
                    if (replica.isServerAlive()) {
                        DatabaseNodeInterface stub = getReplicaStub(replica.getTableName());
                        return stub.selectNoSQL();
                    }
                }
            } catch (RemoteException | NotBoundException e) {
                e.printStackTrace();
            }
        }
        return "";
    }
    /**
     * Updates data in a SQL database, considering partitioning and ensuring all relevant replicas are updated.
     *
     * @param columns the columns to update.
     * @param values the new values for these columns.
     * @param where the condition specifying which records to update.
     * @throws CannotWriteException if not all replicas are active, preventing the update.
     */
    public void updateSQL(List<String> columns, List<String> values, String where) throws CannotWriteException {
        String[] whereParts = where.split("=");
        String[] whereArr = {whereParts[0].trim(), whereParts[1].trim()};

        if (this.partitionType.equals("horizontal")) {
            // update by key % numPartitions
            // need to be WHERE id = xxx to delete by key
            int partitionId = Integer.parseInt(whereArr[1]) % this.numPartitions;
            checkAlive(partitionId);
            for (DatabaseNodeReplica replica : reps.get(partitionId)) {
                try {
                    if (!replica.isServerAlive()) {
                        // one of replicas is down, should not able to update
                        throw new CannotWriteException("One of the replicas is down");
                    }
                    DatabaseNodeInterface stub = getReplicaStub(replica.getTableName());
                    List<String> serializableColumns = new ArrayList<>(columns);
                    List<String> serializableValues = new ArrayList<>(values);
                    stub.updateSQL(serializableColumns, serializableValues, whereArr);
                } catch (RemoteException | NotBoundException e) {
                    e.printStackTrace();
                }
            }
        } else if (this.partitionType.equals("vertical")) {
            // only support same partition update, so if the where clause and update columns are
            // not in the same partition, it will not work
            int partitionId = columnToPartition.get(whereArr[0]);
            checkAlive(partitionId);
            // should locate the partition by the column in the where clause
            // update all the replicas in the partition
            for (DatabaseNodeReplica replica : reps.get(partitionId)) {
                try {
                    DatabaseNodeInterface stub = getReplicaStub(replica.getTableName());
                    List<String> serializableColumns = new ArrayList<>(columns);
                    List<String> serializableValues = new ArrayList<>(values);
                    stub.updateSQL(serializableColumns, serializableValues, whereArr);
                } catch (RemoteException | NotBoundException e) {
                    e.printStackTrace();
                }
            }
        } else if (this.partitionType.equals("none")) {
            // update all replicas
            int partitionId = 0;
            checkAlive(partitionId);
            for (DatabaseNodeReplica replica : reps.get(0)) {
                try {
                    // where clause is now FirstName = 'John'
                    // convert to ["FirstName", "John"]
                    DatabaseNodeInterface stub = getReplicaStub(replica.getTableName());
                    List<String> serializableColumns = new ArrayList<>(columns);
                    List<String> serializableValues = new ArrayList<>(values);
                    stub.updateSQL(serializableColumns, serializableValues, whereArr);
                } catch (RemoteException | NotBoundException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    /**
     * Updates data in a NoSQL database, based on key-value pairs and a condition.
     *
     * @param kvPairs the key-value pairs to update.
     * @param where the condition specifying which records to update.
     * @throws CannotWriteException if the update cannot be completed due to inactive replicas.
     */
    public void updateNoSQL(List<String> kvPairs, List<String> where) throws CannotWriteException {
        if (this.partitionType.equals("horizontal")) {
            // update by key % numPartitions
            // need to be WHERE id = xxx to update by key
            int partitionId = Integer.parseInt(where.get(1)) % this.numPartitions;
            checkAlive(partitionId);
            for (DatabaseNodeReplica replica : reps.get(partitionId)) {
                try {
                    DatabaseNodeInterface stub = getReplicaStub(replica.getTableName());
                    List<String> serializableList = new ArrayList<>(kvPairs);
                    List<String> serializableWhere = new ArrayList<>(where);
                    stub.updateNoSQL(serializableList, serializableWhere);
                } catch (RemoteException | NotBoundException e) {
                    e.printStackTrace();
                }
            }
        } else if (this.partitionType.equals("none")) {
            // update all replicas
            int partitionId = 0;
            checkAlive(partitionId);
            for (DatabaseNodeReplica replica : reps.get(0)) {
                try {
                    DatabaseNodeInterface stub = getReplicaStub(replica.getTableName());
                    List<String> serializableList = new ArrayList<>(kvPairs);
                    List<String> serializableWhere = new ArrayList<>(where);
                    stub.updateNoSQL(serializableList, serializableWhere);
                } catch (RemoteException | NotBoundException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    /**
     * Deletes records from a SQL database based on a condition, considering partitioning and replication.
     *
     * @param where the condition specifying which records to delete.
     * @throws CannotWriteException if the delete operation cannot proceed due to inactive replicas.
     */
    public void deleteSQL(String where) throws CannotWriteException {
        String[] whereSplit = where.split("=");
        String[] whereArr = {whereSplit[0].trim(), whereSplit[1].trim()};
        if (this.partitionType.equals("horizontal")) {
            // delete by key % numPartitions
            // need to be WHERE id = xxx to delete by key
            int partitionId = Integer.parseInt(whereArr[1]) % this.numPartitions;
            checkAlive(partitionId);
            for (DatabaseNodeReplica replica : reps.get(partitionId)) {
                try {
                    DatabaseNodeInterface stub = getReplicaStub(replica.getTableName());
                    stub.deleteSQL(whereArr);
                } catch (RemoteException | NotBoundException e) {
                    e.printStackTrace();
                }
            }
        } else if (this.partitionType.equals("vertical")) {
            int partitionId = columnToPartition.get(whereArr[0]);
            checkAlive(partitionId);
            // delete from all replicas in the partition
            // should delete the same rows from all partitions
            List<Integer> deletedRows = new ArrayList<>();
            for (DatabaseNodeReplica replica : reps.get(partitionId)) {
                try {
                    if (!replica.isServerAlive()) {
                        // one of replicas is down, should not able to delete
                        throw new CannotWriteException("One of the replicas is down");
                    }
                    DatabaseNodeInterface stub = getReplicaStub(replica.getTableName());
                    deletedRows = stub.deleteSQL(whereArr);
                } catch (RemoteException | NotBoundException e) {
                    e.printStackTrace();
                }
            }
            // delete the same rows from all partitions
            for (int i = 0; i < numPartitions; i++) {
                if (i == partitionId) {
                    continue;
                }
                for (DatabaseNodeReplica replica : reps.get(i)) {
                    try {
                        DatabaseNodeInterface stub = getReplicaStub(replica.getTableName());
                        stub.deleteByRowSQL(deletedRows);
                    } catch (RemoteException | NotBoundException e) {
                        e.printStackTrace();
                    }
                }
            }
        } else if (this.partitionType.equals("none")) {
            // delete from all replicas
            int partitionId = 0;
            checkAlive(partitionId);
            for (DatabaseNodeReplica replica : reps.get(0)) {
                try {
                    DatabaseNodeInterface stub = getReplicaStub(replica.getTableName());
                    stub.deleteSQL(whereArr);
                } catch (RemoteException | NotBoundException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    /**
     * Deletes records from a NoSQL database based on a condition, considering all active replicas.
     *
     * @param where the list defining the condition for deletion.
     * @throws CannotWriteException if the deletion cannot be executed due to inactive replicas.
     */
    public void deleteNoSQL(List<String> where) throws CannotWriteException {
        if (this.partitionType.equals("horizontal")) {
            // delete by key % numPartitions
            // need to be WHERE id = xxx to delete by key
            int partitionId = Integer.parseInt(where.get(1)) % this.numPartitions;
            checkAlive(partitionId);
            for (DatabaseNodeReplica replica : reps.get(partitionId)) {
                try {
                    DatabaseNodeInterface stub = getReplicaStub(replica.getTableName());
                    List<String> serializableWhere = new ArrayList<>(where);
                    stub.deleteNoSQL(serializableWhere);
                } catch (RemoteException | NotBoundException e) {
                    e.printStackTrace();
                }
            }
        } else if (this.partitionType.equals("none")) {
            // delete from all replicas
            int partitionId = 0;
            checkAlive(partitionId);
            for (DatabaseNodeReplica replica : reps.get(partitionId)) {
                try {
                    DatabaseNodeInterface stub = getReplicaStub(replica.getTableName());
                    List<String> serializableWhere = new ArrayList<>(where);
                    stub.deleteNoSQL(serializableWhere);
                } catch (RemoteException | NotBoundException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    // check if all replicas are alive
    private void checkAlive(int partitionId) throws CannotWriteException {
        for (DatabaseNodeReplica replica : reps.get(partitionId)) {
            if (!replica.isServerAlive()) {
                throw new CannotWriteException("One of the replicas is down");
            }
        }
    }
}
