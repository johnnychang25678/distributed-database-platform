package org.example;

import org.example.config.PartitionConfig;
import org.example.config.VerticalPartitionConfig;
import org.example.exception.CannotWriteException;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;

public class DatabaseNodeClient {
    private String tableName;
    private List<String> columns;
    private int replicaCount;
    private int numPartitions;
    private String partitionType;
    private String dbType;

    // partitionId -> list of replicas
    private Map<Integer, List<DatabaseNodeReplica>> reps = new HashMap<>();
    // for testing
    public void stopReplica(int partitionId, int replicaId) {
        System.out.println("Stopping replica " + partitionId + "-" + replicaId);
        try {
            DatabaseNodeReplica replica = reps.get(partitionId).get(replicaId);
            // boolean r = UnicastRemoteObject.unexportObject(replica, true);
            // System.out.println("Unexported: " + r + " " + replica.getTableName());
            Registry registry = LocateRegistry.getRegistry(1099);
            System.out.println("Unbinding replica " + replica.getTableName());
            registry.unbind(replica.getTableName());
        } catch (RemoteException | NotBoundException e) {
            e.printStackTrace();
        }
    }
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
                    System.out.println("Binding replica " + uniqueName);
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

    public void startHeartbeat() {
        new Thread(() -> {
            while (true) {
                for (List<DatabaseNodeReplica> replicas : reps.values()) {
                    for (DatabaseNodeReplica replica : replicas) {
                        try {
                            // System.out.println("Checking replica: " + replica.getTableName() + " heartbeat");
                            DatabaseNodeInterface stub = getReplicaStub(replica.getTableName()); // this can throw
                            if (stub == null) {
                                replica.setServerAlive(false);
                                continue;
                            }
                            if (!stub.heartbeatRequest()){
                                System.out.println("Replica " + replica.getTableName() + " is not alive");
                                replica.setServerAlive(false);
                            } else {
//                                if (replica.getTableName().equals("students-SQL-0-0"))
//                                    System.out.println("Replica " + replica.getTableName() + " is alive");
                                replica.setServerAlive(true);
                            }
                        } catch (RemoteException | NotBoundException e) {
                            System.out.println("Error checking replica's heartbeat, setting alive to false...");
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

    private DatabaseNodeInterface getReplicaStub(String tableName) throws RemoteException, NotBoundException {
        Registry registry = LocateRegistry.getRegistry(1099);
        return (DatabaseNodeInterface) registry.lookup(tableName);
    }
    public void insertSQL(List<String> columns, List<String> values) throws CannotWriteException {
        if (this.partitionType.equals("horizontal")) {
            // insert by key % numPartitions
            int partitionId = Integer.parseInt(values.get(0)) % this.numPartitions;
            System.out.println("Inserting into partition " + partitionId);
            for (DatabaseNodeReplica replica : reps.get(partitionId)) {
                try {
                    if (!replica.isServerAlive()) {
                        // one of replicas is down, should not able to insert
                        throw new CannotWriteException("One of the replicas is down");
                    }
                    DatabaseNodeInterface stub = getReplicaStub(replica.getTableName());
                    stub.insertSQL(columns, values);
                } catch (RemoteException | NotBoundException e) {
                    System.out.println("RMI error inserting into replica");
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
            for (int i = 0; i < numPartitions; i++) {
                List<String> rearrangedValues = new ArrayList<>();
                for (String col : rearrangedColumns.get(i)) {
                    rearrangedValues.add(kvMap.get(col));
                }
                System.out.println("Inserting into partition " + i);
                for (DatabaseNodeReplica replica : reps.get(i)) {
                    try {
                        if (!replica.isServerAlive()) {
                            // one of replicas is down, should not able to insert
                            throw new CannotWriteException("One of the replicas is down");
                        }
                        DatabaseNodeInterface stub = getReplicaStub(replica.getTableName());
                        stub.insertSQL(rearrangedColumns.get(i), rearrangedValues);
                    } catch (RemoteException | NotBoundException e) {
                        System.out.println("RMI error inserting into replica");
                        e.printStackTrace();
                    }
                }
            }
        } else if (this.partitionType.equals("none")) {
            // insert into all replicas
            System.out.println("Inserting into replicas");
            for (DatabaseNodeReplica replica : reps.get(0)) {
                try {
                    if (!replica.isServerAlive()) {
                        // one of replicas is down, should not able to insert
                        throw new CannotWriteException("One of the replicas is down");
                    }
                    DatabaseNodeInterface stub = getReplicaStub(replica.getTableName());
                    stub.insertSQL(columns, values);
                } catch (RemoteException | NotBoundException e) {
                    System.out.println("RMI error inserting into replica");
                    e.printStackTrace();
                }
            }
        }
    }

    public void insertNoSQL(List<String> kvPairs) {
        if (this.partitionType.equals("horizontal")) {
            // insert by key % numPartitions
            // example [id, 1, name, "John"]
            int partitionId = Integer.parseInt(kvPairs.get(1)) % this.numPartitions;
            System.out.println("Inserting into partition " + partitionId);
            for (DatabaseNodeReplica replica : reps.get(partitionId)) {
                try {
                    DatabaseNodeInterface stub = getReplicaStub(replica.getTableName());
                    stub.insertNoSQL(kvPairs);
                } catch (RemoteException | NotBoundException e) {
                    System.out.println("RMI error inserting into replica");
                    e.printStackTrace();
                }
            }
        } else if (this.partitionType.equals("none")) {
            // insert into all replicas
            System.out.println("Inserting into replicas");
            for (DatabaseNodeReplica replica : reps.get(0)) {
                try {
                    DatabaseNodeInterface stub = getReplicaStub(replica.getTableName());
                    List<String> serializableList = new ArrayList<>(kvPairs);
                    stub.insertNoSQL(serializableList);
                } catch (RemoteException | NotBoundException e) {
                    System.out.println("RMI error inserting into replica");
                    e.printStackTrace();
                }
            }
        }
    }

    public String selectSQL() {
        if (this.partitionType.equals("horizontal") || this.partitionType.equals("vertical")) {
            // read from all partitions and aggregate
            // System.out.println("Selecting from replicas");
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
                System.out.println("RMI error selecting from replica");
                e.printStackTrace();
            }
            // convert the result array to a string with newlines
            return String.join("\n", resultList);
        } else if (this.partitionType.equals("none")) {
            // read from first replica
            System.out.println("Selecting from replicas");
            try {
                // only one partition
                for (DatabaseNodeReplica replica : reps.get(0)) {
                    if (replica.isServerAlive()) {
                        DatabaseNodeInterface stub = getReplicaStub(replica.getTableName());
                        return stub.selectSQL();
                    }
                }
            } catch (RemoteException | NotBoundException e) {
                System.out.println("RMI error selecting from replica");
                e.printStackTrace();
            }
        }
        return "";
    }
    public String selectNoSQL() {
        if (this.partitionType.equals("horizontal")){
            // read from all partitions and aggregate
            System.out.println("Selecting from replicas");
            List<String> resultList = new ArrayList<>();
            try {
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
            } catch (RemoteException | NotBoundException e) {
                System.out.println("RMI error selecting from replica");
                e.printStackTrace();
            }
            // convert the result array to a string with newlines
            return String.join("\n", resultList);
        } else if (this.partitionType.equals("none")) {
            // read from first replica
            System.out.println("Selecting from replicas");
            try {
                for (DatabaseNodeReplica replica : reps.get(0)) {
                    if (replica.isServerAlive()) {
                        DatabaseNodeInterface stub = getReplicaStub(replica.getTableName());
                        return stub.selectNoSQL();
                    }
                }
            } catch (RemoteException | NotBoundException e) {
                System.out.println("RMI error selecting from replica");
                e.printStackTrace();
            }
        }
        return "";
    }

    public void updateSQL(List<String> columns, List<String> values, String where) {
        String[] whereParts = where.split("=");
        String[] whereArr = {whereParts[0].trim(), whereParts[1].trim()};

        if (this.partitionType.equals("horizontal")) {
            // update by key % numPartitions
            // need to be WHERE id = xxx to delete by key
            int partitionId = Integer.parseInt(whereArr[1]) % this.numPartitions;
            // System.out.println("Updating partition " + partitionId);
            for (DatabaseNodeReplica replica : reps.get(partitionId)) {
                try {
                    DatabaseNodeInterface stub = getReplicaStub(replica.getTableName());
                    stub.updateSQL(columns, values, whereArr);
                } catch (RemoteException | NotBoundException e) {
                    System.out.println("RMI error updating replica");
                    e.printStackTrace();
                }
            }
        } else if (this.partitionType.equals("vertical")) {
            // only support same partition update, so if the where clause and update columns are
            // not in the same partition, it will not work
            int partitionId = columnToPartition.get(whereArr[0]);
            // should locate the partition by the column in the where clause
            // System.out.println("Updating partition " + partitionId);
            // update all the replicas in the partition
            for (DatabaseNodeReplica replica : reps.get(partitionId)) {
                try {
                    DatabaseNodeInterface stub = getReplicaStub(replica.getTableName());
                    stub.updateSQL(columns, values, whereArr);
                } catch (RemoteException | NotBoundException e) {
                    System.out.println("RMI error updating replica");
                    e.printStackTrace();
                }
            }
        } else if (this.partitionType.equals("none")) {
            // update all replicas
            System.out.println("Updating replicas");
            for (DatabaseNodeReplica replica : reps.get(0)) {
                try {
                    // where clause is now FirstName = 'John'
                    // convert to ["FirstName", "John"]
                    DatabaseNodeInterface stub = getReplicaStub(replica.getTableName());
                    stub.updateSQL(columns, values, whereArr);
                } catch (RemoteException | NotBoundException e) {
                    System.out.println("RMI error updating replica");
                    e.printStackTrace();
                }
            }
        }
    }

    public void updateNoSQL(List<String> kvPairs, List<String> where) {
        if (this.partitionType.equals("horizontal")) {
            // update by key % numPartitions
            // need to be WHERE id = xxx to update by key
            int partitionId = Integer.parseInt(where.get(1)) % this.numPartitions;
            // System.out.println("Updating partition " + partitionId);
            for (DatabaseNodeReplica replica : reps.get(partitionId)) {
                try {
                    DatabaseNodeInterface stub = getReplicaStub(replica.getTableName());
                    List<String> serializableList = new ArrayList<>(kvPairs);
                    List<String> serializableWhere = new ArrayList<>(where);
                    stub.updateNoSQL(serializableList, serializableWhere);
                } catch (RemoteException | NotBoundException e) {
                    System.out.println("RMI error updating replica");
                    e.printStackTrace();
                }
            }
        } else if (this.partitionType.equals("none")) {
            // update all replicas
            System.out.println("Updating replicas");
            for (DatabaseNodeReplica replica : reps.get(0)) {
                try {
                    DatabaseNodeInterface stub = getReplicaStub(replica.getTableName());
                    List<String> serializableList = new ArrayList<>(kvPairs);
                    List<String> serializableWhere = new ArrayList<>(where);
                    stub.updateNoSQL(serializableList, serializableWhere);
                } catch (RemoteException | NotBoundException e) {
                    System.out.println("RMI error updating replica");
                    e.printStackTrace();
                }
            }
        }
    }

    public void deleteSQL(String where) {
        String[] whereSplit = where.split("=");
        String[] whereArr = {whereSplit[0].trim(), whereSplit[1].trim()};
        if (this.partitionType.equals("horizontal")) {
            // delete by key % numPartitions
            // need to be WHERE id = xxx to delete by key
            int partitionId = Integer.parseInt(whereArr[1]) % this.numPartitions;
            // System.out.println("Deleting from partition " + partitionId);
            for (DatabaseNodeReplica replica : reps.get(partitionId)) {
                try {
                    DatabaseNodeInterface stub = getReplicaStub(replica.getTableName());
                    stub.deleteSQL(whereArr);
                } catch (RemoteException | NotBoundException e) {
                    System.out.println("RMI error deleting from replica");
                    e.printStackTrace();
                }
            }
        } else if (this.partitionType.equals("vertical")) {
            int partitionId = columnToPartition.get(whereArr[0]);
            // System.out.println("Deleting from partition " + partitionId);
            // delete from all replicas in the partition
            // should delete the same rows from all partitions
            List<Integer> deletedRows = new ArrayList<>();
            for (DatabaseNodeReplica replica : reps.get(partitionId)) {
                try {
                    DatabaseNodeInterface stub = getReplicaStub(replica.getTableName());
                    deletedRows = stub.deleteSQL(whereArr);
                } catch (RemoteException | NotBoundException e) {
                    System.out.println("RMI error deleting from replica");
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
                        System.out.println("RMI error deleting from replica");
                        e.printStackTrace();
                    }
                }
            }
        } else if (this.partitionType.equals("none")) {
            // delete from all replicas
            // System.out.println("Deleting from replicas");
            for (DatabaseNodeReplica replica : reps.get(0)) {
                try {
                    DatabaseNodeInterface stub = getReplicaStub(replica.getTableName());
                    stub.deleteSQL(whereArr);
                } catch (RemoteException | NotBoundException e) {
                    System.out.println("RMI error deleting from replica");
                    e.printStackTrace();
                }
            }
        }
    }

    public void deleteNoSQL(List<String> where) {
        if (this.partitionType.equals("horizontal")) {
            // delete by key % numPartitions
            // need to be WHERE id = xxx to delete by key
            int partitionId = Integer.parseInt(where.get(1)) % this.numPartitions;
            // System.out.println("Deleting from partition " + partitionId);
            for (DatabaseNodeReplica replica : reps.get(partitionId)) {
                try {
                    DatabaseNodeInterface stub = getReplicaStub(replica.getTableName());
                    List<String> serializableWhere = new ArrayList<>(where);
                    stub.deleteNoSQL(serializableWhere);
                } catch (RemoteException | NotBoundException e) {
                    System.out.println("RMI error deleting from replica");
                    e.printStackTrace();
                }
            }
        } else if (this.partitionType.equals("none")) {
            // delete from all replicas
            // System.out.println("Deleting from replicas");
            for (DatabaseNodeReplica replica : reps.get(0)) {
                try {
                    DatabaseNodeInterface stub = getReplicaStub(replica.getTableName());
                    List<String> serializableWhere = new ArrayList<>(where);
                    stub.deleteNoSQL(serializableWhere);
                } catch (RemoteException | NotBoundException e) {
                    System.out.println("RMI error deleting from replica");
                    e.printStackTrace();
                }
            }
        }
    }
}
