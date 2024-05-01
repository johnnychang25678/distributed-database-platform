package org.example;

import org.example.config.PartitionConfig;
import org.example.config.VerticalPartitionConfig;

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

    // list of RMI servers of replicaCount
//    private List<DatabaseNodeInterface> replicas = new ArrayList<>();

    // partitionId -> list of replicas
    private Map<Integer, List<DatabaseNodeReplica>> reps = new HashMap<>();
    private Map<String, Integer> columnToPartition = new HashMap<>(); // for vertical partitioning

    public DatabaseNodeClient(String tableName, List<String> columns,
                              int replicaCount, PartitionConfig partitionConfig) throws RemoteException {
        this.tableName = tableName;
        this.columns = columns;
        this.replicaCount = replicaCount;

        this.dbType = columns == null ? "NoSQL" : "SQL";
        if (this.dbType.equals("NoSQL") && partitionConfig.getPartitionType().equals("vertical")) {
            throw new IllegalArgumentException("NoSQL databases cannot have vertical partitioning");
        }

        this.numPartitions = partitionConfig == null ? 1 : partitionConfig.getNumPartitions();
        this.partitionType = partitionConfig == null ? "none" : partitionConfig.getPartitionType();

        if (partitionType.equals("horizontal") || partitionType.equals("none")) {
            for (int i = 0; i < this.numPartitions; i++) {
                // create replicas
                List<DatabaseNodeReplica> replicas = new ArrayList<>();
                Registry registry = LocateRegistry.getRegistry(1099);
                for (int j = 0; j < replicaCount; j++) {
                    // table-DBType-partitionId-replicaId
                    String uniqueName = tableName + "-" + this.dbType + "-" + i + "-" + j;
                    System.out.println("Unique name: " + uniqueName);
                    DatabaseNodeReplica dbReplica = new DatabaseNodeReplica(uniqueName, columns);
                    registry.rebind(uniqueName, dbReplica);
                    replicas.add(dbReplica);
                }
                // .csv files aren't created until the first insert
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
    }

    public List<String> getColumns() {
        return columns;
    }

    public void insertSQL(List<String> columns, List<String> values) {
        System.out.println("columns: " + columns + "values: " + values);
        if (this.partitionType.equals("horizontal")) {
            // insert by key % numPartitions
            int partitionId = Integer.parseInt(values.get(0)) % this.numPartitions;
            System.out.println("Inserting into partition " + partitionId);
            for (DatabaseNodeInterface replica : reps.get(partitionId)) {
                try {
                    replica.insertSQL(columns, values);
                } catch (RemoteException e) {
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
                for (DatabaseNodeInterface replica : reps.get(i)) {
                    try {
                        replica.insertSQL(rearrangedColumns.get(i), rearrangedValues);
                    } catch (RemoteException e) {
                        System.out.println("RMI error inserting into replica");
                        e.printStackTrace();
                    }
                }
            }
        } else if (this.partitionType.equals("none")) {
            // insert into all replicas
            System.out.println("Inserting into replicas");
            for (DatabaseNodeInterface replica : reps.get(0)) {
                try {
                    replica.insertSQL(columns, values);
                } catch (RemoteException e) {
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
            for (DatabaseNodeInterface replica : reps.get(partitionId)) {
                try {
                    replica.insertNoSQL(kvPairs);
                } catch (RemoteException e) {
                    System.out.println("RMI error inserting into replica");
                    e.printStackTrace();
                }
            }
        } else if (this.partitionType.equals("none")) {
            // insert into all replicas
            System.out.println("Inserting into replicas");
            for (DatabaseNodeInterface replica : reps.get(0)) {
                try {
                    replica.insertNoSQL(kvPairs);
                } catch (RemoteException e) {
                    System.out.println("RMI error inserting into replica");
                    e.printStackTrace();
                }
            }
        }
    }

    public String selectSQL() {
        if (this.partitionType.equals("horizontal") || this.partitionType.equals("vertical")) {
            // read from all partitions and aggregate
            System.out.println("Selecting from replicas");
            List<String> resultList = new ArrayList<>();
            try {
                if (this.partitionType.equals("horizontal")) {
                    for (int i = 0; i < numPartitions; i++) {
                        // read from the first replica with isFileExist = true && isServerAlive = true
                        for (DatabaseNodeReplica replica : reps.get(i)) {
                            if (replica.isFileExist() && replica.isServerAlive()) {
                                resultList.add(replica.selectSQL());
                                break;
                            }
                        }
                    }
                } else {
                    // vertical
                    Map<Integer, List<String>> partitionResults = new HashMap<>();
                    for (int i = 0; i < numPartitions; i++) {
                        String res = reps.get(i).get(0).selectSQL(); // 1.John,4,John,
                        // split by \n to array list
                        List<String> resList = new ArrayList<>(Arrays.asList(res.split("\n")));
                        if (!partitionResults.containsKey(i)) {
                            partitionResults.put(i, resList);
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
            } catch (RemoteException e) {
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
                    if (replica.isFileExist() && replica.isServerAlive()) {
                        return replica.selectSQL();
                    }
                }
            } catch (RemoteException e) {
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
                    // read from the first replica with isFileExist = true && isServerAlive = true
                    for (DatabaseNodeReplica replica : reps.get(i)) {
                        if (replica.isFileExist() && replica.isServerAlive()) {
                            resultList.add(replica.selectSQL());
                            break;
                        }
                    }
                }
            } catch (RemoteException e) {
                System.out.println("RMI error selecting from replica");
                e.printStackTrace();
            }
            // convert the result array to a string with newlines
            return String.join("\n", resultList);
        } else if (this.partitionType.equals("none")) {
            // read from first replica
            System.out.println("Selecting from replicas");
            try {
                boolean find = false;
                for (DatabaseNodeReplica replica : reps.get(0)) {
                    if (replica.isFileExist() && replica.isServerAlive()) {
                        find = true;
                        return replica.selectNoSQL();
                    }
                }
                if (!find) {
                    System.out.println("No replica is alive");
                }
            } catch (RemoteException e) {
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
            System.out.println("Updating partition " + partitionId);
            for (DatabaseNodeInterface replica : reps.get(partitionId)) {
                try {
                    replica.updateSQL(columns, values, whereArr);
                } catch (RemoteException e) {
                    System.out.println("RMI error updating replica");
                    e.printStackTrace();
                }
            }
        } else if (this.partitionType.equals("vertical")) {
            // only support same partition update, so if the where clause and update columns are
            // not in the same partition, it will not work
            int partitionId = columnToPartition.get(whereArr[0]);
            // should locate the partition by the column in the where clause
            System.out.println("Updating partition " + partitionId);
            // update all the replicas in the partition
            for (DatabaseNodeInterface replica : reps.get(partitionId)) {
                try {
                    replica.updateSQL(columns, values, whereArr);
                } catch (RemoteException e) {
                    System.out.println("RMI error updating replica");
                    e.printStackTrace();
                }
            }
        } else if (this.partitionType.equals("none")) {
            // update all replicas
            System.out.println("Updating replicas");
            for (DatabaseNodeInterface replica : reps.get(0)) {
                try {
                    // where clause is now FirstName = 'John'
                    // convert to ["FirstName", "John"]
                    replica.updateSQL(columns, values, whereArr);
                } catch (RemoteException e) {
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
            System.out.println("Updating partition " + partitionId);
            for (DatabaseNodeInterface replica : reps.get(partitionId)) {
                try {
                    replica.updateNoSQL(kvPairs, where);
                } catch (RemoteException e) {
                    System.out.println("RMI error updating replica");
                    e.printStackTrace();
                }
            }
        } else if (this.partitionType.equals("none")) {
            // update all replicas
            System.out.println("Updating replicas");
            for (DatabaseNodeInterface replica : reps.get(0)) {
                try {
                    replica.updateNoSQL(kvPairs, where);
                } catch (RemoteException e) {
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
            System.out.println("Deleting from partition " + partitionId);
            for (DatabaseNodeInterface replica : reps.get(partitionId)) {
                try {
                    replica.deleteSQL(whereArr);
                } catch (RemoteException e) {
                    System.out.println("RMI error deleting from replica");
                    e.printStackTrace();
                }
            }
        } else if (this.partitionType.equals("vertical")) {
            int partitionId = columnToPartition.get(whereArr[0]);
            System.out.println("Deleting from partition " + partitionId);
            // delete from all replicas in the partition
            // should delete the same rows from all partitions
            List<Integer> deletedRows = new ArrayList<>();
            for (DatabaseNodeInterface replica : reps.get(partitionId)) {
                try {
                    deletedRows = replica.deleteSQL(whereArr);
                } catch (RemoteException e) {
                    System.out.println("RMI error deleting from replica");
                    e.printStackTrace();
                }
            }
            // delete the same rows from all partitions
            for (int i = 0; i < numPartitions; i++) {
                if (i == partitionId) {
                    continue;
                }
                for (DatabaseNodeInterface replica : reps.get(i)) {
                    try {
                        replica.deleteByRowSQL(deletedRows);
                    } catch (RemoteException e) {
                        System.out.println("RMI error deleting from replica");
                        e.printStackTrace();
                    }
                }
            }
        } else if (this.partitionType.equals("none")) {
            // delete from all replicas
            System.out.println("Deleting from replicas");
            for (DatabaseNodeInterface replica : reps.get(0)) {
                try {
                    replica.deleteSQL(whereArr);
                } catch (RemoteException e) {
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
            System.out.println("Deleting from partition " + partitionId);
            for (DatabaseNodeInterface replica : reps.get(partitionId)) {
                try {
                    replica.deleteNoSQL(where);
                } catch (RemoteException e) {
                    System.out.println("RMI error deleting from replica");
                    e.printStackTrace();
                }
            }
        } else if (this.partitionType.equals("none")) {
            // delete from all replicas
            System.out.println("Deleting from replicas");
            for (DatabaseNodeInterface replica : reps.get(0)) {
                try {
                    replica.deleteNoSQL(where);
                } catch (RemoteException e) {
                    System.out.println("RMI error deleting from replica");
                    e.printStackTrace();
                }
            }
        }
    }
}
