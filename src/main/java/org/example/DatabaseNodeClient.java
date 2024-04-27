package org.example;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;

public class DatabaseNodeClient {
    private String tableName;
    private List<String> columns;
    private int replicaCount;

    // list of RMI servers of replicaCount
    private List<DatabaseNodeInterface> replicas = new ArrayList<>();

    public DatabaseNodeClient(String tableName, List<String> columns, int replicaCount) throws RemoteException {
        this.tableName = tableName;
        this.columns = columns;
        this.replicaCount = replicaCount;
        // create replicas
        Registry registry = LocateRegistry.getRegistry(1099);
        for (int i = 0; i < replicaCount; i++) {
            String uniqueName = tableName + "-" + i;
            DatabaseNodeInterface dbReplica = new DatabaseNodeReplica(uniqueName, columns);
            registry.rebind(uniqueName, dbReplica);
            replicas.add(dbReplica);
        }
    }

    public List<String> getColumns() {
        return columns;
    }

    public void insert(List<String> columns, List<String> values) {
        // insert into all replicas
        System.out.println("Inserting into replicas");
        for (DatabaseNodeInterface replica : replicas) {
            try {
                replica.insert(columns, values);
            } catch (RemoteException e) {
                System.out.println("RMI error inserting into replica");
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public String select() {
        // read from first replica
        try {
            return replicas.get(0).select();
        } catch (RemoteException e) {
            System.out.println("RMI error selecting from replica");
            e.printStackTrace();
        }
        return "";
    }
}
