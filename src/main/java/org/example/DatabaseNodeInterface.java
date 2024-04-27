package org.example;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

// remote service interface
public interface DatabaseNodeInterface extends Remote {
    // TODO: UPDATE, DELETE, SELECT
    void select() throws RemoteException;
    void insert(List<String> columns, List<String> values) throws RemoteException;
    void update() throws RemoteException;
}
