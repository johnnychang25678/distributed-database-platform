package org.example;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

// remote service interface
public interface DatabaseNodeInterface extends Remote {
    String select() throws RemoteException;
    void insertSQL(List<String> columns, List<String> values) throws RemoteException;
    void updateSQL(List<String> columns, List<String> values, String[] where) throws RemoteException;
    void deleteSQL(String[] where) throws RemoteException;
    void insertNoSQL(List<String> kvPairs) throws RemoteException;
    void updateNoSQL(List<String> columns, List<String> values, String[] where) throws RemoteException;
    void deleteNoSQL(String[] where) throws RemoteException;
}
