package org.example;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

// remote service interface
public interface DatabaseNodeInterface extends Remote {
    String selectSQL() throws RemoteException;
    void insertSQL(List<String> columns, List<String> values) throws RemoteException;
    List<Integer> updateSQL(List<String> columns, List<String> values, String[] where) throws RemoteException;
    List<Integer> deleteSQL(String[] where) throws RemoteException;
    void deleteByRowSQL(List<Integer> rows) throws RemoteException;
    String selectNoSQL() throws RemoteException;
    void insertNoSQL(List<String> kvPairs) throws RemoteException;
    void updateNoSQL(List<String> kvPairs, List<String> where) throws RemoteException;
    void deleteNoSQL(List<String> where) throws RemoteException;

    boolean heartbeatRequest() throws RemoteException;
}
