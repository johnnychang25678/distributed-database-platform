package org.example;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

/**
 * Remote service interface for interacting with a database node.
 */
public interface DatabaseNodeInterface extends Remote {

    /**
     * Retrieves the result of a SQL SELECT query.
     *
     * @return The result of the SQL SELECT query as a string.
     * @throws RemoteException If there is an error communicating with the remote object.
     */
    String selectSQL() throws RemoteException;

    /**
     * Inserts a new row into the database using a SQL INSERT query.
     *
     * @param columns  The list of column names to insert.
     * @param values   The list of values to insert.
     * @throws RemoteException If there is an error communicating with the remote object.
     */
    void insertSQL(List<String> columns, List<String> values) throws RemoteException;

    /**
     * Updates a row in the database using a SQL UPDATE query.
     *
     * @param columns  The list of column names to update.
     * @param values   The list of values to update.
     * @param where     The list of conditions to apply to the update.
     * @return The number of rows affected by the update operation.
     * @throws RemoteException If there is an error communicating with the remote object.
     */
    List<Integer> updateSQL(List<String> columns, List<String> values, String[] where) throws RemoteException;

    /**
     * Deletes a row from the database using a SQL DELETE query.
     *
     * @param where The list of conditions to apply to the delete operation.
     * @return The number of rows affected by the delete operation.
     * @throws RemoteException If there is an error communicating with the remote object.
     */
    List<Integer> deleteSQL(String[] where) throws RemoteException;

    /**
     * Deletes a row from the database using a SQL DELETE query by providing a list of row IDs.
     *
     * @param rows The list of row IDs to delete.
     * @throws RemoteException If there is an error communicating with the remote object.
     */
    void deleteByRowSQL(List<Integer> rows) throws RemoteException;

    /**
     * Retrieves the result of a NoSQL SELECT query.
     *
     * @return The result of the NoSQL SELECT query.
     * @throws RemoteException If there is an error communicating with the remote object.
     */
    String selectNoSQL() throws RemoteException;

    /**
     * Inserts a new key-value pair into the database using a NoSQL INSERT query.
     *
     * @param kvPairs The list of key-value pairs to insert.
     * @throws RemoteException If there is an error communicating with the remote object.
     */
    void insertNoSQL(List<String> kvPairs) throws RemoteException;

    /**
     * Updates a key-value pair in the database using a NoSQL UPDATE query.
     *
     * @param kvPairs  The list of key-value pairs to update.
     * @param where     The list of conditions to apply to the update.
     * @throws RemoteException If there is an error communicating with the remote object.
     */
    void updateNoSQL(List<String> kvPairs, List<String> where) throws RemoteException;

    /**
     * Deletes a key-value pair from the database using a NoSQL DELETE query.
     *
     * @param where The list of conditions to apply to the delete operation.
     * @throws RemoteException If there is an error communicating with the remote object.
     */
    void deleteNoSQL(List<String> where) throws RemoteException;

    /**
     * Sends a heartbeat request to the remote object.
     *
     * @return True if the heartbeat request was successful, false otherwise.
     * @throws RemoteException If there is an error communicating with the remote object.
     */
    boolean heartbeatRequest() throws RemoteException;
}