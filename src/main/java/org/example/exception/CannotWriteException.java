package org.example.exception;

/**
 * Thrown when an attempt to write to a file or stream fails.
 */
public class CannotWriteException extends Exception {

    /**
     * Constructs a new exception with the specified detail message.
     *
     * @param message the detail message
     */
    public CannotWriteException(String message) {
        super(message);
    }
}
