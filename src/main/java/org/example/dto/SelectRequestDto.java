package org.example.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * SelectRequestDto is a data transfer object (DTO) that represents to select from a database.
 */
public class SelectRequestDto {
    @JsonProperty(value = "statement", required = true)
    private String statement;
    @JsonProperty(value = "databaseType", required = true)
    private String databaseType;

    /**
     * Getter for the SQL statement.
     *
     * @return the SQL statement
     */
    public String getStatement() {
        return statement;
    }

    /**
     * Setter for the SQL statement.
     *
     * @param statement the SQL statement to be set
     */
    public void setStatement(String statement) {
        this.statement = statement;
    }

    /**
     * Getter for the database type.
     *
     * @return the database type
     */
    public String getDatabaseType() {
        return databaseType;
    }

    /**
     * Setter for the database type.
     *
     * @param databaseType the database type to be set
     */
    public void setDatabaseType(String databaseType) {
        this.databaseType = databaseType;
    }
}
