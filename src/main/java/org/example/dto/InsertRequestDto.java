package org.example.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * InsertRequestDto is a data transfer object (DTO) that represents to insert a statement into a database.
 */
public class InsertRequestDto {
    @JsonProperty(value = "statement", required = true)
    private String statement;
    @JsonProperty(value = "databaseType", required = true)
    private String databaseType;

    /**
     * Getter for the statement field.
     *
     * @return the statement
     */
    public String getStatement() {
        return statement;
    }

    /**
     * Setter for the statement field.
     *
     * @param statement the statement to be set
     */
    public void setStatement(String statement) {
        this.statement = statement;
    }

    /**
     * Getter for the databaseType field.
     *
     * @return the database type
     */
    public String getDatabaseType() {
        return databaseType;
    }

    /**
     * Setter for the databaseType field.
     *
     * @param databaseType the database type to be set
     */
    public void setDatabaseType(String databaseType) {
        this.databaseType = databaseType;
    }

}
