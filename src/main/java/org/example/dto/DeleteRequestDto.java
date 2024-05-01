package org.example.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DeleteRequestDto {
    @JsonProperty(value = "statement", required = true)
    private String statement;

    @JsonProperty(value = "databaseType", required = true)
    private String databaseType;

    public String getStatement() {
        return statement;
    }
    public void setStatement(String statement) {
        this.statement = statement;
    }

    public String getDatabaseType() {
        return databaseType;
    }

    public void setDatabaseType(String databaseType) {
        this.databaseType = databaseType;
    }
}
