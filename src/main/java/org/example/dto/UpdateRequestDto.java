package org.example.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public class UpdateRequestDto {
    @JsonProperty("statement")
    private String statement;

    @JsonProperty("databaseType")
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
