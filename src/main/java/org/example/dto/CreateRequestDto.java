package org.example.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CreateRequestDto {
    @JsonProperty("statement")
    private String statement;

    @JsonProperty("databaseType")
    private String databaseType;

    @JsonProperty("replicaCount")
    private int replicaCount;
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

    public int getReplicaCount() {
        return replicaCount;
    }

    public void setReplicaCount(int replicaCount) {
        this.replicaCount = replicaCount;
    }
}
