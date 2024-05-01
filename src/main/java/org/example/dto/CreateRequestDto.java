package org.example.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class CreateRequestDto {
    @JsonProperty(value = "statement", required = true)
    private String statement;

    @JsonProperty(value = "databaseType", required = true)
    private String databaseType;

    @JsonProperty(value = "replicaCount", required = true)
    private int replicaCount;
    @JsonProperty(value = "partitionType", required = true)
    private String partitionType; // horizontal or vertical or none
    @JsonProperty(value = "numPartitions", required = true)
    private int numPartitions;

    @JsonProperty(value = "verticalPartitionColumns", required = true)
    private List<List<String>> verticalPartitionColumns;

    // validate all fields non-null and throws exception if any field is null
    public void validate() throws IllegalArgumentException {
        if (statement == null) {
            throw new IllegalArgumentException("statement cannot be null");
        }
        if (databaseType == null || (!databaseType.equals("SQL") && !databaseType.equals("NoSQL"))) {
            throw new IllegalArgumentException("invalid databaseType");
        }
        if (partitionType == null || (!partitionType.equals("horizontal") && !partitionType.equals("vertical") && !partitionType.equals("none"))) {
            throw new IllegalArgumentException("invalid partitionType");
        }
        if (numPartitions < 1) {
            throw new IllegalArgumentException("numPartitions must be greater than 0");
        }
        if (partitionType.equals("vertical") && (verticalPartitionColumns == null || verticalPartitionColumns.size() == 0)) {
            throw new IllegalArgumentException("verticalPartitionColumns cannot be null or empty");
        }
    }

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

    public String getPartitionType() {
        return partitionType;
    }

    public void setPartitionType(String partitionType) {
        this.partitionType = partitionType;
    }

    public int getNumPartitions() {
        return numPartitions;
    }
    public void setNumPartitions(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    public List<List<String>> getVerticalPartitionColumns() {
        return verticalPartitionColumns;
    }

    public void setVerticalPartitionColumns(List<List<String>> verticalPartitionColumns) {
        this.verticalPartitionColumns = verticalPartitionColumns;
    }

}
