package org.example.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * CreateRequestDto is a data transfer object (DTO) that represents to create a table into a database.
 */
public class CreateRequestDto {
    @JsonProperty(value = "statement", required = true)
    private String statement;
    @JsonProperty(value = "databaseType", required = true)
    private String databaseType;
    @JsonProperty(value = "replicaCount", required = true)
    private int replicaCount;
    @JsonProperty(value = "partitionType", required = true)
    private String partitionType;
    @JsonProperty(value = "numPartitions", required = true)
    private int numPartitions;
    @JsonProperty(value = "verticalPartitionColumns", required = true)
    private List<List<String>> verticalPartitionColumns;

    /**
     * Validates all fields in the DTO are non-null.
     *
     * @throws IllegalArgumentException if any field is null
     */
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
        if (partitionType.equals("vertical") && (numPartitions != verticalPartitionColumns.size())) {
            throw new IllegalArgumentException("numPartitions must be equal to the number of vertical partitions");
        }
    }

    /**
     * Gets the SQL or NoSQL statement to be executed on the database.
     *
     * @return the SQL or NoSQL statement
     */
    public String getStatement() {
        return statement;
    }

    /**
     * Sets the SQL or NoSQL statement to be executed on the database.
     *
     * @param statement the SQL or NoSQL statement
     */
    public void setStatement(String statement) {
        this.statement = statement;
    }

    /**
     * Gets the type of database to be created, either SQL or NoSQL.
     *
     * @return the type of database
     */
    public String getDatabaseType() {
        return databaseType;
    }

    /**
     * Sets the type of database to be created, either SQL or NoSQL.
     *
     * @param databaseType the type of database
     */
    public void setDatabaseType(String databaseType) {
        this.databaseType = databaseType;
    }

    /**
     * Gets the number of replicas for the database.
     *
     * @return the number of replicas
     */
    public int getReplicaCount() {
        return replicaCount;
    }

    /**
     * Sets the number of replicas for the database.
     *
     * @param replicaCount the number of replicas
     */
    public void setReplicaCount(int replicaCount) {
        this.replicaCount = replicaCount;
    }

    /**
     * Gets the partition type for the database, either horizontal or vertical.
     *
     * @return the partition type
     */
    public String getPartitionType() {
        return partitionType;
    }

    /**
     * Sets the partition type for the database, either horizontal or vertical.
     *
     * @param partitionType the partition type
     */
    public void setPartitionType(String partitionType) {
        this.partitionType = partitionType;
    }

    /**
     * Gets the number of partitions for the database.
     *
     * @return the number of partitions
     */
    public int getNumPartitions() {
        return numPartitions;
    }

    /**
     * Sets the number of partitions for the database.
     *
     * @param numPartitions the number of partitions
     */
    public void setNumPartitions(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    /**
     * Gets the list of columns to be used for vertical partitioning.
     *
     * @return the list of columns for vertical partitioning
     */
    public List<List<String>> getVerticalPartitionColumns() {
        return verticalPartitionColumns;
    }

    /**
     * Sets the list of columns to be used for vertical partitioning.
     *
     * @param verticalPartitionColumns the list of columns for vertical partitioning
     */
    public void setVerticalPartitionColumns(List<List<String>> verticalPartitionColumns) {
        this.verticalPartitionColumns = verticalPartitionColumns;
    }

}
