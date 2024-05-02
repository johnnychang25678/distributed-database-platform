package org.example.config;

import java.util.List;

/**
 * A configuration class for vertical partitioning.
 */
public class VerticalPartitionConfig extends PartitionConfig {
    private List<List<String>> columns;
    /**
     * Constructs a new VerticalPartitionConfig object.
     *
     * @param numPartitions the number of partitions
     * @param columns       the list of lists of column names, representing the column groups
     * @throws IllegalArgumentException if the number of column groups exceeds 3
     */
    public VerticalPartitionConfig(int numPartitions, List<List<String>> columns) {
        if (columns.size() > 3) {
            throw new IllegalArgumentException("Number of column groups cannot exceed 3");
        }
        this.partitionType = "vertical";
        this.numPartitions = numPartitions;
        this.columns = columns;
    }
    /**
     * Returns the list of lists of column names, representing the column groups.
     *
     * @return the list of lists of column names
     */
    public List<List<String>> getColumns() {
        return columns;
    }
}
