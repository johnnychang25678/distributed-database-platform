package org.example.config;

import java.util.List;

public class VerticalPartitionConfig extends PartitionConfig {
    private List<List<String>> columns;
    public VerticalPartitionConfig(int numPartitions, List<List<String>> columns) {
        if (columns.size() > 3) {
            throw new IllegalArgumentException("Number of column groups cannot exceed 3");
        }
        this.partitionType = "vertical";
        this.numPartitions = numPartitions;
        this.columns = columns;
    }

    public List<List<String>> getColumns() {
        return columns;
    }
}
