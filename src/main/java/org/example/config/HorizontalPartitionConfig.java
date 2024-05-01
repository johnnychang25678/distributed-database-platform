package org.example.config;

public class HorizontalPartitionConfig extends PartitionConfig {
    public HorizontalPartitionConfig(int numPartitions) {
        if (numPartitions > 3) {
            throw new IllegalArgumentException("Number of partitions cannot exceed 3");
        }
        this.partitionType = "horizontal";
        this.numPartitions = numPartitions;
    }
}
