package org.example.config;

public abstract class PartitionConfig {
    // supports both vertical and horizontal partitioning
    // if SQL, can do both, if NoSQL, can only do horizontal
    // if horizontal, choose number of partitions, at most 3 partitions
    // if vertical, choose columns to partition, at most 3 groups of columns
    protected String partitionType;
    protected int numPartitions;

    public String getPartitionType() {
        return partitionType;
    }
    public int getNumPartitions() {
        return numPartitions;
    }

}

