package org.example.config;

/**
 * Abstract class representing a partition configuration.
 * This class supports both vertical and horizontal partitioning.
 * If SQL, it can do both, if NoSQL, it can only do horizontal partitioning.
 * If horizontal, it chooses the number of partitions, at most 3 partitions.
 * If vertical, it chooses columns to partition, at most 3 groups of columns.
 */
public abstract class PartitionConfig {
    protected String partitionType;
    protected int numPartitions;
    /**
     * Get the type of partitioning.
     *
     * @return the partition type (either "vertical" or "horizontal")
     */
    public String getPartitionType() {
        return partitionType;
    }
    /**
     * Get the number of partitions.
     *
     * @return the number of partitions, at most 3
     */
    public int getNumPartitions() {
        return numPartitions;
    }

}

