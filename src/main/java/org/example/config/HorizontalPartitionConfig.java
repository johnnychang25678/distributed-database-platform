package org.example.config;

/**
 * A configuration class for horizontal partitioning.
 */
public class HorizontalPartitionConfig extends PartitionConfig {
    /**
     * Constructs a new instance of {@code HorizontalPartitionConfig} with the specified number of partitions.
     *
     * @param numPartitions the number of partitions for the horizontal partitioning.
     * @throws IllegalArgumentException if the number of partitions exceeds 3.
     */
    public HorizontalPartitionConfig(int numPartitions) {
        if (numPartitions > 3) {
            throw new IllegalArgumentException("Number of partitions cannot exceed 3");
        }
        this.partitionType = "horizontal";
        this.numPartitions = numPartitions;
    }
}
