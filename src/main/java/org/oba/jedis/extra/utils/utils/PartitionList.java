package org.oba.jedis.extra.utils.utils;

import java.util.ArrayList;
import java.util.List;

// https://stackoverflow.com/questions/5824825/efficient-way-to-divide-a-list-into-lists-of-n-size
public class PartitionList {

    static public <T> List<List<T>> partition(List<T> sourceList, int partitionSize) {
        if (partitionSize <= 0) {
            throw new IllegalArgumentException("Partition size must be greater than 0");
        } else if (sourceList == null) {
            throw new IllegalArgumentException("Source list cannot be null");
        } else if (sourceList.isEmpty()) {
            return new ArrayList<>();
        } else {
            List<List<T>> partitions = new ArrayList<>();
            for (int i = 0; i < sourceList.size(); i += partitionSize) {
                partitions.add(sourceList.subList(i, Math.min(i + partitionSize, sourceList.size())));
            }
            return partitions;
        }
    }

    private PartitionList() {
        // private constructor to prevent instantiation
    }

}
