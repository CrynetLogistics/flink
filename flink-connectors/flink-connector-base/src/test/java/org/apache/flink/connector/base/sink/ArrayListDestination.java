package org.apache.flink.connector.base.sink;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ArrayListDestination {

    private final List<Integer> store = new ArrayList<>();

    /**
     * Returns a list of indices of elements that failed to insert, fails to insert if the
     * integer value of the {@code newRecord} is greater than 1000.
     */
    public List<Integer> putRecords(List<Integer> newRecords) {
        store.addAll(
                newRecords.stream()
                        .filter(record -> record <= 1000)
                        .collect(Collectors.toList()));
        return newRecords.stream().filter(record -> record > 1000).collect(Collectors.toList());
    }

    public List<Integer> getStore() {
        return store;
    }
}
