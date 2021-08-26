package org.apache.flink.connector.base.sink;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Collectors;

public class ArrayListDestination {

    private static BlockingDeque<Integer> store = new LinkedBlockingDeque<>();

    /**
     * Returns a list of indices of elements that failed to insert, fails to insert if the
     * integer value of the {@code newRecord} is greater than 1000.
     */
    protected static List<Integer> putRecords(List<Integer> newRecords) {
        store.addAll(
                newRecords.stream()
                        .filter(record -> record <= 1000)
                        .collect(Collectors.toList()));
        if (newRecords.contains(1_000_000)) {
            throw new RuntimeException("Intentional error on persisting 1_000_000 to ArrayListDestination");
        }
        return newRecords.stream().filter(record -> record > 1000).collect(Collectors.toList());
    }

    protected static int getStore() {
        return store.size();
    }

    protected static void clearStore() {
        store.clear();
    }

}
