package org.apache.flink.connector.base.sink;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.ResultFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ArrayListAsyncSinkWriter extends AsyncSinkWriter<String, Integer> {

    protected ArrayListAsyncSinkWriter(Sink.InitContext context) {
        super((element, x) -> Integer.parseInt(element), context);
    }

    @Override
    protected void submitRequestEntries(
            List<Integer> requestEntries, ResultFuture<Integer> requestResult) {
        ArrayListDestination.putRecords(requestEntries);
        //System.out.println(requestEntries);
        // List<Integer> failedIndices = ArrayListDestination.putRecords(requestEntries);
        // requestResult.complete(
        //         failedIndices.stream().map(requestEntries::get).collect(Collectors.toList()));
        requestResult.complete(List.of());
    }
}
