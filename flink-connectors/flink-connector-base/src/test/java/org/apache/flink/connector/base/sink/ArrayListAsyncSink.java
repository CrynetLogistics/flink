package org.apache.flink.connector.base.sink;


import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.ResultFuture;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;


/** Dummy destination that records puts and generate failed request occasionally. */
public class ArrayListAsyncSink extends AsyncSinkBase<String, Integer> {

    private static final int MAX_BATCH_SIZE = 10;
    private static final int MAX_IN_FLIGHT_REQUESTS = 1;
    private static final int MAX_BUFFERED_REQUESTS = 100;

    @Override
    public SinkWriter<String, Void, Collection<Integer>> createWriter(
            InitContext context, List<Collection<Integer>> states) {
        return new ArrayListAsyncSinkWriter(context, MAX_BATCH_SIZE,
                MAX_IN_FLIGHT_REQUESTS, MAX_BUFFERED_REQUESTS);
    }

    @Override
    public Optional<SimpleVersionedSerializer<Collection<Integer>>> getWriterStateSerializer() {
        return Optional.empty();
    }

}
